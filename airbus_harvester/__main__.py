import hashlib
import json
import logging
import os
from json import JSONDecodeError

import boto3
import click
import requests
from botocore.exceptions import ClientError
from eodhp_utils.aws.s3 import upload_file_s3
from inflection import underscore
from pulsar import Client as PulsarClient

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG") else logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()],
)

minimum_message_entries = int(os.environ.get("MINIMUM_MESSAGE_ENTRIES", 100))


def load_config(config_path):
    with open(config_path, "r") as f:
        return json.load(f)


@click.group()
# you can implement any global flags here that will apply to all commands, e.g. debug
# @click.option('--debug/--no-debug', default=False) # don't feel the need to implement this, just an example
def cli():
    """This is just a placeholder to act as the entrypoint, you can do things with global options here
    if required"""
    pass


@cli.command()
# not currently used but keeping the same structure as the other harvester repos
# @click.argument("source_url", type=str, nargs=1)
@click.argument("workspace_name", type=str)
@click.argument(
    "catalog", type=str
)  # not currently used but keeping the same structure as the other harvester repos
@click.argument("s3_bucket", type=str)
def harvest(workspace_name: str, catalog: str, s3_bucket: str):
    """Harvest a given Airbus catalog, and all records beneath it. Send a pulsar message
    containing all added, updated, and deleted links since the last time the catalog was
    harvested"""

    if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        session = boto3.session.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        s3_client = session.client("s3")
    else:
        s3_client = boto3.client("s3")

    pulsar_url = os.environ.get("PULSAR_URL")
    pulsar_client = PulsarClient(pulsar_url)

    config_key = os.getenv("HARVESTER_CONFIG_KEY", "")
    config = load_config("airbus_harvester/config.json").get(config_key.upper())
    if not config:
        logging.error(f"Configuration key {config_key} not found in config file.")

    harvested_keys = {"added_keys": set(), "updated_keys": set()}

    logging.info(f"Harvesting from Airbus {config_key}")

    key_root = "git-harvester/supported-datasets/airbus"

    metadata_s3_key = f"harvested-metadata/{config['collection_name']}"
    previously_harvested = get_metadata(s3_bucket, metadata_s3_key, s3_client)
    logging.info(f"Previously harvested URLs: {previously_harvested}")
    latest_harvested = {}

    catalogue_data = make_catalogue()
    catalogue_key = "git-harvester/supported-datasets/airbus.json"
    previous_hash = previously_harvested.pop(catalogue_key, None)
    harvested_keys, latest_harvested[catalogue_key] = compare_to_previous_version(
        catalogue_key, catalogue_data, previous_hash, harvested_keys, s3_bucket, s3_client
    )

    collection_key = f"{key_root}/{config['collection_name']}.json"

    catalogue_data_summary = previously_harvested.pop("summary", None)
    if not catalogue_data_summary:
        catalogue_data_summary = {"start_time": [], "stop_time": [], "coordinates": []}

    next_url = config["url"]
    url_count = 0

    deleted_keys = []

    while next_url:
        url_count += 1

        body = get_next_page(next_url, config)

        for entry in body["features"]:
            data = generate_stac_item(entry, config)
            try:
                file_name = f"{entry['properties'][config['item_id_key']]}.json"
                key = f"{key_root}/{config['collection_name']}/{file_name}"

                previous_hash = previously_harvested.pop(key, None)
                harvested_keys, latest_harvested[key] = compare_to_previous_version(
                    key, data, previous_hash, harvested_keys, s3_bucket, s3_client
                )
            except KeyError:
                logging.error(f"Invalid entry in {next_url}")

            catalogue_data_summary = add_to_catalogue_data_summary(
                catalogue_data_summary, json.loads(data)
            )

        catalogue_data_summary = simplify_catalogue_data_summary(catalogue_data_summary)

        if config["pagination_method"] == "link":
            try:
                next_url = body.get("_links").get("next")
            except AttributeError as e:
                logging.error(e)
                raise
        elif config["pagination_method"] == "counter":
            config["body"]["startPage"] = url_count + 1
            if not body["features"]:
                next_url = None

        logging.info(f"Page {url_count} next URL: {next_url}")

        summary = get_stac_collection_summary(catalogue_data_summary)

        # Collection updates every loop so that start/stop times and bbox values are the latest
        # ones from the Airbus catalogue
        collection_data = generate_stac_collection(summary, config)
        last_run_hash = latest_harvested.get(collection_key)
        previous_hash = last_run_hash if last_run_hash else previously_harvested.get(collection_key)

        harvested_keys, latest_harvested[collection_key] = compare_to_previous_version(
            collection_key, collection_data, previous_hash, harvested_keys, s3_bucket, s3_client
        )

        latest_harvested["summary"] = catalogue_data_summary

        # Don't send empty or nearly empty pulsar messages
        if (
            len(harvested_keys["added_keys"])
            + len(harvested_keys["updated_keys"])
            + len(deleted_keys)
            > minimum_message_entries
        ):
            # Record harvested hash data in S3
            upload_file_s3(json.dumps(latest_harvested), s3_bucket, metadata_s3_key, s3_client)

            output_data = {
                "id": f"{workspace_name}/{config['collection_name']}_{url_count}",
                "workspace": workspace_name,
                "bucket_name": s3_bucket,
                "added_keys": list(harvested_keys["added_keys"]),
                "updated_keys": list(harvested_keys["updated_keys"]),
                "deleted_keys": deleted_keys,
                "source": "airbus",
                "target": "/",
            }

            send_pulsar_message(output_data, pulsar_client)

            harvested_keys = {"added_keys": set(), "updated_keys": set()}

    upload_file_s3(json.dumps(latest_harvested), s3_bucket, metadata_s3_key, s3_client)

    if latest_harvested.get(collection_key) == previously_harvested.get(collection_key):
        harvested_keys["updated_keys"].discard(collection_key)

    # Remove this otherwise it will be marked for deletion
    previously_harvested.pop(collection_key, None)

    deleted_keys = list(previously_harvested.keys())
    output_data = {
        "id": f"{workspace_name}/airbus_final",
        "workspace": workspace_name,
        "bucket_name": s3_bucket,
        "added_keys": list(harvested_keys["added_keys"]),
        "updated_keys": list(harvested_keys["updated_keys"]),
        "deleted_keys": deleted_keys,
        "source": "airbus",
        "target": "/",
    }

    if any([harvested_keys["added_keys"], harvested_keys["updated_keys"], deleted_keys]):
        # Send Pulsar message containing harvested links
        send_pulsar_message(output_data, pulsar_client)
        logging.info(f"Sent harvested message {output_data}")
    else:
        logging.info("No changes made to previously harvested state")

    return output_data


def send_pulsar_message(output_data: dict, pulsar_client: PulsarClient) -> None:
    """Sends pulsar message for a given set of output data"""

    producer = pulsar_client.create_producer(
        topic="harvested", producer_name="stac_harvester/airbus", chunking_enabled=True
    )
    producer.send((json.dumps(output_data)).encode("utf-8"))
    producer.close()
    logging.info(f"Sent harvested message {output_data}")


def compare_to_previous_version(
    key: str,
    data: str,
    previous_hash: str,
    harvested_keys: dict,
    s3_bucket: str,
    s3_client: boto3.session.Session.client,
) -> tuple:
    """Compares a file to a previous version of a file as determined by the hash. New or updated
    files are uploaded to S3"""
    file_hash = get_file_hash(data)

    if not previous_hash:
        # URL was not harvested previously
        logging.info(f"Added: {key}")
        harvested_keys["added_keys"].add(key)
        upload_file_s3(data, s3_bucket, key, s3_client)
    elif previous_hash != file_hash:
        # URL has changed since last run
        logging.info(f"Updated: {key}")
        harvested_keys["updated_keys"].add(key)
        upload_file_s3(data, s3_bucket, key, s3_client)

    return harvested_keys, file_hash


def add_to_catalogue_data_summary(all_data: dict, data: dict) -> dict:
    """Combines new data with existing data for whole catalogue"""
    all_data["coordinates"] += data["geometry"]["coordinates"][0]
    all_data["start_time"].append(
        data["properties"].get("start_datetime", data["properties"].get("datetime"))
    )
    all_data["stop_time"].append(
        data["properties"].get("end_datetime", data["properties"].get("datetime"))
    )

    return all_data


def simplify_catalogue_data_summary(all_data: dict) -> dict:
    """Finds the coordinates containing bbox values, and start and stop time of all data so far"""
    biggest_lat = smallest_lat = biggest_long = smallest_long = all_data["coordinates"][0]

    for coordinates in all_data["coordinates"]:
        if coordinates[0] > biggest_lat[0]:
            biggest_lat = coordinates
        elif coordinates[0] < smallest_lat[0]:
            smallest_lat = coordinates
        elif coordinates[1] > biggest_long[1]:
            biggest_long = coordinates
        elif coordinates[1] < smallest_long[1]:
            smallest_long = coordinates

    coordinates_summary = [biggest_lat, biggest_long, smallest_lat, smallest_long]

    return {
        "coordinates": coordinates_summary,
        "start_time": [min(all_data["start_time"])],
        "stop_time": [max(all_data["stop_time"])],
    }


def generate_access_token(env: str = "dev") -> str:
    """Generate access token for Airbus API"""
    if env == "prod":
        url = "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token"
    else:
        url = "https://authenticate-int.idp.private.geoapi-airbusds.com/auth/realms/IDP/protocol/openid-connect/token"

    api_key = os.environ["AIRBUS_API_KEY"]

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = [
        ("apikey", api_key),
        ("grant_type", "api_key"),
        ("client_id", "IDP"),
    ]

    response = requests.post(url, headers=headers, data=data)

    return response.json().get("access_token")


def get_next_page(url: str, config: dict, retry_count: int = 0) -> dict:
    """Collects body of next page of Airbus data"""

    try:
        headers = {"accept": "application/json"}
        if config["auth_env"]:
            access_token = generate_access_token(config["auth_env"])
            headers["Authorization"] = "Bearer " + access_token

        if config["request_method"].upper() == "POST":
            response = requests.post(url, json=config["body"], headers=headers)
        else:
            response = requests.get(url, json=config["body"], headers=headers)
        response.raise_for_status()

        return response.json()

    except (JSONDecodeError, requests.exceptions.HTTPError):
        logging.warning(f"Retrying retrieval of {url}. Attempt {retry_count}")
        if retry_count > 3:
            raise

        return get_next_page(url, config, retry_count=retry_count + 1)


def get_file_hash(data: str) -> str:
    """Returns hash of data available"""

    def _md5_hash(byte_str: bytes) -> str:
        """Calculates an md5 hash for given bytestring"""
        md5 = hashlib.md5()
        md5.update(byte_str)
        return md5.hexdigest()

    return _md5_hash(data.encode("utf-8"))


def get_file_s3(bucket: str, key: str, s3_client: boto3.client) -> str:
    """Retrieve data from an S3 bucket"""
    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        return file_obj["Body"].read().decode("utf-8")
    except ClientError as e:
        logging.warning(f"File retrieval failed for {key}: {e}")
        return None


def get_metadata(bucket: str, key: str, s3_client: boto3.client) -> dict:
    """Read file at given S3 location and parse as JSON"""
    previously_harvested = get_file_s3(bucket, key, s3_client)
    try:
        previously_harvested = json.loads(previously_harvested)
    except TypeError:
        previously_harvested = {}
    return previously_harvested


def coordinates_to_bbox(coordinates: list) -> list:
    """Finds the biggest and smallest x and y coordinates"""

    unzipped = list(zip(*coordinates, strict=False))

    max_values = [max(index) for index in unzipped]
    min_values = [min(index) for index in unzipped]

    return min_values + max_values


def get_stac_collection_summary(all_data: dict) -> dict:
    """Gets the area and start/stop times of all data points"""

    bbox = coordinates_to_bbox(all_data["coordinates"])

    return {
        "bbox": bbox,
        "start_time": min(all_data["start_time"]),
        "stop_time": max(all_data["stop_time"]),
    }


def generate_stac_collection(all_data_summary: dict, config: dict) -> str:
    """Top level collection for Airbus data"""

    stac_template = load_config(f"airbus_harvester/{config['collection_name']}.json")

    stac_template["extent"] = {
        "spatial": {"bbox": [all_data_summary["bbox"]]},
        "temporal": {"interval": [[all_data_summary["start_time"], all_data_summary["stop_time"]]]},
    }
    return json.dumps(stac_template, indent=4)


def handle_external_url(
    data: dict, links: list, assets: dict, mapped_keys: set, name: str, path: str
):
    """Convert external URL to link and asset"""
    mime_types = {
        ".tiff": "image/tiff; application=geotiff; profile=cloud-optimized",
        ".tif": "image/tiff; application=geotiff; profile=cloud-optimized",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
    }

    keys = path.split(".")
    value = data
    for key in keys:
        value = value.get(key)
        if value is None:
            break
    external_url = value if isinstance(value, str) else None

    if external_url:
        file_extension = os.path.splitext(external_url)[1].lower()
        mime_type = mime_types.get(file_extension, "application/octet-stream")
        links.append(
            {
                "rel": name,
                "href": external_url,
                "type": mime_type,
            }
        )
        assets[name] = {
            "href": external_url,
            "type": mime_type,
        }
        mapped_keys.add(key)


def modify_value(key, value) -> str:
    """Modify a specific value to STAC format depending on the key"""
    if key == "lookDirection":
        # Convert look direction to human readable format
        return "right" if value.upper() == "R" else "left" if value.upper() == "L" else value
    elif key == "polarizationChannels":
        # Split dual polarisation channels into separate values
        return [value[i : i + 2] for i in range(0, len(value), 2)]
    return value


def generate_stac_item(data: dict, config: dict) -> str:
    """Catalogue items for Airbus data"""
    coordinates = data["geometry"]["coordinates"][0]
    bbox = coordinates_to_bbox(coordinates)

    mapped_keys = set()
    properties = config["stac_properties"]

    links = []
    assets = {}

    for stac_key, airbus_key in config["stac_properties_map"].items():
        if airbus_key in data["properties"]:
            properties[stac_key] = modify_value(airbus_key, data["properties"][airbus_key])
            mapped_keys.add(airbus_key)

    for url_config in config["external_urls"]:
        handle_external_url(
            data, links, assets, mapped_keys, url_config["name"], url_config["path"]
        )

    for key, value in data["properties"].items():
        if key not in mapped_keys and value is not None:
            properties[underscore(key)] = value

    stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": config["stac_extensions"],
        "id": data["properties"][config["item_id_key"]],
        "collection": config["collection_name"],
        "geometry": {"type": "Polygon", "coordinates": [coordinates]},
        "bbox": bbox,
        "properties": properties,
        "links": links,
        "assets": assets,
    }

    return json.dumps(stac_item, indent=4)


def make_catalogue() -> str:
    """Top level catalogue for Airbus data"""
    stac_catalog = {
        "type": "Catalog",
        "id": "airbus",
        "stac_version": "1.0.0",
        "description": "Airbus Datasets",
        "links": [],
    }
    return json.dumps(stac_catalog, indent=4)


if __name__ == "__main__":
    cli(obj={})
