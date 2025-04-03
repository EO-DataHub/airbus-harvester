import hashlib
import json
import logging
import os
import time
import traceback
import uuid
from json import JSONDecodeError
from typing import Optional

import boto3
import click
import requests
from eodhp_utils.aws.s3 import get_file_s3, upload_file_s3
from eodhp_utils.runner import get_boto3_session, get_pulsar_client, setup_logging
from inflection import underscore
from pulsar import ConnectError
from requests.exceptions import ConnectionError, HTTPError, Timeout

from airbus_harvester.airbus_harvester_messager import AirbusHarvesterMessager

setup_logging(verbosity=2)  # DEBUG level


minimum_message_entries = int(os.environ.get("MINIMUM_MESSAGE_ENTRIES", 100))
proxy_base_url = os.environ.get("PROXY_BASE_URL", "")
max_api_retries = int(os.environ.get("MAX_API_RETRIES", 5))

commercial_catalogue_root = os.getenv("COMMERCIAL_CATALOGUE_ROOT", "commercial")


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

    s3_client = get_boto3_session().client("s3")

    config_key = os.getenv("HARVESTER_CONFIG_KEY", "")
    config = load_config("airbus_harvester/config.json").get(config_key.upper())

    if os.getenv("TOPIC"):
        identifier = "_" + os.getenv("TOPIC")
    else:
        identifier = ""

    def get_pulsar_producer(retry_count: int = 0):
        """Initialise pulsar producer. Retry if connection fails"""
        try:
            pulsar_client = get_pulsar_client()
            producer = pulsar_client.create_producer(
                topic=f"harvested{identifier}",
                producer_name=f"stac_harvester/airbus/{config['collection_name']}_{uuid.uuid1().hex}",
                chunking_enabled=True,
            )
            return producer
        except ConnectError as e:
            logging.error(f"Failed to connect to pulsar: {e}")
            if retry_count >= 10:
                logging.error(f"Failed to connect to pulsar after {retry_count + 1} attempts.")
                raise

            logging.error(f"Retrying pulsar initialisation. Attempt {retry_count + 1}")
            time.sleep(2**retry_count)
            return get_pulsar_producer(retry_count=retry_count + 1)

    producer = get_pulsar_producer()

    if not config:
        logging.warning(f"Configuration key {config_key} not found in config file.")

    airbus_harvester_messager = AirbusHarvesterMessager(
        s3_client=s3_client,
        output_bucket=s3_bucket,
        cat_output_prefix="git-harvester/",
        producer=producer,
    )

    harvested_data = {}
    latest_harvested = {}

    logging.info(f"Harvesting from Airbus {config_key}")

    key_root = f"{commercial_catalogue_root}/airbus"

    metadata_s3_key = f"harvested-metadata/{config['collection_name']}"
    previously_harvested = get_metadata(s3_bucket, metadata_s3_key, s3_client)
    logging.info(f"Previously harvested URLs: {previously_harvested}")
    latest_harvested = {}

    catalogue_data = make_catalogue()
    catalogue_key = f"{key_root}.json"
    previous_hash = previously_harvested.pop(catalogue_key, None)
    file_hash = get_file_hash(json.dumps(catalogue_data))
    if not previous_hash or previous_hash != file_hash:
        # URL was not harvested previously
        logging.info(f"Added: {catalogue_key}")
        harvested_data[catalogue_key] = catalogue_data
        latest_harvested[catalogue_key] = file_hash

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
        features = body.get("features", [])
        logging.info(f"Page {url_count} features: {len(features)}")

        for entry in features:
            data = generate_stac_item(entry, config)
            try:
                file_name = f"{entry['properties'][config['item_id_key']]}.json"
                key = f"{key_root}/{config['collection_name']}/{file_name}"

                previous_hash = previously_harvested.pop(key, None)
                file_hash = get_file_hash(json.dumps(data))

                if not previous_hash or previous_hash != file_hash:
                    # Data was not harvested previously
                    logging.info(f"Added: {key}")
                    harvested_data[key] = data
                    latest_harvested[key] = file_hash
                else:
                    logging.info(f"Skipping: {key}")
            except KeyError:
                logging.error(f"Invalid entry in {next_url}")

            catalogue_data_summary = add_to_catalogue_data_summary(catalogue_data_summary, data)

        catalogue_data_summary = simplify_catalogue_data_summary(catalogue_data_summary)

        if config["pagination_method"] == "link":
            try:
                next_url = body.get("_links").get("next")
            except AttributeError as e:
                logging.error(e)
                raise
        elif config["pagination_method"] == "counter":
            if not features:
                next_url = None
            else:
                # The counter can only go up to 50. Limit the search by last update date
                if (url_count + 1) % 50 == 0:
                    config["body"][
                        "lastUpdateDate"
                    ] = f"[2018-10-03T12:00:00Z,{entry['properties']['lastUpdateDate']}]"
                config["body"]["startPage"] = (url_count % 50) + 1

        logging.info(f"Page {url_count} next URL: {next_url}")

        summary = get_stac_collection_summary(catalogue_data_summary)

        # Collection updates every loop so that start/stop times and bbox values are the latest
        # ones from the Airbus catalogue
        collection_data = generate_stac_collection(summary, config)
        last_run_hash = latest_harvested.get(collection_key)
        previous_hash = last_run_hash if last_run_hash else previously_harvested.get(collection_key)

        file_hash = get_file_hash(json.dumps(collection_data))
        if not previous_hash or previous_hash != file_hash:
            # Data was not harvested previously
            logging.info(f"Added: {collection_key}")
            harvested_data[collection_key] = collection_data
            latest_harvested[collection_key] = file_hash

        latest_harvested["summary"] = catalogue_data_summary

        if len(harvested_data.keys()) >= minimum_message_entries:

            # Send message for altered keys
            msg = {
                "harvested_data": harvested_data,
                "deleted_keys": [],
            }
            logging.info(f"Sending message with {len(harvested_data.keys())} entries")
            airbus_harvester_messager.consume(msg)
            logging.info("Uploading metadata to S3")
            upload_file_s3(json.dumps(latest_harvested), s3_bucket, metadata_s3_key, s3_client)
            logging.info("Uploaded metadata to S3")
            harvested_data = {}

    # Do not updated collection
    if latest_harvested.get(collection_key) == previously_harvested.get(collection_key):
        harvested_data.discard(collection_key)

    # Remove this otherwise it will be marked for deletion
    previously_harvested.pop(collection_key, None)

    deleted_keys = list(previously_harvested.keys())

    # Send message for altered keys
    msg = {"harvested_data": harvested_data, "deleted_keys": deleted_keys}
    logging.info(
        f"Sending message with {len(harvested_data.keys())} entries and {len(deleted_keys)} deleted keys"
    )
    airbus_harvester_messager.consume(msg)

    logging.info("Uploading metadata to S3")
    upload_file_s3(json.dumps(latest_harvested), s3_bucket, metadata_s3_key, s3_client)
    logging.info("Uploaded metadata to S3")


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


def generate_access_token(env: str = "dev", retry_count: int = 0) -> str:
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

    try:
        logging.info(f"Making POST request to {url} for access token")
        response = requests.post(url, headers=headers, data=data, timeout=10)
        logging.info(f"Response status code: {response.status_code}")
        response.raise_for_status()
        access_token = response.json().get("access_token")

        if access_token:
            return access_token
        else:
            raise ValueError("Access token is None")

    except (ConnectionError, HTTPError, Timeout, ValueError) as e:
        logging.error(e)
        logging.error(traceback.format_exc())
        if retry_count >= max_api_retries:
            logging.error(f"Failed to generate access token after {retry_count + 1} attempts.")
            raise

        logging.error(f"Retrying access token generation. Attempt {retry_count + 1}")
        time.sleep(2**retry_count)
        return generate_access_token(env, retry_count=retry_count + 1)


def get_next_page(url: str, config: dict, retry_count: int = 0) -> dict:
    """Collects body of next page of Airbus data"""

    try:
        headers = {"accept": "application/json"}
        if config["auth_env"]:
            access_token = generate_access_token(config["auth_env"])
            headers["Authorization"] = "Bearer " + access_token

        logging.info(
            f"Making {config['request_method'].upper()} request to {url} with body {config['body']}"
        )
        if config["request_method"].upper() == "POST":
            response = requests.post(url, json=config["body"], headers=headers, timeout=10)
        else:
            response = requests.get(url, json=config["body"], headers=headers, timeout=10)
        logging.info(f"Response status code: {response.status_code}")
        response.raise_for_status()

        return response.json()

    except (JSONDecodeError, ConnectionError, HTTPError, Timeout) as e:
        logging.error(e)
        logging.error(traceback.format_exc())
        if retry_count > max_api_retries:
            logging.error(f"Failed to obtain valid response after {retry_count + 1} attempts.")
            raise

        logging.error(f"Retrying retrieval of {url}. Attempt {retry_count + 1}")
        time.sleep(5**retry_count)
        return get_next_page(url, config, retry_count=retry_count + 1)


def get_file_hash(data: str) -> str:
    """Returns hash of data available"""

    def _md5_hash(byte_str: bytes) -> str:
        """Calculates an md5 hash for given bytestring"""
        md5 = hashlib.md5()
        md5.update(byte_str)
        return md5.hexdigest()

    return _md5_hash(data.encode("utf-8"))


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


def generate_stac_collection(all_data_summary: dict, config: dict) -> dict:
    """Top level collection for Airbus data"""

    stac_template = load_config(f"airbus_harvester/{config['collection_name']}.json")

    stac_template["extent"] = {
        "spatial": {"bbox": [all_data_summary["bbox"]]},
        "temporal": {"interval": [[all_data_summary["start_time"], all_data_summary["stop_time"]]]},
    }
    for _asset_name, asset in stac_template.get("assets").items():
        if "href" in asset:
            asset["href"] = asset["href"].replace("{EODHP_BASE_URL}", proxy_base_url)
    return stac_template


def handle_external_url(
    data: dict,
    links: list,
    assets: dict,
    mapped_keys: set,
    name: str,
    path: str,
    proxy: Optional[str] = None,
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

        def add_asset(rel_name, href):
            assets[rel_name] = {"href": href, "type": mime_type}

        if proxy:
            proxy_url = f"{proxy}/{name}"
            add_asset(f"external_{name}", external_url)
            add_asset(name, proxy_url)
        else:
            add_asset(name, external_url)
        mapped_keys.add(key)


def modify_value(key, value) -> str:
    """Modify a specific value to STAC format depending on the key"""
    if key == "lookDirection":
        # Convert look direction to human readable format
        return "right" if value.upper() == "R" else "left" if value.upper() == "L" else value
    elif key == "polarizationChannels":
        # Split dual polarisation channels into separate values
        return [value[i : i + 2] for i in range(0, len(value), 2)]
    elif key == "geometry_centroid":
        # Convert list to dict for consistency throughout the catalogue
        if type(value) is list and len(value) == 2:
            return {"lat": value[1], "lon": value[0]}
        # Type not understood - ignore it
        return None

    return value


def generate_stac_item(data: dict, config: dict) -> dict:
    """Catalogue items for Airbus data"""
    item_id = data["properties"][config["item_id_key"]]
    logging.info(f"Processing item {item_id}")

    coordinates = data["geometry"]["coordinates"][0]
    bbox = coordinates_to_bbox(coordinates)

    mapped_keys = set()
    properties = config["stac_properties"].copy()

    links = []
    assets = {}

    for stac_key, airbus_key in config["stac_properties_map"].items():
        if airbus_key in data["properties"]:
            properties[stac_key] = modify_value(airbus_key, data["properties"][airbus_key])
            mapped_keys.add(airbus_key)

    for url_config in config["external_urls"]:
        proxy_url = None
        if url_config.get("proxy", False):
            proxy_url = (
                f"{proxy_base_url}/api/catalogue/stac/catalogs/{commercial_catalogue_root}/catalogs/airbus/"
                f"collections/{config['collection_name']}/items/{item_id}"
            )
        handle_external_url(
            data, links, assets, mapped_keys, url_config["name"], url_config["path"], proxy_url
        )

    for key, value in data["properties"].items():
        if key not in mapped_keys and value is not None:
            property_key = underscore(key)
            properties[property_key] = modify_value(property_key, value)

    stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": config["stac_extensions"],
        "id": item_id,
        "collection": config["collection_name"],
        "geometry": {"type": "Polygon", "coordinates": [coordinates]},
        "bbox": bbox,
        "properties": properties,
        "links": links,
        "assets": assets,
    }

    return stac_item


def make_catalogue() -> dict:
    """Top level catalogue for Airbus data"""
    stac_catalog = {
        "type": "Catalog",
        "id": "airbus",
        "stac_version": "1.0.0",
        "description": "Airbus Datasets",
        "links": [],
    }
    return stac_catalog


if __name__ == "__main__":
    cli(obj={})
