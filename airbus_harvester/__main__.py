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
@click.argument("metadata_s3_bucket", type=str)
def harvest(workspace_name: str, catalog: str, s3_bucket: str, metadata_s3_bucket: str):
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

    all_keys = {"added_keys": [], "updated_keys": []}

    logging.info("Harvesting from Airbus")

    key_root = "git-harvester/supported-datasets/airbus"

    metadata_s3_key = "harvested-metadata/airbus"
    previously_harvested = get_metadata(metadata_s3_bucket, metadata_s3_key, s3_client)
    logging.info(f"Previously harvested URLs: {previously_harvested}")
    latest_harvested = {}

    file_name = "airbus.json"
    catalogue_data = make_catalogue()
    catalogue_key = f"git-harvester/supported-datasets/{file_name}"
    previous_hash = previously_harvested.pop(catalogue_key, None)
    all_keys, latest_harvested[catalogue_key] = compare_to_previous_version(
        catalogue_key, catalogue_data, previous_hash, all_keys, s3_bucket, s3_client
    )

    # file_hash = get_file_hash(catalogue_data)
    # previous_hash = previously_harvested.pop(key, None)
    #
    # if not previous_hash:
    #     # URL was not harvested previously
    #     logging.info("Appended URL to 'added' list")
    #     added_keys.append(key)
    #     upload_file_s3(catalogue_data, s3_bucket, key, s3_client)
    # elif previous_hash != file_hash:
    #     # URL has changed since last run
    #     logging.info("Appended URL to 'updated' list")
    #     updated_keys.append(key)
    #     upload_file_s3(catalogue_data, s3_bucket, key, s3_client)
    #
    # latest_harvested[key] = file_hash

    all_data_summary = previously_harvested.pop("summary", None)

    if not all_data_summary:
        all_data_summary = {"start_time": [], "stop_time": [], "coordinates": []}

    next_url = "https://sar.api.oneatlas.airbus.com/v1/sar/catalogue/replication"

    while next_url:

        body = get_next_page(next_url)

        for entry in body["features"]:
            all_data_summary = add_to_all_data_summary(all_data_summary, entry)

            data = generate_stac_item(entry)
            file_name = f"{entry['properties']['acquisitionId']}.json"
            key = f"{key_root}/airbus_sar_data/{file_name}"

            previous_hash = previously_harvested.pop(key, None)
            all_keys, latest_harvested[key] = compare_to_previous_version(
                key, data, previous_hash, all_keys, s3_bucket, s3_client
            )
            # file_hash = get_file_hash(data)
            #
            # if not previous_hash:
            #     # URL was not harvested previously
            #     logging.info("Appended URL to 'added' list")
            #     added_keys.append(key)
            #     upload_file_s3(data, s3_bucket, key, s3_client)
            # elif previous_hash != file_hash:
            #     # URL has changed since last run
            #     logging.info("Appended URL to 'updated' list")
            #     updated_keys.append(key)
            #     upload_file_s3(data, s3_bucket, key, s3_client)
            #
            # latest_harvested[key] = file_hash

        all_data_summary = simplify_all_data_summary(all_data_summary)

        try:
            next_url = body.get("_links").get("next")
        except AttributeError as e:
            logging.error(e)
            raise
        logging.info(f"Next URL: {next_url}")

    summary = get_stac_collection_summary(all_data_summary)

    file_name = "airbus_sar_data.json"
    collection_data = generate_stac_collection(summary)

    collection_key = f"{key_root}/{file_name}"
    previous_hash = previously_harvested.pop(collection_key, None)

    logging.error(all_keys)
    all_keys, latest_harvested[collection_key] = compare_to_previous_version(
        collection_key, collection_data, previous_hash, all_keys, s3_bucket, s3_client
    )
    logging.error(all_keys)
    # file_hash = get_file_hash(catalogue_data)
    #
    # if not previous_hash:
    #     # URL was not harvested previously
    #     logging.info("Appended URL to 'added' list")
    #     added_keys.append(key)
    #     upload_file_s3(collection_data, s3_bucket, key, s3_client)
    # elif previous_hash != file_hash:
    #     # URL has changed since last run
    #     logging.info("Appended URL to 'updated' list")
    #     updated_keys.append(key)
    #     upload_file_s3(collection_data, s3_bucket, key, s3_client)
    #
    # latest_harvested[key] = file_hash

    latest_harvested["summary"] = all_data_summary

    # Record harvested hash data in S3
    upload_file_s3(json.dumps(latest_harvested), metadata_s3_bucket, metadata_s3_key, s3_client)

    pulsar_url = os.environ.get("PULSAR_URL")
    pulsar_client = PulsarClient(pulsar_url)

    producer = pulsar_client.create_producer(
        topic="harvested", producer_name="stac_harvester/airbus"
    )

    deleted_keys = list(previously_harvested.keys())
    logging.info(f"Previously harvested URLs not found: {deleted_keys}")

    output_data = {
        "id": f"{workspace_name}/airbus",
        "workspace": workspace_name,
        "bucket_name": s3_bucket,
        "added_keys": all_keys["added_keys"],
        "updated_keys": all_keys["updated_keys"],
        "deleted_keys": deleted_keys,
        "source": "airbus",
        "target": "/",
    }

    if any([all_keys["added_keys"], all_keys["updated_keys"], deleted_keys]):
        # Send Pulsar message containing harvested links
        producer.send((json.dumps(output_data)).encode("utf-8"))
        logging.info(f"Sent harvested message {output_data}")
    else:
        logging.info("No changes made to previously harvested state")

    logging.info(output_data)

    return output_data


def compare_to_previous_version(key, data, previous_hash, all_keys, s3_bucket, s3_client):
    file_hash = get_file_hash(data)

    if not previous_hash:
        # URL was not harvested previously
        logging.info("Appended key to 'added' list")
        all_keys["added_keys"].append(key)
        upload_file_s3(data, s3_bucket, key, s3_client)
    elif previous_hash != file_hash:
        # URL has changed since last run
        logging.info("Appended key to 'updated' list")
        all_keys["updated_keys"].append(key)
        upload_file_s3(data, s3_bucket, key, s3_client)

    return all_keys, file_hash


def add_to_all_data_summary(all_data, data):
    all_data["coordinates"].append(data["geometry"]["coordinates"][0][0])
    all_data["start_time"].append(data["properties"]["startTime"])
    all_data["stop_time"].append(data["properties"]["stopTime"])

    return all_data


def simplify_all_data_summary(all_data: dict) -> dict:
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


def generate_access_token(env="dev"):
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


def get_next_page(url, retry_count=0):
    body = {}

    try:
        access_token = generate_access_token(env="prod")
        headers = {"accept": "application/json", "Authorization": "Bearer " + access_token}
        response = requests.get(url, headers=headers, json=body)
        response.raise_for_status()

        body = response.json()

        return body

    except JSONDecodeError:
        logging.warning(f"Retrying retrieval of {url}. Attempt {retry_count}")
        if retry_count > 3:
            raise

        return get_next_page(url, retry_count=retry_count + 1)


def get_file_hash(data: str) -> str:
    """Returns hash of data available"""

    def _md5_hash(byte_str: bytes) -> str:
        """Calculates an md5 hash for given bytestring"""
        md5 = hashlib.md5()
        md5.update(byte_str)
        return md5.hexdigest()

    # if retries == 3:
    #     # Max number of retries
    #     return None
    # try:
    #     with urlopen(url, timeout=5) as response:
    #         body = response.read()
    # except urllib.error.URLError:
    #     logging.error(f"Unable to access {url}, retrying...")
    #     return get_file_hash(url, retries + 1)
    return _md5_hash(data.encode("utf-8"))


def get_file_s3(bucket: str, key: str, s3_client: boto3.client) -> str:
    """Retrieve data from an S3 bucket"""
    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        return file_obj["Body"].read().decode("utf-8")
    except ClientError as e:
        logging.error(f"File retrieval failed for {key}: {e}")
        return None


def get_metadata(bucket: str, key: str, s3_client: boto3.client) -> dict:
    """Read file at given S3 location and parse as JSON"""
    previously_harvested = get_file_s3(bucket, key, s3_client)
    try:
        previously_harvested = json.loads(previously_harvested)
    except TypeError:
        previously_harvested = {}
    return previously_harvested


def coordinates_to_bbox(coordinates):
    """Finds the biggest and smallest x and y coordinates"""

    unzipped = list(zip(*coordinates, strict=False))

    max_values = [max(index) for index in unzipped]
    min_values = [min(index) for index in unzipped]

    return min_values + max_values


def get_stac_collection_summary(all_data):
    """Gets the area and start/stop times of all data points"""

    bbox = coordinates_to_bbox(all_data["coordinates"])

    return {
        "bbox": bbox,
        "start_time": min(all_data["start_time"]),
        "stop_time": max(all_data["stop_time"]),
    }


def generate_stac_collection(all_data_summary: dict):
    """Top level collection for Airbus data"""

    stac_collection = {
        "type": "Collection",
        "id": "airbus_sar_data",
        "stac_version": "1.0.0",
        "stac_extensions": [
            "https://stac-extensions.github.io/sar/v1.0.0/schema.json",
            "https://stac-extensions.github.io/sat/v1.0.0/schema.json",
        ],
        "description": (
            "The German TerraSAR-X / TanDEM-X satellite formation and the Spanish PAZ satellite "
            "(managed by Hisdesat Servicios Estratégicos S.A.) are being operated in the same "
            "orbit tube and feature identical ground swaths and imaging modes - allowing Airbus "
            "and Hisdesat to establish a unique commercial Radar Constellation. The satellites "
            "carry a high frequency X-band Synthetic Aperture Radar (SAR) sensor in order to "
            "acquire datasets ranging from very high-resolution imagery to wide area coverage."
        ),
        "links": [],
        "title": "Airbus SAR Data",
        "geometry": {"type": "Polygon"},
        "extent": {
            "spatial": {"bbox": [all_data_summary["bbox"]]},
            "temporal": {
                "interval": [[all_data_summary["start_time"], all_data_summary["stop_time"]]]
            },
        },
        "license": "proprietary",
        "keywords": ["airbus"],
        "summaries": {
            "platform": ["TSX-1", "PAZ-1", "TDX-1"],
            "constellation": ["TSX", "PAZ"],
            "sar:product_type": ["SSC", "MGD", "GEC", "EEC"],
            "sat:orbit_state": ["ascending", "descending"],
            "sar:polarizations": [
                ["VV", "VH"],
                ["HH", "HV"],
                ["HH", "VV"],
                ["VV"],
                ["VH"],
                ["HH"],
                ["HV"],
            ],
            "sar:frequency_band": ["X"],
            "sar:instrument_mode": [
                "SAR_ST_S",
                "SAR_HS_S",
                "SAR_HS_S_300",
                "SAR_HS_S_150",
                "SAR_HS_D",
                "SAR_HS_D_300",
                "SAR_HS_D_150",
                "SAR_SL_S",
                "SAR_SL_D",
                "SAR_SM_S",
                "SAR_SM_D",
                "SAR_SC_S",
                "SAR_WS_S",
            ],
            "sar:center_frequency": [9.65],
            "sar:observation_direction": ["right", "left"],
        },
        "item_assets": {
            "thumbnail": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["thumbnail"],
                "title": "Thumbnail Image",
            }
        },
    }
    return json.dumps(stac_collection, indent=4)


def handle_quicklook_url(data, links, assets, mapped_keys):
    """Convert quicklook URL to thumbnail asset"""
    mime_types = {
        ".tiff": "image/tiff; application=geotiff; profile=cloud-optimized",
        ".tif": "image/tiff; application=geotiff; profile=cloud-optimized",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
    }

    if quicklook_url := data["properties"].get("quicklookUrl"):
        file_extension = os.path.splitext(quicklook_url)[1].lower()
        mime_type = mime_types.get(file_extension, "application/octet-stream")
        links.append(
            {
                "rel": "thumbnail",
                "href": data["properties"]["quicklookUrl"],
                "type": mime_type,
            }
        )
        assets["thumbnail"] = {
            "href": data["properties"]["quicklookUrl"],
            "type": mime_type,
        }
        mapped_keys.add("quicklookUrl")


def modify_value(key, value):
    """Modify a specific value to STAC format depending on the key"""
    if key == "lookDirection":
        # Convert look direction to human readable format
        return "right" if value.upper() == "R" else "left" if value.upper() == "L" else value
    elif key == "polarizationChannels":
        # Split dual polarisation channels into separate values
        return [value[i : i + 2] for i in range(0, len(value), 2)]
    return value


def generate_stac_item(data):
    """Catalogue items for Airbus data"""
    coordinates = data["geometry"]["coordinates"][0]
    bbox = coordinates_to_bbox(coordinates)

    mapped_keys = set()
    properties = {"access": ["HTTPServer"], "sar:frequency_band": "X", "sar:center_frequency": 9.65}

    links = []
    assets = {}

    def map_if_exists(key, source_key):
        if source_key in data["properties"]:
            properties[key] = modify_value(source_key, data["properties"][source_key])
            mapped_keys.add(source_key)

    map_if_exists("datetime", "catalogueTime")
    map_if_exists("start_datetime", "startTime")
    map_if_exists("end_datetime", "stopTime")
    map_if_exists("updated", "lastUpdateTime")
    map_if_exists("sar:instrument_mode", "sensorMode")
    map_if_exists("sar:polarizations", "polarizationChannels")
    map_if_exists("sar:observation_direction", "lookDirection")
    map_if_exists("sar:product_type", "productType")
    map_if_exists("sat:platform_international_designator", "satellite")
    map_if_exists("sat:orbit_state", "pathDirection")
    map_if_exists("sat:relative_orbit", "relativeOrbit")
    map_if_exists("sat:absolute_orbit", "absoluteOrbit")

    handle_quicklook_url(data, links, assets, mapped_keys)

    for key, value in data["properties"].items():
        if key not in mapped_keys and value is not None:
            properties[underscore(key)] = value

    stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": [
            "https://stac-extensions.github.io/sar/v1.0.0/schema.json",
            "https://stac-extensions.github.io/sat/v1.0.0/schema.json",
        ],
        "id": data["properties"]["acquisitionId"],
        "collection": "airbus_sar_data",
        "geometry": {"type": "Polygon", "coordinates": [coordinates]},
        "bbox": bbox,
        "properties": properties,
        "links": links,
        "assets": assets,
    }

    return json.dumps(stac_item, indent=4)


def make_catalogue():
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
