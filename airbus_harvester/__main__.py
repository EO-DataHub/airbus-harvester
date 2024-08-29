import json
import logging
import os
import re

import boto3
import click
import requests
from eodhp_utils.aws.s3 import upload_file_s3
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

    added_keys = []
    updated_keys = []
    deleted_keys = []

    key_root = "git-harvester/supported-datasets/airbus"

    all_data = get_catalogue(
        env=os.getenv("ENVIRONMENT", None), limit=int(os.getenv("NUMBER_OF_ENTRIES", 1))
    )

    file_name = "airbus.json"
    key = f"git-harvester/supported-datasets/{file_name}"
    upload_file_s3(make_catalogue(), s3_bucket, key, s3_client)
    added_keys.append(key)

    file_name = "airbus_sar_data.json"
    key = f"{key_root}/{file_name}"
    upload_file_s3(generate_stac_collection(all_data["features"]), s3_bucket, key, s3_client)
    added_keys.append(key)

    for raw_data in all_data["features"]:
        data = generate_stac_item(raw_data)
        file_name = f"{raw_data['properties']['acquisitionId']}.json"
        key = f"{key_root}/airbus_sar_data/{file_name}"
        upload_file_s3(data, s3_bucket, key, s3_client)

        added_keys.append(key)

    pulsar_url = os.environ.get("PULSAR_URL")
    pulsar_client = PulsarClient(pulsar_url)

    producer = pulsar_client.create_producer(
        topic="harvested", producer_name="stac_harvester/airbus"
    )
    logging.info("Harvesting from Airbus")

    # Adapted from other collection code. Keeping this in but commented out for now in case something
    # similar should be used here

    # for url in url_list:
    #     logging.info(f"Parsing URL: {url}")
    #     file_hash = get_file_hash(url)
    #
    #     previous_hash = previously_harvested.pop(url, None)
    #     if not previous_hash:
    #         # URL was not harvested previously
    #         logging.info("Appended URL to 'added' list")
    #         added_keys.append(url)
    #     elif previous_hash != file_hash:
    #         # URL has changed since last run
    #         logging.info("Appended URL to 'updated' list")
    #         updated_keys.append(url)
    #     latest_harvested[url] = file_hash
    #
    # logging.info(f"Previously harvested URLs not found: {deleted_keys}")

    output_data = {
        "id": f"{workspace_name}/airbus",
        "workspace": workspace_name,
        "bucket_name": s3_bucket,
        "added_keys": added_keys,
        "updated_keys": updated_keys,
        "deleted_keys": deleted_keys,
        "source": "airbus",
        "target": "/",
    }

    if any([added_keys, updated_keys, deleted_keys]):
        # Send Pulsar message containing harvested links
        producer.send((json.dumps(output_data)).encode("utf-8"))
        logging.info(f"Sent harvested message {output_data}")
    else:
        logging.info("No changes made to previously harvested state")

    logging.info(output_data)

    return output_data


def get_catalogue(env="dev", limit=1):
    if env == "prod":
        url = "https://sar.api.oneatlas.airbus.com"
    else:
        url = "https://dev.sar.api.oneatlas.airbus.com"
    # Authorisation not required for get catalogue
    body = {
        "limit": limit,
        # "aoi": {
        #     "type": "Polygon",
        #     "coordinates": [
        #     [[9.346, 47.788], [9.291, 47.644], [9.538, 47.592],
        #     [9.62, 47.75], [9.511, 47.802], [9.346, 47.788]]
        #     ]
        # },
        # "time": {
        #     "from": "2020-10-14T13:28:03.569Z",
        #     "to": "2023-10-29T13:28:03.569Z"
        # }
    }
    response = requests.post(f"{url}/v1/sar/catalogue", json=body)
    response.raise_for_status()
    body = response.json()
    return body


def coordinates_to_bbox(coordinates):
    """Finds the biggest and smallest x and y coordinates"""

    unzipped = list(zip(*coordinates, strict=False))

    max_values = [max(index) for index in unzipped]
    min_values = [min(index) for index in unzipped]

    return min_values + max_values


def get_stac_collection_summary(all_data):
    """Gets the area and start/stop times of all data points"""
    all_coordinates = [x["geometry"]["coordinates"][0][0] for x in all_data]
    all_start_times = [x["properties"]["startTime"] for x in all_data]
    all_stop_times = [x["properties"]["stopTime"] for x in all_data]

    bbox = coordinates_to_bbox(all_coordinates)

    return {"bbox": bbox, "start_time": min(all_start_times), "stop_time": max(all_stop_times)}


def generate_stac_collection(all_data):
    """Top level collection for Airbus data"""
    summary = get_stac_collection_summary(all_data)

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
            "spatial": {"bbox": [summary["bbox"]]},
            "temporal": {"interval": [[summary["start_time"], summary["stop_time"]]]},
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

    if "quicklookUrl" in data["properties"]:
        quicklook_url = data["properties"]["quicklookUrl"]
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
        return "right" if value == "R" else "left" if value == "L" else value
    elif key == "polarizationChannels":
        # Split dual polarisation channels into separate values
        return [value[i : i + 2] for i in range(0, len(value), 2)]
    return value


def camel_to_snake(name):
    """Convert camelCase to snake_case."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def generate_stac_item(data):
    """Catalogue items for Airbus data"""
    coordinates = data["geometry"]["coordinates"][0]
    bbox = coordinates_to_bbox(coordinates)

    mapped_keys = set()
    properties = {}
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
    properties["access"] = ["HTTPServer"]
    properties["sar:frequency_band"] = "X"
    properties["sar:center_frequency"] = 9.65

    handle_quicklook_url(data, links, assets, mapped_keys)

    for key, value in data["properties"].items():
        if key not in mapped_keys and value is not None:
            properties[camel_to_snake(key)] = value

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
