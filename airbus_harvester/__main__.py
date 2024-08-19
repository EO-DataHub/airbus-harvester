import json
import logging
import os

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

    return max_values + min_values


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

    return f"""{{
  "type": "Collection",
  "id": "airbus_data_example",
  "stac_version": "1.0.0",
  "description": "Airbus data",
  "links": [
  ],
  "title": "Airbus Data",
  "geometry": {{
    "type": "Polygon"
  }},
  "extent": {{
    "spatial": {{
      "bbox": [
        {summary['bbox']}
      ]
    }},
    "temporal": {{
      "interval": [
        [
          "{summary['start_time']}",
          "{summary['stop_time']}"
        ]
      ]
    }}
  }},
  "license": "proprietary",
  "keywords": [
    "airbus"
  ]
}}"""


def generate_stac_item(data):
    """Catalogue items for Airbus data"""
    coordinates = data["geometry"]["coordinates"][0]
    bbox = coordinates_to_bbox(coordinates)

    return f"""{{
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "id": "airbus_data_example_{data['properties']['itemId']}",
        "collection": "airbus_data_example",
        "geometry": {{
            "type": "Polygon",
            "coordinates": [
                {coordinates}
            ]
        }},
        "bbox": {bbox},
        "properties": {{
            "datetime": "{data['properties']['startTime']}",
            "start_datetime": "{data['properties']['startTime']}",
            "end_datetime": "{data['properties']['stopTime']}",
            "access": [
                "HTTPServer"
            ]
        }},
        "links": [],
        "assets": {{}}
    }}"""


def make_catalogue():
    """Top level catalogue for Airbus data"""
    return """{
      "type": "Catalog",
      "id": "airbus",
      "stac_version": "1.0.0",
      "description": "Airbus Datasets",
      "links": []
    }"""


@click.command()
# not currently used but keeping the same structure as the other harvester repos
# @click.option("source_url", type=str, nargs=1)
@click.argument("workspace_name", type=str, nargs=1)
@click.argument(
    "catalog", type=str, nargs=1
)  # not currently used but keeping the same structure as the other harvester repos
@click.argument("s3_bucket", type=str, nargs=1)
def main(workspace_name, catalog, s3_bucket):
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

    key_root = "git-harvester/supported_datasets/airbus"

    all_data = get_catalogue(
        env=os.getenv("ENVIRONMENT", None), limit=int(os.getenv("NUMBER_OF_ENTRIES", 1))
    )

    file_name = "airbus.json"
    key = f"git-harvester/supported_datasets/{file_name}"
    upload_file_s3(make_catalogue(), s3_bucket, key, s3_client)
    added_keys.append(key)

    file_name = "airbus_data_example.json"
    key = f"{key_root}/{file_name}"
    upload_file_s3(generate_stac_collection(all_data["features"]), s3_bucket, key, s3_client)
    added_keys.append(key)

    for raw_data in all_data["features"]:
        data = generate_stac_item(raw_data)
        file_name = f"{str(hash(data))}.json"
        key = f"{key_root}/airbus_data_example/{file_name}"
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


if __name__ == "__main__":
    main()
