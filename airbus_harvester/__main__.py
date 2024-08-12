import argparse
import json
import logging
import os

import boto3
import requests
from eodhp_utils.aws.s3 import upload_file_s3
from pulsar import Client as PulsarClient

parser = argparse.ArgumentParser()
# parser.add_argument("source_url", help="URL of STAC catalog or collection to harvest", type=str)
parser.add_argument("workspace_name", help="Name of the workspace", type=str)
parser.add_argument("catalog", help="Path of the STAC catalog", type=str)
parser.add_argument("s3_bucket", help="S3 bucket to store harvested data", type=str)


logging.basicConfig(level=logging.DEBUG, format="%(message)s")


def get_catalogue(env="dev", limit=10):
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


def generate_stac_entry(data):
    coordinates = data["geometry"]["coordinates"][0]
    bbox = coordinates_to_bbox(coordinates)

    return f"""{{
  "type": "Collection",
  "id": "airbus_data_example_{data['properties']['itemId']}",
  "stac_version": "1.0.0",
  "description": "Converted Airbus data",
  "links": [
  ],
  "title": "Airbus Data",
  "geometry": {{
    "type": "Polygon",
    "coordinates": [
        {coordinates}
    ]
  }},
  "extent": {{
    "spatial": {{
      "bbox": [
        {bbox}
      ]
    }},
    "temporal": {{
      "interval": [
        [
          "{data['properties']['startTime']}",
          "{data['properties']['stopTime']}"
        ]
      ]
    }}
  }},
  "license": "proprietary",
  "keywords": [
    "airbus"
  ]
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


def main():
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

    args = parser.parse_args()
    added_keys = []
    updated_keys = []
    deleted_keys = []

    key_root = "git-harvester/airbus"

    all_data = get_catalogue(
        env=os.getenv("ENVIRONMENT", None), limit=os.getenv("NUMBER_OF_ENTRIES", 10)
    )

    file_name = "airbus.json"
    key = f"{key_root}/{file_name}"
    upload_file_s3(make_catalogue(), args.s3_bucket, key, s3_client)
    added_keys.append(key)

    for raw_data in all_data["features"]:
        data = generate_stac_entry(raw_data)
        file_name = f"{str(hash(data))}.json"
        key = f"{key_root}/{file_name}"
        upload_file_s3(data, args.s3_bucket, key, s3_client)

        added_keys.append(key)

    pulsar_url = os.environ.get("PULSAR_URL")
    pulsar_client = PulsarClient(pulsar_url)

    producer = pulsar_client.create_producer(
        topic="harvested", producer_name="stac_harvester/airbus"
    )
    logging.info("Harvesting from Airbus")

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
        "id": f"{args.workspace_name}/airbus",
        "workspace": args.workspace_name,
        "bucket_name": args.s3_bucket,
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
