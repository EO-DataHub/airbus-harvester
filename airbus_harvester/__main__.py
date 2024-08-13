import argparse
import json
import logging
import os

import boto3
import requests
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
    print(s3_client)
    print(deleted_keys)

    key_root = "git-harvester/airbus"

    # all_data = get_catalogue(
    #     env=os.getenv("ENVIRONMENT", None), limit=os.getenv("NUMBER_OF_ENTRIES", 10)
    # )
    #
    # file_name = "airbus.json"
    # key = f"git-harvester/{file_name}"
    # upload_file_s3(make_catalogue(), args.s3_bucket, key, s3_client)
    # added_keys.append(key)
    #
    # for raw_data in all_data["features"]:
    #     data = generate_stac_entry(raw_data)
    #     file_name = f"{str(hash(data))}.json"
    #     key = f"{key_root}/{file_name}"
    #     upload_file_s3(data, args.s3_bucket, key, s3_client)
    #
    #     added_keys.append(key)

    deleted_keys_string = """-2049705932438091386.json
        -2386519659230935482.json
        -2766364387309597199.json
        -2872426756758816819.json
        -2905995955646545547.json
        -293857486172066265.json
        -3069254879436288508.json
        -3098650186220064637.json
        -315897347886121655.json
        -3201032876773978964.json
        -3201553027971744635.json
        -3479103622956610585.json
        -3557410366069735503.json
        -3756208633202079941.json
        -3855428226394438226.json
        -4100622161392611832.json
        -4812348121153688175.json
        -5147960797284077255.json
        -5586701697231309719.json
        -5686656551680466320.json
        -5726795281919118389.json
        -6290484079678060125.json
        -6317347217522173219.json
        -6396191179836776613.json
        -6435410476263104229.json
        -652118690733096948.json
        -6593965725011420526.json
        -6612049263924421888.json
        -676921649230533589.json
        -7187691873020746785.json
        -7337182249891752389.json
        -7449146859765626961.json
        -7840659349911904973.json
        -7967783975079991936.json
        -7980409026660115823.json
        -8176452451968160758.json
        -8186323882444939290.json
        -8212233504520634492.json
        -8307741328142632391.json
        -849112916442743979.json
        -8571947117006007933.json
        -8676837801654723373.json
        -8986961841584992761.json
        -9186842709433207768.json
        -983466811561452922.json
        1159769114740699119.json
        1779186935173586471.json
        1946293209498012937.json
        2094325802569503017.json
        2309357298284883977.json
        2417690065019782786.json
        2454539672106161732.json
        2479776623275728546.json
        2522048735233095749.json
        2791043819102135120.json
        3221720585534781585.json
        3445117355344329924.json
        3628403037979066017.json
        3938451443789857792.json
        4565547833898588049.json
        4883362823893150160.json
        5381670708020374937.json
        563520575707820558.json
        5641327693309720614.json
        5991434083271536047.json
        6107654857655067588.json
        6213002731287763097.json
        6267453303907660381.json
        6450335068722405047.json
        6479579018111872820.json
        6521984428066681414.json
        6583795484271623486.json
        6954700444266649643.json
        7142601386801879640.json
        7145520808213136845.json
        7723619232910184425.json
        781459953586200322.json
        8060844145860631318.json
        8102151093323509978.json
        831130576868985854.json
        8348998414492909710.json
        841695216671687547.json
        8841628390958028711.json
        8973601383182179270.json
        929883787114410831.json
        """
    deleted_keys = [f"{key_root}/{k.strip()}" for k in deleted_keys_string.split()]

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
