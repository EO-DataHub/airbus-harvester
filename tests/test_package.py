import json
import os
from unittest import mock
from unittest.mock import patch

import boto3
import moto
import pytest
from click.testing import CliRunner

from airbus_harvester.__main__ import (
    add_to_catalogue_data_summary,
    coordinates_to_bbox,
    generate_stac_collection,
    generate_stac_item,
    get_stac_collection_summary,
    handle_quicklook_url,
    harvest,
    make_catalogue,
    modify_value,
)


@pytest.fixture(autouse=True)
def setenvvar(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "AIRBUS_API_KEY": "41rbu5-4p1-k3y",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield


@pytest.fixture
def mock_catalogue_response():
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [15.5374877, 60.4735848],
                            [15.5183265, 60.5203787],
                            [15.3515055, 60.5030195],
                            [15.3729703, 60.456454],
                            [15.5374877, 60.4735848],
                        ]
                    ],
                },
                "properties": {
                    "absoluteOrbit": 4670,
                    "acquisitionId": "TSX-1_HS300_S_spot_037R_4670_A15048840_754",
                    "antennaMode": "SRA",
                    "beamId": "spot_037",
                    "catalogueTime": "2022-02-12T17:51:37.686Z",
                    "incidenceAngle": {"minimum": 33.46, "maximum": 34.28},
                    "lookDirection": "R",
                    "mission": "TSX",
                    "outOfFullPerformance": False,
                    "pathDirection": "ascending",
                    "polarizationChannels": "HH",
                    "quality": "AUTO_APPROVED",
                    "relativeOrbit": 161,
                    "satellite": "TSX-1",
                    "sensorMode": "SAR_HS_S_300",
                    "startTime": "2008-04-17T16:28:39.715Z",
                    "stopTime": "2008-04-17T16:28:40.469Z",
                    "quicklookUrl": "https://content.sar.api.oneatlas.airbus.com/quicklooks/radar/4326/TSX-1_HS300_S_spot_037R_4670_A15048840_754.tif",
                },
                "assets": {
                    "browse": {
                        "href": "https://content.sar.api.oneatlas.airbus.com/quicklooks/radar/4326/TSX-1_HS300_S_spot_037R_4670_A15048840_754.tif",
                        "roles": ["overview"],
                        "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    }
                },
            }
        ],
        "_links": {"self": "my_link.com"},
    }


@pytest.fixture
def mock_response():
    return {
        "limit": 1,
        "total": 566933,
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-27.9837603, 38.6070725],
                            [-28.0150768, 38.7135605],
                            [-28.1373002, 38.6958769],
                            [-28.1166933, 38.5878242],
                            [-27.9837603, 38.6070725],
                        ]
                    ],
                },
                "properties": {
                    "itemId": "9e1c25de-0712-4260-85ca-45ff1f6c611e",
                    "itemType": "catalogue",
                    "acquisitionId": "TDX-1_SL_D_spot_029R_75359_A12865033_1711",
                    "mission": "TSX",
                    "satellite": "TDX-1",
                    "absoluteOrbit": 75359,
                    "relativeOrbit": 72,
                    "pathDirection": "ascending",
                    "lookDirection": "R",
                    "beamId": "spot_029",
                    "incidenceAngle": {"minimum": 29.88, "maximum": 31.02},
                    "polarizationChannels": "HHVV",
                    "startTime": "2024-01-22T19:42:44.735Z",
                    "stopTime": "2024-01-22T19:42:46.446Z",
                    "outOfFullPerformance": False,
                    "status": "availableInArchive",
                    "lastUpdateTime": "2024-08-30T15:06:20Z",
                    "sensorMode": "SAR_SL_D",
                    "expiry": "2024-09-13T15:06:19Z",
                    "orderTemplate": "default",
                    "quality": "AUTO_APPROVED",
                    "catalogueTime": "2024-01-23T05:33:36Z",
                    "antennaMode": "SRA",
                    "quicklookUrl": "https://dev.content.sar.api.intelligence.airbus.com/quicklooks/radar/4326/TDX-1_SL_D_spot_029R_75359_A12865033_1711.tif",
                    "datastackCount": 39,
                },
                "assets": {
                    "browse": {
                        "href": "https://dev.content.sar.api.intelligence.airbus.com/quicklooks/radar/4326/TDX-1_SL_D_spot_029R_75359_A12865033_1711.tif",
                        "roles": ["overview"],
                        "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    }
                },
            }
        ],
        "bbox": [-28.1373002, 38.5878242, -27.9837603, 38.7135605],
    }


@pytest.fixture
def mock_data():
    return {
        "coordinates": [[-27.9837603, 38.6070725]],
        "start_time": ["2024-01-22T19:42:44.735Z"],
        "stop_time": ["2024-01-22T19:42:46.446Z"],
    }


@moto.mock_aws
@patch("airbus_harvester.__main__.PulsarClient")
def test_harvest(mock_create_client, requests_mock, mock_catalogue_response):
    requests_mock.get(
        "https://sar.api.oneatlas.airbus.com/v1/sar/catalogue/replication",
        text=json.dumps(mock_catalogue_response),
    )
    requests_mock.post(
        "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token",
        text='{"access_token": "my_access_token"}',
    )

    mock_client = mock.MagicMock()
    mock_producer = mock.MagicMock()
    mock_create_client.return_value = mock_client
    mock_client.create_producer.return_value = mock_producer

    bucket_name = "my-bucket"

    s3 = boto3.resource("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket_name)

    os.environ["PULSAR_URL"] = "mypulsar.com/pulsar"

    runner = CliRunner()
    runner.invoke(harvest, f"workspace catalogue {bucket_name}".split())

    s3 = boto3.resource("s3")
    my_bucket = s3.Bucket(bucket_name)

    assert len(list(my_bucket.objects.all())) == 4

    args, kwargs = mock_producer.send.call_args
    call_args = json.loads(args[0])
    assert {
        "id",
        "workspace",
        "bucket_name",
        "added_keys",
        "updated_keys",
        "deleted_keys",
        "source",
        "target",
    }.issubset(call_args.keys())
    assert call_args["bucket_name"] == bucket_name
    assert len(call_args["added_keys"]) == 3
    assert len(call_args["updated_keys"]) == len(call_args["deleted_keys"]) == 0


def test__coordinates_to_bbox__success(mock_response):
    coordinates = mock_response["features"][0]["geometry"]["coordinates"]
    expected_bbox = [-28.1373002, 38.5878242, -27.9837603, 38.7135605]

    actual_bbox = coordinates_to_bbox(coordinates[0])

    assert expected_bbox == actual_bbox


def test_get_stac_collection_summary(mock_data, mock_response):
    expected_summary = {
        "bbox": [-27.9837603, 38.6070725, -27.9837603, 38.6070725],
        "start_time": mock_response["features"][0]["properties"]["startTime"],
        "stop_time": mock_response["features"][0]["properties"]["stopTime"],
    }

    actual_summary = get_stac_collection_summary(mock_data)

    assert actual_summary == expected_summary


def test_generate_stac_collection(mock_data):
    mock_data_summary = {
        "bbox": [1, 2, 3, 4],
        "start_time": "2024-01-22T19:42:44.735Z",
        "stop_time": "2024-01-22T19:42:46.446Z",
    }

    actual_collection = generate_stac_collection(mock_data_summary)
    json_collection = json.loads(actual_collection)

    assert isinstance(actual_collection, str)
    assert {
        "type",
        "id",
        "stac_version",
        "stac_extensions",
        "description",
        "links",
        "title",
        "geometry",
        "extent",
        "license",
        "keywords",
        "summaries",
        "item_assets",
    }.issubset(set(json_collection.keys()))


def test_handle_quicklook_url__with_quicklook():
    mapped_keys = set()
    links = []
    assets = {}

    data = {"properties": {"quicklookUrl": "www.test.com"}}

    handle_quicklook_url(data, links, assets, mapped_keys)

    assert len(assets) == len(links) == len(mapped_keys) == 1
    assert "quicklookUrl" in mapped_keys
    assert {"rel", "href", "type"}.issubset(links[0].keys())
    assert {"href", "type"}.issubset(assets["thumbnail"].keys())


def test_handle_quicklook_url__without_quicklook():
    mapped_keys = set()
    links = []
    assets = {}

    data = {"properties": {"notQuicklookUrl": "www.test.com"}}

    handle_quicklook_url(data, links, assets, mapped_keys)

    assert len(assets) == len(links) == len(mapped_keys) == 0


@pytest.mark.parametrize(
    "key, value, expected_return_value",
    [
        pytest.param("any", "any", "any", id="any"),
        pytest.param("lookDirection", "R", "right", id="lookDirection_R"),
        pytest.param("lookDirection", "L", "left", id="lookDirection_L"),
        pytest.param("lookDirection", "r", "right", id="lookDirection_r"),
        pytest.param("lookDirection", "l", "left", id="lookDirection_l"),
        pytest.param("lookDirection", "any", "any", id="lookDirection_any"),
        pytest.param("polarizationChannels", "HHVV", ["HH", "VV"], id="polarizationChannels"),
    ],
)
def test_modify_value(key, value, expected_return_value):
    actual_return_value = modify_value(key, value)

    assert expected_return_value == actual_return_value


def test_generate_stac_item(mock_response):
    actual_item = generate_stac_item(mock_response["features"][0])
    json_collection = json.loads(actual_item)

    assert isinstance(actual_item, str)
    assert {
        "type",
        "id",
        "stac_version",
        "stac_extensions",
        "collection",
        "geometry",
        "bbox",
        "properties",
        "links",
        "assets",
    }.issubset(set(json_collection.keys()))


def test_make_catalogue():
    expected_catalogue = {
        "type": "Catalog",
        "id": "airbus",
        "stac_version": "1.0.0",
        "description": "Airbus Datasets",
        "links": [],
    }

    actual_catalogue = make_catalogue()

    assert json.loads(actual_catalogue) == expected_catalogue


def test_add_to_all_data_summary(mock_response):
    data = mock_response["features"][0]
    all_data = {"coordinates": [], "start_time": [], "stop_time": []}

    all_data = add_to_catalogue_data_summary(all_data, data)

    assert all_data["coordinates"][0] == data["geometry"]["coordinates"][0][0]
    assert all_data["start_time"][0] == data["properties"]["startTime"]
    assert all_data["stop_time"][0] == data["properties"]["stopTime"]
