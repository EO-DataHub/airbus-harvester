from __future__ import annotations

import json
import os
import tempfile
from typing import Any
from unittest import mock
from unittest.mock import patch

import boto3
import moto
import pytest
from click.testing import CliRunner

from airbus_harvester.__main__ import (
    add_to_catalogue_data_summary,
    coordinates_to_bbox,
    find_deleted_keys,
    generate_stac_collection,
    generate_stac_item,
    get_file_hash,
    get_item_id,
    get_stac_collection_summary,
    handle_external_url,
    harvest,
    load_config,
    make_catalogue,
    modify_value,
    next_counter_page,
)


@pytest.fixture(autouse=True)
def setenvvar(monkeypatch: pytest.MonkeyPatch) -> Any:
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "AIRBUS_API_KEY": "41rbu5-4p1-k3y",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield


@pytest.fixture
def parameters() -> dict:
    """Parameters used by stac harvester tests"""
    return {
        "metadata_bucket_name": "test_metadata_bucket",
        "files_bucket_name": "test_files_bucket",
        "url": "https://test-url.co.uk/collection",
        "previously_harvested": {"some/file/key/to/delete.json": "hash"},
    }


@pytest.fixture
def mock_catalogue_response() -> dict:
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
def mock_response() -> dict:
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
def mock_data() -> dict:
    return {
        "coordinates": [[-27.9837603, 38.6070725]],
        "start_time": ["2024-01-22T19:42:44.735Z"],
        "stop_time": ["2024-01-22T19:42:46.446Z"],
    }


@pytest.fixture
def mock_config() -> dict:
    return {
        "collection_name": "airbus_sar_data",
        "url": "https://sar.api.oneatlas.airbus.com/v1/sar/catalogue/replication",
        "auth_env": "prod",
        "body": None,
        "stac_properties": {
            "access": ["HTTPServer"],
            "sar:frequency_band": "X",
            "sar:center_frequency": 9.65,
        },
        "stac_properties_map": {
            "datetime": "catalogueTime",
            "start_datetime": "startTime",
            "end_datetime": "stopTime",
            "updated": "lastUpdateTime",
            "sar:instrument_mode": "sensorMode",
            "sar:polarizations": "polarizationChannels",
            "sar:observation_direction": "lookDirection",
            "sar:product_type": "productType",
            "sat:platform_international_designator": "satellite",
            "sat:orbit_state": "pathDirection",
            "sat:relative_orbit": "relativeOrbit",
            "sat:absolute_orbit": "absoluteOrbit",
        },
        "external_urls": [{"name": "thumbnail", "path": "properties.quicklookUrl"}],
        "stac_extensions": [
            "https://stac-extensions.github.io/sar/v1.0.0/schema.json",
            "https://stac-extensions.github.io/sat/v1.0.0/schema.json",
        ],
        "item_id_key": "acquisitionId",
        "pagination_method": "link",
    }


@moto.mock_aws
@patch("airbus_harvester.__main__.get_pulsar_client")
def test_harvest(mock_create_client: Any, requests_mock: Any, mock_catalogue_response: dict) -> None:
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

    s3_resource: Any = boto3.resource("s3", region_name="us-east-1")
    s3_resource.create_bucket(Bucket=bucket_name)

    os.environ["PULSAR_URL"] = "mypulsar.com/pulsar"
    os.environ["HARVESTER_CONFIG_KEY"] = "SAR"

    runner = CliRunner()
    runner.invoke(harvest, f"workspace catalogue {bucket_name}".split())

    s3_resource = boto3.resource("s3")
    my_bucket = s3_resource.Bucket(bucket_name)

    assert len(list(my_bucket.objects.all())) == 4

    args, _kwargs = mock_producer.send.call_args
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


@moto.mock_aws
@patch("airbus_harvester.__main__.get_pulsar_client")
def test_harvest_delete(
    mock_create_client: Any,
    requests_mock: Any,
    mock_catalogue_response: dict,
    mock_config: dict,
    parameters: dict,
) -> None:
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

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create local file imitating stored harvested files from a previous run
        harvested_file_name = "harvested.txt"
        folder_path = f"{temp_dir}/test"
        os.makedirs(folder_path)
        local_file_path = f"{folder_path}/{harvested_file_name}"
        with open(local_file_path, "w") as temp_file:
            temp_file.write(json.dumps(parameters["previously_harvested"]))
            temp_file.flush()

        # Create S3 resources and upload file to S3
        s3_client: Any = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.upload_file(
            local_file_path,
            bucket_name,
            f"harvested-metadata/{mock_config['collection_name']}",
        )
        s3_client.upload_file(
            local_file_path,  # any example file
            bucket_name,
            "git-harvester/some/file/key/to/delete.json",
        )
        s3_resource: Any = boto3.resource("s3")
        my_bucket = s3_resource.Bucket(bucket_name)
        assert len(list(my_bucket.objects.all())) == 2

    os.environ["PULSAR_URL"] = "mypulsar.com/pulsar"
    os.environ["HARVESTER_CONFIG_KEY"] = "SAR"

    runner = CliRunner()
    runner.invoke(harvest, f"workspace catalogue {bucket_name}".split())

    assert len(list(my_bucket.objects.all())) == 4

    args, _kwargs = mock_producer.send.call_args
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
    assert len(call_args["updated_keys"]) == 0
    assert len(call_args["deleted_keys"]) == 1


def test__coordinates_to_bbox__success(mock_response: dict) -> None:
    coordinates = mock_response["features"][0]["geometry"]["coordinates"]
    expected_bbox = [-28.1373002, 38.5878242, -27.9837603, 38.7135605]

    actual_bbox = coordinates_to_bbox(coordinates[0])

    assert expected_bbox == actual_bbox


def test_get_stac_collection_summary(mock_data: dict, mock_response: dict) -> None:
    expected_summary = {
        "bbox": [-27.9837603, 38.6070725, -27.9837603, 38.6070725],
        "start_time": mock_response["features"][0]["properties"]["startTime"],
        "stop_time": mock_response["features"][0]["properties"]["stopTime"],
    }

    actual_summary = get_stac_collection_summary(mock_data)

    assert actual_summary == expected_summary


def test_generate_stac_collection(mock_data: dict, mock_config: dict) -> None:
    mock_data_summary = {
        "bbox": [1, 2, 3, 4],
        "start_time": "2024-01-22T19:42:44.735Z",
        "stop_time": "2024-01-22T19:42:46.446Z",
    }

    actual_collection = generate_stac_collection(mock_data_summary, mock_config)

    assert isinstance(actual_collection, dict)
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
    }.issubset(set(actual_collection.keys()))


def test_handle_external_url__with_quicklook(mock_config: dict) -> None:
    external_url = mock_config["external_urls"][0]

    mapped_keys: set[str] = set()
    links: list[dict] = []
    assets: dict[str, dict] = {}

    data = {"properties": {"quicklookUrl": "www.test.com"}}

    handle_external_url(data, links, assets, mapped_keys, external_url["name"], external_url["path"])

    assert len(assets) == len(mapped_keys) == 1
    assert len(links) == 0
    assert "quicklookUrl" in mapped_keys
    assert {"href", "type"}.issubset(assets["thumbnail"].keys())


def test_handle_external_url__without_quicklook(mock_config: dict) -> None:
    external_url = mock_config["external_urls"][0]

    mapped_keys: set[str] = set()
    links: list[dict] = []
    assets: dict[str, dict] = {}

    data = {"properties": {"notQuicklookUrl": "www.test.com"}}

    handle_external_url(data, links, assets, mapped_keys, external_url["name"], external_url["path"])

    assert len(assets) == len(links) == len(mapped_keys) == 0


@pytest.mark.parametrize(
    ("key", "value", "expected_return_value"),
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
def test_modify_value(key: str, value: str, expected_return_value: Any) -> None:
    actual_return_value = modify_value(key, value)

    assert expected_return_value == actual_return_value


def test_generate_stac_item(mock_response: dict, mock_config: dict) -> None:
    actual_item = generate_stac_item(mock_response["features"][0], mock_config)
    json_collection = actual_item

    assert isinstance(actual_item, dict)
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


def test_make_catalogue() -> None:
    expected_catalogue = {
        "type": "Catalog",
        "id": "airbus",
        "stac_version": "1.0.0",
        "description": "Airbus Datasets",
        "links": [],
    }

    actual_catalogue = make_catalogue()

    assert actual_catalogue == expected_catalogue


def test_add_to_all_data_summary(mock_response: dict) -> None:
    data = {
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
            "start_datetime": "2024-01-22T19:42:44.735Z",
            "end_datetime": "2024-01-22T19:42:46.446Z",
        },
    }
    all_data: dict[str, list] = {"coordinates": [], "start_time": [], "stop_time": []}

    all_data = add_to_catalogue_data_summary(all_data, data)

    assert all_data["coordinates"][0] == data["geometry"]["coordinates"][0][0]
    assert all_data["start_time"][0] == data["properties"]["start_datetime"]
    assert all_data["stop_time"][0] == data["properties"]["end_datetime"]


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [({"a", "b", "c"}, {"a": 10, "d": 2, "e": 3}, ["d", "e"])],
)
def test_find_deleted_keys(first: set, second: dict, expected: list) -> None:
    actual = find_deleted_keys(first, second)

    assert set(actual) == set(expected)


def optical_record(item_id: str | None, last_update: str = "2026-01-01T00:00:00Z", **properties: Any) -> dict:
    """A OneAtlas opensearch record with the fields the harvester reads"""
    return {
        "type": "Feature",
        "geometry": {"type": "Polygon", "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]},
        "properties": {
            "acquisitionIdentifier": item_id,
            "sourceIdentifier": item_id,
            "acquisitionDate": "2026-01-01T00:00:00Z",
            "lastUpdateDate": last_update,
            "productCategory": "image",
            "id": f"uuid-{item_id}",
            **properties,
        },
        "_links": {},
    }


def coupling_record(source_id: str, last_update: str = "2026-01-01T00:00:00Z") -> dict:
    return optical_record(
        "", last_update, sourceIdentifier=source_id, productCategory="coupling", id=f"uuid-{source_id}"
    )


def phr_config() -> dict:
    with open("airbus_harvester/config.json") as f:
        return json.load(f)["PHR"]


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        pytest.param(optical_record("DS_PHR1A_1"), "DS_PHR1A_1", id="image"),
        pytest.param(coupling_record("CP_PHR1A_1_ST_0-43"), "CP_PHR1A_1_ST_0-43", id="coupling_empty_id"),
        pytest.param(
            optical_record("UNSET", sourceIdentifier="CP_PHR1A_1", productCategory="coupling"),
            "CP_PHR1A_1",
            id="coupling_unset_id",
        ),
        pytest.param(
            optical_record("ACQ_CP_PHR1A_1", sourceIdentifier="CP_PHR1A_1", productCategory="coupling"),
            "ACQ_CP_PHR1A_1",
            id="coupling_with_id",
        ),
        pytest.param(optical_record(None, sourceIdentifier="DS_PHR1A_1"), None, id="image_null_id"),
        pytest.param(optical_record("", sourceIdentifier="DS_PHR1A_1"), None, id="image_empty_id"),
        pytest.param(coupling_record(""), None, id="coupling_no_source_id"),
    ],
)
def test_get_item_id(record: dict, expected: str | None) -> None:
    assert get_item_id(record, phr_config()) == expected


def test_get_item_id__no_coupling_key(mock_config: dict) -> None:
    record = {"properties": {"acquisitionId": "", "productCategory": "coupling", "sourceIdentifier": "CP_1"}}

    assert get_item_id(record, mock_config) is None


def test_generate_stac_item__coupling_with_empty_id() -> None:
    item = generate_stac_item(coupling_record("CP_PHR1A_1_ST_0-43"), phr_config())

    assert item["id"] == "CP_PHR1A_1_ST_0-43"
    assert item["properties"]["source_identifier"] == "CP_PHR1A_1_ST_0-43"
    assert "acquisition_identifier" not in item["properties"]


def run_phr_harvest(pages: list[dict], previous_metadata: dict, requests_mock: Any, mock_create_client: Any) -> tuple:
    """Run the PHR harvest against a fake search that serves the given pages in order"""
    requests_mock.post(
        "https://search.foundation.api.oneatlas.airbus.com/api/v2/opensearch",
        [{"text": json.dumps(page)} for page in pages],
    )
    mock_client = mock.MagicMock()
    mock_producer = mock.MagicMock()
    mock_create_client.return_value = mock_client
    mock_client.create_producer.return_value = mock_producer

    bucket_name = "my-bucket"
    s3_client: Any = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.put_object(
        Bucket=bucket_name, Key="harvested-metadata/airbus_phr_data", Body=json.dumps(previous_metadata)
    )

    os.environ["PULSAR_URL"] = "mypulsar.com/pulsar"
    os.environ["HARVESTER_CONFIG_KEY"] = "PHR"
    result = CliRunner().invoke(harvest, f"workspace catalogue {bucket_name}".split())

    messages = [json.loads(call.args[0]) for call in mock_producer.send.call_args_list]
    metadata = json.loads(
        s3_client.get_object(Bucket=bucket_name, Key="harvested-metadata/airbus_phr_data")["Body"].read()
    )
    return result, messages, metadata


ITEMS = "commercial/catalogs/airbus/collections/airbus_phr_data/items"


@moto.mock_aws
@patch("airbus_harvester.__main__.get_pulsar_client")
def test_harvest__records_without_id(mock_create_client: Any, requests_mock: Any) -> None:
    records = [
        optical_record("DS_PHR1A_1", "2026-01-04T00:00:00Z"),
        coupling_record("CP_PHR1A_1", "2026-01-03T00:00:00Z"),
        coupling_record("CP_PHR1A_2", "2026-01-02T00:00:00Z"),
        optical_record(None, "2026-01-01T00:00:00Z", sourceIdentifier="DS_PHR1A_1"),
    ]
    pages = [
        {"totalResults": len(records), "features": records},
        {"totalResults": len(records), "features": []},
    ]
    unchanged = optical_record("DS_PHR1A_0", "2026-01-05T00:00:00Z")
    records.insert(0, unchanged)
    previous_metadata = {
        f"{ITEMS}/.json": "hash",
        f"{ITEMS}/None.json": "hash",
        f"{ITEMS}/DS_PHR1A_gone.json": "hash",
        f"{ITEMS}/DS_PHR1A_0.json": get_file_hash(json.dumps(generate_stac_item(unchanged, phr_config()))),
    }

    result, messages, metadata = run_phr_harvest(pages, previous_metadata, requests_mock, mock_create_client)

    assert result.exit_code == 0, result.output
    added = {key for message in messages for key in message["added_keys"]}
    deleted = {key for message in messages for key in message["deleted_keys"]}
    sent_items = f"git-harvester/{ITEMS}"
    assert {f"{sent_items}/DS_PHR1A_1.json", f"{sent_items}/CP_PHR1A_1.json", f"{sent_items}/CP_PHR1A_2.json"} <= added
    assert not any(key.endswith(("/.json", "/None.json")) for key in added)
    assert f"{sent_items}/DS_PHR1A_0.json" not in added
    assert deleted == {f"{sent_items}/DS_PHR1A_gone.json"}
    assert f"{ITEMS}/CP_PHR1A_1.json" in metadata
    assert f"{ITEMS}/DS_PHR1A_0.json" in metadata
    assert not any(key.endswith(("/.json", "/None.json")) for key in metadata)


class FakeOpenSearch:
    """Serves records sorted by -lastUpdateDate like the OneAtlas search, up to page 50"""

    def __init__(self, records: list[dict]) -> None:
        self.records = sorted(records, key=lambda r: r["properties"]["lastUpdateDate"], reverse=True)
        self.requests: list[dict] = []

    def __call__(self, request: Any, context: Any) -> dict:
        body = request.json()
        self.requests.append(body)
        assert body["startPage"] <= 50
        records = self.records
        if "lastUpdateDate" in body:
            low, high = body["lastUpdateDate"].strip("[]").split(",")
            records = [r for r in records if low <= r["properties"]["lastUpdateDate"] <= high]
        start = (body["startPage"] - 1) * body["itemsPerPage"]
        return {"totalResults": len(records), "features": records[start : start + body["itemsPerPage"]]}


def small_page_config(path: str) -> Any:
    """load_config with two records per page, so a search window holds 100 records"""
    config = load_config(path)
    if path.endswith("config.json"):
        config["PHR"]["body"]["itemsPerPage"] = 2
    return config


@moto.mock_aws
@patch("airbus_harvester.__main__.get_pulsar_client")
def test_harvest__counter_pagination_reaches_every_record(mock_create_client: Any, requests_mock: Any) -> None:
    # 150 records: the first window holds 100, the second the remaining 50 plus the boundary record.
    # Records 95 to 104 share an update time, so they straddle the window boundary.
    records = [optical_record(f"DS_PHR1A_{i:03}", f"2026-01-01T00:00:{150 - i:03}Z") for i in range(150)]
    for record in records[95:105]:
        record["properties"]["lastUpdateDate"] = "2026-01-01T00:00:050Z"
    search = FakeOpenSearch(records)
    requests_mock.post("https://search.foundation.api.oneatlas.airbus.com/api/v2/opensearch", json=search)

    mock_client = mock.MagicMock()
    mock_producer = mock.MagicMock()
    mock_create_client.return_value = mock_client
    mock_client.create_producer.return_value = mock_producer
    s3_client: Any = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="my-bucket")
    os.environ["PULSAR_URL"] = "mypulsar.com/pulsar"
    os.environ["HARVESTER_CONFIG_KEY"] = "PHR"

    with patch("airbus_harvester.__main__.load_config", side_effect=small_page_config):
        result = CliRunner().invoke(harvest, ["workspace", "catalogue", "my-bucket"])

    assert result.exit_code == 0, result.output
    added = {key for call in mock_producer.send.call_args_list for key in json.loads(call.args[0])["added_keys"]}
    assert {f"git-harvester/{ITEMS}/DS_PHR1A_{i:03}.json" for i in range(150)} <= added
    windows = [(r["startPage"], r.get("lastUpdateDate")) for r in search.requests]
    assert windows[49] == (50, None)
    assert windows[50] == (1, "[2018-10-03T12:00:00Z,2026-01-01T00:00:050Z]")


def test_next_counter_page__next_page() -> None:
    request = {"startPage": 3, "itemsPerPage": 2}
    features = [optical_record("a"), optical_record("b")]

    assert next_counter_page(request, {"totalResults": 10}, features) == {"startPage": 4, "itemsPerPage": 2}


@pytest.mark.parametrize(
    ("page", "count", "total"),
    [
        pytest.param(1, 0, 0, id="empty"),
        pytest.param(5, 0, 100, id="empty_page"),
        pytest.param(5, 2, 10, id="last_full_page"),
        pytest.param(5, 1, 9, id="last_short_page"),
        pytest.param(50, 2, 100, id="last_page_of_window"),
    ],
)
def test_next_counter_page__done(page: int, count: int, total: int) -> None:
    features = [optical_record(str(i)) for i in range(count)]

    assert next_counter_page({"startPage": page, "itemsPerPage": 2}, {"totalResults": total}, features) is None


def test_next_counter_page__new_window() -> None:
    request = {"startPage": 50, "itemsPerPage": 2, "lastUpdateDate": "[2018-10-03T12:00:00Z,2026-02-01T00:00:00Z]"}
    features = [optical_record("a", "2026-01-02T00:00:00Z"), optical_record("b", "2026-01-01T00:00:00Z")]

    assert next_counter_page(request, {"totalResults": 1000}, features) == {
        "startPage": 1,
        "itemsPerPage": 2,
        "lastUpdateDate": "[2018-10-03T12:00:00Z,2026-01-01T00:00:00Z]",
    }


def test_next_counter_page__window_does_not_advance() -> None:
    request = {"startPage": 50, "itemsPerPage": 2, "lastUpdateDate": "[2018-10-03T12:00:00Z,2026-01-01T00:00:00Z]"}
    features = [optical_record("a", "2026-01-01T00:00:00Z"), optical_record("b", "2026-01-01T00:00:00Z")]

    with pytest.raises(RuntimeError):
        next_counter_page(request, {"totalResults": 1000}, features)


@moto.mock_aws
@patch("airbus_harvester.__main__.get_pulsar_client")
def test_harvest__first_page_without_ids(mock_create_client: Any, requests_mock: Any) -> None:
    skipped = [optical_record(None, "2026-01-02T00:00:00Z", sourceIdentifier=f"DS_{i}") for i in range(3)]
    kept = optical_record("DS_PHR1A_1", "2026-01-01T00:00:00Z")
    pages = [
        {"totalResults": 6, "features": skipped},
        {"totalResults": 6, "features": [kept]},
        {"totalResults": 6, "features": []},
    ]

    result, messages, _metadata = run_phr_harvest(pages, {}, requests_mock, mock_create_client)

    assert result.exit_code == 0, result.output
    added = {key for message in messages for key in message["added_keys"]}
    assert f"git-harvester/{ITEMS}/DS_PHR1A_1.json" in added
