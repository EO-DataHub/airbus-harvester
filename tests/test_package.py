import json

import pytest
import requests

from airbus_harvester.__main__ import (
    coordinates_to_bbox,
    generate_stac_collection,
    generate_stac_item,
    get_catalogue,
    get_stac_collection_summary,
    handle_quicklook_url,
    make_catalogue,
    modify_value,
)


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


def test_get_catalogue__success(requests_mock, mock_response):
    requests_mock.post(
        "https://dev.sar.api.oneatlas.airbus.com/v1/sar/catalogue", text=json.dumps(mock_response)
    )
    catalogue_data = get_catalogue()

    assert catalogue_data == mock_response


def test_get_catalogue__fail(requests_mock, mock_response):
    requests_mock.post(
        "https://dev.sar.api.oneatlas.airbus.com/v1/sar/catalogue",
        status_code=404,
        text="Not Found",
    )

    with pytest.raises(requests.exceptions.HTTPError) as error:
        get_catalogue()

    assert "404" in error.value.args[0]


def test__coordinates_to_bbox__success(mock_response):
    coordinates = mock_response["features"][0]["geometry"]["coordinates"]
    expected_bbox = [-28.1373002, 38.5878242, -27.9837603, 38.7135605]

    actual_bbox = coordinates_to_bbox(coordinates[0])

    assert expected_bbox == actual_bbox


def test_get_stac_collection_summary(mock_response):
    expected_summary = {
        "bbox": [-27.9837603, 38.6070725, -27.9837603, 38.6070725],
        "start_time": mock_response["features"][0]["properties"]["startTime"],
        "stop_time": mock_response["features"][0]["properties"]["stopTime"],
    }

    actual_summary = get_stac_collection_summary(mock_response["features"])

    assert actual_summary == expected_summary


def test_generate_stac_collection(mock_response):
    actual_collection = generate_stac_collection(mock_response["features"])
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
        pytest.param("lookDirection", "r", "r", id="lookDirection_r"),
        pytest.param("lookDirection", "l", "l", id="lookDirection_l"),
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
