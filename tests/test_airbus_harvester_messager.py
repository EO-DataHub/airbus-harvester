import json
from unittest import mock

from eodhp_utils.messagers import Messager
from pytest_mock import MockerFixture

from airbus_harvester.airbus_harvester_messager import AirbusHarvesterMessager


def test_process_msg_updated(mocker: MockerFixture):

    mock_s3_client = mock.MagicMock()
    mock_producer = mock.MagicMock()

    test_airbus_harvester = AirbusHarvesterMessager(
        s3_client=mock_s3_client,
        output_bucket="files_bucket_name",
        cat_output_prefix="git-harvester/",
        producer=mock_producer,
    )

    test_msg = {
        "harvested_data": {
            "key/to/data1": {"id": "data1-id"},
            "key/to/data2": {"id": "data2-id"},
        },
        "deleted_keys": [],
        "latest_harvested": {
            "data": {"data1-id": "hash1", "data2-id": "hash2"},
            "metadata_s3_key": "metadata/s3/key",
        },
    }

    expected_action_1 = Messager.OutputFileAction(
        file_body=json.dumps({"id": "data1-id"}),
        cat_path="key/to/data1",
    )
    expected_action_2 = Messager.OutputFileAction(
        file_body=json.dumps({"id": "data2-id"}),
        cat_path="key/to/data2",
    )

    result = test_airbus_harvester.process_msg(test_msg)

    assert result == [expected_action_1, expected_action_2]


def test_process_msg_deleted(mocker: MockerFixture):
    mock_s3_client = mock.MagicMock()
    mock_producer = mock.MagicMock()

    test_airbus_harvester = AirbusHarvesterMessager(
        s3_client=mock_s3_client,
        output_bucket="files_bucket_name",
        cat_output_prefix="stac-harvester/",
        producer=mock_producer,
    )

    test_msg = {
        "harvested_data": {},
        "deleted_keys": ["key/to/data1", "key/to/data2"],
        "latest_harvested": {
            "data": {},
            "metadata_s3_key": "metadata/s3/key",
        },
    }

    expected_action_1 = Messager.OutputFileAction(
        file_body=None,
        cat_path="key/to/data1",
    )
    expected_action_2 = Messager.OutputFileAction(
        file_body=None,
        cat_path="key/to/data2",
    )

    result = test_airbus_harvester.process_msg(test_msg)

    assert result == [expected_action_1, expected_action_2]


def test_process_msg_update_and_delete(mocker: MockerFixture):
    mock_s3_client = mock.MagicMock()
    mock_producer = mock.MagicMock()

    test_airbus_harvester = AirbusHarvesterMessager(
        s3_client=mock_s3_client,
        output_bucket="files_bucket_name",
        cat_output_prefix="stac-harvester/",
        producer=mock_producer,
    )

    test_msg = {
        "harvested_data": {
            "key/to/data1": {"id": "data1-id"},
            "key/to/data2": {"id": "data2-id"},
        },
        "deleted_keys": ["key/to/data3", "key/to/data4"],
    }
    expected_action_1 = Messager.OutputFileAction(
        file_body=json.dumps({"id": "data1-id"}),
        cat_path="key/to/data1",
    )
    expected_action_2 = Messager.OutputFileAction(
        file_body=json.dumps({"id": "data2-id"}),
        cat_path="key/to/data2",
    )
    expected_action_3 = Messager.OutputFileAction(
        file_body=None,
        cat_path="key/to/data3",
    )
    expected_action_4 = Messager.OutputFileAction(
        file_body=None,
        cat_path="key/to/data4",
    )

    result = test_airbus_harvester.process_msg(test_msg)

    assert result == [
        expected_action_1,
        expected_action_2,
        expected_action_3,
        expected_action_4,
    ]


def test_gen_empty_catalogue_message():

    mock_s3_client = mock.MagicMock()
    mock_producer = mock.MagicMock()

    test_airbus_harvester = AirbusHarvesterMessager(
        s3_client=mock_s3_client,
        output_bucket="files_bucket_name",
        cat_output_prefix="stac-harvester/",
        producer=mock_producer,
    )

    test_msg = {
        "harvested_data": {
            "key/to/data1": {"id": "data1-id"},
            "key/to/data2": {"id": "data2-id"},
        },
        "deleted_keys": ["key/to/data3", "key/to/data4"],
        "latest_harvested": {
            "data": {"data1-id": "hash1", "data2-id": "hash2"},
            "metadata_s3_key": "metadata/s3/key",
        },
    }

    result = test_airbus_harvester.gen_empty_catalogue_message(test_msg)

    assert result == {
        "id": "harvester/airbus",
        "workspace": "default_workspace",
        "repository": "",
        "branch": "",
        "bucket_name": "files_bucket_name",
        "source": "",
        "target": "",
    }
