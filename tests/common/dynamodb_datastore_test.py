import base64
import binascii
import zlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import Thread
from unittest.mock import Mock, patch
from uuid import uuid4

import simplejson
from boto3.dynamodb.types import Binary
from freezegun import freeze_time
from parameterized import parameterized

from eps_spine_shared.common import indexes
from eps_spine_shared.common.dynamodb_client import EpsDataStoreError
from eps_spine_shared.common.dynamodb_common import (
    NEXT_ACTIVITY_DATE_PARTITIONS,
    Attribute,
    Key,
    ProjectedAttribute,
    SortKey,
    replace_decimals,
)
from eps_spine_shared.common.dynamodb_datastore import EpsDynamoDbDataStore
from eps_spine_shared.common.prescription.record import PrescriptionStatus
from eps_spine_shared.nhsfundamentals.time_utilities import TimeFormats
from eps_spine_shared.testing.mock_logger import MockLogObject
from tests.dynamodb_test import DynamoDbTest


class EpsDynamoDbDataStoreTest(DynamoDbTest):
    """
    Tests relating to DynamoDbDataStore.
    """

    def test_insert_record(self):
        """
        Test datastore can insert records.
        """
        prescription_id, nhs_number = self.get_new_record_keys()
        record = self.get_record(nhs_number)

        response = self.datastore.insert_eps_record_object(
            self.internal_id, prescription_id, record
        )

        self.assertEqual(response["ResponseMetadata"]["HTTPStatusCode"], 200)

    def test_include_record_type(self):
        """
        Test datastore can insert records including recordType and retrieve records with it included.
        """
        repeat_dispense = "RepeatDispense"
        prescription_id, nhs_number = self.get_new_record_keys()
        record = self.get_record(nhs_number)

        self.datastore.insert_eps_record_object(
            self.internal_id, prescription_id, record, None, repeat_dispense
        )
        returned_record = self.datastore.return_record_for_process(
            self.internal_id, prescription_id
        )

        self.assertEqual(returned_record["recordType"], repeat_dispense)

    def test_insert_duplicate(self):
        """
        Test datastore will not overwrite records.
        """
        prescription_id, nhs_number = self.get_new_record_keys()
        record = self.get_record(nhs_number)
        self.datastore.insert_eps_record_object(self.internal_id, prescription_id, record)

        record["instances"]["1"]["prescriptionStatus"] = PrescriptionStatus.AWAITING_RELEASE_READY

        with self.assertRaises(EpsDataStoreError) as cm:
            self.datastore.insert_eps_record_object(self.internal_id, prescription_id, record)
        self.assertEqual(cm.exception.error_topic, EpsDataStoreError.DUPLICATE_ERROR)

        returned_record = self.datastore.return_record_for_process(
            self.internal_id, prescription_id
        )
        returned_record_status = returned_record["value"]["instances"]["1"]["prescriptionStatus"]

        self.assertEqual(returned_record_status, PrescriptionStatus.TO_BE_DISPENSED)
        self.assertEqual(self.logger.log_occurrence_count("DDB0021"), 1)

    def test_insert_multiple(self):
        """
        Test client can insert multiple items.
        """
        items = []
        for _ in range(2):
            record_key, _ = self.get_new_record_keys()
            items.append({Key.PK.name: record_key, Key.SK.name: "DEF"})

        response = self.datastore.client.insert_items(self.internal_id, items)

        self.assertEqual(response["ResponseMetadata"]["HTTPStatusCode"], 200)

    def test_client_put(self):
        """
        Test put_item is used when one item.
        """
        mock_client = Mock()
        self.datastore.client.client = mock_client
        self.datastore.client.insert_items(self.internal_id, [{}], log_item_size=False)
        mock_client.put_item.assert_called_once()

    def test_client_transact(self):
        """
        Test transact_write_items is used when multiple items.
        """
        mock_client = Mock()
        self.datastore.client.client = mock_client
        self.datastore.client.insert_items(self.internal_id, [{}, {}], log_item_size=False)
        mock_client.transact_write_items.assert_called_once()

    def test_return_record_for_process(self):
        """
        Test querying against the prescriptionId index and
        returning a record with additional required attributes.
        """
        prescription_id, nhs_number = self.get_new_record_keys()
        self.assertFalse(self.datastore.is_record_present(self.internal_id, prescription_id))

        record = self.get_record(nhs_number)
        self.datastore.insert_eps_record_object(self.internal_id, prescription_id, record)

        returned_record = self.datastore.return_record_for_process(
            self.internal_id, prescription_id
        )

        expected_record = {"value": record, "vectorClock": "vc", "releaseVersion": "R2"}

        self.assertEqual(expected_record, returned_record)
        self.assertEqual(type(returned_record["value"]["prescription"]["daysSupply"]), int)

    def test_return_record_for_update(self):
        """
        Test querying against the prescriptionId index and
        returning a record with additional required attributes, including setting it on the dataStore.
        """
        prescription_id, nhs_number = self.get_new_record_keys()
        self.assertFalse(self.datastore.is_record_present(self.internal_id, prescription_id))

        record = self.get_record(nhs_number)
        self.datastore.insert_eps_record_object(self.internal_id, prescription_id, record)

        returned_record = self.datastore.return_record_for_update(self.internal_id, prescription_id)

        expected_record = {"value": record, "vectorClock": "vc", "releaseVersion": "R2"}

        self.assertEqual(expected_record, returned_record)
        self.assertEqual(record, self.datastore.dataObject)

    def test_change_eps_object(self):
        """
        Test update to existing record.
        """
        prescription_id, nhs_number = self.get_new_record_keys()
        self.assertFalse(self.datastore.is_record_present(self.internal_id, prescription_id))

        record = self.get_record(nhs_number)
        self.datastore.insert_eps_record_object(self.internal_id, prescription_id, record)

        record["SCN"] = 2
        self.datastore.insert_eps_record_object(
            self.internal_id, prescription_id, record, is_update=True
        )

        updated_record = self.datastore.return_record_for_process(self.internal_id, prescription_id)

        expected_record = {"value": record, "vectorClock": "vc", "releaseVersion": "R2"}

        self.assertEqual(expected_record, updated_record)

    def test_change_eps_object_same_scn(self):
        """
        Test failed update to existing record due to no increment to SCN.
        """
        prescription_id, nhs_number = self.get_new_record_keys()
        self.assertFalse(self.datastore.is_record_present(self.internal_id, prescription_id))

        record = self.get_record(nhs_number)
        self.datastore.insert_eps_record_object(self.internal_id, prescription_id, record)

        modified_record = self.get_record(nhs_number)
        modified_record["instances"]["1"][
            "prescriptionStatus"
        ] = PrescriptionStatus.AWAITING_RELEASE_READY

        with self.assertRaises(EpsDataStoreError) as cm:
            self.datastore.insert_eps_record_object(
                self.internal_id, prescription_id, modified_record, is_update=True
            )
        self.assertEqual(cm.exception.error_topic, EpsDataStoreError.CONDITIONAL_UPDATE_FAILURE)

        self.assertEqual(self.logger.log_occurrence_count("DDB0022"), 1)

        updated_record = self.datastore.return_record_for_process(self.internal_id, prescription_id)

        expected_record = {"value": record, "vectorClock": "vc", "releaseVersion": "R2"}

        self.assertEqual(expected_record, updated_record)

    def test_timer(self):
        """
        Test timer decorator writes desired log.
        """
        prescription_id, nhs_number = self.get_new_record_keys()
        record = self.get_record(nhs_number)

        self.datastore.insert_eps_record_object(self.internal_id, prescription_id, record)

        occurrences = self.logger.get_log_occurrences("DDB0002")
        self.assertEqual(len(occurrences), 1)
        self.assertEqual(occurrences[0]["func"], "insert_eps_record_object")
        self.assertEqual(occurrences[0]["cls"], "EpsDynamoDbDataStore")

    def test_insert_and_get_eps_work_list(self):
        """
        Test insertion and retrieval of EPS worklist, compressing/decompressing its XML.
        """
        message_id = str(uuid4())
        self.keys.append((message_id, SortKey.WORK_LIST.value))

        xml = "<data />"
        xml_bytes = xml.encode("utf-8")

        for response_details in [xml, xml_bytes]:
            work_list = {
                Key.SK.name: SortKey.WORK_LIST.value,
                "keyList": [],
                "responseDetails": {"XML": response_details},
            }
            self.datastore.insert_eps_work_list(self.internal_id, message_id, work_list)

            returned_work_list = self.datastore.get_work_list(self.internal_id, message_id)

            self.assertEqual(returned_work_list["responseDetails"]["XML"], xml_bytes)
            self.assertEqual(work_list["responseDetails"]["XML"], response_details)

    def test_fetch_next_sequence_number(self):
        """
        Test fetching and incrementing claims sequence number.
        """
        self.keys.append((self.datastore.CLAIM_SEQUENCE_NUMBER_KEY, SortKey.SEQUENCE_NUMBER.value))
        self.datastore.client.delete_item(
            self.datastore.CLAIM_SEQUENCE_NUMBER_KEY, SortKey.SEQUENCE_NUMBER.value
        )

        sequence_number = self.datastore.fetch_next_sequence_number(self.internal_id, 2)
        self.assertEqual(sequence_number, 1)

        sequence_number = self.datastore.fetch_next_sequence_number(self.internal_id, 2, True)
        self.assertEqual(sequence_number, 2)

        sequence_number = self.datastore.fetch_next_sequence_number(self.internal_id, 2)
        self.assertEqual(sequence_number, 2)

        sequence_number = self.datastore.fetch_next_sequence_number(self.internal_id, 2)
        self.assertEqual(sequence_number, 1)

    def test_fetch_next_sequence_number_nwssp(self):
        """
        Test fetching and incrementing claims sequence number.
        """
        self.keys.append(
            (self.datastore.NWSSP_CLAIM_SEQUENCE_NUMBER_KEY, SortKey.SEQUENCE_NUMBER.value)
        )
        self.datastore.client.delete_item(
            self.datastore.NWSSP_CLAIM_SEQUENCE_NUMBER_KEY, SortKey.SEQUENCE_NUMBER.value
        )

        sequence_number = self.datastore.fetch_next_sequence_number_nwssp(self.internal_id, 2)
        self.assertEqual(sequence_number, 1)

        sequence_number = self.datastore.fetch_next_sequence_number_nwssp(self.internal_id, 2, True)
        self.assertEqual(sequence_number, 2)

        sequence_number = self.datastore.fetch_next_sequence_number_nwssp(self.internal_id, 2)
        self.assertEqual(sequence_number, 2)

        sequence_number = self.datastore.fetch_next_sequence_number_nwssp(self.internal_id, 2)
        self.assertEqual(sequence_number, 1)

    @patch("random.randint")
    def test_store_batch_claim(self, patched_randint):
        """
        Test creating and storing a batch claim.
        """
        patched_randint.return_value = 7

        self.keys.append(("batchGuid", SortKey.CLAIM.value))
        batch_claim = {
            "Batch GUID": "batchGuid",
            "Claim ID List": ["claimId1", "claimId2"],
            "Handle Time": "handleTime",
            "Sequence Number": 1,
            "Nwssp Sequence Number": 2,
            "Batch XML": b"<xml />",
        }
        dt_now = datetime.now(timezone.utc)
        with freeze_time(dt_now):
            self.datastore.store_batch_claim(self.internal_id, batch_claim)

        returned_batch_claim = self.datastore.client.get_item(
            self.internal_id, "batchGuid", SortKey.CLAIM.value
        )
        replace_decimals(returned_batch_claim)
        returned_batch_claim["body"]["Batch XML"] = bytes(returned_batch_claim["body"]["Batch XML"])

        expected = {
            Key.PK.name: "batchGuid",
            Key.SK.name: SortKey.CLAIM.value,
            ProjectedAttribute.BODY.name: batch_claim,
            ProjectedAttribute.INDEXES.name: {
                self.datastore.INDEX_CLAIMID: ["claimId1", "claimId2"],
                self.datastore.INDEX_CLAIMHANDLETIME: ["handleTime"],
                self.datastore.INDEX_CLAIM_SEQNUMBER: [1],
                self.datastore.INDEX_SCN: [
                    f"{dt_now.strftime(TimeFormats.STANDARD_DATE_TIME_FORMAT)}|1"
                ],
                self.datastore.INDEX_CLAIM_SEQNUMBER_NWSSP: [2],
            },
            ProjectedAttribute.CLAIM_IDS.name: ["claimId1", "claimId2"],
            Attribute.SEQUENCE_NUMBER_NWSSP.name: 2,
            ProjectedAttribute.EXPIRE_AT.name: int(
                (dt_now + timedelta(days=self.datastore.DEFAULT_EXPIRY_DAYS)).timestamp()
            ),
            Attribute.RIAK_LM.name: float(str(dt_now.timestamp())),
            Attribute.LM_DAY.name: dt_now.strftime("%Y%m%d") + ".7",
            Attribute.BATCH_CLAIM_ID.name: "batchGuid",
        }
        self.assertEqual(returned_batch_claim, expected)

        fetched_batch_claim = self.datastore.fetch_batch_claim(self.internal_id, "batchGuid")
        batch_xml = fetched_batch_claim["Batch XML"]
        self.assertEqual(batch_xml, "<xml />")

    def test_delete_claim_notification(self):
        """
        Test deleting a claim notification from the table.
        """
        document_key = uuid4()
        notification_key = self.datastore.NOTIFICATION_PREFIX + str(document_key)
        content = self.get_document_content()
        self.datastore.insert_eps_document_object(
            self.internal_id, notification_key, {"content": content}
        )

        returned_body = self.datastore.return_document_for_process(
            self.internal_id, notification_key
        )
        self.assertEqual(returned_body, {"content": content})

        self.datastore.delete_claim_notification(self.internal_id, document_key)
        self.assertRaises(
            EpsDataStoreError,
            self.datastore.return_document_for_process,
            notification_key,
            self.internal_id,
        )

    def test_return_claim_notification(self):
        """
        Test returning a claim notification from the table.
        Claim notification has content under payload key instead of content, so won't be b64 decoded/encoded.
        """
        document_key = uuid4()
        notification_key = self.datastore.NOTIFICATION_PREFIX + str(document_key)
        content = self.get_document_content()
        index = {
            indexes.INDEX_STORE_TIME_DOC_REF_TITLE: ["ClaimNotification_20250911"],
            indexes.INDEX_DELETE_DATE: ["20250911"],
            indexes.INDEX_PRESCRIPTION_ID: str(uuid4()),
        }
        self.datastore.insert_eps_document_object(
            self.internal_id, notification_key, {"payload": content}, index
        )

        returned_body = self.datastore.return_document_for_process(
            self.internal_id, notification_key
        )
        self.assertEqual(returned_body, {"payload": content})

    def test_delete_document(self):
        """
        Test deleting a document from the table.
        """
        document_key = self.generate_document_key()
        content = self.get_document_content()
        self.datastore.insert_eps_document_object(
            self.internal_id, document_key, {"content": content}
        )

        self.assertTrue(self.datastore.delete_document(self.internal_id, document_key))

    def test_delete_record(self):
        """
        Test deleting a record from the table.
        """
        record_key = self.generate_record_key()
        nhs_number = self.generate_nhs_number()
        record = self.get_record(nhs_number)
        self.datastore.insert_eps_record_object(self.internal_id, record_key, record)

        self.datastore.delete_record(self.internal_id, record_key)

        self.assertFalse(
            self.datastore.client.get_item(
                self.internal_id, record_key, SortKey.RECORD.value, expect_exists=False
            )
        )

    def test_convert_index_keys_to_lower_case(self):
        """
        Test converting all keys in a dict to lower case. Returns unchanged if unexpected type.
        """
        index_dict = {
            "nhsNumber_bin": ["nhsNumberA", "nhsNumberB"],
            "nhsNumberPrescDispDate_bin": [
                "nhsNumberA|prescA|dispA|dateA",
                "nhsNumberB|prescB|dispB|dateB",
            ],
            "nextActivityNAD_bin": ["purge", "delete"],
        }

        expected = {
            "nhsnumber_bin": ["nhsNumberA", "nhsNumberB"],
            "nhsnumberprescdispdate_bin": [
                "nhsNumberA|prescA|dispA|dateA",
                "nhsNumberB|prescB|dispB|dateB",
            ],
            "nextactivitynad_bin": ["purge", "delete"],
        }

        converted_dict = self.datastore.convert_index_keys_to_lower_case(index_dict)

        self.assertEqual(converted_dict, expected)

        index_wrong_type = "NoTaDiCt"
        converted_wrong_type = self.datastore.convert_index_keys_to_lower_case(index_wrong_type)

        self.assertEqual(converted_wrong_type, index_wrong_type)

    @patch("random.randint")
    def test_add_last_modified_to_item(self, patched_randint):
        """
        Test adding last modified timestamp and date to items.
        """
        patched_randint.return_value = 7

        item = {"a": 1}

        date_time = datetime(
            year=2025, month=9, day=11, hour=10, minute=11, second=12, microsecond=123456
        )
        with freeze_time(date_time):
            self.datastore.client.add_last_modified_to_item(item)

        expected = {"a": 1, "_riak_lm": Decimal("1757585472.123456"), "_lm_day": "20250911.7"}
        self.assertEqual(item, expected)

    @parameterized.expand(
        [
            ["string that is not base64 encoded", ValueError, "Document content not b64 encoded"],
            ["xxx", binascii.Error, "Incorrect padding"],
        ]
    )
    def test_document_decode_error(self, content, expected_error_type, expected_log_value):
        """
        Test error handling when base64 decoding the document.
        """
        document = {"content": content}
        with self.assertRaises(expected_error_type):
            self.datastore.insert_eps_document_object(self.internal_id, None, document)

        log_value = self.datastore.log_object.logger.get_logged_value("DDB0031", "error")
        self.assertEqual(log_value, expected_log_value)

    def test_document_encode_error(self):
        """
        Test error handling when base64 encoding the document.
        """
        document_key = "testDocument"
        self.keys.append((document_key, SortKey.DOCUMENT.value))
        document = {
            Key.PK.name: document_key,
            Key.SK.name: SortKey.DOCUMENT.value,
            ProjectedAttribute.BODY.name: {"content": None},
        }
        self.datastore.client.put_item(self.internal_id, document, log_item_size=False)

        with self.assertRaises(TypeError):
            self.datastore.return_document_for_process(self.internal_id, document_key)

        was_logged = self.datastore.log_object.logger.was_logged("DDB0032")
        self.assertTrue(was_logged)

    def test_batch_claim_xml_decode_error(self):
        """
        Test error handling when decoding the batch claim xml.
        """
        batch_claim_key = "testBatchClaim"
        self.keys.append((batch_claim_key, SortKey.CLAIM.value))
        batch_claim = {
            Key.PK.name: batch_claim_key,
            Key.SK.name: SortKey.CLAIM.value,
            ProjectedAttribute.BODY.name: {"Batch XML": None},
        }
        self.datastore.client.put_item(self.internal_id, batch_claim, log_item_size=False)

        with self.assertRaises(TypeError):
            self.datastore.fetch_batch_claim(self.internal_id, batch_claim_key)

        was_logged = self.datastore.log_object.logger.was_logged("DDB0033")
        self.assertTrue(was_logged)

    def test_record_expire_at_datetime_format(self):
        """
        Test that the expireAt attribute added to a record defaults to 18 months from its creation.
        Provided prescriptionTime is in %Y%m%d%H%M%S format.
        """
        prescription_id, nhs_number = self.get_new_record_keys()

        date_time = datetime(
            year=2025,
            month=9,
            day=11,
            hour=10,
            minute=11,
            second=12,
            microsecond=123456,
            tzinfo=timezone.utc,
        )
        date_time_string = datetime.strftime(date_time, TimeFormats.STANDARD_DATE_TIME_FORMAT)
        record = self.get_record(nhs_number, date_time_string)

        expected_timestamp = int(
            datetime(
                year=2027, month=3, day=11, hour=10, minute=11, second=12, tzinfo=timezone.utc
            ).timestamp()
        )

        built_record = self.datastore.build_record(prescription_id, record, None, None)

        expire_at = built_record["expireAt"]
        self.assertEqual(expire_at, expected_timestamp)

    def test_record_expire_at_date_format(self):
        """
        Test that the expireAt attribute added to a record defaults to 18 months from its creation.
        Provided prescriptionTime is in %Y%m%d format.
        """
        prescription_id, nhs_number = self.get_new_record_keys()

        date_time = datetime(
            year=2025,
            month=9,
            day=11,
            hour=10,
            minute=11,
            second=12,
            microsecond=123456,
            tzinfo=timezone.utc,
        )
        date_string = datetime.strftime(date_time, TimeFormats.STANDARD_DATE_FORMAT)
        record = self.get_record(nhs_number, date_string)

        expected_timestamp = int(
            datetime(year=2027, month=3, day=11, tzinfo=timezone.utc).timestamp()
        )

        built_record = self.datastore.build_record(prescription_id, record, None, None)

        expire_at = built_record["expireAt"]
        self.assertEqual(expire_at, expected_timestamp)

    def test_record_expire_at_next_activity_delete(self):
        """
        Test that the expireAt will be calculated based on the nextActivityDate
        when a record has a next activity of delete.
        """
        prescription_id, nhs_number = self.get_new_record_keys()

        record = self.get_record(nhs_number, "20260101101112")
        record["indexes"]["nextActivityNAD_bin"] = ["delete_20260101"]

        expected_timestamp = int(
            datetime(year=2027, month=1, day=1, tzinfo=timezone.utc).timestamp()
        )

        built_record = self.datastore.build_record(prescription_id, record, None, None)

        expire_at = built_record["expireAt"]
        self.assertEqual(expire_at, expected_timestamp)

    def test_record_expire_at_next_activity_purge(self):
        """
        Test that the expireAt will be calculated based on the nextActivityDate
        when a record has a next activity of purge.
        """
        prescription_id, nhs_number = self.get_new_record_keys()

        record = self.get_record(nhs_number, "20260101101112")
        record["indexes"]["nextActivityNAD_bin"] = ["purge_20260101"]

        expected_timestamp = int(
            datetime(year=2026, month=1, day=1, tzinfo=timezone.utc).timestamp()
        )

        built_record = self.datastore.build_record(prescription_id, record, None, None)

        expire_at = built_record["expireAt"]
        self.assertEqual(expire_at, expected_timestamp)

    def test_record_expire_at_no_date_element(self):
        """
        Test that the expireAt will be set to default when a record has a next activity of purge
        but no date element in nextActivityNAD_bin.
        """
        prescription_id, nhs_number = self.get_new_record_keys()

        date_time = datetime(
            year=2025,
            month=9,
            day=11,
            hour=10,
            minute=11,
            second=12,
            microsecond=123456,
            tzinfo=timezone.utc,
        )
        date_string = datetime.strftime(date_time, TimeFormats.STANDARD_DATE_FORMAT)
        record = self.get_record(nhs_number, date_string)
        record["indexes"]["nextActivityNAD_bin"] = ["purge"]

        expected_timestamp = int(
            datetime(year=2027, month=3, day=11, tzinfo=timezone.utc).timestamp()
        )

        built_record = self.datastore.build_record(prescription_id, record, None, None)

        expire_at = built_record["expireAt"]
        self.assertEqual(expire_at, expected_timestamp)

    def test_record_expire_at_max_date(self):
        """
        Test that the expireAt will be set to default when a record has a next activity of purge
        but the date element in nextActivityNAD_bin is the max date of 99991231.
        """
        prescription_id, nhs_number = self.get_new_record_keys()

        date_time = datetime(
            year=2025,
            month=9,
            day=11,
            hour=10,
            minute=11,
            second=12,
            microsecond=123456,
            tzinfo=timezone.utc,
        )
        date_string = datetime.strftime(date_time, TimeFormats.STANDARD_DATE_FORMAT)
        record = self.get_record(nhs_number, date_string)
        record["indexes"]["nextActivityNAD_bin"] = ["purge_99991231"]

        expected_timestamp = int(
            datetime(year=2027, month=3, day=11, tzinfo=timezone.utc).timestamp()
        )

        built_record = self.datastore.build_record(prescription_id, record, None, None)

        expire_at = built_record["expireAt"]
        self.assertEqual(expire_at, expected_timestamp)

    @parameterized.expand([("delete_20260101", "delete", "20260101"), ("delete", "delete", None)])
    def test_parse_next_activity_nad(self, next_activity_nad, expected_activity, expected_date):
        """
        Test parsing nextActivityNAD_bin to obtain nextActivity and nextActivityDate.
        """
        indexes = {"nextActivityNAD_bin": [next_activity_nad]}
        next_activity, _, next_activity_date = self.datastore.parse_next_activity_nad(indexes)

        self.assertEqual(next_activity, expected_activity)
        self.assertEqual(next_activity_date, expected_date)

    def test_document_expire_at(self):
        """
        Test that the expireAt attribute added to a document
        defaults to 18 months from when it is written to the database.
        """
        content = self.get_document_content()
        document = {"content": content}

        date_time = datetime(
            year=2025,
            month=9,
            day=11,
            hour=10,
            minute=11,
            second=12,
            microsecond=123456,
            tzinfo=timezone.utc,
        )

        expected_timestamp = int(
            datetime(
                year=2027, month=3, day=11, hour=10, minute=11, second=12, tzinfo=timezone.utc
            ).timestamp()
        )

        with freeze_time(date_time):
            built_document = self.datastore.build_document(self.internal_id, document, None)

        expire_at = built_document["expireAt"]
        self.assertEqual(expire_at, expected_timestamp)

    def test_document_expire_at_from_index(self):
        """
        Test that the expireAt attribute added to a document matches that provided in the index.
        """
        content = self.get_document_content()
        document = {"content": content}
        index = {
            indexes.INDEX_STORE_TIME_DOC_REF_TITLE: [
                f"{self.datastore.STORE_TIME_DOC_REF_TITLE_PREFIX}_20250911"
            ],
            indexes.INDEX_DELETE_DATE: ["20250911"],
            indexes.INDEX_PRESCRIPTION_ID: str(uuid4()),
        }

        expected_timestamp = int(
            datetime(year=2025, month=9, day=11, tzinfo=timezone.utc).timestamp()
        )

        built_document = self.datastore.build_document(self.internal_id, document, index)

        expire_at = built_document["expireAt"]
        self.assertEqual(expire_at, expected_timestamp)

    def test_concurrent_inserts(self):
        """
        Test that concurrent inserts to a record will raise a EpsDataStoreError and log correctly
        """
        prescription_id, nhs_number = self.get_new_record_keys()
        record = self.get_record(nhs_number)

        exceptions_thrown = []

        def insert_record(datastore: EpsDynamoDbDataStore, insert_args):
            try:
                datastore.insert_eps_record_object(*insert_args)
            except Exception as e:
                exceptions_thrown.append(e)

        # Create several processes that try to insert the record concurrently
        processes = []
        loggers = []
        for _ in range(5):
            logger = MockLogObject()
            loggers.append(logger)

            datastore = EpsDynamoDbDataStore(logger, self.system_config)

            process = Thread(
                target=insert_record, args=(datastore, (self.internal_id, prescription_id, record))
            )
            processes.append(process)

        # Start processes
        for process in processes:
            process.start()

        # Wait for processes to finish
        for process in processes:
            process.join()

        logs = set()
        [logs.add(log) for logger in loggers for log in logger.called_references]
        self.assertTrue("DDB0021" in logs, "Expected a log DDB0021 for concurrent insert failure")

        self.assertTrue(
            len(exceptions_thrown) > 0, "Expected exception to be thrown for concurrent insertions"
        )
        self.assertTrue(
            isinstance(exceptions_thrown[0], EpsDataStoreError),
            "Expected EpsDataStoreError for concurrent insertions",
        )
        self.assertEqual(
            exceptions_thrown[0].error_topic,
            EpsDataStoreError.DUPLICATE_ERROR,
            "Expected EpsDataStoreError.DUPLICATE_ERROR for concurrent insertions",
        )

    def test_concurrent_updates(self):
        """
        Test that concurrent updates to a record will raise a EpsDataStoreError and log correctly
        """
        # Insert the initial record
        prescription_id, nhs_number = self.get_new_record_keys()
        record = self.get_record(nhs_number)

        response = self.datastore.insert_eps_record_object(
            self.internal_id, prescription_id, record
        )

        self.assertEqual(response["ResponseMetadata"]["HTTPStatusCode"], 200)

        # Make a change to the record
        record["prescription"]["daysSupply"] = 30
        record["SCN"] = 5

        exceptions_thrown = []

        def change_record(datastore, change_args):
            try:
                datastore.insert_eps_record_object(*change_args)
            except Exception as e:
                exceptions_thrown.append(e)

        # Create several processes that try to update the record concurrently
        processes = []
        loggers = []
        for _ in range(5):
            logger = MockLogObject()
            loggers.append(logger)

            datastore = EpsDynamoDbDataStore(logger, self.system_config)

            index = None
            record_type = None
            is_update = True

            process = Thread(
                target=change_record,
                args=(
                    datastore,
                    (self.internal_id, prescription_id, record, index, record_type, is_update),
                ),
            )
            processes.append(process)

        # Start processes
        for process in processes:
            process.start()

        # Wait for processes to finish
        for process in processes:
            process.join()

        logs = set()
        [logs.add(log) for logger in loggers for log in logger.called_references]
        self.assertTrue("DDB0022" in logs, "Expected a log DDB0022 for concurrent update failure")

        self.assertTrue(
            len(exceptions_thrown) > 0, "Expected exception to be thrown for concurrent updates"
        )
        self.assertTrue(
            isinstance(exceptions_thrown[0], EpsDataStoreError),
            "Expected EpsDataStoreError for concurrent updates",
        )
        self.assertEqual(
            exceptions_thrown[0].error_topic,
            EpsDataStoreError.CONDITIONAL_UPDATE_FAILURE,
            "Expected EpsDataStoreError.CONDITIONAL_UPDATE_FAILURE for concurrent updates",
        )

    def test_add_claim_notification_store_date(self):
        """
        Test that the claimNotificationStoreDate attribute is added only when docRefTitle is ClaimNotification.
        """
        content = self.get_document_content()
        document = {"content": content}

        for doc_ref_title in ["ClaimNotification", "Other"]:
            index = {
                indexes.INDEX_STORE_TIME_DOC_REF_TITLE: [f"{doc_ref_title}_20250911"],
                indexes.INDEX_DELETE_DATE: ["20250911"],
                indexes.INDEX_PRESCRIPTION_ID: str(uuid4()),
            }

            built_document = self.datastore.build_document(self.internal_id, document, index)

            if doc_ref_title == "ClaimNotification":
                claim_notification_store_date = built_document["claimNotificationStoreDate"]
                self.assertEqual("20250911", claim_notification_store_date)
            else:
                self.assertTrue("claimNotificationStoreDate" not in built_document)

    def test_record_next_activity_sharding(self):
        """
        Test that building a record correctly shards the nextActivity attribute
        """
        prescription_id, nhs_number = self.get_new_record_keys()

        record = self.get_record(nhs_number)

        item = self.datastore.build_record(prescription_id, record, None, None)

        next_activity = item[Attribute.NEXT_ACTIVITY.name]
        activity, shard = next_activity.split(".")
        shard = int(shard)

        self.assertEqual(activity, "createNoClaim")
        self.assertTrue(shard >= 1 and shard <= NEXT_ACTIVITY_DATE_PARTITIONS)

    @parameterized.expand(
        [
            [
                ["C51BB3D6-6948-11F0-9F54-EDAF56A204B4N", "C51BB3D6-6948-11F0-9F54-EDAF56A204B4"],
                "R1.7",
            ],
            [["5HLBWE-U5QENL-24XBU", "5HLBWE-U5QENL-24XBUX"], "R2.7"],
            [["5HLBWE-U5QENL-24XB"], "UNKNOWN"],
        ]
    )
    def test_build_record_adds_release_version(self, prescription_ids, expected):
        """
        Test that the build_record method adds an R1/R2 releaseVersion attribute to a record.
        Defaults to UNKNOWN when id is too short.
        """
        nhs_number = self.generate_nhs_number()
        record = self.get_record(nhs_number)

        for prescription_id in prescription_ids:
            with patch("random.randint") as patched_randint:
                patched_randint.return_value = 7
                item = self.datastore.build_record(prescription_id, record, None, None)
                self.assertEqual(item["releaseVersion"], expected)

    @parameterized.expand(
        [
            [
                ["C51BB3D6-6948-11F0-9F54-EDAF56A204B4N", "C51BB3D6-6948-11F0-9F54-EDAF56A204B4"],
                "R1",
            ],
            [["5HLBWE-U5QENL-24XBU", "5HLBWE-U5QENL-24XBUX"], "R2"],
            [["5HLBWE-U5QENL-24XB"], "UNKNOWN"],
        ]
    )
    def test_build_record_to_return_adds_release_version(self, prescription_ids, expected):
        """
        Test that the _build_record_to_return method adds an R1/R2 releaseVersion attribute to a record
        if it is missing. Defaults to UNKNOWN when id is too short.
        """
        for prescription_id in prescription_ids:
            item = {"pk": prescription_id}
            record = self.datastore._build_record_to_return(item, {})
            self.assertEqual(record["releaseVersion"], expected)

    def test_is_record_present(self):
        """
        Ensure that the is_record_present returns the correct boolean depending on presence of a record.
        """
        prescription_id, nhs_number = self.get_new_record_keys()
        self.assertFalse(self.datastore.is_record_present(self.internal_id, prescription_id))

        record = self.get_record(nhs_number)
        self.datastore.insert_eps_record_object(self.internal_id, prescription_id, record)

        self.assertTrue(self.datastore.is_record_present(self.internal_id, prescription_id))

    def test_claim_notification_binary_encoding(self):
        """
        Ensure that fetching documents handles stringified and binary payloads
        """
        document_key = self.generate_document_key()
        content = self.get_document_content()
        index = {
            indexes.INDEX_STORE_TIME_DOC_REF_TITLE: ["ClaimNotification_20250911"],
            indexes.INDEX_DELETE_DATE: ["20250911"],
        }
        self.datastore.insert_eps_document_object(
            self.internal_id, document_key, {"payload": content}, index
        )

        # Document should be stored as a string in DynamoDB
        self.assertTrue(
            isinstance(
                self.datastore.client.get_item(
                    self.internal_id, document_key, SortKey.DOCUMENT.value
                )["body"]["payload"],
                str,
            )
        )

        string_response = self.datastore.return_document_for_process(self.internal_id, document_key)

        binary_content = base64.b64encode(
            zlib.compress(simplejson.dumps({"a": 1, "b": True}).encode("utf-8"))
        )
        document_key2 = self.generate_document_key()
        self.datastore.insert_eps_document_object(
            self.internal_id, document_key2, {"payload": binary_content}, index
        )

        # Document should be stored as a binary in DynamoDB
        self.assertTrue(
            isinstance(
                self.datastore.client.get_item(
                    self.internal_id, document_key2, SortKey.DOCUMENT.value
                )["body"]["payload"],
                Binary,
            )
        )

        binary_response = self.datastore.return_document_for_process(
            self.internal_id, document_key2
        )

        self.assertEqual(string_response, binary_response)
