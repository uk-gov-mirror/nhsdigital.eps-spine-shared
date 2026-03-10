from unittest.mock import Mock

from eps_spine_shared.common import indexes
from eps_spine_shared.common.dynamodb_client import EpsDataStoreError
from eps_spine_shared.common.dynamodb_common import SortKey
from eps_spine_shared.common.prescription.statuses import PrescriptionStatus
from eps_spine_shared.errors import EpsSystemError
from eps_spine_shared.interactions.updates import apply_blind_update, apply_smart_update
from tests.dynamodb_test import DynamoDbTest


class BlindUpdateTest(DynamoDbTest):
    """
    Tests of the blind update function
    """

    def setUp(self):
        super().setUp()
        self.internal_id = "test-internal-id"

    def test_blind_insert_document(self):
        """
        Test a happy path insert of a document
        """
        document_key = self.generate_document_key()
        content = self.get_document_content()
        document = {"content": content}

        object_to_store = {
            "key": document_key,
            "value": document,
        }

        apply_blind_update(
            object_to_store,
            "epsDocument",
            self.internal_id,
            self.logger,
            self.datastore,
        )

        self.assertTrue(
            "EPS0127" in self.logger.called_references, "Expected EPS0127 log entry not found"
        )

    def test_blind_insert_record(self):
        """
        Test a happy path insert of a record
        """
        prescription_id, nhs_number = self.get_new_record_keys()
        record = self.get_record(nhs_number)

        object_to_store = {"key": prescription_id, "value": record}

        apply_blind_update(
            object_to_store,
            "epsRecord",
            self.internal_id,
            self.logger,
            self.datastore,
        )

        self.assertTrue(
            "EPS0127" in self.logger.called_references, "Expected EPS0127 log entry not found"
        )

    def test_insert_failure(self):
        """
        Test a failure to insert a record
        """

        def throw_data_store_error(*_):
            raise EpsDataStoreError(
                self.datastore.client, None, EpsDataStoreError.CONDITIONAL_UPDATE_FAILURE
            )

        self.datastore.insert_eps_record_object = Mock(side_effect=throw_data_store_error)

        prescription_id, nhs_number = self.get_new_record_keys()
        record = self.get_record(nhs_number)

        object_to_store = {"key": prescription_id, "value": record}

        with self.assertRaises(EpsSystemError) as cm:
            apply_blind_update(
                object_to_store, "epsRecord", self.internal_id, self.logger, self.datastore
            )

        self.assertEqual(
            cm.exception.error_topic,
            EpsSystemError.IMMEDIATE_REQUEUE,
            "Expected EpsSystemError with IMMEDIATE_REQUEUE topic not raised",
        )

        self.assertTrue(
            "EPS0126" in self.logger.called_references, "Expected EPS0126 log entry not found"
        )


class SmartUpdateTest(DynamoDbTest):
    """
    Test of the smart update function
    """

    def setUp(self):
        """
        Assigns aliases for update applier bindings
        """
        super().setUp()
        self.internal_id = "test-internal-id"

    def test_update(self):
        """
        Test a happy path update of a record
        """
        prescription_id, nhs_number = self.get_new_record_keys()
        record = self.get_record(nhs_number)

        self.datastore.insert_eps_record_object(self.internal_id, prescription_id, record)

        record["SCN"] += 1
        object_to_store = {"key": prescription_id, "value": record, "vectorClock": None}

        apply_smart_update(object_to_store, 0, self.internal_id, self.logger, self.datastore)

        self.assertTrue(
            "EPS0127a" in self.logger.called_references, "Expected EPS0127a log entry not found"
        )

    def test_update_pending_cancellation_with_scn(self):
        """
        Test an update of a record that is pending cancellation
        and has a set SCN
        """
        prescription_id, nhs_number = self.get_new_record_keys()
        record = self.get_record(nhs_number)

        record["indexes"][indexes.INDEX_PRESCRIBER_STATUS] = [
            f"_{PrescriptionStatus.PENDING_CANCELLATION}"
        ]
        record["SCN"] = 5
        self.datastore.insert_eps_record_object(self.internal_id, prescription_id, record)

        object_to_store = {"key": prescription_id, "value": record, "vectorClock": None}

        apply_smart_update(object_to_store, 0, self.internal_id, self.logger, self.datastore)

        self.assertTrue(
            "EPS0127a" in self.logger.called_references, "Expected EPS0127a log entry not found"
        )
        self.assertEqual(self.logger.logged_messages[8][1]["scn"], 6)

    def test_update_pending_cancellation_without_scn(self):
        """
        Test an update of a record that is pending cancellation
        and does not have a set SCN
        """
        prescription_id, nhs_number = self.get_new_record_keys()
        record = self.get_record(nhs_number)

        record["indexes"][indexes.INDEX_PRESCRIBER_STATUS] = [
            f"_{PrescriptionStatus.PENDING_CANCELLATION}"
        ]
        record["SCN"] = 0
        self.datastore.insert_eps_record_object(self.internal_id, prescription_id, record)

        object_to_store = {"key": prescription_id, "value": record, "vectorClock": None}

        apply_smart_update(object_to_store, 0, self.internal_id, self.logger, self.datastore)

        self.assertTrue(
            "EPS0127a" in self.logger.called_references, "Expected EPS0127a log entry not found"
        )
        self.assertEqual(self.logger.logged_messages[8][1]["scn"], 2)

    def test_conditional_update_failure(self):
        """
        Test a conditional update failure will raise an immediate requeue and delete
        inserted documents correctly
        """
        self._balanceIncrementInFailureCount = Mock()

        # Insert documents to test deletion on failure
        document_key = self.generate_document_key()
        content = self.get_document_content()
        document = {"content": content}

        docs_to_store = [
            {"key": key}
            for key in [document_key, self.datastore.NOTIFICATION_PREFIX + document_key]
        ]
        self.keys.append((docs_to_store[1]["key"], SortKey.DOCUMENT.value))
        self.datastore.insert_eps_document_object(
            self.internal_id, docs_to_store[0]["key"], document
        )
        self.datastore.insert_eps_document_object(
            self.internal_id, docs_to_store[1]["key"], document
        )

        # Insert record
        prescription_id, nhs_number = self.get_new_record_keys()
        record = self.get_record(nhs_number)

        self.datastore.insert_eps_record_object(self.internal_id, prescription_id, record)

        # Update record
        record["SCN"] += 1
        object_to_store = {"key": prescription_id, "value": record, "vectorClock": None}

        def throw_data_store_error(*_, is_update=None):
            raise EpsDataStoreError(
                self.datastore.client, None, EpsDataStoreError.CONDITIONAL_UPDATE_FAILURE
            )

        self.datastore.insert_eps_record_object = Mock(side_effect=throw_data_store_error)
        self.datastore.delete_document = Mock()

        with self.assertRaises(EpsSystemError) as cm:
            apply_smart_update(
                object_to_store, 0, self.internal_id, self.logger, self.datastore, docs_to_store
            )

        self.assertEqual(
            cm.exception.error_topic,
            EpsSystemError.IMMEDIATE_REQUEUE,
            "Expected EpsSystemError with IMMEDIATE_REQUEUE topic not raised",
        )

        # Check that smart update failure is logged
        self.assertTrue(
            "EPS0126a" in self.logger.called_references, "Expected EPS0126a log entry not found"
        )

        # Check that document deletion is checked
        log_keys_b = [
            keys["key"] for (ref, keys) in self.logger.logged_messages if ref == "EPS0126b"
        ]
        for doc in docs_to_store:
            self.assertTrue(
                doc["key"] in log_keys_b,
                f"Expected EPS0126b log entry for {doc['key']} not found",
            )

        # Check that non-notifications are not deleted
        log_keys_d = [
            keys["key"] for (ref, keys) in self.logger.logged_messages if ref == "EPS0126d"
        ]
        self.assertTrue(
            docs_to_store[0]["key"] in log_keys_d,
            f"Expected EPS0126d log entry for {docs_to_store[0]['key']} not found",
        )

        # Check that notifications are deleted
        log_keys_c = [
            keys["key"] for (ref, keys) in self.logger.logged_messages if ref == "EPS0126c"
        ]
        self.assertTrue(
            docs_to_store[1]["key"] in log_keys_c,
            f"Expected EPS0126c log entry for {docs_to_store[1]['key']} not found",
        )

        self.datastore.delete_document.assert_called_once_with(
            self.internal_id, documentKey=docs_to_store[1]["key"], deleteNotification=True
        )
