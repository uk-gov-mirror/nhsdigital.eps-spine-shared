import json
import os.path
from datetime import datetime, timedelta
from unittest.case import TestCase
from unittest.mock import MagicMock

from parameterized.parameterized import parameterized

from eps_spine_shared.common import indexes
from eps_spine_shared.common.prescription import fields
from eps_spine_shared.common.prescription.record import PrescriptionRecord
from eps_spine_shared.common.prescription.repeat_dispense import RepeatDispenseRecord
from eps_spine_shared.common.prescription.repeat_prescribe import RepeatPrescribeRecord
from eps_spine_shared.common.prescription.single_prescribe import SinglePrescribeRecord
from eps_spine_shared.common.prescription.types import PrescriptionTreatmentType
from eps_spine_shared.errors import EpsSystemError
from eps_spine_shared.nhsfundamentals.time_utilities import TimeFormats
from eps_spine_shared.testing.mock_logger import MockLogObject


def load_test_example_json(mock_log_object, filename):
    """
    Load prescription data from JSON files in the test resources directory.

    :type filename: str
    :rtype: PrescriptionRecord
    """
    # load the JSON dict
    test_dir_path = os.path.dirname(__file__)
    full_path = os.path.join(test_dir_path, "resources", filename)
    with open(full_path) as json_file:
        prescription_dict = json.load(json_file)
        json_file.close()

    # wrap it in a PrescriptionRecord - need to create the
    # appropriate subclass based on treatment type
    treatment_type = prescription_dict["prescription"]["prescriptionTreatmentType"]
    if treatment_type == PrescriptionTreatmentType.ACUTE_PRESCRIBING:
        prescription = SinglePrescribeRecord(mock_log_object, "test")
    elif treatment_type == PrescriptionTreatmentType.REPEAT_PRESCRIBING:
        prescription = RepeatPrescribeRecord(mock_log_object, "test")
    elif treatment_type == PrescriptionTreatmentType.REPEAT_DISPENSING:
        prescription = RepeatDispenseRecord(mock_log_object, "test")
    else:
        raise ValueError("Unknown treatment type %s" % str(treatment_type))

    prescription.create_record_from_store(prescription_dict)

    return prescription


class PrescriptionRecordTest(TestCase):
    """
    Test Case for PrescriptionRecord class
    """

    def setUp(self):
        self.mock_log_object = MagicMock()

    def test_basic_properties(self):
        """
        Test basic property access of a record loaded from JSON
        """
        prescription = load_test_example_json(self.mock_log_object, "7D9625-Z72BF2-11E3A.json")

        self.assertEqual(prescription.id, "7D9625-Z72BF2-11E3AC")
        self.assertEqual(prescription.max_repeats, 3)

    def test_current_issue(self):
        """
        Test that we can access the current issue
        """
        prescription = load_test_example_json(self.mock_log_object, "7D9625-Z72BF2-11E3A.json")

        self.assertEqual(prescription.current_issue_number, 3)
        self.assertEqual(prescription.current_issue.number, 3)
        self.assertEqual(prescription.current_issue.status, "0006")

        # try changing the current issue number and make sure that this is picked up
        prescription.current_issue_number = 1
        self.assertEqual(prescription.current_issue_number, 1)
        self.assertEqual(prescription.current_issue.number, 1)
        self.assertEqual(prescription.current_issue.status, "0009")

    def test_issues(self):
        """
        Test that we can access the prescription issues
        """
        prescription = load_test_example_json(self.mock_log_object, "7D9625-Z72BF2-11E3A.json")

        self.assertEqual(prescription.issue_numbers, [1, 2, 3])

        issues = prescription.issues
        self.assertEqual(len(issues), 3)

        issue_numbers = [issue.number for issue in issues]
        self.assertEqual(issue_numbers, [1, 2, 3])

    def test_claims(self):
        """
        Test that we can access the prescription issue claims
        """
        prescription = load_test_example_json(self.mock_log_object, "7D9625-Z72BF2-11E3A.json")

        issue = prescription.get_issue(1)
        claim = issue.claim

        self.assertEqual(claim.received_date_str, "20140408")

        # make sure we can also update the received date
        claim.received_date_str = "20131225"
        self.assertEqual(claim.received_date_str, "20131225")

    def test_find_next_future_issue_number_future_issue_available(self):
        """
        Test that a future issue can be found in a prescription.
        """
        prescription = load_test_example_json(self.mock_log_object, "DD0180-ZBED5C-11E3A.json")

        # check the future issue can be found
        self.assertEqual(prescription._find_next_future_issue_number("1"), "2")

        # check that there are no more beyond the last issue
        self.assertEqual(prescription.max_repeats, 2)
        self.assertEqual(prescription._find_next_future_issue_number("2"), None)

    def test_find_next_future_issue_number_issues_already_dispensed(self):
        """
        Test that no future issues can be found if they're all dispensed.
        """
        prescription = load_test_example_json(self.mock_log_object, "7D9625-Z72BF2-11E3A.json")

        # chekc that dispensed issues can not be found
        self.assertEqual(prescription._find_next_future_issue_number("1"), None)
        self.assertEqual(prescription._find_next_future_issue_number("2"), None)

        # check that there are no more beyond the last issue
        self.assertEqual(prescription.max_repeats, 3)
        self.assertEqual(prescription._find_next_future_issue_number("3"), None)

    def test_get_issue_numbers_in_range(self):
        """
        Test that we can correctly retrieve ranges of issue numbers.
        """
        prescription = load_test_example_json(self.mock_log_object, "7D9625-Z72BF2-11E3A.json")

        self.assertEqual(prescription.issue_numbers, [1, 2, 3])

        # test lower bound only
        self.assertEqual(prescription.get_issue_numbers_in_range(0, None), [1, 2, 3])
        self.assertEqual(prescription.get_issue_numbers_in_range(1, None), [1, 2, 3])
        self.assertEqual(prescription.get_issue_numbers_in_range(2, None), [2, 3])
        self.assertEqual(prescription.get_issue_numbers_in_range(3, None), [3])
        self.assertEqual(prescription.get_issue_numbers_in_range(4, None), [])

        # test upper bound only
        self.assertEqual(prescription.get_issue_numbers_in_range(None, 4), [1, 2, 3])
        self.assertEqual(prescription.get_issue_numbers_in_range(None, 3), [1, 2, 3])
        self.assertEqual(prescription.get_issue_numbers_in_range(None, 2), [1, 2])
        self.assertEqual(prescription.get_issue_numbers_in_range(None, 1), [1])
        self.assertEqual(prescription.get_issue_numbers_in_range(None, 0), [])

        # test both bounds
        self.assertEqual(prescription.get_issue_numbers_in_range(0, 4), [1, 2, 3])
        self.assertEqual(prescription.get_issue_numbers_in_range(1, 3), [1, 2, 3])
        self.assertEqual(prescription.get_issue_numbers_in_range(2, 3), [2, 3])
        self.assertEqual(prescription.get_issue_numbers_in_range(2, 2), [2])
        self.assertEqual(prescription.get_issue_numbers_in_range(2, 1), [])

        # test no bounds
        self.assertEqual(prescription.get_issue_numbers_in_range(None, None), [1, 2, 3])
        self.assertEqual(prescription.get_issue_numbers_in_range(), [1, 2, 3])

    def test_missing_issue_numbers(self):
        """
        Test that we can deal correctly with prescriptions with missing instances.
        """
        # this 12-issue prescription has issues 1 and 2 missing because of migration
        prescription = load_test_example_json(self.mock_log_object, "50EE48-B83002-490F7.json")

        self.assertEqual(prescription.issue_numbers, [3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
        self.assertEqual(prescription.missing_issue_numbers, [1, 2])

        # make sure the range fetches work as well
        self.assertEqual(
            prescription.get_issue_numbers_in_range(None, None), [3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        )
        self.assertEqual(
            prescription.get_issue_numbers_in_range(2, None), [3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        )
        self.assertEqual(
            prescription.get_issue_numbers_in_range(3, None), [3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        )
        self.assertEqual(
            prescription.get_issue_numbers_in_range(4, None), [4, 5, 6, 7, 8, 9, 10, 11, 12]
        )
        self.assertEqual(
            prescription.get_issue_numbers_in_range(None, 13), [3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        )
        self.assertEqual(
            prescription.get_issue_numbers_in_range(None, 12), [3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        )
        self.assertEqual(
            prescription.get_issue_numbers_in_range(None, 11), [3, 4, 5, 6, 7, 8, 9, 10, 11]
        )
        self.assertEqual(prescription.get_issue_numbers_in_range(5, 8), [5, 6, 7, 8])
        self.assertEqual(prescription.get_issue_numbers_in_range(10, 7), [])

    def _assert_find_instances_to_action_update(
        self, prescription: PrescriptionRecord, handle_time, action, expected_issue_number_strs
    ):
        """
        Helper to test that find_instances_to_action_update() returns expected instances
        """
        mock_context = MagicMock()
        mock_context.handleTime = handle_time
        mock_context.instancesToUpdate = None
        prescription.find_instances_to_action_update(mock_context, action)
        self.assertEqual(mock_context.instancesToUpdate, expected_issue_number_strs)

    def test_find_instances_to_action_update(self):
        """
        Test that we can find instances that need updating at a particular time.
        """
        prescription = load_test_example_json(self.mock_log_object, "7D9625-Z72BF2-11E3A.json")

        # first, try a date that will pick up all next actions
        handle_time = datetime(year=2050, month=1, day=1)

        action = fields.NEXTACTIVITY_DELETE
        self._assert_find_instances_to_action_update(prescription, handle_time, action, ["1"])

        action = fields.NEXTACTIVITY_CREATENOCLAIM
        self._assert_find_instances_to_action_update(prescription, handle_time, action, ["2", "3"])

        action = fields.NEXTACTIVITY_EXPIRE
        self._assert_find_instances_to_action_update(prescription, handle_time, action, None)

        # then try a date in the past that won't pick up actions
        handle_time = datetime(year=2010, month=1, day=1)
        action = fields.NEXTACTIVITY_CREATENOCLAIM
        self._assert_find_instances_to_action_update(prescription, handle_time, action, None)

        # first, try a date that will pick up all next actions
        handle_time = datetime(year=2050, month=1, day=1)
        # same as above json but with nextActivityNAD_bin and instance 1 nextActivity set to purge
        prescription = load_test_example_json(self.mock_log_object, "7D9625-Z72BF2-11E3B.json")
        action = fields.NEXTACTIVITY_PURGE
        self._assert_find_instances_to_action_update(prescription, handle_time, action, ["1"])

    def test_find_instances_to_action_update_missing_instances(self):
        """
        SPII-10492 - Test that we can find instances that need updating in a migrated
        prescription with missing instances.
        """
        # this 12-issue prescription has issues 1 and 2 missing because of migration
        prescription = load_test_example_json(self.mock_log_object, "50EE48-B83002-490F7.json")

        # first, try a date that will pick up all next actions
        handle_time = datetime(year=2050, month=1, day=1)

        action = fields.NEXTACTIVITY_DELETE
        self._assert_find_instances_to_action_update(prescription, handle_time, action, ["3"])

        action = fields.NEXTACTIVITY_EXPIRE
        self._assert_find_instances_to_action_update(
            prescription, handle_time, action, ["5", "6", "7", "8", "9", "10", "11", "12"]
        )

    def test_reset_current_instance(self):
        """
        Test that resetting the current instance chooses the correct instance.
        """
        prescription = load_test_example_json(self.mock_log_object, "50EE48-B83002-490F7.json")
        self.assertEqual(prescription.current_issue_number, 4)
        (old, new) = prescription.reset_current_instance()
        self.assertEqual((old, new), (4, 4))
        self.assertEqual(prescription.current_issue_number, 4)

        prescription = load_test_example_json(self.mock_log_object, "DD0180-ZBED5C-11E3A.json")
        self.assertEqual(prescription.current_issue_number, 1)
        (old, new) = prescription.reset_current_instance()
        self.assertEqual((old, new), (1, 1))
        self.assertEqual(prescription.current_issue_number, 1)

        prescription = load_test_example_json(self.mock_log_object, "7D9625-Z72BF2-11E3A.json")
        self.assertEqual(prescription.current_issue_number, 3)
        (old, new) = prescription.reset_current_instance()
        self.assertEqual((old, new), (3, 3))
        self.assertEqual(prescription.current_issue_number, 3)

    def test_handle_overdue_expiry_none(self):
        """
        SPII-31379 due to old prescrptions the NAD index is set to None
        """
        nad = [None]
        self.assertFalse(PrescriptionRecord._is_expiry_overdue(nad))

    def test_handle_overdue_expiry_empty(self):
        """
        SPII-31379 due to old prescrptions the NAD index is empty
        """
        nad = []
        self.assertFalse(PrescriptionRecord._is_expiry_overdue(nad))

    def test_handle_overdue_expiry_not_expired(self):
        """
        Expiry is set to tomorrow
        """
        nad = [
            "expire:{}".format(
                (datetime.now() + timedelta(days=1)).strftime(TimeFormats.STANDARD_DATE_FORMAT)
            )
        ]
        self.assertFalse(PrescriptionRecord._is_expiry_overdue(nad))

    def test_handle_overdue_expiry_expired(self):
        """
        Expiry is set to yesterday
        """
        nad = [
            "expire:{}".format(
                (datetime.now() - timedelta(days=1)).strftime(TimeFormats.STANDARD_DATE_FORMAT)
            )
        ]
        self.assertTrue(PrescriptionRecord._is_expiry_overdue(nad))

    def test_get_line_item_cancellations(self):
        """
        Test that we can get the line item cancellations for a prescription
        """
        prescription = load_test_example_json(self.mock_log_object, "23C1BC-Z75FB1-11EE84.json")
        current_issue = prescription.current_issue

        cancelled_line_item_id = "02ED7776-21CD-4E7B-AC9D-D1DBFEE7B8CF"
        cancellations = current_issue.get_line_item_cancellations(cancelled_line_item_id)
        self.assertEqual(len(cancellations), 1)

        not_cancelled_line_item_id = "45D5FB11-D793-4D51-9ADD-95E0F54D2786"
        cancellations = current_issue.get_line_item_cancellations(not_cancelled_line_item_id)
        self.assertEqual(len(cancellations), 0)

    def test_get_line_item_first_cancellation_time(self):
        prescription = load_test_example_json(self.mock_log_object, "23C1BC-Z75FB1-11EE84.json")
        current_issue = prescription.current_issue

        cancelled_line_item_id = "02ED7776-21CD-4E7B-AC9D-D1DBFEE7B8CF"
        first_cancellation_time = current_issue.get_line_item_first_cancellation_time(
            cancelled_line_item_id
        )
        self.assertEqual(first_cancellation_time, "20240415101553")

        not_cancelled_line_item_id = "45D5FB11-D793-4D51-9ADD-95E0F54D2786"
        first_cancellation_time = current_issue.get_line_item_first_cancellation_time(
            not_cancelled_line_item_id
        )
        self.assertEqual(first_cancellation_time, None)

    def test_set_initial_prescription_status_active_prescription(self):
        """
        Test that a prescription with a start date of today or earlier is marked as TO_BE_DISPENSED.
        """
        prescription = load_test_example_json(self.mock_log_object, "7D9625-Z72BF2-11E3A.json")

        current_time = datetime.now()
        prescription.set_initial_prescription_status(current_time)

        self.assertEqual(prescription.get_issue(1).status, "0001")

    def test_set_initial_prescription_status_future_dated(self):
        """
        Test that a prescription with a future start date is marked as FUTURE_DATED_PRESCRIPTION.
        """
        prescription = load_test_example_json(self.mock_log_object, "0DA698-A83008-F50593.json")

        future_time = datetime.now() + timedelta(days=10)
        prescription.set_initial_prescription_status(future_time)

        self.assertEqual(prescription.get_issue(1).status, "9001")

    def test_add_index_to_record(self):
        """
        Test that we can add an index to the record.
        """
        prescription = PrescriptionRecord(self.mock_log_object, "test")
        prescription.prescription_record = {}

        prescription.add_index_to_record({"testIndex": "testValue"})

        self.assertEqual(
            prescription.prescription_record.get(fields.FIELD_INDEXES), {"testIndex": "testValue"}
        )

    def test_increment_scn(self):
        """
        Test that we can increment the SCN.
        """
        prescription = PrescriptionRecord(self.mock_log_object, "test")
        prescription.prescription_record = {"SCN": 5}

        prescription.increment_scn()

        self.assertEqual(prescription.prescription_record.get("SCN"), 6)

    def test_add_document_refs(self):
        """
        Test that we can add document refs.
        """
        prescription = PrescriptionRecord(self.mock_log_object, "test")
        prescription.prescription_record = {}

        prescription.add_document_references(["doc1", "doc2"])

        self.assertEqual(
            prescription.prescription_record.get(fields.FIELDS_DOCUMENTS), ["doc1", "doc2"]
        )

    @parameterized.expand(
        [
            ("upper", indexes.INDEX_NEXTACTIVITY, "next_activity_nad"),
            ("lower", indexes.INDEX_NEXTACTIVITY.lower(), "next_activity_nad"),
            ("invalid", "invalid_index", None),
        ]
    )
    def test_return_next_activity_nad_bin(self, _, index_key, expected):
        """
        Test that we can return the next activity NAD bin.
        """
        prescription = PrescriptionRecord(self.mock_log_object, "test")
        prescription.prescription_record = {fields.FIELD_INDEXES: {index_key: "next_activity_nad"}}

        nad_bin = prescription.return_next_activity_nad_bin()

        self.assertEqual(nad_bin, expected)

    def test_name_map_on_create(self):
        """
        Test that the names are mapped correctly when creating a record.
        """
        context = MagicMock()
        context.agentOrganization = "testOrg"
        context.prescriptionRepeatHigh = "repeatHigh"
        context.daysSupplyValidLow = "daysLow"
        context.daysSupplyValidHigh = "daysHigh"
        prescription = PrescriptionRecord(self.mock_log_object, "test")

        prescription.name_map_on_create(context)

        self.assertEqual(context.prescribingOrganization, "testOrg")
        self.assertEqual(context.maxRepeats, "repeatHigh")
        self.assertEqual(context.dispenseWindowLowDate, "daysLow")
        self.assertEqual(context.dispenseWindowHighDate, "daysHigh")

    def test_return_prechange_issue_status_dict(self):
        """
        Test that we can return the pre-change issue status dict.
        """
        prescription = PrescriptionRecord(self.mock_log_object, "test")
        prescription.pre_change_issue_status_dict = "pre_change_status_dict"

        result = prescription.return_prechange_issue_status_dict()

        self.assertEqual(result, "pre_change_status_dict")

    def test_return_prechange_current_issue(self):
        """
        Test that we can return the pre-change current issue.
        """
        prescription = PrescriptionRecord(self.mock_log_object, "test")
        prescription.pre_change_current_issue = "pre_change_current_issue"

        result = prescription.return_prechange_current_issue()

        self.assertEqual(result, "pre_change_current_issue")

    def test_create_initial_record(self):
        """
        Test that we can create the initial record.
        """
        prescription = PrescriptionRecord(self.mock_log_object, "test")
        context = MagicMock()
        prescription.create_initial_record(context)

        self.assertTrue(
            all(
                field in prescription.prescription_record[fields.FIELD_PRESCRIPTION]
                for field in fields.PRESCRIPTION_DETAILS
            )
        )


class PrescriptionRecordChangeLogTest(TestCase):
    """
    For testing aspects of the change log in the prescription record.
    """

    def setUp(self):
        self.log_object = MockLogObject()
        self.mock_record = PrescriptionRecord(self.log_object, "test")

    def test_error_log_change_log_too_big(self):
        """
        When a change log cannot be pruned small enough an error is raised.
        """
        self.mock_record.prescription_record = {
            "prescription": {fields.FIELD_PRESCRIPTION_ID: "testID"},
            "SCN": 10,
            "changeLog": {
                "438eb94f-9da7-46ca-ba2a-72c4f83b2a06": {"SCN": 10},
                "438eb94f-9da7-46ca-ba2a-72c4f83b2a46": {"SCN": 10},
            },
        }
        self.mock_record.SCN_MAX = 1
        self.assertRaises(
            EpsSystemError,
            self.mock_record.add_event_to_change_log,
            "ce6c4a39-e239-44c5-81e2-adf3612a7391",
            {},
        )
        self.assertTrue(self.log_object.was_logged("EPS0336"))
        self.assertTrue(self.log_object.was_value_logged("EPS0336", "prescriptionID", "testID"))
