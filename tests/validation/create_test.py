import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from parameterized import parameterized

import eps_spine_shared.validation.create as create_validator
from eps_spine_shared.errors import EpsValidationError
from eps_spine_shared.logger import EpsLogger
from eps_spine_shared.testing.mock_logger import MockLogObject
from eps_spine_shared.validation import constants, message_vocab


class CreatePrescriptionValidatorTest(unittest.TestCase):
    def setUp(self):
        self.log_object = EpsLogger(MockLogObject())
        self.internal_id = "test-internal-id"

        self.context = MagicMock()
        self.context.msgOutput = {}
        self.context.outputFields = set()


class TestCheckHcplOrg(CreatePrescriptionValidatorTest):
    def test_valid_hcpl_org(self):
        self.context.msgOutput[message_vocab.HCPLORG] = "ORG12345"
        create_validator.check_hcpl_org(self.context)

    def test_invalid_format_raises_error(self):
        self.context.msgOutput[message_vocab.HCPLORG] = "ORG@1234"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_hcpl_org(self.context)

        self.assertEqual(str(cm.exception), message_vocab.HCPLORG + " has invalid format")


class TestCheckSignedTime(CreatePrescriptionValidatorTest):
    def test_valid_signed_time(self):
        self.context.msgOutput[message_vocab.SIGNED_TIME] = "20260911123456"
        create_validator.check_signed_time(self.context, self.internal_id, self.log_object)

        self.assertIn(message_vocab.SIGNED_TIME, self.context.outputFields)

    @parameterized.expand(
        [
            ("+0100"),
            ("-0000"),
            ("+0000"),
        ]
    )
    def test_valid_international_signed_time(self, date_suffix):
        self.context.msgOutput[message_vocab.SIGNED_TIME] = "20260911123456" + date_suffix
        create_validator.check_signed_time(self.context, self.internal_id, self.log_object)

        self.assertIn(message_vocab.SIGNED_TIME, self.context.outputFields)

    def test_invalid_international_signed_time_raises_error(self):
        self.context.msgOutput[message_vocab.SIGNED_TIME] = "20260911123456+0200"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_signed_time(self.context, self.internal_id, self.log_object)

        self.assertEqual(
            str(cm.exception),
            message_vocab.SIGNED_TIME
            + " is not a valid time or in the valid format; expected format %Y%m%d%H%M%S",
        )

    def test_wrong_length_raises_error(self):
        self.context.msgOutput[message_vocab.SIGNED_TIME] = "202609111234567"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_signed_time(self.context, self.internal_id, self.log_object)

        self.assertEqual(
            str(cm.exception),
            message_vocab.SIGNED_TIME
            + " is not a valid time or in the valid format; expected format %Y%m%d%H%M%S",
        )


class TestCheckDaysSupply(CreatePrescriptionValidatorTest):
    def test_none(self):
        self.context.msgOutput[message_vocab.DAYS_SUPPLY] = None
        create_validator.check_days_supply(self.context)

        self.assertIn(message_vocab.DAYS_SUPPLY, self.context.outputFields)

    def test_valid_integer(self):
        self.context.msgOutput[message_vocab.DAYS_SUPPLY] = "30"
        create_validator.check_days_supply(self.context)

        self.assertIn(message_vocab.DAYS_SUPPLY, self.context.outputFields)
        self.assertEqual(self.context.msgOutput[message_vocab.DAYS_SUPPLY], 30)

    def test_non_integer(self):
        self.context.msgOutput[message_vocab.DAYS_SUPPLY] = "one"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_days_supply(self.context)

        self.assertEqual(str(cm.exception), "daysSupply is not an integer")

    def test_negative_integer(self):
        self.context.msgOutput[message_vocab.DAYS_SUPPLY] = "-5"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_days_supply(self.context)

        self.assertEqual(str(cm.exception), "daysSupply is not an integer")

    def test_exceeds_max(self):
        self.context.msgOutput[message_vocab.DAYS_SUPPLY] = str(constants.MAX_DAYSSUPPLY + 1)

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_days_supply(self.context)

        self.assertEqual(
            str(cm.exception), "daysSupply cannot exceed " + str(constants.MAX_DAYSSUPPLY)
        )


class TestCheckRepeatDispenseWindow(CreatePrescriptionValidatorTest):
    def setUp(self):
        super().setUp()
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_REPEAT_DISP
        self.handle_time = datetime(2026, 9, 11, 12, 34, 56)

    def test_non_repeat(self):
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_ACUTE
        create_validator.check_repeat_dispense_window(self.context, self.handle_time)

        self.assertEqual(self.context.msgOutput[message_vocab.DAYS_SUPPLY_LOW], "20260911")
        self.assertEqual(self.context.msgOutput[message_vocab.DAYS_SUPPLY_HIGH], "20270911")

        self.assertIn(message_vocab.DAYS_SUPPLY_LOW, self.context.outputFields)
        self.assertIn(message_vocab.DAYS_SUPPLY_HIGH, self.context.outputFields)

    def test_missing_low_and_high(self):
        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_repeat_dispense_window(self.context, self.handle_time)

        self.assertEqual(
            str(cm.exception),
            "daysSupply effective time not provided but prescription treatment type is repeat",
        )

    @parameterized.expand(
        [
            ("20260911", "202709111", "daysSupplyValidHigh"),
            ("202609111", "20270911", "daysSupplyValidLow"),
        ]
    )
    def test_invalid_dates(self, low_date, high_date, incorrect_field):
        self.context.msgOutput[message_vocab.DAYS_SUPPLY_LOW] = low_date
        self.context.msgOutput[message_vocab.DAYS_SUPPLY_HIGH] = high_date

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_repeat_dispense_window(self.context, self.handle_time)

        self.assertEqual(
            str(cm.exception),
            f"{incorrect_field} is not a valid time or in the valid format; expected format %Y%m%d",
        )

    def test_high_date_exceeds_limit(self):
        self.context.msgOutput[message_vocab.DAYS_SUPPLY_LOW] = "20260911"
        self.context.msgOutput[message_vocab.DAYS_SUPPLY_HIGH] = "20280911"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_repeat_dispense_window(self.context, self.handle_time)

        self.assertEqual(
            str(cm.exception),
            f"daysSupplyValidHigh is more than {str(constants.MAX_FUTURESUPPLYMONTHS)} months beyond current day",
        )

    def test_high_date_in_the_past(self):
        self.context.msgOutput[message_vocab.DAYS_SUPPLY_LOW] = "20260911"
        self.context.msgOutput[message_vocab.DAYS_SUPPLY_HIGH] = "20260910"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_repeat_dispense_window(self.context, self.handle_time)

        self.assertEqual(str(cm.exception), "daysSupplyValidHigh is in the past")

    def test_low_after_high(self):
        self.context.msgOutput[message_vocab.DAYS_SUPPLY_LOW] = "20260912"
        self.context.msgOutput[message_vocab.DAYS_SUPPLY_HIGH] = "20260911"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_repeat_dispense_window(self.context, self.handle_time)

        self.assertEqual(str(cm.exception), "daysSupplyValid low is after daysSupplyValidHigh")


class TestCheckPrescriberDetails(CreatePrescriptionValidatorTest):
    def test_8_char_alphanumeric(self):
        self.context.msgOutput[message_vocab.AGENT_PERSON] = "ABCD1234"
        create_validator.check_prescriber_details(self.context, self.internal_id, self.log_object)

        self.assertIn(message_vocab.AGENT_PERSON, self.context.outputFields)
        self.assertFalse(self.log_object.logger.was_logged("EPS0323a"))

    def test_12_char_alphanumeric(self):
        self.context.msgOutput[message_vocab.AGENT_PERSON] = "ABCD12345678"
        create_validator.check_prescriber_details(self.context, self.internal_id, self.log_object)

        self.assertIn(message_vocab.AGENT_PERSON, self.context.outputFields)
        self.assertTrue(self.log_object.logger.was_logged("EPS0323a"))

    def test_too_long_raises_error(self):
        self.context.msgOutput[message_vocab.AGENT_PERSON] = "ABCD123456789"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_prescriber_details(
                self.context, self.internal_id, self.log_object
            )

        self.assertEqual(str(cm.exception), message_vocab.AGENT_PERSON + " has invalid format")
        self.assertTrue(
            self.log_object.logger.was_multiple_value_logged(
                "EPS0323a", {"internalID": self.internal_id, "prescribingGpCode": "ABCD123456789"}
            )
        )

    def test_special_chars_raises_error(self):
        self.context.msgOutput[message_vocab.AGENT_PERSON] = "ABC@1234"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_prescriber_details(
                self.context, self.internal_id, self.log_object
            )

        self.assertEqual(str(cm.exception), message_vocab.AGENT_PERSON + " has invalid format")

    def test_adds_to_outputFields(self):
        self.context.msgOutput[message_vocab.AGENT_PERSON] = "ABCD1234"
        create_validator.check_prescriber_details(self.context, self.internal_id, self.log_object)

        self.assertIn(message_vocab.AGENT_PERSON, self.context.outputFields)


class TestCheckPatientName(CreatePrescriptionValidatorTest):
    def test_adds_to_outputFields(self):
        create_validator.check_patient_name(self.context)

        self.assertIn(message_vocab.PREFIX, self.context.outputFields)
        self.assertIn(message_vocab.SUFFIX, self.context.outputFields)
        self.assertIn(message_vocab.GIVEN, self.context.outputFields)
        self.assertIn(message_vocab.FAMILY, self.context.outputFields)


class TestCheckPrescriptionTreatmentType(CreatePrescriptionValidatorTest):
    def test_unrecognised_treatment_type_raises_error(self):
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = "9999"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_prescription_treatment_type(self.context)

        self.assertEqual(str(cm.exception), "prescriptionTreatmentType is not of expected type")

    def test_valid_treatment_type(self):
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_ACUTE
        create_validator.check_prescription_treatment_type(self.context)

        self.assertIn(message_vocab.TREATMENTTYPE, self.context.outputFields)


class TestCheckPrescriptionType(CreatePrescriptionValidatorTest):
    def test_unrecognised_prescription_type(self):
        self.context.msgOutput[message_vocab.PRESCTYPE] = "9999"
        create_validator.check_prescription_type(self.context, self.internal_id, self.log_object)

        self.assertEqual(self.context.msgOutput[message_vocab.PRESCTYPE], "NotProvided")
        self.assertIn(message_vocab.PRESCTYPE, self.context.outputFields)


class TestCheckRepeatDispenseInstances(CreatePrescriptionValidatorTest):
    def setUp(self):
        super().setUp()
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_REPEAT_DISP

    def test_acute_prescription_without_repeat_values(self):
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_ACUTE
        self.context.msgOutput[message_vocab.REPEATLOW] = None
        self.context.msgOutput[message_vocab.REPEATHIGH] = None

        create_validator.check_repeat_dispense_instances(
            self.context, self.internal_id, self.log_object
        )

        self.assertNotIn(message_vocab.REPEATLOW, self.context.outputFields)
        self.assertNotIn(message_vocab.REPEATHIGH, self.context.outputFields)

    def test_non_acute_without_repeat_values_raises_error(self):
        self.context.msgOutput[message_vocab.REPEATLOW] = None
        self.context.msgOutput[message_vocab.REPEATHIGH] = None

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_repeat_dispense_instances(
                self.context, self.internal_id, self.log_object
            )

        self.assertIn("must both be provided", str(cm.exception))

    @parameterized.expand(
        [
            ("1", "abc", message_vocab.REPEATHIGH),
            ("abc", "1", message_vocab.REPEATLOW),
        ]
    )
    def test_repeat_high_or_low_not_integer_raises_error(
        self, low_value, high_value, incorrect_field
    ):
        self.context.msgOutput[message_vocab.REPEATLOW] = low_value
        self.context.msgOutput[message_vocab.REPEATHIGH] = high_value

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_repeat_dispense_instances(
                self.context, self.internal_id, self.log_object
            )

        self.assertEqual(str(cm.exception), incorrect_field + " is not an integer")

    def test_repeat_low_not_one_raises_error(self):
        self.context.msgOutput[message_vocab.REPEATLOW] = "2"
        self.context.msgOutput[message_vocab.REPEATHIGH] = "6"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_repeat_dispense_instances(
                self.context, self.internal_id, self.log_object
            )

        self.assertEqual(str(cm.exception), message_vocab.REPEATLOW + " must be 1")

    def test_repeat_high_exceeds_max_raises_error(self):
        self.context.msgOutput[message_vocab.REPEATLOW] = "1"
        self.context.msgOutput[message_vocab.REPEATHIGH] = str(
            constants.MAX_PRESCRIPTIONREPEATS + 1
        )

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_repeat_dispense_instances(
                self.context, self.internal_id, self.log_object
            )

        self.assertIn("must not be over configured maximum", str(cm.exception))

    def test_repeat_low_greater_than_high_raises_error(self):
        self.context.msgOutput[message_vocab.REPEATLOW] = "1"
        self.context.msgOutput[message_vocab.REPEATHIGH] = "0"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_repeat_dispense_instances(
                self.context, self.internal_id, self.log_object
            )

        self.assertIn("is greater than", str(cm.exception))

    def test_repeat_prescription_with_multiple_instances_logs_warning(self):
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_REPEAT
        self.context.msgOutput[message_vocab.REPEATLOW] = "1"
        self.context.msgOutput[message_vocab.REPEATHIGH] = "6"

        create_validator.check_repeat_dispense_instances(
            self.context, self.internal_id, self.log_object
        )

        self.assertTrue(self.log_object.logger.was_logged("EPS0509"))
        self.assertIn(message_vocab.REPEATLOW, self.context.outputFields)
        self.assertIn(message_vocab.REPEATHIGH, self.context.outputFields)

    def test_valid_repeat_dispense_instances(self):
        self.context.msgOutput[message_vocab.REPEATLOW] = "1"
        self.context.msgOutput[message_vocab.REPEATHIGH] = "6"

        create_validator.check_repeat_dispense_instances(
            self.context, self.internal_id, self.log_object
        )

        self.assertIn(message_vocab.REPEATLOW, self.context.outputFields)
        self.assertIn(message_vocab.REPEATHIGH, self.context.outputFields)


class TestCheckBirthDate(CreatePrescriptionValidatorTest):
    def setUp(self):
        super().setUp()
        self.handle_time = datetime(2026, 9, 11, 12, 34, 56)

    def test_valid_birth_date(self):
        self.context.msgOutput[message_vocab.BIRTHTIME] = "20000101"
        create_validator.check_birth_date(self.context, self.handle_time)

        self.assertIn(message_vocab.BIRTHTIME, self.context.outputFields)

    def test_birth_date_in_future_raises_error(self):
        self.context.msgOutput[message_vocab.BIRTHTIME] = "20260912"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_birth_date(self.context, self.handle_time)

        self.assertEqual(str(cm.exception), message_vocab.BIRTHTIME + " is in the future")

    def test_invalid_birth_date_format_raises_error(self):
        self.context.msgOutput[message_vocab.BIRTHTIME] = "2000010112"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_birth_date(self.context, self.handle_time)

        self.assertEqual(
            str(cm.exception),
            message_vocab.BIRTHTIME
            + " is not a valid time or in the valid format; expected format %Y%m%d",
        )


class TestValidateLineItems(CreatePrescriptionValidatorTest):
    def setUp(self):
        super().setUp()
        self.line_item_1_id = "12345678-1234-1234-1234-123456789012"
        self.context.msgOutput[message_vocab.LINEITEM_PX + "1" + message_vocab.LINEITEM_SX_ID] = (
            self.line_item_1_id
        )

    def test_no_line_items_raises_error(self):
        del self.context.msgOutput[message_vocab.LINEITEM_PX + "1" + message_vocab.LINEITEM_SX_ID]
        with self.assertRaises(EpsValidationError) as cm:
            create_validator.validate_line_items(self.context, self.internal_id, self.log_object)

        self.assertEqual(str(cm.exception), "No valid line items found")

    def test_single_valid_line_item(self):
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_ACUTE

        create_validator.validate_line_items(self.context, self.internal_id, self.log_object)

        self.assertEqual(len(self.context.msgOutput[message_vocab.LINEITEMS]), 1)
        self.assertEqual(
            self.context.msgOutput[message_vocab.LINEITEMS][0][message_vocab.LINEITEM_SX_ID],
            self.line_item_1_id,
        )
        self.assertIn(message_vocab.LINEITEMS, self.context.outputFields)

    def test_multiple_valid_line_items(self):
        self.context.msgOutput[message_vocab.LINEITEM_PX + "2" + message_vocab.LINEITEM_SX_ID] = (
            "12345678-1234-1234-1234-123456789013"
        )
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_ACUTE

        create_validator.validate_line_items(self.context, self.internal_id, self.log_object)

        self.assertEqual(len(self.context.msgOutput[message_vocab.LINEITEMS]), 2)
        self.assertIn(message_vocab.LINEITEMS, self.context.outputFields)

    def test_exceeds_max_line_items_raises_error(self):
        for i in range(1, constants.MAX_LINEITEMS + 2):
            self.context.msgOutput[
                message_vocab.LINEITEM_PX + str(i) + message_vocab.LINEITEM_SX_ID
            ] = f"12345678-1234-1234-1234-1234567890{i:02d}"
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_ACUTE

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.validate_line_items(self.context, self.internal_id, self.log_object)

        self.assertIn("over expected max count", str(cm.exception))

    def test_line_item_with_repeat_values(self):
        self.context.msgOutput[
            message_vocab.LINEITEM_PX + "1" + message_vocab.LINEITEM_SX_REPEATHIGH
        ] = "6"
        self.context.msgOutput[
            message_vocab.LINEITEM_PX + "1" + message_vocab.LINEITEM_SX_REPEATLOW
        ] = "1"
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_REPEAT_DISP
        self.context.msgOutput[message_vocab.REPEATHIGH] = 6

        create_validator.validate_line_items(self.context, self.internal_id, self.log_object)

        line_items = self.context.msgOutput[message_vocab.LINEITEMS]
        self.assertEqual(len(line_items), 1)
        self.assertEqual(line_items[0][message_vocab.LINEITEM_DT_MAXREPEATS], "6")
        self.assertEqual(line_items[0][message_vocab.LINEITEM_DT_CURRINSTANCE], "1")

    def test_prescription_repeat_less_than_line_item_repeat_raises_error(self):
        self.context.msgOutput[
            message_vocab.LINEITEM_PX + "1" + message_vocab.LINEITEM_SX_REPEATHIGH
        ] = "6"
        self.context.msgOutput[
            message_vocab.LINEITEM_PX + "1" + message_vocab.LINEITEM_SX_REPEATLOW
        ] = "1"
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_REPEAT_DISP
        self.context.msgOutput[message_vocab.REPEATHIGH] = 3

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.validate_line_items(self.context, self.internal_id, self.log_object)

        self.assertIn("must not be greater than prescriptionRepeatHigh", str(cm.exception))

    def test_prescription_repeat_greater_than_all_line_item_repeats_raises_error(self):
        self.context.msgOutput[message_vocab.LINEITEM_PX + "2" + message_vocab.LINEITEM_SX_ID] = (
            "12345678-1234-1234-1234-123456789013"
        )
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_ACUTE

        self.context.msgOutput[message_vocab.REPEATHIGH] = 3

        self.context.msgOutput[
            message_vocab.LINEITEM_PX + "1" + message_vocab.LINEITEM_SX_REPEATHIGH
        ] = "1"
        self.context.msgOutput[
            message_vocab.LINEITEM_PX + "1" + message_vocab.LINEITEM_SX_REPEATLOW
        ] = "1"

        self.context.msgOutput[
            message_vocab.LINEITEM_PX + "2" + message_vocab.LINEITEM_SX_REPEATHIGH
        ] = "1"
        self.context.msgOutput[
            message_vocab.LINEITEM_PX + "2" + message_vocab.LINEITEM_SX_REPEATLOW
        ] = "1"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.validate_line_items(self.context, self.internal_id, self.log_object)

        self.assertIn(
            "Prescription repeat count must not be greater than all Line Item repeat counts",
            str(cm.exception),
        )


class TestValidateLineItem(CreatePrescriptionValidatorTest):
    def setUp(self):
        super().setUp()
        self.line_item_id = "12345678-1234-1234-1234-123456789012"
        self.line_item = 1
        self.line_dict = {}
        self.context = MagicMock()
        self.context.msgOutput = {}
        self.context.outputFields = set()

    def test_invalid_line_item_id(self):
        self.line_dict[message_vocab.LINEITEM_DT_ID] = "invalid-line-item-id"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.validate_line_item(
                self.context, self.line_item, self.line_dict, 1, self.internal_id, self.log_object
            )

        self.assertEqual(str(cm.exception), "invalid-line-item-id is not a valid GUID format")

    def test_missing_items_from_line_dict(self):
        self.line_dict[message_vocab.LINEITEM_DT_ID] = self.line_item_id
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_ACUTE

        max_repeats = create_validator.validate_line_item(
            self.context, self.line_item, self.line_dict, 1, self.internal_id, self.log_object
        )

        self.assertEqual(max_repeats, 1)

    def test_repeat_high_not_integer(self):
        p = patch(
            "eps_spine_shared.validation.create.check_for_invalid_line_item_repeat_combinations",
            MagicMock(),
        )
        p.start()

        self.line_dict[message_vocab.LINEITEM_DT_ID] = self.line_item_id
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_REPEAT_DISP
        self.line_dict[message_vocab.LINEITEM_DT_MAXREPEATS] = "abc"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.validate_line_item(
                self.context, self.line_item, self.line_dict, 1, self.internal_id, self.log_object
            )

        self.assertEqual(str(cm.exception), "repeat.High for line item 1 is not an integer")
        p.stop()

    def test_repeat_high_less_than_one(self):
        p = patch(
            "eps_spine_shared.validation.create.check_for_invalid_line_item_repeat_combinations",
            MagicMock(),
        )
        p.start()

        self.line_dict[message_vocab.LINEITEM_DT_ID] = self.line_item_id
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_REPEAT_DISP
        self.line_dict[message_vocab.LINEITEM_DT_MAXREPEATS] = "0"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.validate_line_item(
                self.context, self.line_item, self.line_dict, 1, self.internal_id, self.log_object
            )

        self.assertEqual(str(cm.exception), "repeat.High for line item 1 must be greater than zero")
        p.stop()

    def test_repeat_high_exceeds_prescription_repeat_high(self):
        p = patch(
            "eps_spine_shared.validation.create.check_for_invalid_line_item_repeat_combinations",
            MagicMock(),
        )
        p.start()

        self.line_dict[message_vocab.LINEITEM_DT_ID] = self.line_item_id
        self.line_dict[message_vocab.LINEITEM_DT_MAXREPEATS] = "6"

        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_REPEAT_DISP
        self.context.msgOutput[message_vocab.REPEATHIGH] = "3"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.validate_line_item(
                self.context, self.line_item, self.line_dict, 1, self.internal_id, self.log_object
            )

        self.assertEqual(
            str(cm.exception),
            "repeat.High of 6 for line item 1 must not be greater than "
            "prescriptionRepeatHigh of 3",
        )
        p.stop()

    def test_repeat_high_not_1_when_treatment_type_is_repeat(self):
        p = patch(
            "eps_spine_shared.validation.create.check_for_invalid_line_item_repeat_combinations",
            MagicMock(),
        )
        p.start()

        self.line_dict[message_vocab.LINEITEM_DT_ID] = self.line_item_id
        self.line_dict[message_vocab.LINEITEM_DT_MAXREPEATS] = "3"
        self.line_dict[message_vocab.LINEITEM_DT_CURRINSTANCE] = "1"

        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_REPEAT
        self.context.msgOutput[message_vocab.REPEATHIGH] = "3"

        create_validator.validate_line_item(
            self.context, self.line_item, self.line_dict, 1, self.internal_id, self.log_object
        )

        self.assertTrue(self.log_object.logger.was_logged("EPS0509"))
        p.stop()

    def test_repeat_low_not_integer(self):
        p = patch(
            "eps_spine_shared.validation.create.check_for_invalid_line_item_repeat_combinations",
            MagicMock(),
        )
        p.start()

        self.line_dict[message_vocab.LINEITEM_DT_ID] = self.line_item_id
        self.line_dict[message_vocab.LINEITEM_DT_MAXREPEATS] = "3"
        self.line_dict[message_vocab.LINEITEM_DT_CURRINSTANCE] = "abc"

        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_ACUTE
        self.context.msgOutput[message_vocab.REPEATHIGH] = "3"
        self.context.msgOutput[message_vocab.REPEATLOW] = "1"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.validate_line_item(
                self.context, self.line_item, self.line_dict, 1, self.internal_id, self.log_object
            )

        self.assertEqual(str(cm.exception), "repeat.Low for line item 1 is not an integer")
        p.stop()

    def test_repeat_low_not_1(self):
        p = patch(
            "eps_spine_shared.validation.create.check_for_invalid_line_item_repeat_combinations",
            MagicMock(),
        )
        p.start()

        self.line_dict[message_vocab.LINEITEM_DT_ID] = self.line_item_id
        self.line_dict[message_vocab.LINEITEM_DT_MAXREPEATS] = "3"
        self.line_dict[message_vocab.LINEITEM_DT_CURRINSTANCE] = "2"

        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_ACUTE
        self.context.msgOutput[message_vocab.REPEATHIGH] = "3"
        self.context.msgOutput[message_vocab.REPEATLOW] = "1"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.validate_line_item(
                self.context, self.line_item, self.line_dict, 1, self.internal_id, self.log_object
            )

        self.assertEqual(str(cm.exception), "repeat.Low for line item 1 is not set to 1")
        p.stop()


class TestCheckForInvalidLineItemRepeatCombinations(CreatePrescriptionValidatorTest):
    def setUp(self):
        super().setUp()
        self.line_item = 1
        self.line_dict = {message_vocab.LINEITEM_DT_ID: "12345678-1234-1234-1234-123456789012"}

    def test_repeat_dispense_without_repeat_values_raises_error(self):
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_REPEAT_DISP

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_for_invalid_line_item_repeat_combinations(
                self.context, self.line_dict, self.line_item
            )

        self.assertEqual(
            str(cm.exception),
            "repeat.High and repeat.Low values must both be provided for lineItem 1 if not acute prescription",
        )

    def test_acute_prescription_with_repeat_values_raises_error(self):
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_ACUTE
        self.line_dict[message_vocab.LINEITEM_DT_MAXREPEATS] = "6"
        self.line_dict[message_vocab.LINEITEM_DT_CURRINSTANCE] = "1"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_for_invalid_line_item_repeat_combinations(
                self.context, self.line_dict, self.line_item
            )

        self.assertEqual(
            str(cm.exception), "Line item 1 repeat value provided for non-repeat prescription"
        )

    def test_repeat_dispense_with_only_repeat_high_raises_error(self):
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_REPEAT_DISP
        self.line_dict[message_vocab.LINEITEM_DT_MAXREPEATS] = "6"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_for_invalid_line_item_repeat_combinations(
                self.context, self.line_dict, self.line_item
            )

        self.assertEqual(
            str(cm.exception), "repeat.High provided but not repeat.Low for line item 1"
        )

    def test_repeat_dispense_with_only_repeat_low_raises_error(self):
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_REPEAT_DISP
        self.line_dict[message_vocab.LINEITEM_DT_CURRINSTANCE] = "1"

        with self.assertRaises(EpsValidationError) as cm:
            create_validator.check_for_invalid_line_item_repeat_combinations(
                self.context, self.line_dict, self.line_item
            )

        self.assertEqual(
            str(cm.exception), "repeat.Low provided but not repeat.High for line item 1"
        )

    def test_repeat_dispense_with_valid_repeat_values(self):
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_REPEAT_DISP
        self.context.msgOutput[message_vocab.REPEATHIGH] = "6"

        self.line_dict[message_vocab.LINEITEM_DT_MAXREPEATS] = "6"
        self.line_dict[message_vocab.LINEITEM_DT_CURRINSTANCE] = "1"

        create_validator.check_for_invalid_line_item_repeat_combinations(
            self.context, self.line_dict, self.line_item
        )


class TestRunValidations(CreatePrescriptionValidatorTest):
    def setUp(self):
        super().setUp()
        self.handle_time = datetime(2026, 9, 11, 12, 34, 56)

    def test_validations_happy_path(self):
        self.context.msgOutput[message_vocab.AGENT_PERSON] = "ABCD1234"
        self.context.msgOutput[message_vocab.AGENTORG] = "ORG12345"
        self.context.msgOutput[message_vocab.ROLEPROFILE] = "123456789012345"
        self.context.msgOutput[message_vocab.ROLE] = "ROLE"
        self.context.msgOutput[message_vocab.PATIENTID] = "9434765919"
        self.context.msgOutput[message_vocab.PRESCTIME] = "20240101120000"
        self.context.msgOutput[message_vocab.TREATMENTTYPE] = constants.STATUS_ACUTE
        self.context.msgOutput[message_vocab.PRESCTYPE] = "0001"
        self.context.msgOutput[message_vocab.REPEATLOW] = None
        self.context.msgOutput[message_vocab.REPEATHIGH] = 1
        self.context.msgOutput[message_vocab.BIRTHTIME] = "20000101"
        self.context.msgOutput[message_vocab.HL7EVENTID] = "C0AB090A-FDDC-4B64-97AD-2319A2309C2F"
        self.context.msgOutput[message_vocab.LINEITEM_PX + "1" + message_vocab.LINEITEM_SX_ID] = (
            "12345678-1234-1234-1234-123456789012"
        )
        self.context.msgOutput[message_vocab.HCPLORG] = "ORG12345"
        self.context.msgOutput[message_vocab.NOMPERFORMER] = "VALID123"
        self.context.msgOutput[message_vocab.NOMPERFORMER_TYPE] = "P1"
        self.context.msgOutput[message_vocab.PRESCID] = "7D9625-Z72BF2-11E3AC"
        self.context.msgOutput[message_vocab.SIGNED_TIME] = "20260911123456"
        self.context.msgOutput[message_vocab.DAYS_SUPPLY] = "30"

        create_validator.run_validations(
            self.context, self.handle_time, self.internal_id, self.log_object
        )
