import unittest
from unittest.mock import MagicMock

import eps_spine_shared.validation.common as common_validator
from eps_spine_shared.errors import EpsValidationError
from eps_spine_shared.logger import EpsLogger
from eps_spine_shared.testing.mock_logger import MockLogObject
from eps_spine_shared.validation import message_vocab


class CommonPrescriptionValidatorTest(unittest.TestCase):
    def setUp(self):
        self.log_object = EpsLogger(MockLogObject())
        self.internal_id = "test-internal-id"

        self.context = MagicMock()
        self.context.msgOutput = {}
        self.context.outputFields = set()


class TestCheckNominatedPerformer(CommonPrescriptionValidatorTest):
    def test_valid_nominated_performer(self):
        self.context.msgOutput[message_vocab.NOMPERFORMER] = "VALID123"
        self.context.msgOutput[message_vocab.NOMPERFORMER_TYPE] = "P1"

        common_validator.check_nominated_performer(self.context)

        self.assertIn(message_vocab.NOMPERFORMER, self.context.outputFields)
        self.assertIn(message_vocab.NOMPERFORMER_TYPE, self.context.outputFields)

    def test_present_but_empty_nominated_performer(self):
        self.context.msgOutput[message_vocab.NOMPERFORMER] = ""

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_nominated_performer(self.context)

        self.assertEqual(str(cm.exception), "nominatedPerformer is present but empty")

    def test_invalid_nominated_performer_format(self):
        self.context.msgOutput[message_vocab.NOMPERFORMER] = "invalid_format"
        self.context.msgOutput[message_vocab.NOMPERFORMER_TYPE] = "P1"

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_nominated_performer(self.context)

        self.assertEqual(str(cm.exception), "nominatedPerformer has invalid format")

    def test_invalid_nominated_performer_type(self):
        self.context.msgOutput[message_vocab.NOMPERFORMER] = "VALID123"
        self.context.msgOutput[message_vocab.NOMPERFORMER_TYPE] = "invalid_type"

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_nominated_performer(self.context)

        self.assertEqual(str(cm.exception), "nominatedPerformer has invalid type")


class TestCheckPrescriptionId(CommonPrescriptionValidatorTest):
    def test_valid_prescription_id(self):
        self.context.msgOutput[message_vocab.PRESCID] = "7D9625-Z72BF2-11E3AC"

        common_validator.check_prescription_id(self.context, self.internal_id, self.log_object)

        self.assertIn(message_vocab.PRESCID, self.context.outputFields)

    def test_invalid_prescription_id_format(self):
        self.context.msgOutput[message_vocab.PRESCID] = "invalid_format"

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_prescription_id(self.context, self.internal_id, self.log_object)

        self.assertEqual(str(cm.exception), message_vocab.PRESCID + " has invalid format")

    def test_invalid_prescription_id_checksum(self):
        self.context.msgOutput[message_vocab.PRESCID] = "7D9625-Z72BF2-11E3AX"

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_prescription_id(self.context, self.internal_id, self.log_object)

        self.assertEqual(str(cm.exception), message_vocab.PRESCID + " has invalid checksum")


class TestCheckOrganisationAndRoles(CommonPrescriptionValidatorTest):
    def setUp(self):
        super().setUp()
        self.context.msgOutput[message_vocab.AGENTORG] = "ORG12345"
        self.context.msgOutput[message_vocab.ROLEPROFILE] = "123456789012345"
        self.context.msgOutput[message_vocab.ROLE] = "ROLE"

    def test_valid_organisation_and_roles(self):
        common_validator.check_organisation_and_roles(
            self.context, self.internal_id, self.log_object
        )

        self.assertIn(message_vocab.AGENTORG, self.context.outputFields)
        self.assertIn(message_vocab.ROLEPROFILE, self.context.outputFields)
        self.assertIn(message_vocab.ROLE, self.context.outputFields)

    def test_invalid_organisation_format(self):
        self.context.msgOutput[message_vocab.AGENTORG] = "invalid_org"

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_organisation_and_roles(
                self.context, self.internal_id, self.log_object
            )

        self.assertEqual(str(cm.exception), message_vocab.AGENTORG + " has invalid format")

    def test_invalid_role_profile_format(self):
        self.context.msgOutput[message_vocab.ROLEPROFILE] = "invalid_role_profile"

        common_validator.check_organisation_and_roles(
            self.context, self.internal_id, self.log_object
        )

        self.assertTrue(self.log_object.logger.was_logged("EPS0323b"))
        self.assertIn(message_vocab.AGENTORG, self.context.outputFields)
        self.assertIn(message_vocab.ROLEPROFILE, self.context.outputFields)
        self.assertIn(message_vocab.ROLE, self.context.outputFields)

    def test_role_not_provided(self):
        self.context.msgOutput[message_vocab.ROLE] = "NotProvided"

        common_validator.check_organisation_and_roles(
            self.context, self.internal_id, self.log_object
        )

        self.assertTrue(self.log_object.logger.was_logged("EPS0330"))
        self.assertIn(message_vocab.AGENTORG, self.context.outputFields)
        self.assertIn(message_vocab.ROLEPROFILE, self.context.outputFields)
        self.assertIn(message_vocab.ROLE, self.context.outputFields)

    def test_invalid_role_format(self):
        self.context.msgOutput[message_vocab.ROLE] = "invalid_role"

        common_validator.check_organisation_and_roles(
            self.context, self.internal_id, self.log_object
        )

        self.assertTrue(self.log_object.logger.was_logged("EPS0323"))
        self.assertIn(message_vocab.AGENTORG, self.context.outputFields)
        self.assertIn(message_vocab.ROLEPROFILE, self.context.outputFields)
        self.assertIn(message_vocab.ROLE, self.context.outputFields)


class TestCheckNhsNumber(CommonPrescriptionValidatorTest):
    def test_valid_nhs_number(self):
        self.context.msgOutput[message_vocab.PATIENTID] = "9434765919"

        common_validator.check_nhs_number(self.context)

        self.assertIn(message_vocab.PATIENTID, self.context.outputFields)

    def test_invalid_nhs_number_format(self):
        self.context.msgOutput[message_vocab.PATIENTID] = "invalid_format"

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_nhs_number(self.context)

        self.assertEqual(str(cm.exception), message_vocab.PATIENTID + " is not valid")

    def test_invalid_nhs_number_checksum(self):
        self.context.msgOutput[message_vocab.PATIENTID] = "9434765918"

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_nhs_number(self.context)

        self.assertEqual(str(cm.exception), message_vocab.PATIENTID + " is not valid")


class TestCheckStandardDateTime(CommonPrescriptionValidatorTest):
    def test_valid_standard_date_time(self):
        self.context.msgOutput[message_vocab.CLAIM_DATE] = "20240101120000"

        common_validator.check_standard_date_time(
            self.context, message_vocab.CLAIM_DATE, self.internal_id, self.log_object
        )

        self.assertIn(message_vocab.CLAIM_DATE, self.context.outputFields)

    def test_valid_international_standard_date_time(self):
        self.context.msgOutput[message_vocab.CLAIM_DATE] = "20240101120000+0100"

        common_validator.check_standard_date_time(
            self.context, message_vocab.CLAIM_DATE, self.internal_id, self.log_object
        )

        self.assertIn(message_vocab.CLAIM_DATE, self.context.outputFields)

    def test_invalid_standard_date_time_format(self):
        self.context.msgOutput[message_vocab.CLAIM_DATE] = "invalid_format"

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_standard_date_time(
                self.context, message_vocab.CLAIM_DATE, self.internal_id, self.log_object
            )

        self.assertEqual(
            str(cm.exception),
            message_vocab.CLAIM_DATE
            + " is not a valid time or in the valid format; expected format %Y%m%d%H%M%S",
        )

    def test_invalid_international_standard_date_time_format(self):
        self.context.msgOutput[message_vocab.CLAIM_DATE] = "20240101120000+0200"

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_standard_date_time(
                self.context, message_vocab.CLAIM_DATE, self.internal_id, self.log_object
            )

        self.assertEqual(
            str(cm.exception),
            message_vocab.CLAIM_DATE
            + " is not a valid time or in the valid format; expected format %Y%m%d%H%M%S",
        )


class TestCheckStandardDate(CommonPrescriptionValidatorTest):
    def test_valid_standard_date(self):
        self.context.msgOutput[message_vocab.CLAIM_DATE] = "20240101"

        common_validator.check_standard_date(self.context, message_vocab.CLAIM_DATE)

        self.assertIn(message_vocab.CLAIM_DATE, self.context.outputFields)

    def test_invalid_standard_date_format(self):
        self.context.msgOutput[message_vocab.CLAIM_DATE] = "invalid_format"

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_standard_date(self.context, message_vocab.CLAIM_DATE)

        self.assertEqual(
            str(cm.exception),
            message_vocab.CLAIM_DATE
            + " is not a valid time or in the valid format; expected format %Y%m%d",
        )

    def test_invalid_standard_date_format_correct_length(self):
        self.context.msgOutput[message_vocab.CLAIM_DATE] = "20240132"

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_standard_date(self.context, message_vocab.CLAIM_DATE)

        self.assertEqual(
            str(cm.exception),
            message_vocab.CLAIM_DATE
            + " is not a valid time or in the valid format; expected format %Y%m%d",
        )


class TestCheckHL7EventID(CommonPrescriptionValidatorTest):
    def test_valid_hl7_event_id(self):
        self.context.msgOutput[message_vocab.HL7EVENTID] = "C0AB090A-FDDC-4B64-97AD-2319A2309C2F"

        common_validator.check_hl7_event_id(self.context)

        self.assertIn(message_vocab.HL7EVENTID, self.context.outputFields)

    def test_invalid_hl7_event_id_format(self):
        self.context.msgOutput[message_vocab.HL7EVENTID] = "invalid_format"

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_hl7_event_id(self.context)

        self.assertEqual(str(cm.exception), message_vocab.HL7EVENTID + " has invalid format")


class TestCheckMandatoryItems(CommonPrescriptionValidatorTest):
    def test_all_mandatory_items_present(self):
        mandatory_items = [message_vocab.PATIENTID, message_vocab.PRESCID]

        for item in mandatory_items:
            self.context.msgOutput[item] = "test_value"

        common_validator.check_mandatory_items(self.context, mandatory_items)

    def test_missing_mandatory_item(self):
        mandatory_items = [message_vocab.PATIENTID, message_vocab.PRESCID]

        self.context.msgOutput[message_vocab.PATIENTID] = "test"

        with self.assertRaises(EpsValidationError) as cm:
            common_validator.check_mandatory_items(self.context, mandatory_items)

        self.assertEqual(str(cm.exception), f"Mandatory field {message_vocab.PRESCID} missing")
