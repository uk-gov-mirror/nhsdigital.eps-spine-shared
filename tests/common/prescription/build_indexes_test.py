from unittest.case import TestCase
from unittest.mock import Mock

from eps_spine_shared.common.prescription.record import PrescriptionRecord


class BuildIndexesTest(TestCase):
    """
    Test Case for testing that indexes are built correctly
    """

    def setUp(self):
        """
        Set up all valid values - tests will overwrite these where required.
        """
        mock = Mock()
        attrs = {"writeLog.return_value": None}
        mock.configure_mock(**attrs)
        log_object = mock
        internal_id = "test"

        self.prescription = PrescriptionRecord(log_object, internal_id)
        self.prescription.prescription_record = {}
        self.prescription.prescription_record["prescription"] = {}
        self.prescription.prescription_record["instances"] = {}
        self.prescription.prescription_record["patient"] = {}
        self.prescription.prescription_record["patient"]["nhsNumber"] = "TESTPatient"

    def test_add_release_and_status_string(self):
        """
        tests that release and status are added to the passed in index.
        """
        is_string = True
        index_prefix = "indexPrefix"
        # set prescription to be 37 characters long ie R1
        temp = "0123456789012345678901234567890123456"
        self.prescription.prescription_record["prescription"]["prescriptionID"] = temp
        self.prescription.prescription_record["instances"]["0"] = {}
        self.prescription.prescription_record["instances"]["0"]["prescriptionStatus"] = "0001"
        result_set = self.prescription.add_release_and_status(index_prefix, is_string)
        self.assertEqual(
            result_set,
            ["indexPrefix|R1|0001"],
            "Failed to create expected release and status suffix",
        )

    def test_add_release_and_status_list(self):
        """
        tests that release and status are added to the passed in index where the passed in index is a list of indexes.
        """
        is_string = False
        index_prefix = ["indexPrefix1", "indexPrefix2"]
        # set prescription to be 37 characters long ie R1
        temp = "0123456789012345678901234567890123456"
        self.prescription.prescription_record["prescription"]["prescriptionID"] = temp
        self.prescription.prescription_record["instances"]["0"] = {}
        self.prescription.prescription_record["instances"]["0"]["prescriptionStatus"] = "0001"
        result_set = self.prescription.add_release_and_status(index_prefix, is_string)
        self.assertEqual(
            result_set,
            ["indexPrefix1|R1|0001", "indexPrefix2|R1|0001"],
            "Failed to create expected release and status suffix for list of indexes",
        )

    def test_add_release_and_status_string_multiple_status(self):
        """
        tests that release and multiple status are added to the passed in index.
        """
        is_string = True
        index_prefix = "indexPrefix"
        # set prescription to be 37 characters long ie R1
        temp = "0123456789012345678901234567890123456"
        self.prescription.prescription_record["prescription"]["prescriptionID"] = temp
        self.prescription.prescription_record["instances"]["0"] = {}
        self.prescription.prescription_record["instances"]["0"]["prescriptionStatus"] = "0001"
        self.prescription.prescription_record["instances"]["1"] = {}
        self.prescription.prescription_record["instances"]["1"]["prescriptionStatus"] = "0002"
        result_set = self.prescription.add_release_and_status(index_prefix, is_string)
        self.assertEqual(
            sorted(result_set),
            sorted(["indexPrefix|R1|0001", "indexPrefix|R1|0002"]),
            "Failed to create expected release and status suffix",
        )

    def test_nhs_num_presc_disp_index(self):
        """
        Given a prescription for a specific NHS Number and Prescriber that has been dispensed
        that the correct index is created
        """
        self.prescription.prescription_record["prescription"][
            "prescribingOrganization"
        ] = "TESTPrescriber"
        self.prescription.prescription_record["prescription"]["prescriptionTime"] = "TESTtime"
        self.prescription.prescription_record["instances"]["0"] = {}
        self.prescription.prescription_record["instances"]["0"]["dispense"] = {}
        self.prescription.prescription_record["instances"]["0"]["dispense"][
            "dispensingOrganization"
        ] = "TESTdispenser"

        [success, created_index] = (
            self.prescription.return_nhs_number_prescriber_dispenser_date_index()
        )
        self.assertEqual(success, True, "Failed to successfully create index")
        expected_index = set(["TESTPatient|TESTPrescriber|TESTdispenser|TESTtime"])
        self.assertEqual(
            created_index,
            expected_index,
            "Created index " + str(created_index) + " expecting " + str(expected_index),
        )

    def test_nhs_num_presc_disp_index_no_dispenser(self):
        """
        Given a prescription for a specific NHS Number and Prescriber that has been dispensed
        that the correct index is created
        """
        self.prescription.prescription_record["prescription"][
            "prescribingOrganization"
        ] = "TESTPrescriber"
        self.prescription.prescription_record["prescription"]["prescriptionTime"] = "TESTtime"

        [success, created_index] = (
            self.prescription.return_nhs_number_prescriber_dispenser_date_index()
        )
        self.assertEqual(success, True, "Failed to successfully create index")
        expected_index = set([])
        self.assertEqual(
            created_index,
            expected_index,
            "Created index " + str(created_index) + " expecting " + str(expected_index),
        )

    def test_presc_disp_index(self):
        """
        Given a prescription for a specific NHS Number and Prescriber that has been dispensed
        that the correct index is created
        """
        self.prescription.prescription_record["prescription"][
            "prescribingOrganization"
        ] = "TESTPrescriber"
        self.prescription.prescription_record["prescription"]["prescriptionTime"] = "TESTtime"
        self.prescription.prescription_record["instances"]["0"] = {}
        self.prescription.prescription_record["instances"]["0"]["dispense"] = {}
        self.prescription.prescription_record["instances"]["0"]["dispense"][
            "dispensingOrganization"
        ] = "TESTdispenser"

        [success, created_index] = self.prescription.return_prescriber_dispenser_date_index()
        self.assertEqual(success, True, "Failed to successfully create index")
        expected_index = set(["TESTPrescriber|TESTdispenser|TESTtime"])
        self.assertEqual(
            created_index,
            expected_index,
            "Created index " + str(created_index) + " expecting " + str(expected_index),
        )

    def test_presc_disp_index_no_dispenser(self):
        """
        Given a prescription for a specific NHS Number and Prescriber that has been dispensed
        that the correct index is created
        """
        self.prescription.prescription_record["prescription"][
            "prescribingOrganization"
        ] = "TESTPrescriber"
        self.prescription.prescription_record["prescription"]["prescriptionTime"] = "TESTtime"

        [success, created_index] = self.prescription.return_prescriber_dispenser_date_index()
        self.assertEqual(success, True, "Failed to successfully create index")
        expected_index = set([])
        self.assertEqual(
            created_index,
            expected_index,
            "Created index " + str(created_index) + " expecting " + str(expected_index),
        )

    def test_disp_index(self):
        """
        Given a prescription for a specific NHS Number and Prescriber that has been dispensed
        that the correct index is created
        """
        self.prescription.prescription_record["prescription"][
            "prescribingOrganization"
        ] = "TESTPrescriber"
        self.prescription.prescription_record["prescription"]["prescriptionTime"] = "TESTtime"
        self.prescription.prescription_record["instances"]["0"] = {}
        self.prescription.prescription_record["instances"]["0"]["dispense"] = {}
        self.prescription.prescription_record["instances"]["0"]["dispense"][
            "dispensingOrganization"
        ] = "TESTdispenser"

        [success, created_index] = self.prescription.return_dispenser_date_index()
        self.assertEqual(success, True, "Failed to successfully create index")
        expected_index = set(["TESTdispenser|TESTtime"])
        self.assertEqual(
            created_index,
            expected_index,
            "Created index " + str(created_index) + " expecting " + str(expected_index),
        )

    def test_disp_index_no_dispenser(self):
        """
        Given a prescription for a specific NHS Number and Prescriber that has been dispensed
        that the correct index is created
        """
        self.prescription.prescription_record["prescription"][
            "prescribingOrganization"
        ] = "TESTPrescriber"
        self.prescription.prescription_record["prescription"]["prescriptionTime"] = "TESTtime"

        [success, created_index] = self.prescription.return_dispenser_date_index()
        self.assertEqual(success, True, "Failed to successfully create index")
        expected_index = set([])
        self.assertEqual(
            created_index,
            expected_index,
            "Created index " + str(created_index) + " expecting " + str(expected_index),
        )

    def test_nhs_num_disp_index(self):
        """
        Given a prescription for a specific NHS Number and Prescriber that has been dispensed
        that the correct index is created
        """
        self.prescription.prescription_record["prescription"][
            "prescribingOrganization"
        ] = "TESTPrescriber"
        self.prescription.prescription_record["prescription"]["prescriptionTime"] = "TESTtime"
        self.prescription.prescription_record["instances"]["0"] = {}
        self.prescription.prescription_record["instances"]["0"]["dispense"] = {}
        self.prescription.prescription_record["instances"]["0"]["dispense"][
            "dispensingOrganization"
        ] = "TESTdispenser"

        [success, created_index] = self.prescription.return_nhs_number_dispenser_date_index()
        self.assertEqual(success, True, "Failed to successfully create index")
        expected_index = set(["TESTPatient|TESTdispenser|TESTtime"])
        self.assertEqual(
            created_index,
            expected_index,
            "Created index " + str(created_index) + " expecting " + str(expected_index),
        )

    def test_nhs_num_disp_index_no_dispenser(self):
        """
        Given a prescription for a specific NHS Number and Prescriber that has been dispensed
        that the correct index is created
        """
        self.prescription.prescription_record["prescription"][
            "prescribingOrganization"
        ] = "TESTPrescriber"
        self.prescription.prescription_record["prescription"]["prescriptionTime"] = "TESTtime"

        [success, created_index] = self.prescription.return_nhs_number_dispenser_date_index()
        self.assertEqual(success, True, "Failed to successfully create index")
        expected_index = set([])
        self.assertEqual(
            created_index,
            expected_index,
            "Created index " + str(created_index) + " expecting " + str(expected_index),
        )
