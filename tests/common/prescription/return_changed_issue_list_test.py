from unittest.case import TestCase
from unittest.mock import Mock

from eps_spine_shared.common.prescription.repeat_dispense import RepeatDispenseRecord


class ReturnChangedIssueListTest(TestCase):
    """
    Returns the list of changed issues.
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

        self.mock_record = RepeatDispenseRecord(log_object, internal_id)
        self.pre_change_dict = {
            "issue1": {"lineItems": {"1": "0001", "2": "0001"}, "prescription": "0006"},
            "issue2": {"lineItems": {"1": "0008", "2": "0008"}, "prescription": "0002"},
            "issue3": {"lineItems": {"1": "0007", "2": "0007"}, "prescription": "9000"},
        }
        self.post_change_dict = {
            "issue1": {"lineItems": {"1": "0001", "2": "0001"}, "prescription": "0006"},
            "issue2": {"lineItems": {"1": "0008", "2": "0008"}, "prescription": "0002"},
            "issue3": {"lineItems": {"1": "0007", "2": "0007"}, "prescription": "9000"},
        }
        self.max_repeats = 3
        self.expected_result = None

    def run_return_changed_issue_list_test(self):
        """
        Execute the test
        """
        result_set = self.mock_record.return_changed_issue_list(
            self.pre_change_dict, self.post_change_dict, self.max_repeats
        )
        self.assertEqual(result_set, self.expected_result)

    def test_identical_dicts(self):
        """
        No difference in content
        """
        self.expected_result = []
        self.run_return_changed_issue_list_test()

    def test_identical_dicts_out_of_order(self):
        """
        Out of order elements, but key:value pairs unchanged
        """
        self.post_change_dict = {
            "issue1": {"lineItems": {"1": "0001", "2": "0001"}, "prescription": "0006"},
            "issue3": {"prescription": "9000", "lineItems": {"2": "0007", "1": "0007"}},
            "issue2": {"lineItems": {"2": "0008", "1": "0008"}, "prescription": "0002"},
        }
        self.expected_result = []
        self.run_return_changed_issue_list_test()

    def test_missing_issue_from_pre_change_dict(self):
        """
        Issue missing from pre change dict
        """
        del self.pre_change_dict["issue2"]
        self.expected_result = ["2"]
        self.run_return_changed_issue_list_test()

    def test_missing_issue_from_post_change_dict(self):
        """
        Issue missing from pre change dict
        """
        del self.post_change_dict["issue2"]
        self.expected_result = ["2"]
        self.run_return_changed_issue_list_test()

    def test_single_item_status_change(self):
        """
        Test that a single line item difference is identified
        """
        self.post_change_dict["issue1"]["lineItems"]["1"] = "0002"
        self.expected_result = ["1"]
        self.run_return_changed_issue_list_test()

    def test_single_prescription_status_change(self):
        """
        Test that a single prescription difference is identified
        """
        self.post_change_dict["issue1"]["prescription"] = "0007"
        self.expected_result = ["1"]
        self.run_return_changed_issue_list_test()

    def test_multiple_combination_status_change(self):
        """
        Test that a multiple line item and prescription differences are identified
        """
        self.post_change_dict["issue1"]["lineItems"]["1"] = "0002"
        self.post_change_dict["issue1"]["lineItems"]["2"] = "0003"
        self.post_change_dict["issue3"]["prescription"] = "0006"
        self.post_change_dict["issue3"]["prescription"] = "0007"
        self.expected_result = ["1", "3"]
        self.run_return_changed_issue_list_test()
