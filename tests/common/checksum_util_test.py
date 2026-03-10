from unittest import TestCase

from eps_spine_shared.common.checksum_util import check_checksum, remove_check_digit
from eps_spine_shared.testing.mock_logger import MockLogObject


class ChecksumUtilTest(TestCase):
    def setUp(self):
        self.logger = MockLogObject()

    def test_check_checksum_letter(self):
        is_valid = check_checksum("7D9625-Z72BF2-11E3AC", "test-internal-id", self.logger)
        self.assertTrue(is_valid)

    def test_check_checksum_plus(self):
        is_valid = check_checksum("E7ZG38-ZBACYU-V38SR+", "test-internal-id", self.logger)
        self.assertTrue(is_valid)

    def test_check_checksum_digit(self):
        is_valid = check_checksum("6FOCBU-E776BJ-CMPMT3", "test-internal-id", self.logger)
        self.assertTrue(is_valid)

    def test_check_checksum_invalid(self):
        is_valid = check_checksum("6FOCBU-E776BJ-CMPMTX", "test-internal-id", self.logger)
        self.assertFalse(is_valid)

    def test_remove_check_digit_removes_check_digit_and_preserves_original(self):
        prescription_id = "7D9625-Z72BF2-11E3AC"
        expected = "7D9625-Z72BF2-11E3A"
        result = remove_check_digit(prescription_id)

        self.assertEqual(result, expected)
        self.assertEqual(prescription_id, "7D9625-Z72BF2-11E3AC")

    def test_remove_check_digit_only_removes_from_correct_length(self):
        prescription_id = "7D9625-Z72BF2-11E3A"
        expected = "7D9625-Z72BF2-11E3A"
        result = remove_check_digit(prescription_id)

        self.assertEqual(result, expected)
