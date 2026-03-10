from unittest.case import TestCase
from unittest.mock import Mock

from eps_spine_shared.common.prescription import fields
from eps_spine_shared.common.prescription.record import PrescriptionRecord


class IncludeNextActivityForInstanceTest(TestCase):
    """
    Test Case for testing the Include Next Activity for Instance Test
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

        self.mock_record: PrescriptionRecord = PrescriptionRecord(log_object, internal_id)

    def test_include_next_activity_1(self):
        """
        Test that 'True' is returned for acute, current, first and final issue when:
         - currentInstance = 1,
         - instanceNumber = 1,
         - max_repeats = 1,
         - nextActivity = expire
        """
        activity = fields.NEXTACTIVITY_EXPIRE
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 1, 1, 1))

    def test_include_next_activity_2(self):
        """
        Test that 'True' is returned for acute, current, first and final issue when:
         - currentInstance = 1,
         - instanceNumber = 1,
         - max_repeats = 1,
         - nextActivity = createNoClaim
        """
        activity = fields.NEXTACTIVITY_CREATENOCLAIM
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 1, 1, 1))

    def test_include_next_activity_3(self):
        """
        Test that 'True' is returned for acute, current, first and final issue when:
         - currentInstance = 1,
         - instanceNumber = 1,
         - max_repeats = 1,
         - nextActivity = ready
        """
        activity = fields.NEXTACTIVITY_READY
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 1, 1, 1))

    def test_include_next_activity_4(self):
        """
        Test that 'True' is returned for acute, current, first and final issue when:
         - currentInstance = 1,
         - instanceNumber = 1,
         - max_repeats = 1,
         - nextActivity = delete
        """
        activity = fields.NEXTACTIVITY_DELETE
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 1, 1, 1))

    def test_include_next_activity_5(self):
        """
        Test that 'True' is returned for repeat dispense, current and first issue when:
         - currentInstance = 1,
         - instanceNumber = 1,
         - max_repeats = 3,
         - nextActivity = expire
        """
        activity = fields.NEXTACTIVITY_EXPIRE
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 1, 1, 3))

    def test_include_next_activity_6(self):
        """
        Test that 'True' is returned for repeat dispense, current but not final issue when:
         - currentInstance = 1,
         - instanceNumber = 1,
         - max_repeats = 3,
         - nextActivity = createNoClaim
        """
        activity = fields.NEXTACTIVITY_CREATENOCLAIM
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 1, 1, 3))

    def test_include_next_activity_7(self):
        """
        Test that 'True' is returned for repeat dispense, current but not final issue when:
         - currentInstance = 1,
         - instanceNumber = 1,
         - max_repeats = 3,
         - nextActivity = ready
        """
        activity = fields.NEXTACTIVITY_READY
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 1, 1, 3))

    def test_include_next_activity_8(self):
        """
        Test that 'False' is returned for repeat dispense, current but not final issue when:
         - currentInstance = 1,
         - instanceNumber = 1,
         - max_repeats = 3,
         - nextActivity = delete
        """
        activity = fields.NEXTACTIVITY_DELETE
        self.assertFalse(self.mock_record._include_next_activity_for_instance(activity, 1, 1, 3))

    def test_include_next_activity_9(self):
        """
        Test that 'False' is returned for repeat dispense, previous issue when:
         - currentInstance = 2,
         - instanceNumber = 1,
         - max_repeats = 3,
         - nextActivity = expire
        """
        activity = fields.NEXTACTIVITY_EXPIRE
        self.assertFalse(self.mock_record._include_next_activity_for_instance(activity, 1, 2, 3))

    def test_include_next_activity_10(self):
        """
        Test that 'True' is returned for repeat dispense, previous issue when:
         - currentInstance = 2,
         - instanceNumber = 1,
         - max_repeats = 3,
         - nextActivity = createNoClaim
        """
        activity = fields.NEXTACTIVITY_CREATENOCLAIM
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 1, 2, 3))

    def test_include_next_activity_11(self):
        """
        Test that 'False' is returned for repeat dispense, previous issue when:
         - currentInstance = 2,
         - instanceNumber = 1,
         - max_repeats = 3,
         - nextActivity = ready
        """
        activity = fields.NEXTACTIVITY_READY
        self.assertFalse(self.mock_record._include_next_activity_for_instance(activity, 1, 2, 3))

    def test_include_next_activity_12(self):
        """
        Test that 'False' is returned for repeat dispense, previous issue when:
         - currentInstance = 2,
         - instanceNumber = 1,
         - max_repeats = 3,
         - nextActivity = delete
        """
        activity = fields.NEXTACTIVITY_DELETE
        self.assertFalse(self.mock_record._include_next_activity_for_instance(activity, 1, 2, 3))

    def test_include_next_activity_13(self):
        """
        Test that 'True' is returned for repeat dispense, current but not first or final issue when:
         - currentInstance = 2,
         - instanceNumber = 2,
         - max_repeats = 3,
         - nextActivity = expire
        """
        activity = fields.NEXTACTIVITY_EXPIRE
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 2, 2, 3))

    def test_include_next_activity_14(self):
        """
        Test that 'True' is returned for repeat dispense, current but not first or final issue when:
         - currentInstance = 2,
         - instanceNumber = 2,
         - max_repeats = 3,
         - nextActivity = createNoClaim
        """
        activity = fields.NEXTACTIVITY_CREATENOCLAIM
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 2, 2, 3))

    def test_include_next_activity_15(self):
        """
        Test that 'True' is returned for repeat dispense, current but not first or final issue when:
         - currentInstance = 2,
         - instanceNumber = 2,
         - max_repeats = 3,
         - nextActivity = ready
        """
        activity = fields.NEXTACTIVITY_READY
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 2, 2, 3))

    def test_include_next_activity_16(self):
        """
        Test that 'False' is returned for repeat dispense, current but not first or final issue when:
         - currentInstance = 2,
         - instanceNumber = 2,
         - max_repeats = 3,
         - nextActivity = delete
        """
        activity = fields.NEXTACTIVITY_DELETE
        self.assertFalse(self.mock_record._include_next_activity_for_instance(activity, 2, 2, 3))

    def test_include_next_activity_17(self):
        """
        Test that 'True' is returned for repeat dispense, current and final issue when:
         - currentInstance = 3,
         - instanceNumber = 3,
         - max_repeats = 3,
         - nextActivity = expire
        """
        activity = fields.NEXTACTIVITY_EXPIRE
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 3, 3, 3))

    def test_include_next_activity_18(self):
        """
        Test that 'True' is returned for repeat dispense, current and final issue when:
         - currentInstance = 3,
         - instanceNumber = 3,
         - max_repeats = 3,
         - nextActivity = createNoClaim
        """
        activity = fields.NEXTACTIVITY_CREATENOCLAIM
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 3, 3, 3))

    def test_include_next_activity_19(self):
        """
        Test that 'True' is returned for repeat dispense, current and final issue when:
         - currentInstance = 3,
         - instanceNumber = 3,
         - max_repeats = 3,
         - nextActivity = ready
        """
        activity = fields.NEXTACTIVITY_READY
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 3, 3, 3))

    def test_include_next_activity_20(self):
        """
        Test that 'True' is returned for repeat dispense, current and final issue when:
         - currentInstance = 3,
         - instanceNumber = 3,
         - max_repeats = 3,
         - nextActivity = delete
        """
        activity = fields.NEXTACTIVITY_DELETE
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 3, 3, 3))

    def test_include_next_activity_21(self):
        """
        Test that 'False' is returned for repeat dispense, future issue when:
         - currentInstance = 1,
         - instanceNumber = 3,
         - max_repeats = 3,
         - nextActivity = expire
        """
        activity = fields.NEXTACTIVITY_EXPIRE
        self.assertFalse(self.mock_record._include_next_activity_for_instance(activity, 3, 1, 3))

    def test_include_next_activity_22(self):
        """
        Test that 'False' is returned for repeat dispense, future issue when:
         - currentInstance = 1,
         - instanceNumber = 3,
         - max_repeats = 3,
         - nextActivity = createNoClaim
        """
        activity = fields.NEXTACTIVITY_CREATENOCLAIM
        self.assertFalse(self.mock_record._include_next_activity_for_instance(activity, 3, 1, 3))

    def test_include_next_activity_23(self):
        """
        Test that 'False' is returned for repeat dispense, future issue when:
         - currentInstance = 1,
         - instanceNumber = 3,
         - max_repeats = 3,
         - nextActivity = ready
        """
        activity = fields.NEXTACTIVITY_READY
        self.assertFalse(self.mock_record._include_next_activity_for_instance(activity, 3, 1, 3))

    def test_include_next_activity_24(self):
        """
        Test that 'False' is returned for repeat dispense, future issue when:
         - currentInstance = 1,
         - instanceNumber = 3,
         - max_repeats = 3,
         - nextActivity = delete
        """
        activity = fields.NEXTACTIVITY_DELETE
        self.assertFalse(self.mock_record._include_next_activity_for_instance(activity, 3, 1, 3))

    def test_include_next_activity_25(self):
        """
        Test that 'True' is returned for acute, curent, first and final issue when:
         - currentInstance = 1,
         - instanceNumber = 1,
         - max_repeats = 1,
         - nextActivity = purge
        """
        activity = fields.NEXTACTIVITY_PURGE
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 1, 1, 1))

    def test_include_next_activity_26(self):
        """
        Test that 'False' is returned for repeat dispense, current but not final issue when:
         - currentInstance = 1,
         - instanceNumber = 1,
         - max_repeats = 3,
         - nextActivity = purge
        """
        activity = fields.NEXTACTIVITY_PURGE
        self.assertFalse(self.mock_record._include_next_activity_for_instance(activity, 1, 1, 3))

    def test_include_next_activity_27(self):
        """
        Test that 'False' is returned for repeat dispense, previous issue when:
         - currentInstance = 2,
         - instanceNumber = 1,
         - max_repeats = 3,
         - nextActivity = purge
        """
        activity = fields.NEXTACTIVITY_PURGE
        self.assertFalse(self.mock_record._include_next_activity_for_instance(activity, 1, 2, 3))

    def test_include_next_activity_28(self):
        """
        Test that 'False' is returned for repeat dispense, current but not first or final issue when:
         - currentInstance = 2,
         - instanceNumber = 2,
         - max_repeats = 3,
         - nextActivity = purge
        """
        activity = fields.NEXTACTIVITY_PURGE
        self.assertFalse(self.mock_record._include_next_activity_for_instance(activity, 2, 2, 3))

    def test_include_next_activity_29(self):
        """
        Test that 'True' is returned for repeat dispense, current and final issue when:
         - currentInstance = 3,
         - instanceNumber = 3,
         - max_repeats = 3,
         - nextActivity = purge
        """
        activity = fields.NEXTACTIVITY_PURGE
        self.assertTrue(self.mock_record._include_next_activity_for_instance(activity, 3, 3, 3))

    def test_include_next_activity_30(self):
        """
        Test that 'False' is returned for repeat dispense, future issue when:
         - currentInstance = 1,
         - instanceNumber = 3,
         - max_repeats = 3,
         - nextActivity = purge
        """
        activity = fields.NEXTACTIVITY_PURGE
        self.assertFalse(self.mock_record._include_next_activity_for_instance(activity, 3, 1, 3))
