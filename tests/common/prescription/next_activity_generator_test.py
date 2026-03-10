from datetime import datetime
from unittest.case import TestCase

from dateutil.relativedelta import relativedelta

from eps_spine_shared.common.prescription.record import NextActivityGenerator


class NextActivityGeneratorTest(TestCase):
    """
    Test Case for the next activity index generator
    """

    def setUp(self):
        """
        Set up all valid values - tests will overwrite these where required.
        """
        self.next_activity_generator = NextActivityGenerator(None, None)

        self.nad_reference = {}
        self.nad_reference["prescriptionExpiryPeriod"] = relativedelta(months=+6)
        self.nad_reference["repeatDispenseExpiryPeriod"] = relativedelta(months=+12)
        self.nad_reference["dataCleansePeriod"] = relativedelta(months=+6)
        self.nad_reference["withDispenserActiveExpiryPeriod"] = relativedelta(days=+180)
        self.nad_reference["expiredDeletePeriod"] = relativedelta(days=+90)
        self.nad_reference["cancelledDeletePeriod"] = relativedelta(days=+180)
        self.nad_reference["claimedDeletePeriod"] = relativedelta(days=+9)
        self.nad_reference["notDispensedDeletePeriod"] = relativedelta(days=+30)
        self.nad_reference["nominatedDownloadDateLeadTime"] = relativedelta(days=+5)
        self.nad_reference["notificationDelayPeriod"] = relativedelta(days=+180)
        self.nad_reference["purgedDeletePeriod"] = relativedelta(days=+365)

        self.nad_status = {}
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionDate"] = "20120101"
        self.nad_status["prescribingSiteTestStatus"] = True
        self.nad_status["dispenseWindowHighDate"] = "20121231"
        self.nad_status["dispenseWindowLowDate"] = "20120101"
        # The nominated download date is the date that the next issue should be released
        # for download (already taking account of the lead time)
        self.nad_status["nominatedDownloadDate"] = "20120101"
        self.nad_status["lastDispenseDate"] = "20120101"
        self.nad_status["completionDate"] = "20120101"
        self.nad_status["claimSentDate"] = "20120101"
        self.nad_status["handleTime"] = "20120101"
        self.nad_status["prescriptionStatus"] = "0001"
        self.nad_status["instanceNumber"] = 1
        self.nad_status["releaseVersion"] = "R2"
        self.nad_status["lastDispenseNotificationMsgRef"] = "20180918150922275520_2FA340_2"

    def perform_test_next_activity_date(self, expected_result):
        """
        Test Runner for next activity and next activity date method. Takes the created
        nad_status (on self) and compares it to the expected result
        """
        results = self.next_activity_generator.next_activity_date(
            self.nad_status, self.nad_reference
        )
        [next_activity, next_activity_date, _] = results
        self.assertTrue([next_activity, next_activity_date] == expected_result)

    def test_next_activity_date_scenario_1(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute - expiry falls 31st -> 1st
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0001"
        self.nad_status["prescriptionDate"] = "20111031"
        self.perform_test_next_activity_date(["expire", "20120430"])

    def test_next_activity_date_scenario_2(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0001"
        self.nad_status["prescriptionDate"] = "20110829"
        self.perform_test_next_activity_date(["expire", "20120229"])

    def test_next_activity_date_scenario_3(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Prescribe - expiry falls 31st -> 1st
        """
        self.nad_status["prescriptionTreatmentType"] = "0002"
        self.nad_status["prescriptionStatus"] = "0001"
        self.nad_status["prescriptionDate"] = "20111031"
        self.perform_test_next_activity_date(["expire", "20120430"])

    def test_next_activity_date_scenario_4(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Prescribe - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0002"
        self.nad_status["prescriptionStatus"] = "0001"
        self.nad_status["prescriptionDate"] = "20110829"
        self.perform_test_next_activity_date(["expire", "20120229"])

    def test_next_activity_date_scenario_5(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - expiry falls 31st -> 1st
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0001"
        self.nad_status["prescriptionDate"] = "20111031"
        self.nad_status["dispenseWindowHighDate"] = "20120601"
        self.perform_test_next_activity_date(["expire", "20120430"])

    def test_next_activity_date_scenario_6(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - check that expiry is not limited by Dispense Window
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0001"
        self.nad_status["prescriptionDate"] = "20120131"
        self.nad_status["dispenseWindowHighDate"] = "20120401"
        self.perform_test_next_activity_date(["expire", "20120731"])

    def test_next_activity_date_scenario_7(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0002"
        self.nad_status["prescriptionDate"] = "20110829"
        self.perform_test_next_activity_date(["expire", "20120229"])

    def test_next_activity_date_scenario_8(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute - expiry falls 31st -> 1st
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0002"
        self.nad_status["prescriptionDate"] = "20111031"
        self.perform_test_next_activity_date(["expire", "20120430"])

    def test_next_activity_date_scenario_9(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Prescribe - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0002"
        self.nad_status["prescriptionStatus"] = "0002"
        self.nad_status["prescriptionDate"] = "20110829"
        self.perform_test_next_activity_date(["expire", "20120229"])

    def test_next_activity_date_scenario_10(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Prescribe - expiry falls 31st -> 1st
        """
        self.nad_status["prescriptionTreatmentType"] = "0002"
        self.nad_status["prescriptionStatus"] = "0002"
        self.nad_status["prescriptionDate"] = "20111031"
        self.perform_test_next_activity_date(["expire", "20120430"])

    def test_next_activity_date_scenario_11(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0002"
        self.nad_status["prescriptionDate"] = "20110829"
        self.nad_status["dispenseWindowHighDate"] = "20120601"
        self.perform_test_next_activity_date(["expire", "20120229"])

    def test_next_activity_date_scenario_12(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - expiry falls 31st -> 1st
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0002"
        self.nad_status["prescriptionDate"] = "20111031"
        self.nad_status["dispenseWindowHighDate"] = "20120601"
        self.perform_test_next_activity_date(["expire", "20120430"])

    def test_next_activity_date_scenario_13(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - check that expiry is not limited by Dispense Window
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0002"
        self.nad_status["prescriptionDate"] = "20111031"
        self.nad_status["dispenseWindowHighDate"] = "20120401"
        self.perform_test_next_activity_date(["expire", "20120430"])

    def test_next_activity_date_scenario_14(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0003"
        self.nad_status["prescriptionDate"] = "20110829"
        self.nad_status["lastDispenseDate"] = "20110928"
        self.perform_test_next_activity_date(["createNoClaim", "20120326"])

    def test_next_activity_date_scenario_14b(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute R1 - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0003"
        self.nad_status["prescriptionDate"] = "20110829"
        self.nad_status["lastDispenseDate"] = "20110928"
        self.nad_status["releaseVersion"] = "R1"
        self.perform_test_next_activity_date(["expire", "20120229"])

    def test_next_activity_date_scenario_15(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute - expiry falls 31st -> 1st
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0003"
        self.nad_status["prescriptionDate"] = "20111031"
        self.nad_status["lastDispenseDate"] = "20111130"
        self.perform_test_next_activity_date(["createNoClaim", "20120528"])

    def test_next_activity_date_scenario_15b(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute R1 - expiry falls 31st -> 1st
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0003"
        self.nad_status["prescriptionDate"] = "20111031"
        self.nad_status["lastDispenseDate"] = "20111130"
        self.nad_status["releaseVersion"] = "R1"
        self.perform_test_next_activity_date(["expire", "20120430"])

    def test_next_activity_date_scenario_16(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Prescribe - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0002"
        self.nad_status["prescriptionStatus"] = "0003"
        self.nad_status["prescriptionDate"] = "20110829"
        self.nad_status["lastDispenseDate"] = "20110928"
        self.perform_test_next_activity_date(["createNoClaim", "20120326"])

    def test_next_activity_date_scenario_17(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Prescribe - expiry falls 31st -> 1st
        """
        self.nad_status["prescriptionTreatmentType"] = "0002"
        self.nad_status["prescriptionStatus"] = "0003"
        self.nad_status["prescriptionDate"] = "20111031"
        self.nad_status["lastDispenseDate"] = "20111130"
        self.perform_test_next_activity_date(["createNoClaim", "20120528"])

    def test_next_activity_date_scenario_18(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0003"
        self.nad_status["prescriptionDate"] = "20110829"
        self.nad_status["dispenseWindowHighDate"] = "20120601"
        self.nad_status["lastDispenseDate"] = "20110928"
        self.perform_test_next_activity_date(["createNoClaim", "20120326"])

    def test_next_activity_date_scenario_19(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - expiry falls 31st -> 1st
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0003"
        self.nad_status["prescriptionDate"] = "20111031"
        self.nad_status["dispenseWindowHighDate"] = "20120601"
        self.nad_status["lastDispenseDate"] = "20111130"
        self.perform_test_next_activity_date(["createNoClaim", "20120528"])

    def test_next_activity_date_scenario_20(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - check that expiry date is not limited by Dispense Window
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0003"
        self.nad_status["prescriptionDate"] = "20111031"
        self.nad_status["dispenseWindowHighDate"] = "20120401"
        self.nad_status["lastDispenseDate"] = "20120301"
        self.perform_test_next_activity_date(["createNoClaim", "20120828"])

    def test_next_activity_date_scenario_21(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - no claim window falls before expiry
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0003"
        self.nad_status["prescriptionDate"] = "20111031"
        self.nad_status["dispenseWindowHighDate"] = "20120601"
        self.nad_status["lastDispenseDate"] = "20111031"
        self.perform_test_next_activity_date(["createNoClaim", "20120428"])

    def test_next_activity_date_scenario_22(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0004"
        self.nad_status["prescriptionDate"] = "20110729"
        self.nad_status["completionDate"] = "20120329"
        self.perform_test_next_activity_date(["delete", "20120627"])

    def test_next_activity_date_scenario_23(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Prescribe - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0002"
        self.nad_status["prescriptionStatus"] = "0004"
        self.nad_status["prescriptionDate"] = "20110729"
        self.nad_status["completionDate"] = "20120329"
        self.perform_test_next_activity_date(["delete", "20120627"])

    def test_next_activity_date_scenario_24(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0004"
        self.nad_status["prescriptionDate"] = "20110729"
        self.nad_status["completionDate"] = "20120329"
        self.perform_test_next_activity_date(["delete", "20120627"])

    def test_next_activity_date_scenario_25(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0005"
        self.nad_status["prescriptionDate"] = "20110729"
        self.nad_status["completionDate"] = "20120329"
        self.perform_test_next_activity_date(["delete", "20120925"])

    def test_next_activity_date_scenario_25a(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Specific test for migrated data scenario where completionDate is false not a valid
        date.
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0005"
        self.nad_status["prescriptionDate"] = "20110729"
        self.nad_status["completionDate"] = False
        expectedDate = datetime.now() + relativedelta(days=+180)
        self.perform_test_next_activity_date(["delete", expectedDate.strftime("%Y%m%d")])

    def test_next_activity_date_scenario_26(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Prescribe - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0002"
        self.nad_status["prescriptionStatus"] = "0005"
        self.nad_status["prescriptionDate"] = "20110729"
        self.nad_status["completionDate"] = "20120329"
        self.perform_test_next_activity_date(["delete", "20120925"])

    def test_next_activity_date_scenario_27(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0005"
        self.nad_status["prescriptionDate"] = "20110729"
        self.nad_status["completionDate"] = "20120329"
        self.perform_test_next_activity_date(["delete", "20120925"])

    def test_next_activity_date_scenario_28(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0006"
        self.nad_status["prescriptionDate"] = "20110729"
        self.nad_status["dispenseWindowHighDate"] = "20120728"
        self.nad_status["lastDispenseDate"] = "20110831"
        self.nad_status["completionDate"] = "20110831"
        self.perform_test_next_activity_date(["createNoClaim", "20120227"])

    def test_next_activity_date_scenario_28b(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute R1 - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0006"
        self.nad_status["prescriptionDate"] = "20110729"
        self.nad_status["dispenseWindowHighDate"] = "20120728"
        self.nad_status["lastDispenseDate"] = "20110831"
        self.nad_status["completionDate"] = "20110831"
        self.nad_status["releaseVersion"] = "R1"
        self.perform_test_next_activity_date(["delete", "20120227"])

    def test_next_activity_date_scenario_29(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute - expiry falls 31st -> 1st
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0006"
        self.nad_status["prescriptionDate"] = "20110331"
        self.nad_status["dispenseWindowHighDate"] = "20120330"
        self.nad_status["lastDispenseDate"] = "20110831"
        self.nad_status["completionDate"] = "20110831"
        self.perform_test_next_activity_date(["createNoClaim", "20120227"])

    def test_next_activity_date_scenario_30(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Prescribe - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0002"
        self.nad_status["prescriptionStatus"] = "0006"
        self.nad_status["prescriptionDate"] = "20110729"
        self.nad_status["dispenseWindowHighDate"] = "20120728"
        self.nad_status["lastDispenseDate"] = "20110831"
        self.nad_status["completionDate"] = "20110831"
        self.perform_test_next_activity_date(["createNoClaim", "20120227"])

    def test_next_activity_date_scenario_31(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0007"
        self.nad_status["prescriptionDate"] = "20110729"
        self.nad_status["completionDate"] = "20120130"
        self.perform_test_next_activity_date(["delete", "20120229"])

    def test_next_activity_date_scenario_32(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Prescribe - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0002"
        self.nad_status["prescriptionStatus"] = "0007"
        self.nad_status["prescriptionDate"] = "20110729"
        self.nad_status["completionDate"] = "20120130"
        self.perform_test_next_activity_date(["delete", "20120229"])

    def test_next_activity_date_scenario_33(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0007"
        self.nad_status["prescriptionDate"] = "20110729"
        self.nad_status["completionDate"] = "20120130"
        self.perform_test_next_activity_date(["delete", "20120229"])

    def test_next_activity_date_scenario_34(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0008"
        self.nad_status["prescriptionDate"] = "20110731"
        self.nad_status["completionDate"] = "20111231"
        self.nad_status["claimSentDate"] = "20120101"
        self.perform_test_next_activity_date(["delete", "20120110"])

    def test_next_activity_date_scenario_37(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Acute - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0001"
        self.nad_status["prescriptionStatus"] = "0009"
        self.nad_status["prescriptionDate"] = "20110731"
        self.nad_status["completionDate"] = "20111231"
        self.nad_status["claimSentDate"] = "20120101"
        self.perform_test_next_activity_date(["delete", "20120110"])

    def test_next_activity_date_scenario_38(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Prescribe - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0002"
        self.nad_status["prescriptionStatus"] = "0009"
        self.nad_status["prescriptionDate"] = "20110731"
        self.nad_status["completionDate"] = "20111231"
        self.nad_status["claimSentDate"] = "20120101"
        self.perform_test_next_activity_date(["delete", "20120110"])

    def test_next_activity_date_scenario_39(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - expiry falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0009"
        self.nad_status["prescriptionDate"] = "20110731"
        self.nad_status["completionDate"] = "20111231"
        self.nad_status["claimSentDate"] = "20120101"
        self.perform_test_next_activity_date(["delete", "20120110"])

    def test_next_activity_date_scenario_40(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Prescribe - Nominated Release before Expiry
        """
        self.nad_status["prescriptionTreatmentType"] = "0002"
        self.nad_status["prescriptionStatus"] = "0000"
        self.nad_status["prescriptionDate"] = "20120731"
        self.nad_status["nominatedDownloadDate"] = "20121101"
        self.perform_test_next_activity_date(["ready", "20121101"])

    def test_next_activity_date_scenario_41(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Prescribe - Expiry before Nominated Release
        """
        self.nad_status["prescriptionTreatmentType"] = "0002"
        self.nad_status["prescriptionStatus"] = "0000"
        self.nad_status["prescriptionDate"] = "20110731"
        self.nad_status["nominatedDownloadDate"] = "20120301"
        self.perform_test_next_activity_date(["expire", "20120131"])

    def test_next_activity_date_scenario_42(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - Nominated Release falls 29th Feb 2012
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0000"
        self.nad_status["prescriptionDate"] = "20111101"
        self.nad_status["nominatedDownloadDate"] = "20120229"
        self.perform_test_next_activity_date(["ready", "20120229"])

    def test_next_activity_date_scenario_43(self):
        """
        Unit test for Next Activity and Next Activity Date Generator:
        Repeat Dispense - Expiry falls 30th Sep 2011
        """
        self.nad_status["prescriptionTreatmentType"] = "0003"
        self.nad_status["prescriptionStatus"] = "0000"
        self.nad_status["prescriptionDate"] = "20110331"
        self.nad_status["nominatedDownloadDate"] = "20120130"
        self.perform_test_next_activity_date(["expire", "20110930"])
