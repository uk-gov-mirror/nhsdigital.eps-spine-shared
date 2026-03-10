from eps_spine_shared.errors import EpsSystemError
from eps_spine_shared.logger import EpsLogger
from eps_spine_shared.nhsfundamentals.time_utilities import time_now_as_string

INDEX_NHSNUMBER_DATE = "nhsNumberDate_bin"
INDEX_NHSNUMBER_PRDATE = "nhsNumberPrescriberDate_bin"
INDEX_NHSNUMBER_PRDSDATE = "nhsNumberPrescDispDate_bin"
INDEX_NHSNUMBER_DSDATE = "nhsNumberDispenserDate_bin"
INDEX_PRESCRIBER_DATE = "prescriberDate_bin"
INDEX_PRESCRIBER_DSDATE = "prescDispDate_bin"
INDEX_PRESCRIBER_STATUS = "prescribingSiteStatus_bin"
INDEX_DISPENSER_DATE = "dispenserDate_bin"
INDEX_DISPENSER_STATUS = "dispensingSiteStatus_bin"

INDEX_NEXTACTIVITY = "nextActivityNAD_bin"
INDEX_NOMPHARM = "nomPharmStatus_bin"
INDEX_NHSNUMBER = "nhsNumber_bin"
INDEX_DELETE_DATE = "backstopdeletedate_bin"
INDEX_PRESCRIPTION_ID = "prescriptionid_bin"
INDEX_STORE_TIME_DOC_REF_TITLE = "storetimebydocreftitle_bin"

REGEX_INDICES = [
    INDEX_NHSNUMBER_DATE,
    INDEX_NHSNUMBER_PRDATE,
    INDEX_NHSNUMBER_PRDSDATE,
    INDEX_NHSNUMBER_DSDATE,
    INDEX_PRESCRIBER_DATE,
    INDEX_PRESCRIBER_DSDATE,
    INDEX_DISPENSER_DATE,
]

SEPERATOR = "|"
INDEX_DELTA = "delta_bin"


class EpsIndexFactory(object):
    """
    Factory for building index details for prescription record
    """

    def __init__(self, log_object, internal_id, test_prescribing_sites, nad_reference):
        """
        Make internal_id available for logging in indexer
        Requires nad_reference - a set of timedeltas to be used when calculating the next
        activity index
        requires test_prescribing_sites - used to differentiate for claims
        """
        self.log_object = EpsLogger(log_object)
        self.internal_id = internal_id
        self.test_prescribing_sites = test_prescribing_sites
        self.nad_reference = nad_reference

    def build_indexes(self, context):
        """
        Create the index values to be used when storing the epsRecord.  There may be
        separate index terms for each individual instance (but only unique index terms
        for the prescription should be returned).

        There are four potential indexes for the epsRecord store:
        nextActivityNAD - the next activity which is due for this prescription and the
        date which it is due (should only contain a single term)
        prescribingSiteStatus - the statuses of the prescription concatenated with the
        prescribing site (to be used in reporting and troubleshooting)
        dispensingSiteStatus - as above (not added until release has occurred)
        nomPharmStatus - as above for any nominated pharmacy (may also be used when bulk
        changes in nomination occur)
        nhsNumber - to be used when managing changes in nomination
        delta - to be used when confirming changes are synchronised between clusters
        """
        index_dict = {}
        try:
            self._add_prescibing_site_status_index(context.epsRecord, index_dict)
            self._add_dispensing_site_status_index(context.epsRecord, index_dict)
            self._add_nominated_pharmacy_status_index(context.epsRecord, index_dict)
            self._add_next_activity_next_activity_date_index(context, index_dict)
            self._add_nhs_number_index(context.epsRecord, index_dict)

            # Adding extra indexes for prescription search
            # overloading each of these indexes with Release version and prescription status in preparation for
            # Riak 1.4
            self._add_nhs_number_date_index(context.epsRecord, index_dict)
            self._add_nhs_number_prescriber_date_index(context.epsRecord, index_dict)
            self._add_nhs_number_prescriber_dispenser_date_index(context.epsRecord, index_dict)
            self._add_nhs_number_dispenser_date_index(context.epsRecord, index_dict)
            self._add_prescriber_date_index(context.epsRecord, index_dict)
            self._add_prescriber_dispenser_date_index(context.epsRecord, index_dict)
            self._add_dispenser_date_index(context.epsRecord, index_dict)
            self._add_delta_index(context.epsRecord, index_dict)
        except EpsSystemError as e:
            self.log_object.write_log(
                "EPS0124", None, {"internalID": self.internal_id, "creatingIndex": e.errorTopic}
            )
            raise EpsSystemError(EpsSystemError.MESSAGE_FAILURE) from e

        return index_dict

    def _add_nhs_number_date_index(self, eps_record, index_dict):
        """
        See build_indexes
        """
        nhs_number = eps_record.return_nhs_number()
        prescription_time = eps_record.return_prescription_time()
        nhs_number_date_bin = nhs_number + SEPERATOR + prescription_time
        index_dict[INDEX_NHSNUMBER_DATE] = eps_record.add_release_and_status(nhs_number_date_bin)

    def _add_nhs_number_prescriber_date_index(self, eps_record, index_dict):
        """
        See build_indexes
        """
        nhs_number = eps_record.return_nhs_number()
        prescriber = eps_record.return_prescribing_organisation()
        prescription_time = eps_record.return_prescription_time()
        index = nhs_number + SEPERATOR + prescriber + SEPERATOR + prescription_time
        new_indexes = eps_record.add_release_and_status(index)
        index_dict[INDEX_NHSNUMBER_PRDATE] = new_indexes

    def _add_nhs_number_prescriber_dispenser_date_index(self, eps_record, index_dict):
        """
        See build_indexes
        """
        result_list = eps_record.return_nhs_number_prescriber_dispenser_date_index()
        [success, nhs_number_presc_disp_date_bin] = result_list
        if not success:
            raise EpsSystemError(INDEX_NHSNUMBER_PRDSDATE)
        if nhs_number_presc_disp_date_bin:
            new_indexes = eps_record.add_release_and_status(nhs_number_presc_disp_date_bin, False)
            index_dict[INDEX_NHSNUMBER_PRDSDATE] = new_indexes

    def _add_prescriber_date_index(self, eps_record, index_dict):
        """
        See build_indexes
        """
        prescriber = eps_record.return_prescribing_organisation()
        prescription_time = eps_record.return_prescription_time()
        prescriber_date_bin = prescriber + SEPERATOR + prescription_time
        index_dict[INDEX_PRESCRIBER_DATE] = eps_record.add_release_and_status(prescriber_date_bin)

    def _add_nhs_number_dispenser_date_index(self, eps_record, index_dict):
        """
        See build_indexes
        """
        result_list = eps_record.return_nhs_number_dispenser_date_index()
        [success, nhs_number_dispenser_date_bin] = result_list
        if not success:
            raise EpsSystemError(INDEX_NHSNUMBER_DSDATE)
        if nhs_number_dispenser_date_bin:
            new_indexes = eps_record.add_release_and_status(nhs_number_dispenser_date_bin, False)
            index_dict[INDEX_NHSNUMBER_DSDATE] = new_indexes

    def _add_prescriber_dispenser_date_index(self, eps_record, index_dict):
        """
        See build_indexes
        """
        result_list = eps_record.return_prescriber_dispenser_date_index()
        [success, presc_disp_dates] = result_list
        if not success:
            raise EpsSystemError(INDEX_PRESCRIBER_DSDATE)
        if presc_disp_dates:
            new_indexes = eps_record.add_release_and_status(presc_disp_dates, False)
            index_dict[INDEX_PRESCRIBER_DSDATE] = new_indexes

    def _add_dispenser_date_index(self, eps_record, index_dict):
        """
        See build_indexes
        """
        result_list = eps_record.return_dispenser_date_index()
        [success, disp_dates] = result_list
        if not success:
            raise EpsSystemError(INDEX_DISPENSER_DATE)
        if disp_dates:
            new_indexes = eps_record.add_release_and_status(disp_dates, False)
            index_dict[INDEX_DISPENSER_DATE] = new_indexes

    def _add_next_activity_next_activity_date_index(self, context, index_dict):
        """
        See build_indexes
        """
        result_list = context.epsRecord.return_next_activity_index(
            self.test_prescribing_sites, self.nad_reference, context
        )

        [next_activity, next_activity_date] = result_list
        next_activity_nad_bin = (
            f"{next_activity}_{next_activity_date}"
            if next_activity_date and next_activity
            else next_activity
        )
        index_dict[INDEX_NEXTACTIVITY] = [next_activity_nad_bin]

    def _add_prescibing_site_status_index(self, eps_record, index_dict):
        """
        See build_indexes
        """
        result_list = eps_record.return_presc_site_status_index()
        [success, presc_site, prescription_status] = result_list
        if not success:
            raise EpsSystemError(INDEX_PRESCRIBER_STATUS)
        index_dict[INDEX_PRESCRIBER_STATUS] = []
        for status in prescription_status:
            index_dict[INDEX_PRESCRIBER_STATUS].append(presc_site + "_" + status)

    def _add_dispensing_site_status_index(self, eps_record, index_dict):
        """
        See build_indexes
        """
        result_list = eps_record.return_disp_site_status_index()
        [success, disp_site_statuses] = result_list
        if not success:
            raise EpsSystemError(INDEX_DISPENSER_STATUS)
        index_dict[INDEX_DISPENSER_STATUS] = list(disp_site_statuses)

    def _add_nominated_pharmacy_status_index(self, eps_record, index_dict):
        """
        See build_indexes
        """
        [nom_pharmacy, prescription_status] = eps_record.return_nom_pharm_status_index()

        if nom_pharmacy:
            index_dict[INDEX_NOMPHARM] = []
            for status in prescription_status:
                index_dict[INDEX_NOMPHARM].append(nom_pharmacy + "_" + status)

            self.log_object.write_log(
                "EPS0617",
                None,
                {
                    "internalID": self.internal_id,
                    "nomPharmacy": nom_pharmacy,
                    "indexes": index_dict[INDEX_NOMPHARM],
                },
            )
        else:
            self.log_object.write_log("EPS0618", None, {"internalID": self.internal_id})

    def _add_nhs_number_index(self, eps_record, index_dict):
        """
        See build_indexes
        """
        nhs_number = eps_record.return_nhs_number()
        index_dict[INDEX_NHSNUMBER] = [nhs_number]

    def _add_delta_index(self, eps_record, index_dict):
        """
        See build_indexes
        """
        index_dict[INDEX_DELTA] = [time_now_as_string() + SEPERATOR + str(eps_record.get_scn())]
