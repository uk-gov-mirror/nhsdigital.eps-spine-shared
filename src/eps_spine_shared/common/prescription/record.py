import datetime
import sys
from copy import copy

from dateutil.relativedelta import relativedelta

from eps_spine_shared.common import indexes
from eps_spine_shared.common.prescription import fields
from eps_spine_shared.common.prescription.issue import PrescriptionIssue
from eps_spine_shared.common.prescription.next_activity_generator import NextActivityGenerator
from eps_spine_shared.common.prescription.statuses import LineItemStatus, PrescriptionStatus
from eps_spine_shared.errors import (
    EpsBusinessError,
    EpsErrorBase,
    EpsSystemError,
)
from eps_spine_shared.logger import EpsLogger
from eps_spine_shared.nhsfundamentals.time_utilities import TimeFormats
from eps_spine_shared.spinecore.base_utilities import handle_encoding_oddities, quoted
from eps_spine_shared.spinecore.changelog import PrescriptionsChangeLogProcessor


class PrescriptionRecord(object):
    """
    Base class for all Prescriptions record objects

    A record object should be created by the validator used by a particular interaction
    The validator can then update the attributes of this object.

    The object should then support creating a new record, or existing an updated record
    using the attributes which have been bound to it
    """

    SCN_MAX = 512
    # Limit beyond which we should stop updating the change log as almost certainly in an
    # uncontrolled loop - and updating the change log may lead to the record being of an
    # unbounded size

    def __init__(self, log_object, internal_id):
        """
        The basic attributes of an epsRecord
        """
        self.log_object = EpsLogger(log_object)
        self.internal_id = internal_id
        self.nad_generator = NextActivityGenerator(log_object, internal_id)
        self.pending_instance_change = None
        self.prescription_record = None
        self.pre_change_issue_status_dict = {}
        self.pre_change_current_issue = None

    def create_initial_record(self, context, prescription=True):
        """
        Take the context of a worker object - which should contain validated output, and
        use to build an initial prescription object

        The prescription boolean is used to indicate that the creation has been caused
        by receipt of an actual prescription.  The creation may be triggered on receipt
        of a cancellation (prior to a prescription) in which case this should be set to
        False.
        """
        self.name_map_on_create(context)

        self.prescription_record = {}
        self.prescription_record[fields.FIELDS_DOCUMENTS] = []
        self.prescription_record[fields.FIELD_PRESCRIPTION] = self.create_prescription_snippet(
            context
        )
        self.prescription_record[fields.FIELD_PRESCRIPTION][
            fields.FIELD_PRESCRIPTION_PRESENT
        ] = prescription
        self.prescription_record[fields.FIELD_PATIENT] = self.create_patient_snippet(context)
        self.prescription_record[fields.FIELD_NOMINATION] = self.create_nomination_snippet(context)
        line_items = self.create_line_items(context)
        self.prescription_record[fields.FIELD_INSTANCES] = self.create_instances(
            context, line_items
        )

    def return_prechange_issue_status_dict(self):
        """
        Returns a dictionary of the initial statuses by issue number.
        """
        return self.pre_change_issue_status_dict

    def return_prechange_current_issue(self):
        """
        Returns the current issue as it was prior to this change
        """
        return self.pre_change_current_issue

    def return_changed_issue_list(
        self,
        pre_change_issue_list,
        post_change_issue_list,
        max_repeats=None,
        changed_issues_list=None,
    ):
        """
        Iterate through the prescription issues comparing the pre and post change status dict
        for each issue number, checking for differences. If a difference is found, add the
        issue number as a string to the returned changed_issues_list.

        Accept an initial changed_issues_list as this may need to include other issues, e.g. in the pending cancellation
        case, an issue can be changed by adding a pending cancellation, even though the statuses don't change.
        """
        if not changed_issues_list:
            changed_issues_list = []

        if not max_repeats:
            max_repeats = self.max_repeats
        for i in range(1, int(max_repeats) + 1):
            issue_ref = self.generate_status_dict_issue_reference(i)
            # The get will handle missing issues from the change log
            if pre_change_issue_list.get(issue_ref, {}) == post_change_issue_list.get(
                issue_ref, {}
            ):
                continue
            changed_issues_list.append(str(i))

        return changed_issues_list

    def generate_status_dict_issue_reference(self, issue_number):
        """
        Create the status dict issue reference. Moved into a separate function as it is used
        in a couple of places.
        """
        return fields.FIELD_ISSUE + str(issue_number)

    def create_issue_current_status_dict(self):
        """
        Cycle through all of the issues in the prescription and add the current prescription
        status and the status of each line item (by order not ID) to a dictionary keyed on issue number
        """
        status_dict = {}
        prescription_issues = self.prescription_record[fields.FIELD_INSTANCES]
        for issue in prescription_issues:
            issue_dict = {}
            issue_dict[fields.FIELD_PRESCRIPTION] = str(
                prescription_issues[issue][fields.FIELD_PRESCRIPTION_STATUS]
            )
            issue_dict[fields.FIELD_LINE_ITEMS] = {}
            for line_item in prescription_issues[issue][fields.FIELD_LINE_ITEMS]:
                line_order = line_item[fields.FIELD_ORDER]
                line_status = line_item[fields.FIELD_STATUS]
                issue_dict[fields.FIELD_LINE_ITEMS][str(line_order)] = str(line_status)
            status_dict[self.generate_status_dict_issue_reference(issue)] = issue_dict
        return status_dict

    def add_event_to_change_log(self, message_id, event_log):
        """
        Add the event_log to the change log under the key of message_id. If the changeLog does
        not exist it will be created.

        Prescriptions change logs will not be be pruned and will grow unbounded.
        """
        # Set the SCN on the change log to be the same as on the record
        event_log[PrescriptionsChangeLogProcessor.SCN] = self.get_scn()
        length_before = len(self.prescription_record.get(fields.FIELD_CHANGE_LOG, []))
        try:
            PrescriptionsChangeLogProcessor.update_change_log(
                self.prescription_record, event_log, message_id, self.SCN_MAX
            )
        except Exception as e:  # noqa: BLE001
            self.log_object.write_log(
                "EPS0336",
                sys.exc_info(),
                {"internalID": self.internal_id, "prescriptionID": self.id, "error": str(e)},
            )
            raise EpsSystemError(EpsSystemError.SYSTEM_FAILURE) from e
        length_after = len(self.prescription_record.get(fields.FIELD_CHANGE_LOG, []))
        if length_after != length_before + 1:
            self.log_object.write_log(
                "EPS0672",
                None,
                {
                    "internalID": self.internal_id,
                    "lengthBefore": str(length_before),
                    "lengthAfter": str(length_after),
                },
            )

    def add_index_to_record(self, index_dict):
        """
        Replace the existing index information with a new set of index information
        """
        self.prescription_record[fields.FIELD_INDEXES] = index_dict

    def increment_scn(self):
        """
        Check for an SCN on the record, if one does not already exist, add it.
        If it does exist, increment it - but throw a system error if this exceed a
        maximum to prevent a prescription ending up in an uncontrolled loop - SPII-14250.
        """
        if fields.FIELDS_SCN not in self.prescription_record:
            self.prescription_record[fields.FIELDS_SCN] = (
                PrescriptionsChangeLogProcessor.INITIAL_SCN
            )
        else:
            self.prescription_record[fields.FIELDS_SCN] += 1

    def get_scn(self):
        """
        Check for an SCN on the record, if one does not already exist, create it.
        If it already exists, return it.
        """
        if fields.FIELDS_SCN not in self.prescription_record:
            self.prescription_record[fields.FIELDS_SCN] = (
                PrescriptionsChangeLogProcessor.INITIAL_SCN
            )

        return self.prescription_record[fields.FIELDS_SCN]

    def add_document_references(self, document_refs):
        """
        Adds a document reference to the high-level document list.
        """
        if fields.FIELDS_DOCUMENTS not in self.prescription_record:
            self.prescription_record[fields.FIELDS_DOCUMENTS] = []

        for document in document_refs:
            self.prescription_record[fields.FIELDS_DOCUMENTS].append(document)

    def return_record_to_be_stored(self):
        """
        Return a copy of the record in a storable format (i.e. note that this is not json
        encoded here - it will be encoded as it is placed onto the WDO)
        """
        return self.prescription_record

    def return_next_activity_nad_bin(self):
        """
        Return the nextActivityNAD_bin index of the prescription record
        """
        if fields.FIELD_INDEXES in self.prescription_record:
            if indexes.INDEX_NEXTACTIVITY in self.prescription_record[fields.FIELD_INDEXES]:
                return self.prescription_record[fields.FIELD_INDEXES][indexes.INDEX_NEXTACTIVITY]
            if indexes.INDEX_NEXTACTIVITY.lower() in self.prescription_record[fields.FIELD_INDEXES]:
                return self.prescription_record[fields.FIELD_INDEXES][
                    indexes.INDEX_NEXTACTIVITY.lower()
                ]
        return None

    def create_record_from_store(self, record):
        """
        Convert the stored format into a self.prescription_record
        """
        self.prescription_record = record
        self.pre_change_issue_status_dict = self.create_issue_current_status_dict()
        self.pre_change_current_issue = self.prescription_record.get(
            fields.FIELD_PRESCRIPTION, {}
        ).get(fields.FIELD_CURRENT_INSTANCE)

    def name_map_on_create(self, context):
        """
        Map any additional names from the original context (e.g. if the property here is
        named differently at the point of extract from the message such as with
        agentOrganization)
        """
        context.prescribingOrganization = context.agentOrganization
        if hasattr(context, fields.FIELD_PRESCRIPTION_REPEAT_HIGH):
            context.maxRepeats = context.prescriptionRepeatHigh
        if hasattr(context, fields.FIELD_DAYS_SUPPLY_LOW):
            context.dispenseWindowLowDate = context.daysSupplyValidLow
        if hasattr(context, fields.FIELD_DAYS_SUPPLY_HIGH):
            context.dispenseWindowHighDate = context.daysSupplyValidHigh

    def create_instances(self, context, line_items):
        """
        Create all prescription instances
        """
        instance_snippet = self.set_all_snippet_details(fields.INSTANCE_DETAILS, context)
        instance_snippet[fields.FIELD_LINE_ITEMS] = line_items
        instance_snippet[fields.FIELD_INSTANCE_NUMBER] = "1"
        instance_snippet[fields.FIELD_DISPENSE] = self.set_all_snippet_details(
            fields.DISPENSE_DETAILS, context
        )
        instance_snippet[fields.FIELD_CLAIM] = self.set_all_snippet_details(
            fields.CLAIM_DETAILS, context
        )
        instance_snippet[fields.FIELD_CANCELLATIONS] = []
        instance_snippet[fields.FIELD_DISPENSE_HISTORY] = {}
        instance_snippet[fields.FIELD_NEXT_ACTIVITY] = {}
        instance_snippet[fields.FIELD_NEXT_ACTIVITY][fields.FIELD_ACTIVITY] = None
        instance_snippet[fields.FIELD_NEXT_ACTIVITY][fields.FIELD_DATE] = None

        return {"1": instance_snippet}

    def create_prescription_snippet(self, context):
        """
        Create the prescription snippet from the prescription details
        """
        presc_details = self.set_all_snippet_details(fields.PRESCRIPTION_DETAILS, context)
        presc_details[fields.FIELD_CURRENT_INSTANCE] = str(1)
        return presc_details

    def create_patient_snippet(self, context):
        """
        Create the patient snippet from the patient details
        """
        return self.set_all_snippet_details(fields.PATIENT_DETAILS, context)

    def create_nomination_snippet(self, context):
        """
        Create the nomination snippet from the nomination details
        """
        nomination_snippet = self.set_all_snippet_details(fields.NOMINATION_DETAILS, context)
        if hasattr(context, fields.FIELD_NOMINATED_PERFORMER):
            if context.nominatedPerformer:
                nomination_snippet[fields.FIELD_NOMINATED] = True
        if not nomination_snippet[fields.FIELD_NOMINATION_HISTORY]:
            nomination_snippet[fields.FIELD_NOMINATION_HISTORY] = []
        return nomination_snippet

    def set_all_snippet_details(self, details_list, context):
        """
        Default any missing value to False
        """
        snippet = {}
        for item_detail in details_list:
            if hasattr(context, item_detail):
                value = getattr(context, item_detail)
            elif isinstance(context, dict) and item_detail in context:
                value = context[item_detail]
            else:
                snippet[item_detail] = False
                continue

            if isinstance(value, datetime.datetime):
                value = value.strftime(TimeFormats.STANDARD_DATE_TIME_FORMAT)
            snippet[item_detail] = value
        return snippet

    def create_line_items(self, context):
        """
        Create individual line items
        """
        complete_line_items = []

        for line_item in context.lineItems:
            line_item_snippet = self.set_all_snippet_details(fields.LINE_ITEM_DETAILS, line_item)
            complete_line_items.append(line_item_snippet)

        return complete_line_items

    def _get_prescription_instance_data(self, instance_number, raise_exception_on_missing=True):
        """
        Internal method to support record access
        """
        prescription_instance_data = self.prescription_record[fields.FIELD_INSTANCES].get(
            instance_number
        )
        if not prescription_instance_data:
            if raise_exception_on_missing:
                self._handle_missing_issue(instance_number)
            else:
                return {}
        return prescription_instance_data

    def get_prescription_instance_data(self, instance_number, raise_exception_on_missing=True):
        """
        Public method to support record access
        """
        return self._get_prescription_instance_data(instance_number, raise_exception_on_missing)

    @property
    def future_issues_available(self):
        """
        Return boolean to indicate if future issues are available or not. Always False for
        Acute and Repeat Prescribe
        """
        return False

    def get_issue(self, issue_number):
        """
        Get a particular issue of this prescription.

        :type issue_number: int
        :rtype: PrescriptionIssue
        """
        # explicitly check that we are receiving an int, as legacy code used strs
        if not isinstance(issue_number, int):
            raise TypeError("Issue number must be an int")

        issue_number_str = str(issue_number)
        issue_data = self.prescription_record[fields.FIELD_INSTANCES].get(issue_number_str)

        if not issue_data:
            self._handle_missing_issue(issue_number)

        issue = PrescriptionIssue(issue_data)
        return issue

    def _handle_missing_issue(self, issue_number):
        """
        Missing instances are a data migration specific issue, and will throw
        a prescription not found error after after being logged
        """
        self.log_object.write_log(
            "EPS0073c",
            None,
            {"internalID": self.internal_id, "prescriptionID": self.id, "issue": issue_number},
        )
        raise EpsBusinessError(EpsErrorBase.MISSING_ISSUE)

    @property
    def id(self):
        """
        The prescription's ID.

        :rtype: str
        """
        return self.prescription_record[fields.FIELD_PRESCRIPTION][fields.FIELD_PRESCRIPTION_ID]

    @property
    def issue_numbers(self):
        """
        Sorted list of issue numbers.

        Note: migrated prescriptions may have missing issues (before the current one)
        so do not be surprised if the list returned here is not the complete range.

        :rtype: list(int)
        """
        # we have to convert instance numbers to ints, as they're stored as strings
        issue_numbers = [int(i) for i in list(self.prescription_record["instances"].keys())]
        return sorted(issue_numbers)

    def get_issue_numbers_in_range(self, lowest=None, highest=None):
        """
        Sorted list of issue numbers in the specified range (inclusive).

        If either lowest or highest threshold is set to None then it will be ignored.

        :type lowest: int or None
        :type highest: int or None
        :rtype: list(int)
        """
        candidate_numbers = self.issue_numbers

        if lowest is not None:
            candidate_numbers = [i for i in candidate_numbers if i >= lowest]

        if highest is not None:
            candidate_numbers = [i for i in candidate_numbers if i <= highest]

        return candidate_numbers

    def get_issues_in_range(self, lowest=None, highest=None):
        """
        Sorted list of issues in the specified range (inclusive).

        If either lowest or highest threshold is set to None then it will be ignored.

        :type lowest: int or None
        :type highest: int or None
        :rtype: list(PrescriptionIssue)
        """
        issues = [self.get_issue(i) for i in self.get_issue_numbers_in_range(lowest, highest)]
        return issues

    def get_issues_from_current_upwards(self):
        """
        Sorted list of issues, starting at the current one.

        :rtype: list(PrescriptionIssue)
        """
        return self.get_issues_in_range(self.current_issue_number, None)

    @property
    def missing_issue_numbers(self):
        """
        Sorted list of numbers of instances missing from the prescription.

        :rtype: list(int)
        """
        expected_issue_numbers = range(1, self.max_repeats + 1)
        actual_issue_numbers = self.issue_numbers
        missing_issue_numbers = set(expected_issue_numbers) - set(actual_issue_numbers)

        return sorted(list(missing_issue_numbers))

    @property
    def issues(self):
        """
        List of issues, ordered by issue number.

        :rtype: list(PrescriptionIssue)
        """
        issues = [self.get_issue(i) for i in self.issue_numbers]
        return issues

    @property
    def _current_instance_data(self):
        """
        Internal property to support record access
        """
        return self._get_prescription_instance_data(str(self.current_issue_number))

    @property
    def current_issue_number(self):
        """
        The current issue number of this prescription.

        :rtype: int
        """
        current_issue_number_str = self.prescription_record[fields.FIELD_PRESCRIPTION].get(
            fields.FIELD_CURRENT_INSTANCE
        )
        if not current_issue_number_str:
            self._handle_missing_issue(fields.FIELD_CURRENT_INSTANCE)
        return int(current_issue_number_str)

    @current_issue_number.setter
    def current_issue_number(self, value):
        """
        The current issue number of this prescription.

        :type value: int
        """
        # explicitly check that we are receiving an int, as legacy code used strs
        if not isinstance(value, int):
            raise TypeError("Issue number must be an int")

        current_issue_number_str = str(value)
        self.prescription_record[fields.FIELD_PRESCRIPTION][
            fields.FIELD_CURRENT_INSTANCE
        ] = current_issue_number_str

    @property
    def current_issue(self):
        """
        The current issue of this prescription.

        :rtype: PrescriptionIssue
        """
        return self.get_issue(self.current_issue_number)

    @property
    def _current_instance_status(self):
        """
        Internal property to support record access

        ..  deprecated::
            use "current_issue.status" instead
        """
        return self._current_instance_data[fields.FIELD_PRESCRIPTION_STATUS]

    @property
    def _pending_cancellations(self):
        """
        Internal property to support record access
        """
        return self.prescription_record[fields.FIELD_PRESCRIPTION][
            fields.FIELD_PENDING_CANCELLATIONS
        ]

    @property
    def _pending_cancellation_flag(self):
        """
        Internal property to support record access
        """
        obj = self.prescription_record.get(fields.FIELD_PRESCRIPTION, {}).get(
            fields.FIELD_PENDING_CANCELLATIONS
        )
        if not obj:
            return False
        if isinstance(obj, list) and obj:
            return True
        return False

    @_pending_cancellations.setter
    def _pending_cancellations(self, value):
        """
        Internal property to support record access
        """
        self.prescription_record[fields.FIELD_PRESCRIPTION][
            fields.FIELD_PENDING_CANCELLATIONS
        ] = value

    @property
    def _nhs_number(self):
        """
        Internal property to support record access
        """
        return self.prescription_record[fields.FIELD_PATIENT][fields.FIELD_NHS_NUMBER]

    @property
    def _prescription_time(self):
        """
        Internal property to support record access

        ..  deprecated::
            use "time" instead (which returns a datetime instead of a str)
            PAB - but note - this field may contain just a date str, not a datetime?!
        :rtype: str
        """
        return self.prescription_record[fields.FIELD_PRESCRIPTION][fields.FIELD_PRESCRIPTION_TIME]

    @property
    def time(self):
        """
        The datetime of the prescription.

        PAB - what does this time actually signify? It needs better naming

        :rtype: datetime.datetime
        """
        prescription_time_str = self.prescription_record[fields.FIELD_PRESCRIPTION][
            fields.FIELD_PRESCRIPTION_TIME
        ]
        prescription_time = datetime.datetime.strptime(
            prescription_time_str, TimeFormats.STANDARD_DATE_TIME_FORMAT
        )
        return prescription_time

    @property
    def _release_version(self):
        """
        Internal property to support record access
        """
        prescriptionID = str(self.return_prescription_id())
        idLength = len(prescriptionID)
        if idLength in fields.R1_PRESCRIPTIONID_LENGTHS:
            return fields.R1_VERSION
        if idLength in fields.R2_PRESCRIPTIONID_LENGTHS:
            return fields.R2_VERSION

    def get_release_version(self):
        """
        Return the prescription release version (R1 or R2)
        """
        return self._release_version

    def add_release_and_status(self, index_prefix, is_string=True):
        """
        Returns a list containing the index prefix concatenated with all applicable release
        versions and Prescription Statuses
        """
        release_version = self._release_version
        status_list = self.return_prescription_status_set()
        return_set = []
        for each_status in status_list:
            if not is_string:
                for each_index in index_prefix:
                    new_value = each_index + "|" + release_version + "|" + each_status
                    return_set.append(new_value)
            else:
                new_value = index_prefix + "|" + release_version + "|" + each_status
                return_set.append(new_value)

        return return_set

    def update_nominated_performer(self, context):
        """
        Update the "nominated performer" field and log the change.
        """
        nomination = self.prescription_record[fields.FIELD_NOMINATION]
        self.log_attribute_change(
            fields.FIELD_NOMINATED_PERFORMER,
            nomination[fields.FIELD_NOMINATED_PERFORMER],
            context.nominatedPerformer,
            context.fieldsToUpdate,
        )
        nomination[fields.FIELD_NOMINATED_PERFORMER] = context.nominatedPerformer

    def return_presc_site_status_index(self):
        """
        Return the prescribing organization and the prescription status
        """
        presc_site = self.prescription_record[fields.FIELD_PRESCRIPTION][
            fields.FIELD_PRESCRIBING_ORG
        ]
        presc_status = self.return_prescription_status_set()
        return [True, presc_site, presc_status]

    def return_nom_pharm_status_index(self):
        """
        Return the Nominated Pharmacy and the prescription status
        """
        nom_pharm = self.return_nom_pharm()
        if not nom_pharm:
            return [None, None]
        presc_status = self.return_prescription_status_set()
        return [nom_pharm, presc_status]

    def return_nom_pharm(self):
        """
        Return the Nominated Pharmacy
        """
        return self.prescription_record.get(fields.FIELD_NOMINATION, {}).get(
            fields.FIELD_NOMINATED_PERFORMER
        )

    def return_disp_site_or_nom_pharm(self, instance):
        """
        Returns the Dispensing Site if available, otherwise, returns the Nominated Pharmacy
        or None if neither exist
        """
        disp_site = instance.get(fields.FIELD_DISPENSE, {}).get(
            fields.FIELD_DISPENSING_ORGANIZATION
        )
        if not disp_site:
            disp_site = self.return_nom_pharm()
        return disp_site

    def return_disp_site_status_index(self):
        """
        Return the dispensing organization and the prescription status.
        If nominated but not yet downloaded, return NomPharm instead of dispensing org
        """
        dispensing_site_statuses = set()
        for instance_key in self.prescription_record[fields.FIELD_INSTANCES]:
            instance = self._get_prescription_instance_data(instance_key)
            disp_site = self.return_disp_site_or_nom_pharm(instance)
            if not disp_site:
                continue
            presc_status = instance[fields.FIELD_PRESCRIPTION_STATUS]
            dispensing_site_statuses.add(disp_site + "_" + presc_status)

        return [True, dispensing_site_statuses]

    def return_nhs_number_prescriber_dispenser_date_index(self):
        """
        Return the NHS Number Prescribing organization dispensingOrganization and the prescription date
        """
        nhs_number = self.return_nhs_number()
        prescriber = self.return_prescribing_organisation()
        index_start = nhs_number + "|" + prescriber + "|"
        prescription_time = self.return_prescription_time()
        nhs_number_presc_disp_dates = set()
        for instance_key in self.prescription_record[fields.FIELD_INSTANCES]:
            instance = self._get_prescription_instance_data(instance_key)
            disp_site = self.return_disp_site_or_nom_pharm(instance)
            if not disp_site:
                continue
            nhs_number_presc_disp_dates.add(index_start + disp_site + "|" + prescription_time)

        return [True, nhs_number_presc_disp_dates]

    def return_prescriber_dispenser_date_index(self):
        """
        Return the Prescribing organization dispensingOrganization and the prescription date
        """
        prescriber = self.return_prescribing_organisation()
        index_start = prescriber + "|"
        prescription_time = self.return_prescription_time()
        presc_disp_dates = set()
        for instance_key in self.prescription_record[fields.FIELD_INSTANCES]:
            instance = self._get_prescription_instance_data(instance_key)
            disp_site = self.return_disp_site_or_nom_pharm(instance)
            if not disp_site:
                continue
            presc_disp_dates.add(index_start + disp_site + "|" + prescription_time)

        return [True, presc_disp_dates]

    def return_dispenser_date_index(self):
        """
        Return the dispensingOrganization and the prescription date
        """
        index_start = ""
        prescription_time = self.return_prescription_time()
        presc_disp_dates = set()
        for instance_key in self.prescription_record[fields.FIELD_INSTANCES]:
            instance = self._get_prescription_instance_data(instance_key)
            disp_site = self.return_disp_site_or_nom_pharm(instance)
            if not disp_site:
                continue
            presc_disp_dates.add(index_start + disp_site + "|" + prescription_time)

        return [True, presc_disp_dates]

    def return_nhs_number_dispenser_date_index(self):
        """
        Return the NHS Number dispensingOrganization and the prescription date
        """
        nhs_number = self.return_nhs_number()
        index_start = nhs_number + "|"
        prescription_time = self.return_prescription_time()
        nhs_number_disp_dates = set()
        for instance_key in self.prescription_record[fields.FIELD_INSTANCES]:
            instance = self._get_prescription_instance_data(instance_key)
            disp_site = self.return_disp_site_or_nom_pharm(instance)
            if not disp_site:
                continue
            nhs_number_disp_dates.add(index_start + disp_site + "|" + prescription_time)

        return [True, nhs_number_disp_dates]

    def return_nominated_performer(self):
        """
        Return the nominated performer (called when determining routing key extension)
        """
        nom_performer = None
        nomination = self.prescription_record.get(fields.FIELD_NOMINATION)
        if nomination:
            nom_performer = nomination.get(fields.FIELD_NOMINATED_PERFORMER)
        return nom_performer

    def return_nominated_performer_type(self):
        """
        Return the nominated performer type
        """
        nom_performer_type = None
        nomination = self.prescription_record.get(fields.FIELD_NOMINATION)
        if nomination:
            nom_performer_type = nomination.get(fields.FIELD_NOMINATED_PERFORMER_TYPE)
        return nom_performer_type

    def return_prescription_status_set(self):
        """
        For single instance prescription - the prescription status is always the current
        status of the first (and only) instance
        """
        status_set = set()
        for instance_key in self.prescription_record[fields.FIELD_INSTANCES]:
            instance = self._get_prescription_instance_data(instance_key)
            status_set.add(instance[fields.FIELD_PRESCRIPTION_STATUS])
        return list(status_set)

    def return_nhs_number(self):
        """
        Return the NHS Number
        """
        return self._nhs_number

    def return_prescription_time(self):
        """
        Return the Prescription Time
        """
        return self._prescription_time

    def return_prescription_id(self):
        """
        Return the Prescription ID
        """
        return self.prescription_record[fields.FIELD_PRESCRIPTION][fields.FIELD_PRESCRIPTION_ID]

    def return_pending_cancellations_flag(self):
        """
        Return the pending cancellations flag
        """
        prescription = self.prescription_record[fields.FIELD_PRESCRIPTION]
        max_repeats = prescription.get(fields.FIELD_MAX_REPEATS)

        if not max_repeats:
            max_repeats = 1

        for prescription_issue in range(1, int(max_repeats) + 1):
            prescription_issue = self.prescription_record[fields.FIELD_INSTANCES].get(
                str(prescription_issue)
            )
            # handle missing issues
            if not prescription_issue:
                continue
            issue_specific_cancellations = {}
            applied_cancellations_for_issue = prescription_issue.get(fields.FIELD_CANCELLATIONS, [])
            cancellation_status_string_prefix = ""
            self._create_cancellation_summary_dict(
                applied_cancellations_for_issue,
                issue_specific_cancellations,
                cancellation_status_string_prefix,
            )
            if str(prescription_issue[fields.FIELD_INSTANCE_NUMBER]) == str(
                prescription[fields.FIELD_CURRENT_INSTANCE]
            ):
                pending_cancellations = prescription[fields.FIELD_PENDING_CANCELLATIONS]
                cancellation_status_string_prefix = "Pending: "
                self._create_cancellation_summary_dict(
                    pending_cancellations,
                    issue_specific_cancellations,
                    cancellation_status_string_prefix,
                )
                for _, val in issue_specific_cancellations.items():
                    if val.get(fields.FIELD_REASONS, "")[:7] == "Pending":
                        return True

        return False

    def _create_cancellation_summary_dict(
        self, recorded_cancellations, issue_cancellation_dict, cancellation_status
    ):
        """
        Process a list of cancellations, creating a dictionary of cancellation reason text
        and applied SCN for each prescription and issue.

        cancellationStatus is used to seed the reasons in the pending scenario.
        """
        if not recorded_cancellations:
            return

        for cancellation in recorded_cancellations:
            subsequent_reason = False
            cancellation_reasons = str(cancellation_status)

            cancellation_id = cancellation.get(fields.FIELD_CANCELLATION_ID, [])
            scn = PrescriptionsChangeLogProcessor.get_scn(
                self.prescription_record["changeLog"].get(cancellation_id, {})
            )
            for cancellation_reason in cancellation.get(fields.FIELD_REASONS, []):
                cancellation_text = cancellation_reason.split(":")[1].strip()
                if subsequent_reason:
                    cancellation_reasons += "; "
                subsequent_reason = True
                cancellation_reasons += str(handle_encoding_oddities(cancellation_text))

            if cancellation.get(fields.FIELD_CANCELLATION_TARGET) == "Prescription":  # noqa: SIM108
                cancellation_target = fields.FIELD_PRESCRIPTION
            else:
                cancellation_target = cancellation.get(fields.FIELD_CANCEL_LINE_ITEM_REF)

            if (
                issue_cancellation_dict.get(cancellation_target, {}).get(fields.FIELD_ID)
                == cancellation_id
            ):
                # Cancellation has already been added and this is pending as multiple cancellations are not possible
                return

            issue_cancellation_dict[cancellation_target] = {
                fields.FIELD_SCN: scn,
                fields.FIELD_REASONS: cancellation_reasons,
                fields.FIELD_ID: cancellation_id,
            }

    def return_current_instance(self):
        """
        Return the current instance

        ..  deprecated::
            use "current_issue_number" instead (which returns int instead of string)
        """
        return str(self.current_issue_number)

    def return_prescription_status(self, instance_number, raise_exception_on_missing=True):
        """
        For single instance prescription - the prescription status is always the current
        status of the first (and only) instance
        """
        return self._get_prescription_instance_data(
            str(instance_number), raise_exception_on_missing
        ).get(fields.FIELD_PRESCRIPTION_STATUS)

    def return_previous_prescription_status(self, instance_number, raise_exception_on_missing=True):
        """
        For single instance prescription - the previous prescription status is always the
        previous status of the first (and only) instance
        """
        return self._get_prescription_instance_data(
            str(instance_number), raise_exception_on_missing
        ).get(fields.FIELD_PREVIOUS_STATUS)

    def return_line_item_by_ref(self, instance_number, line_item_ref):
        """
        Return the line item from the instance that matches the reference provided
        """
        for line_item in self._get_prescription_instance_data(instance_number)[
            fields.FIELD_LINE_ITEMS
        ]:
            if line_item[fields.FIELD_ID] == line_item_ref:
                return line_item
        return None

    def return_prescribing_organisation(self):
        """
        Return the prescribing organisation from the record
        """
        return self.prescription_record[fields.FIELD_PRESCRIPTION][fields.FIELD_PRESCRIBING_ORG]

    def return_last_dn_guid(self, instance_number):
        """
        Return references to the last dispense notification messages
        """
        instance = self._get_prescription_instance_data(instance_number)
        try:
            dispn_msg_guid = instance[fields.FIELD_DISPENSE][
                fields.FIELD_LAST_DISPENSE_NOTIFICATION_GUID
            ]
            return dispn_msg_guid
        except KeyError:
            return None

    def return_last_dc_guid(self, instance_number):
        """
        Return references to the last dispense notification messages
        """
        instance = self._get_prescription_instance_data(instance_number)
        try:
            claim_msg_guid = instance[fields.FIELD_CLAIM][fields.FIELD_CLAIM_GUID]
            return claim_msg_guid
        except KeyError:
            return None

    def return_document_references_for_claim(self, instance_number):
        """
        Return references to prescription, dispense notification and claim messages
        """
        presc_msg_ref = self.prescription_record[fields.FIELD_PRESCRIPTION][
            fields.FIELD_PRESCRIPTION_MSG_REF
        ]
        instance = self._get_prescription_instance_data(instance_number)
        dispn_msg_ref = instance[fields.FIELD_DISPENSE][
            fields.FIELD_LAST_DISPENSE_NOTIFICATION_MSG_REF
        ]
        claim_msg_ref = instance[fields.FIELD_CLAIM][fields.FIELD_DISPENSE_CLAIM_MSG_REF]
        return [presc_msg_ref, dispn_msg_ref, claim_msg_ref]

    def return_claim_date(self, instance_number):
        """
        Returns the claim date recorded for an instance
        """
        instance = self._get_prescription_instance_data(instance_number)
        claim_rcv_date = instance[fields.FIELD_CLAIM][fields.FIELD_CLAIM_RECEIVED_DATE]
        return claim_rcv_date

    def check_real(self):
        """
        Check that the prescription object is real (as opposed to an empty one created
        by a pendingCancellation)

        If the prescriptionPresent flag is not there - act as if True
        """
        try:
            return self.prescription_record[fields.FIELD_PRESCRIPTION][
                fields.FIELD_PRESCRIPTION_PRESENT
            ]
        except KeyError:
            return True

    def check_returned_record_is_real(self, returned_record):
        """
        Check that the returned_record is real (as opposed to an empty one created
        by a pending cancellation). Look for a valid prescription treatment type
        """
        if returned_record[fields.FIELD_PRESCRIPTION][fields.FIELD_PRESCRIPTION_TREATMENT_TYPE]:
            return True

        return False

    def _get_dispense_list_to_check(self, prescription_status):
        """
        Consistency check fields
        """
        if prescription_status == PrescriptionStatus.WITH_DISPENSER:
            check_list = [fields.FIELD_DISPENSING_ORGANIZATION]
        elif prescription_status == PrescriptionStatus.WITH_DISPENSER_ACTIVE:
            check_list = [fields.FIELD_DISPENSING_ORGANIZATION, fields.FIELD_LAST_DISPENSE_DATE]
        elif prescription_status in [PrescriptionStatus.DISPENSED, PrescriptionStatus.CLAIMED]:
            check_list = [fields.FIELD_LAST_DISPENSE_DATE]
        else:
            check_list = []

        return check_list

    def _get_instance_list_to_check(self, prescription_status):
        """
        Consistency check fields
        """
        if prescription_status == PrescriptionStatus.EXPIRED:
            check_list = [fields.FIELD_COMPLETION_DATE, fields.FIELD_EXPIRY_DATE]
        elif prescription_status in [
            PrescriptionStatus.CANCELLED,
            PrescriptionStatus.NOT_DISPENSED,
        ]:
            check_list = [fields.FIELD_COMPLETION_DATE]
        elif prescription_status in [
            PrescriptionStatus.AWAITING_RELEASE_READY,
            PrescriptionStatus.REPEAT_DISPENSE_FUTURE_INSTANCE,
        ]:
            check_list = [
                fields.FIELD_DISPENSE_WINDOW_LOW_DATE,
                fields.FIELD_NOMINATED_DOWNLOAD_DATE,
            ]
        else:
            check_list = []

        return check_list

    def _get_prescription_list_to_check(self, prescription_status):
        """
        Consistency check fields
        """
        if prescription_status in [
            PrescriptionStatus.AWAITING_RELEASE_READY,
            PrescriptionStatus.REPEAT_DISPENSE_FUTURE_INSTANCE,
        ]:
            check_list = [fields.FIELD_PRESCRIPTION_TIME]
        else:
            check_list = [fields.FIELD_PRESCRIPTION_TREATMENT_TYPE, fields.FIELD_PRESCRIPTION_TIME]

        return check_list

    def _get_claim_list_to_check(self, prescription_status):
        """
        Consistency check fields
        """
        return (
            [fields.FIELD_CLAIM_RECEIVED_DATE]
            if prescription_status == PrescriptionStatus.CLAIMED
            else []
        )

    def _get_nominate_list_to_check(self):
        """
        Consistency check fields
        """
        p_t_type = self.prescription_record[fields.FIELD_PRESCRIPTION][
            fields.FIELD_PRESCRIPTION_TREATMENT_TYPE
        ]
        return (
            [fields.FIELD_NOMINATED_PERFORMER]
            if p_t_type == fields.TREATMENT_TYPE_REPEAT_DISPENSE
            else []
        )

    def check_record_consistency(self, context):
        """
        Check each line item to ensure consistency with the prescription status for
        this instance - the epsAdminUpdate can only impact a single instance

        *** Should be called targetInstance not currentInstance ***

        Check for the prescription status for that instance that required data exists
        Check a nominatedPerformer is set for repeat prescriptions (although this may
        not be required as a check due to DPR rules)
        """
        test_failures = []

        instance_dict = self._get_prescription_instance_data(context.currentInstance)

        for line_item_dict in instance_dict[fields.FIELD_LINE_ITEMS]:
            valid = self.validate_line_prescription_status(
                instance_dict[fields.FIELD_PRESCRIPTION_STATUS], line_item_dict[fields.FIELD_STATUS]
            )
            if not valid:
                test_failures.append("lineItemStatus check for " + line_item_dict[fields.FIELD_ID])

        prescription_status = instance_dict[fields.FIELD_PRESCRIPTION_STATUS]

        prescription = self.prescription_record[fields.FIELD_PRESCRIPTION]
        prescription_list = self._get_prescription_list_to_check(prescription_status)
        self.individual_consistency_checks(prescription_list, prescription, test_failures)

        instance_list = self._get_instance_list_to_check(prescription_status)
        self.individual_consistency_checks(instance_list, instance_dict, test_failures)

        nomination = self.prescription_record[fields.FIELD_NOMINATION]
        nominate_list = self._get_nominate_list_to_check()
        self.individual_consistency_checks(nominate_list, nomination, test_failures, False)

        dispense_list = self._get_dispense_list_to_check(prescription_status)
        self.individual_consistency_checks(
            dispense_list, instance_dict[fields.FIELD_DISPENSE], test_failures
        )

        claim_list = self._get_claim_list_to_check(prescription_status)
        self.individual_consistency_checks(
            claim_list, instance_dict[fields.FIELD_CLAIM], test_failures
        )

        if not test_failures:
            return [True, None]

        for failure_reason in test_failures:
            self.log_object.write_log(
                "EPS0073",
                None,
                {
                    "internalID": self.internal_id,
                    "failureReason": failure_reason,
                },
            )

        return [False, "Record consistency check failure"]

    def individual_consistency_checks(
        self, list_of_checks, record_part, test_failures, fail_on_none=True
    ):
        """
        Loop through field names in a list to confirm there is a value on the record_part
        for each field
        """
        for req_field in list_of_checks:
            if req_field not in record_part:
                test_failures.append("Mandatory item " + req_field + " missing")
            if not record_part[req_field]:
                if fail_on_none:
                    test_failures.append("Mandatory item " + req_field + " set to None")
                    return
                self.log_object.write_log(
                    "EPS0073b", None, {"internalID": self.internal_id, "mandatoryItem": req_field}
                )

    def determine_if_final_issue(self, issue_number):
        """
        Check if the issue is the final one, this may be because the current issue is
        already at max_repeats, or becuase subsequent issues are missing
        """
        if issue_number == self.max_repeats:
            return True

        for i in range(int(issue_number) + 1, int(self.max_repeats + 1)):
            issue_data = self._get_prescription_instance_data(str(i), False)
            if issue_data.get(fields.FIELD_PRESCRIPTION_STATUS):
                return False
        return True

    def return_next_activity_index(self, test_sites, nad_reference, context):
        """
        Iterate through all prescription instances, determining the Next Activity and Date
        for each, and then set the lowest to the record.
        Ignore a next activity of delete for all but the last instance
        In the case of a tie-break, set the priority based on user impact (making a
        prescription instance 'ready' for download takes precedence over deleting or
        expiring an instance)
        """
        earliest_activity_date = "99991231"
        delete_date = "99991231"

        earliest_activity = None

        for instance_key in self.prescription_record[fields.FIELD_INSTANCES]:
            instance_dict = self._get_prescription_instance_data(instance_key, False)
            if not instance_dict.get(fields.FIELD_PRESCRIPTION_STATUS):
                continue

            issue = PrescriptionIssue(instance_dict)
            nad_status = self.set_nad_status(test_sites, context, str(issue.number))
            [next_activity, next_activity_date, expiry_date] = (
                self.nad_generator.next_activity_date(nad_status, nad_reference)
            )

            if fields.FIELD_NEXT_ACTIVITY not in instance_dict:
                instance_dict[fields.FIELD_NEXT_ACTIVITY] = {}

            instance_dict[fields.FIELD_NEXT_ACTIVITY][fields.FIELD_ACTIVITY] = next_activity
            instance_dict[fields.FIELD_NEXT_ACTIVITY][fields.FIELD_DATE] = next_activity_date

            if isinstance(expiry_date, datetime.datetime):
                expiry_date = expiry_date.strftime(TimeFormats.STANDARD_DATE_FORMAT)

            instance_dict[fields.FIELD_EXPIRY_DATE] = expiry_date

            issue_is_final = self.determine_if_final_issue(issue.number)

            if not self._include_next_activity_for_instance(
                next_activity,
                issue.number,
                self.current_issue_number,
                self.max_repeats,
                issue_is_final,
            ):
                continue

            # treat deletion separately to next activities
            if next_activity == fields.NEXTACTIVITY_DELETE:
                delete_date = next_activity_date
                continue

            # Note: string comparison of dates in YYYYMMDD format
            if next_activity_date < earliest_activity_date:
                earliest_activity_date = next_activity_date
                earliest_activity = next_activity

            # Note: string comparison of dates in YYYYMMDD format
            if next_activity_date <= earliest_activity_date:
                for activity in fields.USER_IMPACTING_ACTIVITY:
                    if next_activity == activity or earliest_activity == activity:
                        earliest_activity = activity
                        break

        if earliest_activity:
            return [earliest_activity, earliest_activity_date]

        return [fields.NEXTACTIVITY_DELETE, delete_date]

    def _include_next_activity_for_instance(
        self, next_activity, issue_number, current_issue_number, max_repeats, issue_is_final=None
    ):
        """
        Check whether the next_activity should be included for the issue as a position
        within the prescription repeat issues.
         - The final issue (issue_number == max_repeats) supports everything
         - The previous issue(s) (issue_number < currentInstance) support createNoClaim
         - The current issue supports everything other than delete and purge
         - Future issues support nothing

        Note: we shouldn't really need to pass in the current_issue_number and max_repeats
        parameters as these are available from self. However, the unit tests are
        currently written to expect these to be passed in.

        Also note that due to missing prescription issues from Spine1, we need to be extra
        cautious and cannot just assume that later issues are present.

        :type next_activity: str
        :type issue_number: int
        :type current_issue_number: int
        :type max_repeats: int
        :rtype: bool
        """
        issue_is_current = issue_number == current_issue_number
        if not issue_is_final:
            issue_is_final = issue_number == max_repeats
        issue_is_before_current = issue_number < current_issue_number
        all_remaining_issues_missing = (issue_number < current_issue_number) and (issue_is_final)

        # default for future issue
        permitted_activities = []

        if (issue_is_current and issue_is_final) or all_remaining_issues_missing:
            # final issue
            permitted_activities = [
                fields.NEXTACTIVITY_EXPIRE,
                fields.NEXTACTIVITY_CREATENOCLAIM,
                fields.NEXTACTIVITY_READY,
                fields.NEXTACTIVITY_DELETE,
                fields.NEXTACTIVITY_PURGE,
            ]

        elif issue_is_before_current:
            # previous issue
            permitted_activities = [fields.NEXTACTIVITY_CREATENOCLAIM]

        elif issue_is_current:
            # current issue
            permitted_activities = [
                fields.NEXTACTIVITY_EXPIRE,
                fields.NEXTACTIVITY_READY,
                fields.NEXTACTIVITY_CREATENOCLAIM,
            ]

        return next_activity in permitted_activities

    def set_nad_status(self, test_prescribing_sites, context, instance_number_str):
        """
        Create the status fields that are required for the Next Activity Index calculation

        *** Shortcut taken converting time to date for prescriptionTime - relies on
        relationship between standardDate format and standardDateTimeFormat staying
        consistent ***
        """
        presc_details = self.prescription_record[fields.FIELD_PRESCRIPTION]
        inst_details = self._get_prescription_instance_data(instance_number_str, False)

        nad_status = {}
        nad_status[fields.FIELD_PRESCRIPTION_TREATMENT_TYPE] = presc_details[
            fields.FIELD_PRESCRIPTION_TREATMENT_TYPE
        ]
        nad_status[fields.FIELD_PRESCRIPTION_DATE] = presc_details[fields.FIELD_PRESCRIPTION_TIME][
            :8
        ]
        nad_status[fields.FIELD_RELEASE_VERSION] = self._release_version

        if presc_details[fields.FIELD_PRESCRIBING_ORG] in test_prescribing_sites:
            nad_status[fields.FIELD_PRESCRIBING_SITE_TEST_STATUS] = True
        else:
            nad_status[fields.FIELD_PRESCRIBING_SITE_TEST_STATUS] = False

        nad_status[fields.FIELD_DISPENSE_WINDOW_HIGH_DATE] = inst_details[
            fields.FIELD_DISPENSE_WINDOW_HIGH_DATE
        ]
        nad_status[fields.FIELD_DISPENSE_WINDOW_LOW_DATE] = inst_details[
            fields.FIELD_DISPENSE_WINDOW_LOW_DATE
        ]
        nad_status[fields.FIELD_NOMINATED_DOWNLOAD_DATE] = inst_details[
            fields.FIELD_NOMINATED_DOWNLOAD_DATE
        ]
        nad_status[fields.FIELD_LAST_DISPENSE_DATE] = inst_details[fields.FIELD_DISPENSE][
            fields.FIELD_LAST_DISPENSE_DATE
        ]
        nad_status[fields.FIELD_LAST_DISPENSE_NOTIFICATION_MSG_REF] = inst_details[
            fields.FIELD_DISPENSE
        ][fields.FIELD_LAST_DISPENSE_NOTIFICATION_MSG_REF]
        nad_status[fields.FIELD_COMPLETION_DATE] = inst_details[fields.FIELD_COMPLETION_DATE]
        nad_status[fields.FIELD_CLAIM_SENT_DATE] = inst_details[fields.FIELD_CLAIM][
            fields.FIELD_CLAIM_RECEIVED_DATE
        ]
        nad_status[fields.FIELD_HANDLE_TIME] = context.handleTime
        nad_status[fields.FIELD_PRESCRIPTION_STATUS] = self.return_prescription_status(
            instance_number_str
        )
        nad_status[fields.FIELD_INSTANCE_NUMBER] = instance_number_str

        return nad_status

    def roll_forward_instance(self):
        """
        If the currentInstance is changed, it is first stored as a pending_instance_change
        - so that the update can be applied at the end of the process
        """
        if self.pending_instance_change is not None:
            self.current_issue_number = int(self.pending_instance_change)

    def compare_line_items_for_dispense(
        self, passed_line_items, valid_status_changes, instance_number
    ):
        """
        Compare the line items provided on a dispense message with the previous (stored)
        state on the record to determine if this is a valid dispense notification for
        each line items.

        passed_line_items will be a list of line_item dictionaries - with each line_item
        having and:
        fields.FIELD_ID - to match to an ID on the record
        'DN_ID' - a GUID for the dispense notification for that specific line item (this
        will actually be ignored)
        fields.FIELD_STATUS - A changed status following the dispense of which this is a
        notification
        fields.FIELD_MAX_REPEATS - to match the max_repeats of the original record
        fields.FIELD_CURRENT_INSTANCE - to match the instanceNumber of the current record

        Note that as per SPII-6085, we should permit a Repeat Prescribe message without a
        repeat number.
        """
        treatment_type = self.prescription_record[fields.FIELD_PRESCRIPTION][
            fields.FIELD_PRESCRIPTION_TREATMENT_TYPE
        ]
        instance = self._get_prescription_instance_data(instance_number)

        stored_line_items = instance[fields.FIELD_LINE_ITEMS]
        [stored_ids, passed_ids] = [set(), set()]
        for line_item in stored_line_items:
            stored_ids.add(str(line_item[fields.FIELD_ID]))
        for line_item in passed_line_items:
            passed_ids.add(str(line_item[fields.FIELD_ID]))
        if stored_ids != passed_ids:
            self.log_object.write_log(
                "EPS0146",
                None,
                {
                    "internalID": self.internal_id,
                    "storedIDs": str(stored_ids),
                    "passedIDs": str(passed_ids),
                },
            )
            raise EpsBusinessError(EpsErrorBase.ITEM_NOT_FOUND)

        for line_item in passed_line_items:
            stored_line_item = self._return_matching_line_item(stored_line_items, line_item)
            if not stored_line_item:
                continue

            previous_status = stored_line_item[fields.FIELD_STATUS]
            new_status = line_item[fields.FIELD_STATUS]
            if [previous_status, new_status] not in valid_status_changes:
                self.log_object.write_log(
                    "EPS0148",
                    None,
                    {
                        "internalID": self.internal_id,
                        "lineItemID": line_item[fields.FIELD_ID],
                        "previousStatus": previous_status,
                        "newStatus": new_status,
                    },
                )
                raise EpsBusinessError(EpsErrorBase.INVALID_LINE_STATE_TRANSITION)

            if treatment_type == fields.TREATMENT_TYPE_ACUTE:
                continue

            if line_item[fields.FIELD_MAX_REPEATS] != stored_line_item[fields.FIELD_MAX_REPEATS]:
                if treatment_type == fields.TREATMENT_TYPE_REPEAT_PRESCRIBE:
                    self.log_object.write_log(
                        "EPS0147b",
                        None,
                        {
                            "internalID": self.internal_id,
                            "providedRepeatCount": (line_item[fields.FIELD_MAX_REPEATS]),
                            "storedRepeatCount": str(stored_line_item[fields.FIELD_MAX_REPEATS]),
                            "lineItemID": line_item[fields.FIELD_ID],
                        },
                    )
                    continue

                # SPII-14044 - permit the max_repeats for line items to be equal to the
                # prescription max_repeats as is normal when the line item expires sooner
                # than the prescription.
                if line_item.get(fields.FIELD_MAX_REPEATS) is None or self.max_repeats is None:
                    self.log_object.write_log(
                        "EPS0147d",
                        None,
                        {
                            "internalID": self.internal_id,
                            "providedRepeatCount": line_item.get(fields.FIELD_MAX_REPEATS),
                            "storedRepeatCount": (
                                self.max_repeats
                                if self.max_repeats is None
                                else str(self.max_repeats)
                            ),
                            "lineItemID": line_item.get(fields.FIELD_ID),
                        },
                    )
                    raise EpsBusinessError(EpsErrorBase.MAX_REPEAT_MISMATCH)

                if int(line_item[fields.FIELD_MAX_REPEATS]) == int(self.max_repeats):
                    self.log_object.write_log(
                        "EPS0147c",
                        None,
                        {
                            "internalID": self.internal_id,
                            "providedRepeatCount": (line_item[fields.FIELD_MAX_REPEATS]),
                            "storedRepeatCount": str(stored_line_item[fields.FIELD_MAX_REPEATS]),
                            "lineItemID": line_item[fields.FIELD_ID],
                        },
                    )
                    continue

                self.log_object.write_log(
                    "EPS0147",
                    None,
                    {
                        "internalID": self.internal_id,
                        "providedRepeatCount": (line_item[fields.FIELD_MAX_REPEATS]),
                        "storedRepeatCount": str(stored_line_item[fields.FIELD_MAX_REPEATS]),
                        "lineItemID": line_item[fields.FIELD_ID],
                    },
                )
                raise EpsBusinessError(EpsErrorBase.MAX_REPEAT_MISMATCH)

    def _return_matching_line_item(self, stored_line_items, line_item):
        """
        Match on line item ID
        """
        for stored_line_item in stored_line_items:
            if stored_line_item[fields.FIELD_ID] == line_item[fields.FIELD_ID]:
                return stored_line_item
        return None

    def return_details_for_release(self):
        """
        Need to return the status and expiryDate of the current instance - which can then
        be used in validity checks for release request messages
        """
        current_issue = self.current_issue
        details = [
            current_issue.status,
            current_issue.expiry_date_str,
            self.return_nominated_performer(),
        ]
        return details

    def return_details_for_dispense(self):
        """
        For dispense messages the following details are required:
        - Instance status
        - NHS Number
        - Dispensing Organisation
        - Max repeats (if repeat type, otherwise return None)
        """
        current_issue = self.current_issue
        max_repeats = str(
            self.prescription_record[fields.FIELD_PRESCRIPTION][fields.FIELD_MAX_REPEATS]
        )
        details = [
            str(current_issue.number),
            current_issue.status,
            self._nhs_number,
            current_issue.dispensing_organization,
            max_repeats,
        ]
        return details

    def return_last_dispense_status(self, instance_number):
        """
        Return the last_dispense_status for the requested instance
        """
        instance = self._get_prescription_instance_data(instance_number)
        last_dispense_status = instance[fields.FIELD_LAST_DISPENSE_STATUS]
        return last_dispense_status

    def return_last_dispense_date(self, instance_number):
        """
        Return the last_dispense_date for the requested instance
        """
        instance = self._get_prescription_instance_data(instance_number)
        last_dispense_date = instance[fields.FIELD_DISPENSE][fields.FIELD_LAST_DISPENSE_DATE]
        return last_dispense_date

    def return_details_for_claim(self, instance_number_str):
        """
        For claim messages the following details are required:
        - Instance status
        - NHS Number
        - Dispensing Organisation
        - Max repeats (if repeat type, otherwise return None)
        """
        issue_number = int(instance_number_str)
        issue = self.get_issue(issue_number)
        max_repeats = str(
            self.prescription_record[fields.FIELD_PRESCRIPTION][fields.FIELD_MAX_REPEATS]
        )
        details = [
            issue.claim,
            issue.status,
            self._nhs_number,
            issue.dispensing_organization,
            max_repeats,
        ]
        return details

    def return_last_disp_msg_ref(self, instance_number_str):
        """
        returns the last dispense Msg Ref for the issue
        """
        issue_number = int(instance_number_str)
        issue = self.get_issue(issue_number)
        return issue.last_dispense_notification_msg_ref

    def return_details_for_dispense_proposal_return(self):
        """
        For DPR changes currentInstance, instanceStatus and dispensing_org required
        """
        dispensing_org = self._current_instance_data[fields.FIELD_DISPENSE][
            fields.FIELD_DISPENSING_ORGANIZATION
        ]
        return (self.current_issue_number, self._current_instance_status, dispensing_org)

    def update_for_release(self, context):
        """
        Update a prescription to indicate valid release request:
        prescription instance to be changed to with-dispenser
        add dispense section onto the instance - with dispensingOrganization
        update status of individual line items
        """
        self.update_instance_status(self._current_instance_data, PrescriptionStatus.WITH_DISPENSER)
        self._current_instance_data[fields.FIELD_DISPENSE][
            fields.FIELD_DISPENSING_ORGANIZATION
        ] = context.agentOrganization
        release_date = context.handleTime.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        self._current_instance_data[fields.FIELD_RELEASE_DATE] = release_date

        self.update_line_item_status(
            self._current_instance_data,
            LineItemStatus.TO_BE_DISPENSED,
            LineItemStatus.WITH_DISPENSER,
        )
        self.set_exemption_dates()

    def update_for_dispense(
        self,
        context,
        days_supply,
        nom_down_lead_days,
        nom_download_date_enabled,
        maintain_instance=False,
    ):
        """
        Update a prescription to indicate valid dispense notification:
        prescription instance to be changed to reflect passed-in status
        update status of individual line items to reflect passed-in status

        """
        if context.isAmendment:  # noqa: SIM108 - More readable as is
            instance = self._get_prescription_instance_data(context.targetInstance)
        else:
            instance = self._current_instance_data

        instance[fields.FIELD_DISPENSE][fields.FIELD_LAST_DISPENSE_DATE] = context.dispenseDate
        instance[fields.FIELD_LAST_DISPENSE_STATUS] = context.prescriptionStatus

        if hasattr(context, "agentOrganization"):
            if context.agentOrganization:
                instance[fields.FIELD_DISPENSE][
                    fields.FIELD_DISPENSING_ORGANIZATION
                ] = context.agentOrganization

        if context.prescriptionStatus in PrescriptionStatus.COMPLETED_STATES:
            instance[fields.FIELD_COMPLETION_DATE] = context.dispenseDate
            self.set_next_instance_prior_issue_date(context)
            self.release_next_instance(
                context, days_supply, nom_down_lead_days, nom_download_date_enabled
            )
        self.update_line_item_status_from_dispense(instance, context.lineItems)

        if maintain_instance:
            return

        self.update_instance_status(instance, context.prescriptionStatus)

    def update_for_rebuild(
        self, context, days_supply, nom_down_lead_days, dispense_dict, nom_download_date_enabled
    ):
        """
        Complete the actions required to update the prescription instance with the changes
        made in the interaction worker
        """
        instance = self._get_prescription_instance_data(context.targetInstance)
        instance[fields.FIELD_DISPENSE][fields.FIELD_LAST_DISPENSE_DATE] = dispense_dict[
            fields.FIELD_DISPENSE_DATE
        ]
        instance[fields.FIELD_LAST_DISPENSE_STATUS] = dispense_dict[
            fields.FIELD_PRESCRIPTION_STATUS
        ]
        if dispense_dict[fields.FIELD_PRESCRIPTION_STATUS] in PrescriptionStatus.COMPLETED_STATES:
            instance[fields.FIELD_COMPLETION_DATE] = dispense_dict[fields.FIELD_DISPENSE_DATE]
            self.set_next_instance_prior_issue_date(context, context.targetInstance)
            self.release_next_instance(
                context,
                days_supply,
                nom_down_lead_days,
                nom_download_date_enabled,
                context.targetInstance,
            )
        self.update_line_item_status_from_dispense(instance, dispense_dict[fields.FIELD_LINE_ITEMS])
        self.update_instance_status(instance, dispense_dict[fields.FIELD_PRESCRIPTION_STATUS])

    def update_for_claim(self, context, instance_number):
        """
        Update a prescription to indicate valid dispense claim received:
        prescription instance to be changed to reflect passed-in status
        Do not update status of individual line items
        Add Claim details to record
        """
        instance = self._get_prescription_instance_data(instance_number)
        self.update_instance_status(instance, PrescriptionStatus.CLAIMED)
        instance[fields.FIELD_CLAIM][fields.FIELD_CLAIM_RECEIVED_DATE] = context.claimDate
        instance[fields.FIELD_CLAIM][fields.FIELD_CLAIM_STATUS] = fields.FIELD_CLAIMED_DISPLAY_NAME
        instance[fields.FIELD_CLAIM][fields.FIELD_CLAIM_REBUILD] = False
        instance[fields.FIELD_CLAIM][fields.FIELD_CLAIM_GUID] = context.dispenseClaimID

    def update_for_claim_amend(self, context, instance_number):
        """
        Modification of update_for_claim for use when the claim is an amendment.
        - Do not change the claimReceivedDate from the original value
        - Change claimRebuild to True
        Update a prescription to indicate valid dispense claim received:
        prescription instance to be changed to reflect passed-in status
        Do not update status of individual line items
        Append the existing claimGUID into the historicClaimGUID List
        Add Claim details to record
        """
        instance = self._get_prescription_instance_data(instance_number)
        self.update_instance_status(instance, PrescriptionStatus.CLAIMED)
        instance[fields.FIELD_CLAIM][fields.FIELD_CLAIM_RECEIVED_DATE] = context.claimDate
        instance[fields.FIELD_CLAIM][fields.FIELD_CLAIM_STATUS] = fields.FIELD_CLAIMED_DISPLAY_NAME
        instance[fields.FIELD_CLAIM][fields.FIELD_CLAIM_REBUILD] = True
        if fields.FIELD_HISTORIC_CLAIMS not in instance[fields.FIELD_CLAIM]:
            instance[fields.FIELD_CLAIM][fields.FIELD_HISTORIC_CLAIM_GUIDS] = []
        claim_guid = instance[fields.FIELD_CLAIM][fields.FIELD_CLAIM_GUID]
        instance[fields.FIELD_CLAIM][fields.FIELD_HISTORIC_CLAIM_GUIDS].append(claim_guid)
        instance[fields.FIELD_CLAIM][fields.FIELD_CLAIM_GUID] = context.dispenseClaimID

    def update_for_return(self, _, retain_nomination=False):
        """
        If this is a nominated prescription then check that the nominated performer is in
        the nomination history and clear the current value.

        The status then needs to be changed for the prescription and the line items
        """
        self.clear_dispensing_organisation(self._current_instance_data)

        self.update_instance_status(self._current_instance_data, PrescriptionStatus.TO_BE_DISPENSED)
        self.update_line_item_status(
            self._current_instance_data,
            LineItemStatus.WITH_DISPENSER,
            LineItemStatus.TO_BE_DISPENSED,
        )
        if retain_nomination:
            return

        nom_details = self.prescription_record[fields.FIELD_NOMINATION]
        if nom_details[fields.FIELD_NOMINATED]:
            if (
                nom_details[fields.FIELD_NOMINATED_PERFORMER]
                not in nom_details[fields.FIELD_NOMINATION_HISTORY]
            ):
                nom_details[fields.FIELD_NOMINATION_HISTORY].append(
                    nom_details[fields.FIELD_NOMINATED_PERFORMER]
                )
            nom_details[fields.FIELD_NOMINATED_PERFORMER] = None

    def clear_dispensing_organisation(self, instance):
        """
        Clear the dispensing organisation from the instance
        """
        instance[fields.FIELD_DISPENSE][fields.FIELD_DISPENSING_ORGANIZATION] = None

    def check_action_applicability(self, target_instance, action, context):
        """
        The batch worker will always use 'Available' as the target reference, if this isn't
        the target instance then the update has come from a test or admin system that needs
        to take action on a specific instance, so skip the applicability test.
        """
        if target_instance != fields.BATCH_STATUS_AVAILABLE:
            self.set_instance_to_action_update(target_instance, context, action)
        else:
            self.find_instances_to_action_update(context, action)

    def set_instance_to_action_update(self, target_instance, context, action):
        """
        Set the instance to action update based on the value passed in the request
        """
        context.instancesToUpdate = str(target_instance)
        self.log_object.write_log(
            "EPS0407b",
            None,
            {
                "internalID": self.internal_id,
                "passedAction": str(action),
                "instancesToUpdate": str(target_instance),
            },
        )

    def find_instances_to_action_update(self, context, action):
        """
        Check all available instances for any that match the activity and have passed the
        next activity date. This date check is important, as all instances of a prescription
        will have 'expire' as the NAD status to start with.
        """
        issues_to_update = []
        rejected_list = []

        activity_to_look_for = fields.ACTIVITY_LOOKUP[action]
        handle_date = context.handleTime.strftime(TimeFormats.STANDARD_DATE_FORMAT)

        for issue in self.issues:
            # Special case to reset the NextActivityDate for prescriptions that were migrated without a NAD
            if (issue.status == PrescriptionStatus.AWAITING_RELEASE_READY) and (
                action == fields.ADMIN_ACTION_RESET_NAD
            ):
                issues_to_update.append(issue)
            # Special case to allow the reset of the current instance
            if action == fields.SPECIAL_RESET_CURRENT_INSTANCE:
                issues_to_update.append(issue)
                # break the loop once at least one issue has been identified.
                if issues_to_update:
                    break
            # Special case to return the dispense notification to Spine in the case that it is 'hung'
            if action == fields.SPECIAL_DISPENSE_RESET:
                self._confirm_dispense_reset_on_issue(issues_to_update, issue)
            # Special case to apply cancellations to those that weren't set post migration - issue 110898
            if action == fields.SPECIAL_APPLY_PENDING_CANCELLATIONS:
                self._confirm_cancellations_to_apply(issues_to_update, issue)
                # break the loop once the first issue has been identified.
                if issues_to_update:
                    break
            # NOTE: SPII-10495 some migrated prescriptions don't have the 'activity' field
            # populated, so guard against this to avoid killing process.
            if issue.next_activity is not None:
                # Note: string comparison of dates in YYYYMMDD format
                action_is_due = issue.next_activity_date_str <= handle_date

                if (activity_to_look_for == issue.next_activity) and action_is_due:
                    issues_to_update.append(issue)
                else:
                    rejection_ref = str(issue.number)
                    rejection_ref += "|" + issue.next_activity
                    rejection_ref += "|" + issue.next_activity_date_str
                    rejected_list.append(rejection_ref)

        if issues_to_update:
            # Note: calling code currently expects issue numbers as strings
            context.instancesToUpdate = [str(issue.number) for issue in issues_to_update]
            self.log_object.write_log(
                "EPS0407",
                None,
                {
                    "internalID": self.internal_id,
                    "passedAction": str(action),
                    "instancesToUpdate": context.instancesToUpdate,
                },
            )
        else:
            self.log_object.write_log(
                "EPS0405",
                None,
                {
                    "internalID": self.internal_id,
                    "handleDate": handle_date,
                    "passedAction": activity_to_look_for,
                    "recordAction": str(rejected_list),
                },
            )

    def _confirm_cancellations_to_apply(self, issues_to_update, issue):
        """
        Only apply pending cancellations to those issuse that are safe to cancel. It is
        fine to reapply cancellations that have already been successful, and cancellation
        takes precedence over expiry so no need to check the detailed status, only that
        the prescription is in a cancellable state.
        The cancellation worker will apply the cancellation to the first available issue and
        all subsequent issues (due to constraints with active prescriptions, issue n+x must
        be cancellable if issue n is cancellable). So only need to identify the first issue
        """
        if issue.status in PrescriptionStatus.CANCELLABLE_STATES:
            issues_to_update.append(issue)

    def _confirm_dispense_reset_on_issue(self, issues_to_update, issue):
        """
        This code is to handle an exception that happened at go-live whereby some
        prescriptions could not be read and need to be reset in bulk. The conditions for
        reset are:
        1) The issue state is still 0002 - With Dispenser, i.e. it has not progressed to
        with-dispenser active, dispensed or been returned, cancelled or expired.
        2) The prescription issue was downloaded on the 24th, 25th, 26th or 27th August 2014,
        (this is the time that the issue was resolved in Live.)
        The second check is required to protect against the scenario where the one issue
        was downloaded within the target window, but this was successfully processed and
        subsequently dispensed, releasing a new issue which may be status 0002, but will
        not have a release date within the target window.
        """
        # declared here as this whole method should be removed post clean-up
        special_dispense_reset_dates = [
            "20140824",
            "20140825",
            "20140826",
            "20140827",
            "20140828",
            "20140829",
            "20140830",
            "20140831",
            "20140901",
            "20140902",
            "20140903",
            "20140904",
            "20140905",
            "20140906",
            "20140907",
            "20140908",
        ]

        if issue.status != PrescriptionStatus.WITH_DISPENSER:
            return

        release_date = issue.release_date
        if release_date and str(release_date) in special_dispense_reset_dates:
            issues_to_update.append(issue)

    def update_by_action(self, context, nom_download_date_enabled=True):
        """
        Update the record by performing the necessary logic to carry out the specified
        action.

        These actions are responsible for maintaining consistent record state, so the
        calling code does not need to do this.

        Deletion is applied to the whole record (all issues), but other actions will
        apply to all issues in instancesToUpdate. Note that expiring an issue will
        expire all future issues as well.
        """
        action = context.action

        # prescription-wide actions
        if action == fields.NEXTACTIVITY_DELETE:
            self._update_delete(context)
        else:
            # instance-specific actions
            if context.instancesToUpdate:
                for issue_number in context.instancesToUpdate:
                    # make sure this is really an int, and not a str
                    issue_number_int = int(issue_number)
                    self.perform_instance_specific_updates(
                        issue_number_int, context, nom_download_date_enabled
                    )

    def perform_instance_specific_updates(
        self, target_issue_number, context, nom_download_date_enabled
    ):
        """
        Perform the actions that would be specific to an instance and could apply to more
        than one instance.
        Return after nominated download as only Expire and Create No Claim should add a
        completion date and release the next instance
        Release next instance and roll forward instance are both safe to re-apply as they
        check first for the correct instance state (awaiting release ready).

        :type target_issue_number: int
        :type context: ???
        """
        issue = self.get_issue(target_issue_number)

        # dispatch based on action

        if context.action == fields.ACTIVITY_NOMINATED_DOWNLOAD:
            # make an issue available for download
            self._update_make_available_for_nominated_download(issue)

        elif context.action == fields.SPECIAL_RESET_CURRENT_INSTANCE:
            old_current_issue_number, new_current_issue_number = self.reset_current_instance()
            if old_current_issue_number != new_current_issue_number:
                self.log_object.write_log(
                    "EPS0401c",
                    None,
                    {
                        "internalID": self.internal_id,
                        "oldCurrentIssue": old_current_issue_number,
                        "newCurrentIssue": new_current_issue_number,
                        "prescriptionID": context.prescriptionID,
                    },
                )
                self.current_issue_number = new_current_issue_number
            else:
                context.updatesToApply = False

        elif context.action == fields.SPECIAL_DISPENSE_RESET:
            # Special case to reset the dispense status. This needs to perform a dispense
            # proposal return and then re-set the nominated performer
            self.update_for_return(None, True)

        elif context.action == fields.SPECIAL_APPLY_PENDING_CANCELLATIONS:
            # No action to be taken at this level, just pass.
            pass

        elif context.action == fields.NEXTACTIVITY_EXPIRE:
            # NOTE (SPII-10316): when requested to expire an issue, we must expire all
            # subsequent issues as well, and set the current issue indicator to point at
            # the last issue
            issues_to_expire = self.get_issues_in_range(issue.number, None)
            for issue_to_expire in issues_to_expire:
                issue_to_expire.expire(context.handleTime, self)

            self.current_issue_number = self.max_repeats

        elif context.action == fields.NEXTACTIVITY_CREATENOCLAIM:
            self._create_no_claim(issue, context.handleTime)
            issue.mark_completed(context.handleTime, self)
            self._move_to_next_issue_if_possible(issue.number, context, nom_download_date_enabled)

        elif context.action == fields.ADMIN_ACTION_RESET_NAD:
            # Log that the prescription has been touched, but no change should be made
            self.log_object.write_log(
                "EPS0401b",
                None,
                {"internalID": self.internal_id, "prescriptionID": context.prescriptionID},
            )
        else:
            # invalid action
            self.log_object.write_log(
                "EPS0401",
                None,
                {
                    "internalID": self.internal_id,
                    "action": str(context.action),
                },
            )

    def _move_to_next_issue_if_possible(self, issue_number, context, nom_download_date_enabled):
        """
        Release the next issue, if possible, and mark it as the current issue

        :type issue_number: int
        :type context : ???
        """
        # if this isn't the last issue...
        if issue_number < self.max_repeats:
            # Note: we know this is a Repeat Dispensing prescription, as it has multiple
            # issues
            context.prescriptionRepeatLow = context.targetInstance
            self.release_next_instance(
                context,
                self.get_days_supply(),
                fields.NOMINATED_DOWNLOAD_LEAD_DAYS,
                nom_download_date_enabled,
                str(issue_number),
            )
            self.roll_forward_instance()

    def get_days_supply(self):
        """
        Return the days supply from the prescription record, this will have been set to the
        value passed in the original prescription, or the default 28 days
        """
        days_supply = self.prescription_record[fields.FIELD_PRESCRIPTION][fields.FIELD_DAYS_SUPPLY]
        # Habdle records that were migrated with null daysSupply rather than 0.
        if not days_supply:
            return 0
        if isinstance(days_supply, int):
            return days_supply
        # Habdle records that were migrated with blank space in the daysSupply rather than 0.
        if not days_supply.strip():
            return 0
        return int(days_supply)

    def _create_no_claim(self, issue, handle_time):
        """
        Update the prescription status to No Claimed.

        :type issue: PrescriptionIssue
        :type handle_time: datetime.datetime
        """
        issue.update_status(PrescriptionStatus.NO_CLAIMED, self)

        handle_time_str = handle_time.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        issue.claim.received_date_str = handle_time_str
        self.log_attribute_change(fields.FIELD_CLAIM_RECEIVED_DATE, "", handle_time_str, None)

        self.log_object.write_log("EPS0406", None, {"internalID": self.internal_id})

    def _update_make_available_for_nominated_download(self, issue):
        """
        Update the prescription state to make it available for nominated download

        :type issue: PrescriptionIssue
        """
        issue.update_status(PrescriptionStatus.TO_BE_DISPENSED, self)

        self.log_object.write_log("EPS0402", None, {"internalID": self.internal_id})

    def _verify_record_deletion(self):
        """
        Confirm that it is ok to delete the record by checking through the next activities
        of each of the prescription issues, if not then log and return false
        """
        for issue_key in self.prescription_record[fields.FIELD_INSTANCES]:
            issue = self._get_prescription_instance_data(issue_key)
            next_activity_for_issue = issue.get(fields.FIELD_NEXT_ACTIVITY, {}).get(
                fields.FIELD_ACTIVITY
            )
            if next_activity_for_issue == fields.NEXTACTIVITY_DELETE:
                continue

            self.log_object.write_log(
                "EPS0404b",
                None,
                {
                    "internalID": self.internal_id,
                    "prescriptionID": self.id,
                    "nextActivity": next_activity_for_issue,
                    "issue": issue_key,
                },
            )
            return False
        return True

    def _update_delete(self, context):
        """
        Update the entire prescription to delete it
        """
        if not self._verify_record_deletion():
            return

        doc_list = []
        if self.prescription_record.get(fields.FIELDS_DOCUMENTS) is not None:
            for document in self.prescription_record[fields.FIELDS_DOCUMENTS]:
                doc_list.append(document)
        if doc_list:
            context.documentsToDelete = doc_list

        context.recordToDelete = context.prescriptionID[:-1]

        context.updatesToApply = False

        self.log_object.write_log(
            "EPS0404",
            None,
            {
                "internalID": self.internal_id,
                "recordRef": context.recordToDelete,
                "documentRefs": context.documentsToDelete,
            },
        )

    def update_by_admin(self, context):
        """
        Set values from admin message straight into record
        Log each change
        Changes are not validated - the whole record will be validated once the full lot
        of amendments have been made

        If record is a prescription that has not yet been acted upon, there will be no
        previous status

        Perform the prescription level changes
        Determine the instance or range of instances to be updated
        Reset the context.currentInstance as this is used later in the validation
        Run the instance update(s)
        """
        current_instance = context.currentInstance

        if context.handleOverdueExpiry:
            self.handle_overdue_expiry(context)
        # nominatedPerformer will be None in the removal scenario so check for nominatedPerformerType too
        if context.nominatedPerformerType or context.nominatedPerformer:
            self.update_nominated_performer(context)

        [range_flag, start_instance, end_instance] = self.instances_to_update(current_instance)
        context.currentInstance = self.return_current_instance()

        # find out which issues need updating
        lowest = int(start_instance)
        highest = int(end_instance) if range_flag else lowest
        issue_numbers_to_update = self.get_issue_numbers_in_range(lowest, highest)

        # update the issues
        for issue_number in issue_numbers_to_update:
            self._make_admin_instance_updates(context, issue_number)

        return [True, None, None]

    def is_expiry_overdue(self):
        """
        Check the expected Expiry date on the record, if in the past return True
        """
        nad = self.return_next_activity_nad_bin()
        return self._is_expiry_overdue(nad)

    def is_next_activity_purge(self):
        """
        Check if records next activity is purge
        """
        next_activity = self.return_next_activity_nad_bin()
        if next_activity:
            if next_activity[0].startswith(fields.NEXTACTIVITY_PURGE):
                return True
        return False

    @staticmethod
    def _is_expiry_overdue(nad):
        """
        return True if Expiry is overdue or index isn't set
        """
        if not nad:
            return False
        if nad[0] is None:  # badly behaved prescriptions from pre-golive
            return False
        if not nad[0][:6] == fields.NEXTACTIVITY_EXPIRE:
            return False
        if nad[0][7:15] >= datetime.datetime.now().strftime(TimeFormats.STANDARD_DATE_FORMAT):
            return False
        return True

    def handle_overdue_expiry(self, context):
        """
        Check the expected Expiry date on the record, if in the past, expire the line
        and prescription.
        """
        nad = context.epsRecord.return_next_activity_nad_bin()
        if not self._is_expiry_overdue(nad):
            return

        self.log_object.write_log("EPS0335", None, {"internalID": self.internal_id})
        context.overdueExpiry = True

        # Only set the status to Expired if not already part of the admin update
        if (
            not context.prescriptionStatus
            or context.prescriptionStatus not in PrescriptionStatus.EXPIRY_IMMUTABLE_STATES
        ):
            context.prescriptionStatus = PrescriptionStatus.EXPIRED

        # Set the completion date if not already part of the admin update
        if not context.completionDate:
            context.completionDate = datetime.datetime.now().strftime(
                TimeFormats.STANDARD_DATE_FORMAT
            )

        # Create a LineDict if one does not already exist and ensure that all LineItems are included
        if not context.lineDict:
            context.lineDict = {}
        for line_item in context.epsRecord.current_issue.line_items:
            if line_item.id in context.lineDict:
                continue
            context.lineDict[line_item.id] = LineItemStatus.EXPIRED

    def instances_to_update(self, target_instance):
        """
        Check the target_instance value passed in the admin update request and set a
        range or single instance target accordingly.

        The target_instance will be provided as either a integer or 'All', 'Available'
        or 'Current', where the behaviour is:
        All = all instances, including any past (complete) instances
        Available = current through to final instance, not including any past instances
        Current = the recorded current instance only, not a range

        Otherwise, the target_instance passed is an integer identifying the target
        instance.
        """
        recorded_current_instance = self.return_current_instance()
        recorded_max_instance = str(self.max_repeats)

        instance_range = False
        end_instance = None

        if target_instance == fields.BATCH_STATUS_ALL:
            instance_range = True
            start_instance = "1"
            end_instance = recorded_max_instance
        elif target_instance == fields.BATCH_STATUS_AVAILABLE:
            instance_range = True
            start_instance = recorded_current_instance
            end_instance = recorded_max_instance
        elif target_instance == fields.BATCH_STATUS_CURRENT:
            start_instance = recorded_current_instance
        else:
            start_instance = target_instance

        if instance_range:
            self.log_object.write_log(
                "EPS0297a",
                None,
                {
                    "internalID": self.internal_id,
                    "startInstance": start_instance,
                    "endInstance": end_instance,
                },
            )
        else:
            self.log_object.write_log(
                "EPS0297b",
                None,
                {
                    "internalID": self.internal_id,
                    "startInstance": start_instance,
                },
            )

        return [instance_range, start_instance, end_instance]

    def make_withdrawal_updates(self, context):
        """
        Apply instance specific updates into record
        """
        target_instance = context.targetInstance
        prescription = self.prescription_record
        instance = prescription[fields.FIELD_INSTANCES][target_instance]
        instance[fields.FIELD_DISPENSE] = context.dispenseElement
        instance[fields.FIELD_LINE_ITEMS] = context.lineItems
        instance[fields.FIELD_PREVIOUS_STATUS] = instance[fields.FIELD_PRESCRIPTION_STATUS]
        instance[fields.FIELD_PRESCRIPTION_STATUS] = context.prescriptionStatus
        instance[fields.FIELD_LAST_DISPENSE_STATUS] = context.lastDispenseStatus
        instance[fields.FIELD_COMPLETION_DATE] = context.completionDate

    def _make_admin_instance_updates(self, context, instance_number):
        """
        Apply instance specific updates into record
        """
        current_instance = str(instance_number)
        context.updateInstance = instance_number
        prescription = self.prescription_record
        instance = prescription[fields.FIELD_INSTANCES][current_instance]
        dispense = instance[fields.FIELD_DISPENSE]
        claim = instance[fields.FIELD_CLAIM]

        if context.prescriptionStatus:
            self.log_attribute_change(
                fields.FIELD_PRESCRIPTION_STATUS,
                instance[fields.FIELD_PRESCRIPTION_STATUS],
                context.prescriptionStatus,
                context.fieldsToUpdate,
            )
            instance[fields.FIELD_PREVIOUS_STATUS] = instance[fields.FIELD_PRESCRIPTION_STATUS]
            instance[fields.FIELD_PRESCRIPTION_STATUS] = context.prescriptionStatus

        if context.completionDate:
            self.log_attribute_change(
                fields.FIELD_COMPLETION_DATE,
                instance[fields.FIELD_COMPLETION_DATE],
                context.completionDate,
                context.fieldsToUpdate,
            )
            instance[fields.FIELD_COMPLETION_DATE] = context.completionDate

        if context.dispenseWindowLowDate:
            self.log_attribute_change(
                fields.FIELD_DISPENSE_WINDOW_LOW_DATE,
                instance[fields.FIELD_DISPENSE_WINDOW_LOW_DATE],
                context.dispenseWindowLowDate,
                context.fieldsToUpdate,
            )
            instance[fields.FIELD_DISPENSE_WINDOW_LOW_DATE] = context.dispenseWindowLowDate

        if context.nominatedDownloadDate:
            self.log_attribute_change(
                fields.FIELD_NOMINATED_DOWNLOAD_DATE,
                instance[fields.FIELD_NOMINATED_DOWNLOAD_DATE],
                context.nominatedDownloadDate,
                context.fieldsToUpdate,
            )
            instance[fields.FIELD_NOMINATED_DOWNLOAD_DATE] = context.nominatedDownloadDate

        if context.releaseDate:
            self.log_attribute_change(
                fields.FIELD_RELEASE_DATE,
                instance[fields.FIELD_RELEASE_DATE],
                context.releaseDate,
                context.fieldsToUpdate,
            )
            instance[fields.FIELD_RELEASE_DATE] = context.releaseDate

        if context.dispensingOrganization:
            self.log_attribute_change(
                fields.FIELD_DISPENSING_ORGANIZATION,
                dispense[fields.FIELD_DISPENSING_ORGANIZATION],
                context.dispensingOrganization,
                context.fieldsToUpdate,
            )
            dispense[fields.FIELD_DISPENSING_ORGANIZATION] = context.dispensingOrganization

        # This is to reset the dispensing org
        if context.dispensingOrgNullFlavor:
            self.log_attribute_change(
                fields.FIELD_DISPENSING_ORGANIZATION,
                dispense[fields.FIELD_DISPENSING_ORGANIZATION],
                "None",
                context.fieldsToUpdate,
            )
            dispense[fields.FIELD_DISPENSING_ORGANIZATION] = None

        if context.lastDispenseDate:
            self.log_attribute_change(
                fields.FIELD_LAST_DISPENSE_DATE,
                dispense[fields.FIELD_LAST_DISPENSE_DATE],
                context.lastDispenseDate,
                context.fieldsToUpdate,
            )
            dispense[fields.FIELD_LAST_DISPENSE_DATE] = context.lastDispenseDate

        if context.claimSentDate:
            self.log_attribute_change(
                fields.FIELD_CLAIM_SENT_DATE,
                claim[fields.FIELD_CLAIM_RECEIVED_DATE],
                context.claimSentDate,
                context.fieldsToUpdate,
            )
            claim[fields.FIELD_CLAIM_RECEIVED_DATE] = context.claimSentDate

        for line_item_id in context.lineDict:
            for current_line_item in instance[fields.FIELD_LINE_ITEMS]:
                if current_line_item[fields.FIELD_ID] != line_item_id:
                    continue
                current_line_status = current_line_item[fields.FIELD_STATUS]
                if context.overdueExpiry:
                    if current_line_status in LineItemStatus.EXPIRY_IMMUTABLE_STATES:
                        continue
                    changed_line_status = LineItemStatus.EXPIRED
                else:
                    changed_line_status = context.lineDict[line_item_id]
                self.log_object.write_log(
                    "EPS0072",
                    None,
                    {
                        "internalID": self.internal_id,
                        "prescriptionID": context.prescriptionID,
                        "lineItemChanged": line_item_id,
                        "previousStatus": current_line_status,
                        "newStatus": changed_line_status,
                    },
                )
                current_line_item[fields.FIELD_STATUS] = changed_line_status

    def log_attribute_change(self, item_changed, previous_value, new_value, fields_to_update):
        """
        Used by the update record function to change an existing attribute on the record
        Both old and new values as well as the field name are logged
        """
        if fields_to_update is not None:
            fields_to_update.append(item_changed)

        self.log_object.write_log(
            "EPS0071",
            None,
            {
                "internalID": self.internal_id,
                "itemChanged": item_changed,
                "previousValue": previous_value,
                "newValue": new_value,
            },
        )

    def _extract_dispense_date_from_context(self, context):
        """
        Get the Dispense date from context, or use handleTime if not available.

        :type context: ???
        :rtype: str
        """
        dispense_date = context.handleTime.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        if hasattr(context, fields.FIELD_DISPENSE_DATE):
            if context.dispenseDate is not None:
                dispense_date = context.dispenseDate
        return dispense_date

    def _extract_dispense_datetime_from_context(self, context):
        """
        Get the Dispense datetime from context, or use handleTime if not available.

        :type context: ???
        :rtype: str
        """
        dispense_time = context.handleTime.strftime(TimeFormats.STANDARD_DATE_TIME_FORMAT)
        if hasattr(context, fields.FIELD_DISPENSE_TIME):
            if context.dispenseTime is not None:
                dispense_time = context.dispenseTime
        return dispense_time

    def _calculate_nominated_download_date(
        self, prescribe_date, days_supply, lead_days, next_issue_number
    ):
        """
        Calculate the date for nominated download, taking into account lead time and supply length.

        :type prescribe_date: str
        :type days_supply: int
        :type lead_days: int
        :rtype: datetime.datetime
        :type next_issue_number: str
        """
        nominated_download_date = datetime.datetime.strptime(
            prescribe_date, TimeFormats.STANDARD_DATE_FORMAT
        )
        duration = days_supply * (int(next_issue_number) - 1)
        nominated_download_date += relativedelta(days=+duration)
        nominated_download_date += relativedelta(days=-lead_days)
        return nominated_download_date

    def _calculate_nominated_download_date_old(self, dispense_date, days_supply, lead_days):
        """
        Calculate the date for nominated download, taking into account lead time and supply length.

        :type dispense_date: str
        :type days_supply: int
        :type lead_days: int
        :rtype: datetime.datetime
        """
        nominated_download_date = datetime.datetime.strptime(
            dispense_date, TimeFormats.STANDARD_DATE_FORMAT
        )
        nominated_download_date += relativedelta(days=+days_supply)
        nominated_download_date += relativedelta(days=-lead_days)
        return nominated_download_date

    def return_next_issue_number(self, issue_number=None):
        """
        Wrapper for _find_next_future_issue_number, allows an optional start issue to be passed in
        otherwise will use the current issue number
        """
        if not issue_number:
            issue_number = self.current_issue_number

        return self._find_next_future_issue_number(str(issue_number))

    def _find_next_future_issue_number(self, issue_number_str, skip_check_for_correct_status=False):
        """
        Find the next issue number after the specified one, if valid.

        :type issue_number_str: str or ???
        :rtype: str or None
        """
        if not issue_number_str:
            return None

        next_issue_number = int(issue_number_str) + 1

        # make sure the prescription actually has this issue
        if next_issue_number not in self.issue_numbers:
            return None

        if skip_check_for_correct_status:
            return str(next_issue_number)

        # examine the issue to make sure it's in the correct state
        next_issue = self.get_issue(next_issue_number)
        if not next_issue.status == PrescriptionStatus.REPEAT_DISPENSE_FUTURE_INSTANCE:
            return None

        # if we get this far, then we have a valid next issue, so return its number
        # Note: calling code is currently expecting a str, so convert,until we've had
        # a chance to refactor properly
        return str(next_issue_number)

    def set_next_instance_prior_issue_date(self, context, current_issue_number_str=None):
        """
        Set the prior issue date for the next instance, this is done as part of the
        dispense notification process, but may form part of a standard dispense, a
        dispense amendment or a rebuild dispense history.
        """
        if not current_issue_number_str:
            current_issue_number_str = context.prescriptionRepeatLow

        # find the number of the next issue, if there is a valid one. Don't check for
        # valid status of the next instance as this could be a rebuild or amendment
        # and the next issue may already be active.
        next_issue_number_str = self._find_next_future_issue_number(
            current_issue_number_str, skip_check_for_correct_status=True
        )
        if next_issue_number_str:
            instance = self._get_prescription_instance_data(next_issue_number_str)
            instance[fields.FIELD_PREVIOUS_ISSUE_DATE] = (
                self._extract_dispense_datetime_from_context(context)
            )

    def release_next_instance(
        self,
        context,
        days_supply,
        nom_down_lead_days,
        nom_download_date_enabled,
        current_issue_number_str=None,
    ):
        """
        If not a repeat prescription (and no prescriptionRepeatLow provided),
        no future issue to release. Otherwise, use the prescriptionRepeatLow to
        determine the next issue - if it is there then change the status of that
        issue to awaiting-release-ready, and set the dispenseWindowLowDate

        Note that it is possible that this will be invoked as part of an amendment.
        """
        if not current_issue_number_str:
            current_issue_number_str = context.prescriptionRepeatLow

        # find the number of the next issue, if there is a valid one
        next_issue_number_str = self._find_next_future_issue_number(current_issue_number_str)
        if next_issue_number_str is None:
            # give up if there is no next issue
            self.pending_instance_change = None
            return

        # update the issue
        dispense_date = self._extract_dispense_date_from_context(context)
        prescribe_date = context.epsRecord.return_prescription_time()
        if nom_download_date_enabled:
            if prescribe_date is None:
                self.log_object.write_log(
                    "EPS0676",
                    None,
                    {"internalID": self.internal_id, "prescriptionID": context.prescriptionID},
                )
            nominated_download_date = self._calculate_nominated_download_date(
                prescribe_date[:8], days_supply, nom_down_lead_days, next_issue_number_str
            )
            self.log_object.write_log(
                "EPS0675",
                None,
                {
                    "internalID": self.internal_id,
                    "prescriptionID": context.prescriptionID,
                    "nominatedDownloadDate": nominated_download_date.strftime(
                        TimeFormats.STANDARD_DATE_FORMAT
                    ),
                    "prescribeDate": prescribe_date,
                    "daysSupply": str(days_supply),
                    "leadDays": str(nom_down_lead_days),
                    "issueNumber": next_issue_number_str,
                },
            )
        else:
            nominated_download_date = self._calculate_nominated_download_date_old(
                dispense_date, days_supply, nom_down_lead_days
            )

        if nominated_download_date >= datetime.datetime(
            context.handleTime.year, context.handleTime.month, context.handleTime.day
        ):
            new_prescription_status = PrescriptionStatus.AWAITING_RELEASE_READY
        else:
            new_prescription_status = PrescriptionStatus.TO_BE_DISPENSED

        instance = self._get_prescription_instance_data(next_issue_number_str)
        instance[fields.FIELD_PREVIOUS_STATUS] = instance[fields.FIELD_PRESCRIPTION_STATUS]
        instance[fields.FIELD_PRESCRIPTION_STATUS] = new_prescription_status
        instance[fields.FIELD_DISPENSE_WINDOW_LOW_DATE] = dispense_date
        instance[fields.FIELD_NOMINATED_DOWNLOAD_DATE] = nominated_download_date.strftime(
            TimeFormats.STANDARD_DATE_FORMAT
        )

        # mark so that we know to update the prescription's current issue number
        self.pending_instance_change = next_issue_number_str

    def add_release_document_ref(self, rel_req_document_ref):
        """
        Add the reference to the release request document to the instance.
        """
        self._current_instance_data[fields.FIELD_RELEASE_REQUEST_MGS_REF] = rel_req_document_ref

    def add_release_dispenser_details(self, rel_dispenser_details):
        """
        Add the dispenser details from the release request document to the instance.
        """
        self._current_instance_data[fields.FIELD_RELEASE_DISPENSER_DETAILS] = rel_dispenser_details

    def add_dispense_document_ref(self, dn_document_ref, target_instance=None):
        """
        Add the reference to the dispense notification document to the instance.
        """
        instance = (
            self._get_prescription_instance_data(target_instance)
            if target_instance
            else self._current_instance_data
        )
        instance[fields.FIELD_DISPENSE][
            fields.FIELD_LAST_DISPENSE_NOTIFICATION_MSG_REF
        ] = dn_document_ref

    def check_status_complete(self, prescription_status):
        """
        Check if the passed prescription status is in a complete state and return the
        appropriate boolean
        """
        return prescription_status in PrescriptionStatus.COMPLETED_STATES

    def clear_dispense_notifications_from_history(self, target_instance):
        """
        Clear all but the release from the dispense history
        """
        instance = self._get_prescription_instance_data(target_instance)
        new_dispense_history = {}
        if fields.FIELD_RELEASE in instance[fields.FIELD_DISPENSE_HISTORY]:
            release_snippet = copy(instance[fields.FIELD_DISPENSE_HISTORY][fields.FIELD_RELEASE])
            new_dispense_history[fields.FIELD_RELEASE] = release_snippet
        instance[fields.FIELD_DISPENSE_HISTORY] = copy(new_dispense_history)

    def create_dispense_history_entry(self, dn_document_guid, target_instance=None):
        """
        Create a dispense history entry to be used in future if the dispense notification
        is withdrawn. Also need to include the current prescription status

        Use the copy function to take a copy of it as it is prior to the changes
        otherwise a link is created and the data will be added at the post-update state.

        Use the last dispense date from the record unless the last dispense time is passed
        in (used for release only).
        """
        instance = (
            self._get_prescription_instance_data(target_instance)
            if target_instance
            else self._current_instance_data
        )
        instance[fields.FIELD_DISPENSE_HISTORY][dn_document_guid] = {}
        dispense_entry = instance[fields.FIELD_DISPENSE_HISTORY][dn_document_guid]
        dispense_entry[fields.FIELD_DISPENSE] = copy(instance[fields.FIELD_DISPENSE])
        dispense_entry[fields.FIELD_PRESCRIPTION_STATUS] = copy(
            instance[fields.FIELD_PRESCRIPTION_STATUS]
        )
        dispense_entry[fields.FIELD_LAST_DISPENSE_STATUS] = copy(
            instance[fields.FIELD_LAST_DISPENSE_STATUS]
        )
        line_items = []
        for line_item in instance[fields.FIELD_LINE_ITEMS]:
            line_item_copy = copy(line_item)
            line_items.append(line_item_copy)
        dispense_entry[fields.FIELD_LINE_ITEMS] = copy(line_items)
        dispense_entry[fields.FIELD_COMPLETION_DATE] = copy(instance[fields.FIELD_COMPLETION_DATE])

        instance_last_dispense = copy(
            instance[fields.FIELD_DISPENSE][fields.FIELD_LAST_DISPENSE_DATE]
        )
        if not instance_last_dispense:
            release_date = copy(instance[fields.FIELD_RELEASE_DATE])
            dispense_entry[fields.FIELD_DISPENSE][fields.FIELD_LAST_DISPENSE_DATE] = release_date
        else:
            dispense_entry[fields.FIELD_DISPENSE][
                fields.FIELD_LAST_DISPENSE_DATE
            ] = instance_last_dispense

    def create_release_history_entry(self, release_time, dispensing_org):
        """
        Create a dispense history entry specific to the release action

        Use the copy function to take a copy of it as it is prior to the changes
        otherwise a link is created and the data will be added at the post-update state.

        Set the line item status to 0008 as any withdrawal can only return the
        prescription back to 'with dispenser' state.

        Use the release date as the last dispense date to support next activity
        calculation if the dispense history is withdrawn.
        """
        instance = self._current_instance_data

        instance[fields.FIELD_DISPENSE_HISTORY][fields.FIELD_RELEASE] = {}
        dispense_entry = instance[fields.FIELD_DISPENSE_HISTORY][fields.FIELD_RELEASE]
        dispense_entry[fields.FIELD_DISPENSE] = copy(instance[fields.FIELD_DISPENSE])
        dispense_entry[fields.FIELD_PRESCRIPTION_STATUS] = copy(
            instance[fields.FIELD_PRESCRIPTION_STATUS]
        )
        dispense_entry[fields.FIELD_LAST_DISPENSE_STATUS] = copy(
            instance[fields.FIELD_LAST_DISPENSE_STATUS]
        )
        line_items = []
        for line_item in instance[fields.FIELD_LINE_ITEMS]:
            line_item_copy = copy(line_item)
            if (
                line_item_copy[fields.FIELD_STATUS] != LineItemStatus.CANCELLED
                and line_item_copy[fields.FIELD_STATUS] != LineItemStatus.EXPIRED
            ):
                line_item_copy[fields.FIELD_STATUS] = LineItemStatus.WITH_DISPENSER
            line_items.append(line_item_copy)
        dispense_entry[fields.FIELD_LINE_ITEMS] = line_items
        dispense_entry[fields.FIELD_COMPLETION_DATE] = copy(instance[fields.FIELD_COMPLETION_DATE])
        release_time_str = release_time.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        dispense_entry[fields.FIELD_DISPENSE][fields.FIELD_LAST_DISPENSE_DATE] = release_time_str
        dispense_entry[fields.FIELD_DISPENSE][fields.FIELD_DISPENSING_ORGANIZATION] = dispensing_org

    def add_dispense_document_guid(self, dn_document_guid, target_instance=None):
        """
        Add the reference to the dispense notification document to the instance.
        """
        instance = (
            self._get_prescription_instance_data(target_instance)
            if target_instance
            else self._current_instance_data
        )
        instance[fields.FIELD_DISPENSE][
            fields.FIELD_LAST_DISPENSE_NOTIFICATION_GUID
        ] = dn_document_guid

    def add_claim_document_ref(self, dn_claim_ref, instance_number):
        """
        Add the reference to the dispense claim document to the instance.
        """
        instance = self._get_prescription_instance_data(instance_number)
        instance[fields.FIELD_CLAIM][fields.FIELD_DISPENSE_CLAIM_MSG_REF] = dn_claim_ref

    def return_completion_date(self, instance_number):
        """
        Return the completion date for the requested instance
        """
        instance = self._get_prescription_instance_data(instance_number)
        return instance[fields.FIELD_COMPLETION_DATE]

    def add_claim_amend_document_ref(self, dn_claim_ref, instance_number):
        """
        Add the old claim reference to the dispense claim MsgRef history and add the new
        document to the instance.
        """
        instance = self._get_prescription_instance_data(instance_number)

        if not instance[fields.FIELD_CLAIM][fields.FIELD_HISTORIC_DISPENSE_CLAIM_MSG_REF]:
            instance[fields.FIELD_CLAIM][fields.FIELD_HISTORIC_DISPENSE_CLAIM_MSG_REF] = []

        historic_claim_msg_ref = instance[fields.FIELD_CLAIM][fields.FIELD_DISPENSE_CLAIM_MSG_REF]

        instance[fields.FIELD_CLAIM][fields.FIELD_HISTORIC_DISPENSE_CLAIM_MSG_REF].append(
            historic_claim_msg_ref
        )
        instance[fields.FIELD_CLAIM][fields.FIELD_DISPENSE_CLAIM_MSG_REF] = dn_claim_ref

    def update_instance_status(self, instance, new_status):
        """
        Method for updating the status of the current instance
        """
        if fields.FIELD_PRESCRIPTION_STATUS in instance:
            instance[fields.FIELD_PREVIOUS_STATUS] = instance[fields.FIELD_PRESCRIPTION_STATUS]
        else:
            instance[fields.FIELD_PREVIOUS_STATUS] = False
        instance[fields.FIELD_PRESCRIPTION_STATUS] = new_status

    def update_line_item_status(self, issue_dict, status_to_check, new_status):
        """
        Roll through the line items checking for those who have current status of
        status_to_check, then update to new_status and change the previous status.
        Note that this is safe for cancelled and expired line items as it will only update
        if the 'status_to_check' matches.

        :type issue_dict: dict
        :type status_to_check: str
        :type new_status: str
        """
        issue = PrescriptionIssue(issue_dict)
        for line_item in issue.line_items:
            if line_item.status == status_to_check:
                line_item.update_status(new_status)

    def update_line_item_status_from_dispense(self, instance, dn_line_items):
        """
        Roll through the line itesm on the dispense notification, and update the
        prescription record line items to the revised previousStatus and status
        """
        for dn_line_item in dn_line_items:
            for line_item in instance[fields.FIELD_LINE_ITEMS]:
                if line_item[fields.FIELD_ID] == dn_line_item[fields.FIELD_ID]:
                    line_item[fields.FIELD_PREVIOUS_STATUS] = line_item[fields.FIELD_STATUS]
                    line_item[fields.FIELD_STATUS] = dn_line_item[fields.FIELD_STATUS]

    def set_exemption_dates(self):
        """
        Set the exemption dates
        """
        patient_details = self.prescription_record[fields.FIELD_PATIENT]
        birth_time = patient_details[fields.FIELD_BIRTH_TIME]

        lower_age_limit = datetime.datetime.strptime(birth_time, TimeFormats.STANDARD_DATE_FORMAT)
        lower_age_limit += relativedelta(years=fields._YOUNG_AGE_EXEMPTION, days=-1)
        lower_age_limit = lower_age_limit.isoformat()[0:10].replace("-", "")
        higher_age_limit = datetime.datetime.strptime(birth_time, TimeFormats.STANDARD_DATE_FORMAT)
        higher_age_limit += relativedelta(years=fields._OLD_AGE_EXEMPTION)
        higher_age_limit = higher_age_limit.isoformat()[0:10].replace("-", "")
        patient_details[fields.FIELD_LOWER_AGE_LIMIT] = lower_age_limit
        patient_details[fields.FIELD_HIGHER_AGE_LIMIT] = higher_age_limit

    def return_message_ref(self, doc_type):
        """
        Return message references for different document types
        """
        if doc_type == "Prescription":
            return self.prescription_record[fields.FIELD_PRESCRIPTION][
                fields.FIELD_PRESCRIPTION_MSG_REF
            ]
        if doc_type == "ReleaseRequest":
            return self._current_instance_data[fields.FIELD_RELEASE_REQUEST_MGS_REF]
        else:
            raise EpsSystemError("developmentFailure")

    def return_release_dispenser_details(self, target_instance):
        """
        Return release dispenser details of the target instance
        """
        instance = self._get_prescription_instance_data(target_instance)
        return instance.get(fields.FIELD_RELEASE_DISPENSER_DETAILS)

    def fetch_release_response_parameters(self):
        """
        A dictionary of response parameters is required for generating the response
        message to the release request - these are parameters which will be used to
        translate and update the original prescription message
        """
        release_data = {}
        patient_details = self.prescription_record[fields.FIELD_PATIENT]
        presc_details = self.prescription_record[fields.FIELD_PRESCRIPTION]

        release_data[fields.FIELD_LOWER_AGE_LIMIT] = quoted(
            patient_details[fields.FIELD_LOWER_AGE_LIMIT]
        )
        release_data[fields.FIELD_HIGHER_AGE_LIMIT] = quoted(
            patient_details[fields.FIELD_HIGHER_AGE_LIMIT]
        )

        if self._current_instance_data.get(fields.FIELD_PREVIOUS_ISSUE_DATE):
            # SPII-10490 - handle this date not being present
            previous_issue_data = quoted(
                self._current_instance_data[fields.FIELD_PREVIOUS_ISSUE_DATE]
            )
            release_data[fields.FIELD_PREVIOUS_ISSUE_DATE] = previous_issue_data

        # !!! This is for backwards compatibility - does not make sense, should really be
        # the current status.  However Spine 1 returns previous status !!!
        # Note that we also have to remap the prescription status here if this is a GUID
        # release for a '0000' (internal only) prescription status.
        previous_presc_status = self._current_instance_data[fields.FIELD_PREVIOUS_STATUS]
        if previous_presc_status == PrescriptionStatus.AWAITING_RELEASE_READY:
            previous_presc_status = PrescriptionStatus.TO_BE_DISPENSED

        release_data[fields.FIELD_PRESCRIPTION_STATUS] = quoted(previous_presc_status)

        display_name = PrescriptionStatus.PRESCRIPTION_DISPLAY_LOOKUP[previous_presc_status]
        release_data[fields.FIELD_PRESCRIPTION_STATUS_DISPLAY_NAME] = quoted(display_name)
        release_data[fields.FIELD_PRESCRIPTION_CURRENT_INSTANCE] = quoted(
            str(self.current_issue_number)
        )
        release_data[fields.FIELD_PRESCRIPTION_MAX_REPEATS] = quoted(
            presc_details[fields.FIELD_MAX_REPEATS]
        )

        for line_item in self.current_issue.line_items:
            line_item_ref = "lineItem" + str(line_item.order)
            item_status = (
                line_item.previous_status
                if line_item.status == LineItemStatus.WITH_DISPENSER
                else line_item.status
            )

            release_data[line_item_ref + "Status"] = quoted(item_status)
            item_display_name = LineItemStatus.ITEM_DISPLAY_LOOKUP[item_status]
            release_data[line_item_ref + "StatusDisplayName"] = quoted(item_display_name)

            self.add_line_item_repeat_data(release_data, line_item_ref, line_item)

        return release_data

    def add_line_item_repeat_data(self, release_data, line_item_ref, line_item):
        """
        Add line item information (only done for repeat prescriptions)
        Note that due to inconsistency of repeat numbers, it is possible that the
        current instance for the whole prescription is greater than the line item max_repeats
        in which case the line item max_repeats should be used.

        :type release_data: dict
        :type line_item_ref: str
        :type line_item: PrescriptionLineItem
        """
        line_instance = self.current_issue_number

        if line_item.max_repeats < self.current_issue_number:
            line_instance = line_item.max_repeats

        release_data[line_item_ref + "MaxRepeats"] = quoted(str(line_item.max_repeats))
        release_data[line_item_ref + "CurrentInstance"] = quoted(str(line_instance))

    def validate_line_prescription_status(self, prescription_status, line_item_status):
        """
        Compare lineItem status with the prescription status and confirm that the combination is valid
        """
        if line_item_status in LineItemStatus.VALID_STATES[prescription_status]:
            return True

        self.log_object.write_log(
            "EPS0259",
            None,
            {
                "internalID": self.internal_id,
                "lineItemStatus": line_item_status,
                "prescriptionStatus": prescription_status,
            },
        )

        return False

    def force_current_instance_increment(self):
        """
        Force the current instance number to be incremented.
        This is a serious undertaking, but is required where an issue is missing.
        """
        old_current_issue_number = self.current_issue_number

        if self.current_issue_number == self.max_repeats:
            self.log_object.write_log(
                "EPS0625b",
                None,
                {
                    "internalID": self.internal_id,
                    "currentIssueNumber": old_current_issue_number,
                    "reason": "already at max_repeats",
                },
            )
            return

        # Count upwards from the current issue number to max_repeats, looking either for
        # an issue that exists
        new_current_issue_number = False
        for i in range(self.current_issue_number, self.max_repeats + 1):
            try:
                self.prescription_record[fields.FIELD_INSTANCES][str(i)]
                new_current_issue_number = i
                break
            except KeyError:
                continue

        if not new_current_issue_number:
            self.log_object.write_log(
                "EPS0625b",
                None,
                {
                    "internalID": self.internal_id,
                    "currentIssueNumber": old_current_issue_number,
                    "reason": "no issues available",
                },
            )
            return

        self.log_object.write_log(
            "EPS0625",
            None,
            {
                "internalID": self.internal_id,
                "oldCurrentIssueNumber": old_current_issue_number,
                "newCurrentIssueNumber": new_current_issue_number,
            },
        )

        self.current_issue_number = new_current_issue_number

    def reset_current_instance(self):
        """
        Rotate through the instances to find the first instance which is either in a
        future or active state.  Then reset the currentInstance to be this instance.
        This is used in Admin updates.  If no future/active instances - then it should
        be the last instance

        :returns: a list containing the old and new "current instance" number as strings
        :rtype: [str, str]
        """
        # see if we can find an issue from the current one upwards in an active or future state
        new_current_issue_number = None
        acceptable_states = PrescriptionStatus.ACTIVE_STATES + PrescriptionStatus.FUTURE_STATES
        for issue in self.get_issues_from_current_upwards():
            if issue.status in acceptable_states:
                new_current_issue_number = issue.number
                break

        # if we didn't find one, then just set to the last issue
        if new_current_issue_number is None:
            new_current_issue_number = self.issue_numbers[-1]

        # update the current instance number
        old_current_issue_number = self.current_issue_number
        self.current_issue_number = new_current_issue_number

        return (old_current_issue_number, new_current_issue_number)

    def check_current_instance_to_cancel_by_pr_id(self):
        """
        Check for the prescription being in a cancellable status
        """
        return self._current_instance_status in PrescriptionStatus.CANCELLABLE_STATES

    def check_current_instance_w_dispenser_by_pr_id(self):
        """
        Check for the prescription being in a with dispenser status
        """
        return self._current_instance_status in PrescriptionStatus.WITH_DISPENSER_STATES

    def check_include_performer_detail_by_pr_id(self):
        """
        Check whether the prescription status is such that the performer node should be
        included in the cancellation response message.
        """
        return self._current_instance_status in PrescriptionStatus.INCLUDE_PERFORMER_STATES

    def check_current_instance_to_cancel_by_li_id(self, line_item_ref):
        """
        Check for the line item being in a cancellable status
        """
        return self._check_current_instance_by_line_item(
            line_item_ref, LineItemStatus.ITEM_CANCELLABLE_STATES
        )

    def check_current_instance_w_dispenser_by_li_id(self, line_item_ref):
        """
        Check for the line item being in a with dispenser status
        """
        return self._check_current_instance_by_line_item(
            line_item_ref, LineItemStatus.ITEM_WITH_DISPENSER_STATES
        )

    def check_include_performer_detail_by_li_id(self, line_item_ref):
        """
        Check whether the line item status is such that the performer node should be
        included in the cancellation response message.
        """
        return self._check_current_instance_by_line_item(
            line_item_ref, LineItemStatus.INCLUDE_PERFORMER_STATES
        )

    def _check_current_instance_by_line_item(self, line_item_ref, line_item_states):
        """
        Check for the line item being in one of the specified states
        """
        for line_item in self._current_instance_data[
            fields.FIELD_LINE_ITEMS
        ]:  # noqa: SIM110 - More readable as is
            if (line_item_ref == line_item[fields.FIELD_ID]) and (
                line_item[fields.FIELD_STATUS] in line_item_states
            ):
                return True
        return False

    def check_nhs_number_match(self, context):
        """
        Check if the nhsNumber on the prescription record matches the nhsNumber in the
        cancellation. Return True or False.
        """
        return self._nhs_number == context.nhsNumber

    def return_error_for_invalid_cancel_by_pr_id(self):
        """
        Raise the correct cancellation code matching the status of the current
        instance
        """
        presc_status = self._current_instance_status

        self.log_object.write_log(
            "EPS0262",
            None,
            {
                "internalID": self.internal_id,
                "currentInstance": str(self.current_issue_number),
                "cancellationType": fields.FIELD_PRESCRIPTION,
                "currentStatus": presc_status,
            },
        )

        # return values below are to be mapped to equivalent ErrorBase1719 in Spine.
        if presc_status in PrescriptionStatus.COMPLETED_STATES:
            if presc_status == PrescriptionStatus.EXPIRED:
                return EpsErrorBase.NOT_CANCELLED_EXPIRED
            elif presc_status == PrescriptionStatus.CANCELLED:
                return EpsErrorBase.NOT_CANCELLED_CANCELLED
            elif presc_status == PrescriptionStatus.NOT_DISPENSED:
                return EpsErrorBase.NOT_CANCELLED_NOT_DISPENSED
            else:
                return EpsErrorBase.NOT_CANCELLED_DISPENSED

        if presc_status == PrescriptionStatus.WITH_DISPENSER:
            return EpsErrorBase.NOT_CANCELLED_WITH_DISPENSER
        if presc_status == PrescriptionStatus.WITH_DISPENSER_ACTIVE:
            return EpsErrorBase.NOT_CANCELLED_WITH_DISPENSER_ACTIVE

    def return_error_for_invalid_cancel_by_li_id(self, context):
        """
        Confirm if line item exists.  If it does raise the error associated with the
        line item status
        """
        line_item_status = None
        for line_item in self._current_instance_data[fields.FIELD_LINE_ITEMS]:
            if context.cancelLineItemRef != line_item[fields.FIELD_ID]:
                continue
            line_item_status = line_item[fields.FIELD_STATUS]

        self.log_object.write_log(
            "EPS0262",
            None,
            {
                "internalID": self.internal_id,
                "currentInstance": str(self.current_issue_number),
                "cancellationType": "lineItem",
                "currentStatus": line_item_status,
            },
        )

        # return values below are to be mapped to equivalent ErrorBase1719 in Spine.
        if not line_item_status:
            return EpsErrorBase.PRESCRIPTION_NOT_FOUND

        if line_item_status == LineItemStatus.FULLY_DISPENSED:
            return EpsErrorBase.NOT_CANCELLED_DISPENSED
        if line_item_status == LineItemStatus.NOT_DISPENSED:
            return EpsErrorBase.NOT_CANCELLED_NOT_DISPENSED
        if line_item_status == LineItemStatus.CANCELLED:
            return EpsErrorBase.NOT_CANCELLED_CANCELLED
        if line_item_status == LineItemStatus.EXPIRED:
            return EpsErrorBase.NOT_CANCELLED_EXPIRED
        else:
            return EpsErrorBase.NOT_CANCELLED_WITH_DISPENSER_ACTIVE

    def apply_cancellation(self, cancellation_obj, range_to_cancel_start_issue=None):
        """
        Loop through the valid cancellations on the context and change the prescription
        status as appropriate
        """
        instances = self.prescription_record[fields.FIELD_INSTANCES]

        # only apply from the start issue upwards
        if not range_to_cancel_start_issue:
            range_to_cancel_start_issue = self.current_issue_number
        range_to_update = self.get_issues_in_range(int(range_to_cancel_start_issue), None)

        issue_numbers = [issue.number for issue in range_to_update]
        for issue_number in issue_numbers:
            instance = instances[str(issue_number)]
            if cancellation_obj[fields.FIELD_CANCELLATION_TARGET] == "LineItem":
                self.process_line_cancellation(instance, cancellation_obj)
            else:
                self.process_instance_cancellation(instance, cancellation_obj)
        # the current issue may have become cancelled, so find the new current one?
        self.reset_current_instance()
        return [cancellation_obj[fields.FIELD_CANCELLATION_ID], issue_numbers]

    def remove_pending_cancellations(self):
        """
        Once the pending cancellations have been completed, remove any pending
        cancellations from the record
        """
        self.prescription_record[fields.FIELD_PENDING_CANCELLATIONS] = False

    def process_instance_cancellation(self, instance, cancellation_obj):
        """
        Change the prescription status, and set the completion date
        """
        instance[fields.FIELD_PREVIOUS_STATUS] = instance[fields.FIELD_PRESCRIPTION_STATUS]
        instance[fields.FIELD_PRESCRIPTION_STATUS] = PrescriptionStatus.CANCELLED
        instance[fields.FIELD_CANCELLATIONS].append(cancellation_obj)
        completion_date = datetime.datetime.strptime(
            cancellation_obj[fields.FIELD_CANCELLATION_TIME], TimeFormats.STANDARD_DATE_TIME_FORMAT
        )
        instance[fields.FIELD_COMPLETION_DATE] = completion_date.strftime(
            TimeFormats.STANDARD_DATE_FORMAT
        )

    def process_line_cancellation(self, instance, cancellation_obj):
        """
        Loop through the line items to find one relevant to the cancellation,
        If all line items now inactive then cancel the instance
        """
        active_line_item = False
        for line_item in instance[fields.FIELD_LINE_ITEMS]:
            if cancellation_obj[fields.FIELD_CANCEL_LINE_ITEM_REF] != line_item[fields.FIELD_ID]:
                if line_item[fields.FIELD_STATUS] in LineItemStatus.ACTIVE_STATES:
                    active_line_item = True
                continue
            line_item[fields.FIELD_PREVIOUS_STATUS] = line_item[fields.FIELD_STATUS]
            line_item[fields.FIELD_STATUS] = LineItemStatus.CANCELLED
        instance[fields.FIELD_CANCELLATIONS].append(cancellation_obj)

        if not active_line_item:
            self.process_instance_cancellation(instance, cancellation_obj)

    def return_pending_cancellations(self):
        """
        Return the list of pendingCancellations (should be False if none exist)
        """
        return self._pending_cancellations

    def return_cancellation_object(self, context, hl7, reasons):
        """
        Create an object (dict) which describes a cancellation
        """
        cancellation_obj = self.set_all_snippet_details(
            fields.INSTANCE_CANCELLATION_DETAILS, context
        )
        cancellation_obj[fields.FIELD_REASONS] = reasons
        cancellation_obj[fields.FIELD_HL7] = hl7
        return cancellation_obj

    def check_pending_cancellation_unique_w_disp(self, cancellation_obj):
        """
        Check whether the pending cancellation is unique. If not unique, return false and
        a boolean to indicate whether the requesting organisation matches.
        If there are no pendingCancellations already on the prescription then return
        immediately, indicating that the cancellation is unique.

        For both the pending cancellation (if exists) and the cancellationObject, if the
        target is a LineItem, set the target variable to be a string of
        LineItem_<<LineItemRef>> for logging purposes.

        This method is used for pending cancellations when the prescription is with
        dispenser, therefore whilst it is similar to the method used when
        the prescription has not yet been received by Spine
        (check_pending_cancellation_unique), in this case a whole prescription cancellation
        is treated independently to individual line item cancellations, as the action of
        the dispenser could mean that either one, both or neither cancellations are
        possible.
        """
        if not self._pending_cancellations:
            return [True, None]

        cancellation_target = str(cancellation_obj[fields.FIELD_CANCELLATION_TARGET])
        cancellation_org = str(cancellation_obj[fields.FIELD_AGENT_ORGANIZATION])
        if cancellation_target == "LineItem":
            cancellation_target = "LineItem_" + str(
                cancellation_obj[fields.FIELD_CANCEL_LINE_ITEM_REF]
            )

        org_match = True
        for pending_cancellation in self._pending_cancellations:
            pending_target = str(pending_cancellation[fields.FIELD_CANCELLATION_TARGET])
            if pending_target == "LineItem":
                pending_target = "LineItem_" + str(
                    pending_cancellation[fields.FIELD_CANCEL_LINE_ITEM_REF]
                )
            pending_org = str(pending_cancellation[fields.FIELD_AGENT_ORGANIZATION])
            if pending_target == cancellation_target:
                if pending_org != cancellation_org:
                    org_match = False
                self.log_object.write_log(
                    "EPS0264a",
                    None,
                    {
                        "internalID": self.internal_id,
                        "pendingOrg": pending_org,
                        "cancellationTarget": cancellation_target,
                        "cancellationOrg": cancellation_org,
                    },
                )
                return [False, org_match]

        return [True, None]

    def check_pending_cancellation_unique(self, cancellation_obj):
        """
        Check whether the pending cancellation is unique. If not unique, return false and
        a boolean to indicate whether the requesting organisation matches.
        If there are no pendingCancellations already on the prescription then return
        immediately, indicating that the cancellation is unique.

        For both the pending cancellation (if exists) and the cancellationObject, if the
        target is a LineItem, set the target variable to be a string of
        LineItem_<<LineItemRef>> for logging purposes.

        This method is used for pending cancellations when the prescription has not yet
        been received by Spine, therefore whilst it is similar to the method used when
        the prescription is With Dispenser (check_pending_cancellation_unique_w_disp) except
        that in this case a whole prescription cancellation takes precedence over
        individual line item cancellations.
        """
        if not self._pending_cancellations:
            return [True, None]

        cancellation_target = str(cancellation_obj[fields.FIELD_CANCELLATION_TARGET])
        cancellation_org = str(cancellation_obj[fields.FIELD_AGENT_ORGANIZATION])
        if cancellation_target == "LineItem":
            cancellation_target = "LineItem_" + str(
                cancellation_obj[fields.FIELD_CANCEL_LINE_ITEM_REF]
            )

        whole_prescription_cancellation = False
        org_match = True
        for pending_cancellation in self._pending_cancellations:
            pending_target = str(pending_cancellation[fields.FIELD_CANCELLATION_TARGET])
            pending_org = str(pending_cancellation[fields.FIELD_AGENT_ORGANIZATION])
            if pending_target == fields.FIELD_PRESCRIPTION:
                whole_prescription_cancellation = True
            if pending_target == "LineItem":
                pending_target = "LineItem_" + str(
                    pending_cancellation[fields.FIELD_CANCEL_LINE_ITEM_REF]
                )
            if (pending_target == cancellation_target) or whole_prescription_cancellation:
                if pending_org != cancellation_org:
                    org_match = False
                self.log_object.write_log(
                    "EPS0264a",
                    None,
                    {
                        "internalID": self.internal_id,
                        "pendingOrg": pending_org,
                        "cancellationTarget": cancellation_target,
                        "cancellationOrg": cancellation_org,
                    },
                )
                return [False, org_match]

        return [True, None]

    def set_unsuccessful_cancellation(self, cancellation_obj, failure_reason):
        """
        Set on the record details of the cancellation that has been unsuccessful,
        including the a reason. Note that this is used for unsuccessful pending
        cancellations and where a cancellation is a duplicate, and does not apply to
        cancellations that are simply not valid.
        """
        failed_cs = self.prescription_record[fields.FIELD_PRESCRIPTION][
            fields.FIELD_UNSUCCESSFUL_CANCELLATIONS
        ]
        cancellation_obj["failureReason"] = failure_reason

        if not failed_cs:
            failed_cs = []
        failed_cs.append(cancellation_obj)

        self.prescription_record[fields.FIELD_PRESCRIPTION][
            fields.FIELD_UNSUCCESSFUL_CANCELLATIONS
        ] = failed_cs

    def set_pending_cancellation(self, cancellation_obj, prescription_present):
        """
        Set the default Prescription Pending Cancellation status code and then
        Append a cancellation object to the pendingCancellations
        """
        if not prescription_present:
            instance = self._get_prescription_instance_data("1")
            self.update_instance_status(instance, PrescriptionStatus.PENDING_CANCELLATION)

        pending_cs = self._pending_cancellations

        if not pending_cs:
            pending_cs = [cancellation_obj]
            cancellation_date_obj = datetime.datetime.strptime(
                cancellation_obj[fields.FIELD_CANCELLATION_TIME],
                TimeFormats.STANDARD_DATE_TIME_FORMAT,
            )
            cancellation_date = cancellation_date_obj.strftime(TimeFormats.STANDARD_DATE_FORMAT)
            if not self.prescription_record[fields.FIELD_PRESCRIPTION][
                fields.FIELD_PRESCRIPTION_TIME
            ]:
                self.prescription_record[fields.FIELD_PRESCRIPTION][
                    fields.FIELD_PRESCRIPTION_TIME
                ] = cancellation_date
                self.log_object.write_log(
                    "EPS0340",
                    None,
                    {
                        "internalID": self.internal_id,
                        "cancellationDate": cancellation_date,
                        "prescriptionID": self.return_prescription_id(),
                    },
                )
        else:
            pending_cs.append(cancellation_obj)

        self._pending_cancellations = pending_cs

    def set_initial_prescription_status(self, handle_time):
        """
        Create the initial prescription status. For repeat dispense prescriptions, this
        needs to consider both the prescription date and the dispense window low dates,
        therefore this common method will be overridden.

        A prescription should not be available for download before its start date.

        :type handle_time: datetime.datetime
        """
        first_issue = self.get_issue(1)

        future_threshold = handle_time + datetime.timedelta(days=1)
        if self.time > future_threshold:
            first_issue.status = PrescriptionStatus.FUTURE_DATED_PRESCRIPTION
        else:
            first_issue.status = PrescriptionStatus.TO_BE_DISPENSED

    @property
    def max_repeats(self):
        """
        The maximum number of issues of this prescription.

        :rtype: int
        """
        return 1

    def return_instance_details_for_amend(self, instance_number):
        """
        For dispense messages the following details are required:
        Instance status
        NHS Number
        Dispensing Organisation
        None (indicating not a repeat prescription so no max_repeats)
        """
        instance = self._get_prescription_instance_data(instance_number)
        instance_status = instance[fields.FIELD_PRESCRIPTION_STATUS]
        dispensing_org = instance[fields.FIELD_DISPENSE][fields.FIELD_DISPENSING_ORGANIZATION]

        return [
            str(self.current_issue_number),
            instance_status,
            self._nhs_number,
            dispensing_org,
            None,
        ]

    def return_dispense_history_events(self, target_instance):
        """
        Return the dispense history events for a specific instance
        """
        instance = self._get_prescription_instance_data(target_instance)
        return instance[fields.FIELD_DISPENSE_HISTORY]

    def get_withdrawn_status(self, passed_status):
        """
        Dispense Return can only go back as far as 'with dispenser-active' for repeat dispense
        prescriptions, so convert the status for with dispenser, otherwise, return what was provided.
        """
        return passed_status

    def return_prescription_type(self):
        """
        Return the prescription type from the prescription record
        """
        return self.prescription_record[fields.FIELD_PRESCRIPTION].get(
            fields.FIELD_PRESCRIPTION_TYPE, ""
        )

    def return_prescription_treatment_type(self):
        """
        Return the prescription treatment type from the prescription record
        """
        return self.prescription_record[fields.FIELD_PRESCRIPTION].get(
            fields.FIELD_PRESCRIPTION_TREATMENT_TYPE, ""
        )

    def return_parent_prescription_document_key(self):
        """
        Return the parent prescription document key from the prescription record
        """
        return self.prescription_record.get(fields.FIELD_PRESCRIPTION, {}).get(
            fields.FIELD_PRESCRIPTION_MSG_REF
        )

    def return_signed_time(self):
        """
        Return the signed date/time from the prescription record
        """
        return self.prescription_record[fields.FIELD_PRESCRIPTION].get(fields.FIELD_SIGNED_TIME, "")

    def return_change_log(self):
        """
        Return the change log from the prescription record
        """
        return self.prescription_record.get(fields.FIELD_CHANGE_LOG, [])

    def return_nomination_data(self):
        """
        Return the nomination data from the prescription record
        """
        return self.prescription_record.get(fields.FIELD_NOMINATION)

    def return_prescription_field(self):
        """
        Return the complete prescription field
        """
        return self.prescription_record[fields.FIELD_PRESCRIPTION]
