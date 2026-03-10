import datetime
from copy import copy

from eps_spine_shared.common.prescription import fields
from eps_spine_shared.common.prescription.record import PrescriptionRecord
from eps_spine_shared.common.prescription.statuses import LineItemStatus, PrescriptionStatus


class RepeatDispenseRecord(PrescriptionRecord):
    """
    Class defined to handle repeat dispense prescriptions
    """

    def __init__(self, log_object, internal_id):
        """
        Allow the record_type attribute to be set
        """
        super(RepeatDispenseRecord, self).__init__(log_object, internal_id)
        self.record_type = "RepeatDispense"

    def create_instances(self, context, line_items):
        """
        Create all prescription instances

        Expire any lineItems that have a lower max_repeats number than the instance number
        """
        instance_snippets = {}

        range_max = int(context.maxRepeats) + 1
        future_instance_status = PrescriptionStatus.REPEAT_DISPENSE_FUTURE_INSTANCE

        for instance_number in range(1, range_max):
            instance_snippet = self.set_all_snippet_details(fields.INSTANCE_DETAILS, context)
            instance_snippet[fields.FIELD_LINE_ITEMS] = []
            for line_item in line_items:
                line_item_copy = copy(line_item)
                if int(line_item_copy[fields.FIELD_MAX_REPEATS]) < instance_number:
                    line_item_copy[fields.FIELD_STATUS] = LineItemStatus.EXPIRED
                instance_snippet[fields.FIELD_LINE_ITEMS].append(line_item_copy)

            instance_snippet[fields.FIELD_INSTANCE_NUMBER] = str(instance_number)
            if instance_number != 1:
                instance_snippet[fields.FIELD_PRESCRIPTION_STATUS] = future_instance_status
            instance_snippet[fields.FIELD_DISPENSE] = self.set_all_snippet_details(
                fields.DISPENSE_DETAILS, context
            )
            instance_snippet[fields.FIELD_CLAIM] = self.set_all_snippet_details(
                fields.CLAIM_DETAILS, context
            )
            instance_snippet[fields.FIELD_CANCELLATIONS] = []
            instance_snippet[fields.FIELD_DISPENSE_HISTORY] = {}
            instance_snippets[str(instance_number)] = instance_snippet
            instance_snippet[fields.FIELD_NEXT_ACTIVITY] = {}
            instance_snippet[fields.FIELD_NEXT_ACTIVITY][fields.FIELD_ACTIVITY] = None
            instance_snippet[fields.FIELD_NEXT_ACTIVITY][fields.FIELD_DATE] = None

        return instance_snippets

    def set_initial_prescription_status(self, handle_time):
        """
        Create the initial prescription status. For repeat dispense prescriptions, this
        needs to consider both the prescription date and the dispense window low dates.

        If either the prescriptionTime or dispenseWindowLow date is in the future then
        the prescription needs to have a Future Dated Prescription status set and can
        not yet be downloaded.
        If the prescription is not Future Dated, the default To Be Dispensed should be used.

        Note that this only applies to the first instance, the remaining instances will
        already have a Future Repeat Dispense Instance status set.

        :type handle_time: datetime.datetime
        """
        first_issue = self.get_issue(1)

        future_threshold = handle_time + datetime.timedelta(days=1)
        is_future_dated = self.time > future_threshold

        dispense_low_date = first_issue.dispense_window_low_date
        if dispense_low_date is not None and dispense_low_date > future_threshold:
            is_future_dated = True

        if is_future_dated:
            first_issue.status = PrescriptionStatus.FUTURE_DATED_PRESCRIPTION
        else:
            first_issue.status = PrescriptionStatus.TO_BE_DISPENSED

    def get_withdrawn_status(self, passed_status):
        """
        Dispense Return can only go back as far as 'with dispenser-active' for repeat dispense
        prescriptions, so convert the status for with dispenser, otherwise, return what was provided.
        """
        if passed_status == PrescriptionStatus.WITH_DISPENSER:
            return PrescriptionStatus.WITH_DISPENSER_ACTIVE
        return passed_status

    @property
    def max_repeats(self):
        """
        The maximum number of issues of this prescription.

        :rtype: int
        """
        max_repeats = self.prescription_record[fields.FIELD_PRESCRIPTION][fields.FIELD_MAX_REPEATS]
        return int(max_repeats)

    @property
    def future_issues_available(self):
        """
        Return boolean to indicate if future issues are available or not. Always False for
        Acute and Repeat Prescribe

        :rtype: bool
        """
        return self.current_issue_number < self.max_repeats
