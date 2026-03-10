from eps_spine_shared.common.prescription import fields
from eps_spine_shared.common.prescription.record import PrescriptionRecord
from eps_spine_shared.spinecore.base_utilities import quoted


class SinglePrescribeRecord(PrescriptionRecord):
    """
    Class defined to handle single instance (acute) prescriptions
    """

    def __init__(self, log_object, internal_id):
        """
        Allow the record_type attribute to be set
        """
        super(SinglePrescribeRecord, self).__init__(log_object, internal_id)
        self.record_type = "Acute"

    def add_line_item_repeat_data(self, release_data, line_item_ref, line_item):
        """
        Add line item information (This is not required for Acute prescriptions, but
        will invalidate the signature if provided in the prescription and not returned
        in the release.
        It the lineItem.max_repeats is false (not provided inbound), then do not include
        it in the response, otherwise, both MaxRepeats and CurrentInstnace will be 1 for Acute.

        :type release_data: dict
        :type line_item_ref: str
        :type line_item: PrescriptionLineItem
        """
        # Handle the missing inbound max_repeats
        if not line_item.max_repeats:
            return

        # Acute, so both values may only be '1'
        release_data[line_item_ref + "MaxRepeats"] = quoted(str(1))
        release_data[line_item_ref + "CurrentInstance"] = quoted(str(1))

    def return_details_for_dispense(self):
        """
        For dispense messages the following details are required:
        - Issue number
        - Issue status
        - NHS Number
        - Dispensing Organisation
        - None (indicating not a repeat prescription so no max_repeats)
        """
        current_issue = self.current_issue
        details = [
            str(current_issue.number),
            current_issue.status,
            self._nhs_number,
            current_issue.dispensing_organization,
            None,
        ]
        return details

    def return_details_for_claim(self, instance_number_str):
        """
        For dispense messages the following details are required:
        - Issue status
        - NHS Number
        - Dispensing Organisation
        - None (indicating not a repeat prescription so no max_repeats)
        """
        issue_number = int(instance_number_str)
        issue = self.get_issue(issue_number)
        details = [
            issue.claim,
            issue.status,
            self._nhs_number,
            issue.dispensing_organization,
            None,
        ]
        return details

    def return_last_dispense_date(self, instance_number):
        """
        Return the last_dispense_date for the requested instance
        """
        instance = self._get_prescription_instance_data(instance_number)
        last_dispense_date = instance[fields.FIELD_DISPENSE][fields.FIELD_LAST_DISPENSE_DATE]
        return last_dispense_date

    def return_last_disp_msg_ref(self, instance_number_str):
        """
        returns the last dispense Msg Ref for the issue
        """
        issue_number = int(instance_number_str)
        issue = self.get_issue(issue_number)
        return issue.last_dispense_notification_msg_ref
