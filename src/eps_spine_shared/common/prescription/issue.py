import datetime

from eps_spine_shared.common.prescription import fields
from eps_spine_shared.common.prescription.claim import PrescriptionClaim
from eps_spine_shared.common.prescription.line_item import PrescriptionLineItem
from eps_spine_shared.common.prescription.statuses import PrescriptionStatus
from eps_spine_shared.nhsfundamentals.time_utilities import TimeFormats


class PrescriptionIssue(object):
    """
    Wrapper class to simplify interacting with an issue (instance) portion of a prescription record.

    Note: the correct domain terminology is "issue", however there are legacy references
    to "instance" in the code and database records.
    """

    def __init__(self, issue_dict):
        """
        Constructor.

        :type issue_dict: dict
        """
        self._issue_dict = issue_dict

    @property
    def number(self):
        """
        The number of this issue.

        :rtype: int
        """
        # Note: the number is stored as a string, so we need to convert
        number = int(self._issue_dict[fields.FIELD_INSTANCE_NUMBER])
        return number

    @property
    def status(self):
        """
        The status code of the issue

        :rtype: str
        """
        return self._issue_dict[fields.FIELD_PRESCRIPTION_STATUS]

    @status.setter
    def status(self, new_status):
        """
        The status code of the issue

        NOTE: this does not update the previous status - use update_status() to do that
        PAB - should we be using update_status() in places we are using this?
        :type new_status: str
        """
        self._issue_dict[fields.FIELD_PRESCRIPTION_STATUS] = new_status

    @property
    def completion_date_str(self):
        """
        The issue completion date as a YYYYMMDD string, if available.

        :rtype: str or None
        """
        completion_date_str = self._issue_dict[fields.FIELD_COMPLETION_DATE]
        if not completion_date_str:
            return None
        return completion_date_str

    def expire(self, expired_at_time, parent_prescription):
        """
        Update the issue and all its line items to be expired.

        :type expired_at_time: datetime.datetime
        :type parent_prescription: PrescriptionRecord
        """
        currentStatus = self.status

        # update the issue status, if appropriate
        if currentStatus not in PrescriptionStatus.EXPIRY_IMMUTABLE_STATES:
            newStatus = PrescriptionStatus.EXPIRY_LOOKUP[currentStatus]
            self.update_status(newStatus, parent_prescription)

            if currentStatus in PrescriptionStatus.UNACTIONED_STATES:
                parent_prescription.log_object.write_log(
                    "EPS0616",
                    None,
                    {
                        "internalID": parent_prescription.internal_id,
                        "previousStatus": currentStatus,
                        "releaseVersion": parent_prescription.get_release_version(),
                        "prescriptionID": str(parent_prescription.return_prescription_id()),
                    },
                )

        # make sure all the line items are expired as well
        for lineItem in self.line_items:
            lineItem.expire(parent_prescription)

        parent_prescription.log_object.write_log(
            "EPS0403",
            None,
            {
                "internalID": parent_prescription.internal_id,
            },
        )

        # PAB: this will update the completion time of issues that are
        # already in EXPIRY_IMMUTABLE_STATES (ie. already completed) - is
        # this correct, or should this be guarded in the above if statement?
        self.mark_completed(expired_at_time, parent_prescription)

    def mark_completed(self, completion_datetime, parent_prescription):
        """
        Update the completion date of this issue.

        :type completion_datetime: datetime.datetime
        :type parent_prescription: PrescriptionRecord
        """
        current_completion_date_str = self.completion_date_str

        new_completion_date_str = completion_datetime.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        self._issue_dict[fields.FIELD_COMPLETION_DATE] = new_completion_date_str

        parent_prescription.log_attribute_change(
            fields.FIELD_COMPLETION_DATE,
            (current_completion_date_str or ""),
            new_completion_date_str,
            None,
        )

    @property
    def expiry_date_str(self):
        """
        The issue expiry date as a YYYYMMDD string.

        :rtype: str
        """
        return self._issue_dict[fields.FIELD_EXPIRY_DATE]

    @property
    def line_items(self):
        """
        The line items for this issue.

        :rtype: list(PrescriptionLineItem)
        """
        line_item_dicts = self._issue_dict[fields.FIELD_LINE_ITEMS]
        # wrap the dicts to add convenience methods
        line_items = [PrescriptionLineItem(d) for d in line_item_dicts]
        return line_items

    @property
    def claim(self):
        """
        The claim information for this issue.

        :rtype: PrescriptionClaim
        """
        claim_dict = self._issue_dict[fields.FIELD_CLAIM]
        return PrescriptionClaim(claim_dict)

    def update_status(self, new_status, parent_prescription):
        """
        Update the issue status, and record the previous status.

        :type new_status: str
        """
        currentStatus = self.status
        self._issue_dict[fields.FIELD_PREVIOUS_STATUS] = currentStatus
        self._issue_dict[fields.FIELD_PRESCRIPTION_STATUS] = new_status
        parent_prescription.log_attribute_change(
            fields.FIELD_PRESCRIPTION_STATUS, currentStatus, new_status, None
        )

    @property
    def dispensing_organization(self):
        """
        Dispensing organization for this issue.

        :rtype: str
        """
        dispense_dict = self._issue_dict[fields.FIELD_DISPENSE]
        return dispense_dict[fields.FIELD_DISPENSING_ORGANIZATION]

    @property
    def last_dispense_date(self):
        """
        Dispensing date for this issue.

        :rtype: str
        """
        dispense_dict = self._issue_dict[fields.FIELD_DISPENSE]
        return dispense_dict[fields.FIELD_LAST_DISPENSE_DATE]

    @property
    def last_dispense_notification_msg_ref(self):
        """
        Last Dispense Notification MsgRef for this issue.

        :rtype: str
        """
        dispense_dict = self._issue_dict[fields.FIELD_DISPENSE]
        return dispense_dict[fields.FIELD_LAST_DISPENSE_NOTIFICATION_MSG_REF]

    def clear_dispensing_organisation(self):
        """
        Clear the dispensing organisation from this instance.
        """
        dispense_dict = self._issue_dict[fields.FIELD_DISPENSE]
        dispense_dict[fields.FIELD_DISPENSING_ORGANIZATION] = None

    @property
    def dispense_window_low_date(self):
        """
        Dispense window low date

        :rtype: datetime or None
        """
        low_date_str = self._issue_dict.get(fields.FIELD_DISPENSE_WINDOW_LOW_DATE)
        if not low_date_str:
            return None
        return datetime.datetime.strptime(low_date_str, TimeFormats.STANDARD_DATE_FORMAT)

    def has_active_line_item(self):
        """
        See if this instance has any active line items.

        :rtype: bool
        """
        return any(lineItem.is_active() for lineItem in self.line_items)

    def get_line_item_by_id(self, line_item_id):
        """
        Get a particular line item by its ID.

        Raises a KeyError if no item can be found.

        :type line_item_id: str
        :rtype: PrescriptionLineItem
        """
        for lineItem in self.line_items:
            if lineItem.id == line_item_id:
                return lineItem

        raise KeyError("Could not find line item '%s'" % line_item_id)

    @property
    def release_date(self):
        """
        The releaseDate for this issue, if one is specified

        :rtype: str
        """
        release_date = self._issue_dict.get(fields.FIELD_RELEASE_DATE)
        return str(release_date)

    @property
    def next_activity(self):
        """
        The next activity for this issue, if one is specified.

        Note: some migrated prescriptions may not have a next activity specified,
        although this should hopefully be rectified. If so, we may be able to tighten
        up the return type.

        :rtype: str or None
        """
        next_activity_dict = self._issue_dict[fields.FIELD_NEXT_ACTIVITY]
        return next_activity_dict.get(fields.FIELD_ACTIVITY, None)

    @property
    def next_activity_date_str(self):
        """
        The next activity date for this issue, if one is specified.

        :rtype: str or None
        """
        next_activity_dict = self._issue_dict[fields.FIELD_NEXT_ACTIVITY]
        return next_activity_dict.get(fields.FIELD_DATE, None)

    @property
    def cancellations(self):
        """
        The cancellations for this issue.

        :rtype: list()
        """
        return self._issue_dict[fields.FIELD_CANCELLATIONS]

    def get_line_item_cancellations(self, line_item_id):
        """
        Get the cancellations for a particular line item.

        :type line_item_id: str
        :rtype: list()
        """
        return [
            c for c in self.cancellations if c[fields.FIELD_CANCEL_LINE_ITEM_REF] == line_item_id
        ]

    def get_line_item_first_cancellation_time(self, line_item_id):
        """
        Get the time of the first cancellation targetting a particular line item.

        :type line_item_id: str
        :rtype: str or None
        """
        cancellations = self.get_line_item_cancellations(line_item_id)
        cancellation_times = [c[fields.FIELD_CANCELLATION_TIME] for c in cancellations]

        if cancellations:
            return min(cancellation_times, key=lambda x: int(x))
        return None

    @property
    def release_request_msg_ref(self):
        """
        The release request message reference for this issue.

        :rtype: str
        """
        return self._issue_dict[fields.FIELD_RELEASE_REQUEST_MGS_REF]
