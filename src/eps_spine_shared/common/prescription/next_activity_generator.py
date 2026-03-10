import datetime

from eps_spine_shared.common.prescription import fields
from eps_spine_shared.common.prescription.statuses import PrescriptionStatus
from eps_spine_shared.logger import EpsLogger
from eps_spine_shared.nhsfundamentals.time_utilities import TimeFormats


class NextActivityGenerator(object):
    """
    Used to create the next activity for a prescription instance
    """

    INPUT_LIST_1 = [
        fields.FIELD_EXPIRY_PERIOD,
        fields.FIELD_PRESCRIPTION_DATE,
        fields.FIELD_NOMINATED_DOWNLOAD_DATE,
        fields.FIELD_DISPENSE_WINDOW_HIGH_DATE,
    ]
    INPUT_LIST_2 = [
        fields.FIELD_EXPIRY_PERIOD,
        fields.FIELD_PRESCRIPTION_DATE,
        fields.FIELD_DISPENSE_WINDOW_HIGH_DATE,
        fields.FIELD_LAST_DISPENSE_DATE,
    ]
    INPUT_LIST_3 = [
        fields.FIELD_EXPIRY_PERIOD,
        fields.FIELD_PRESCRIPTION_DATE,
        fields.FIELD_COMPLETION_DATE,
    ]
    INPUT_LIST_4 = [
        fields.FIELD_EXPIRY_PERIOD,
        fields.FIELD_PRESCRIPTION_DATE,
        fields.FIELD_COMPLETION_DATE,
        fields.FIELD_DISPENSE_WINDOW_HIGH_DATE,
        fields.FIELD_LAST_DISPENSE_DATE,
        fields.FIELD_CLAIM_SENT_DATE,
    ]
    INPUT_LIST_5 = [
        fields.FIELD_PRESCRIBING_SITE_TEST_STATUS,
        fields.FIELD_PRESCRIPTION_DATE,
        fields.FIELD_CLAIM_SENT_DATE,
    ]
    INPUT_LIST_6 = [
        fields.FIELD_EXPIRY_PERIOD,
        fields.FIELD_PRESCRIPTION_DATE,
        fields.FIELD_NOMINATED_DOWNLOAD_DATE,
        fields.FIELD_DISPENSE_WINDOW_LOW_DATE,
    ]
    INPUT_LIST_7 = [
        fields.FIELD_EXPIRY_PERIOD,
        fields.FIELD_PRESCRIPTION_DATE,
    ]

    INPUT_BY_STATUS = {}
    INPUT_BY_STATUS[PrescriptionStatus.TO_BE_DISPENSED] = INPUT_LIST_1
    INPUT_BY_STATUS[PrescriptionStatus.WITH_DISPENSER] = INPUT_LIST_1
    INPUT_BY_STATUS[PrescriptionStatus.WITH_DISPENSER_ACTIVE] = INPUT_LIST_2
    INPUT_BY_STATUS[PrescriptionStatus.EXPIRED] = INPUT_LIST_3
    INPUT_BY_STATUS[PrescriptionStatus.CANCELLED] = INPUT_LIST_3
    INPUT_BY_STATUS[PrescriptionStatus.DISPENSED] = INPUT_LIST_4
    INPUT_BY_STATUS[PrescriptionStatus.NOT_DISPENSED] = INPUT_LIST_3
    INPUT_BY_STATUS[PrescriptionStatus.CLAIMED] = INPUT_LIST_5
    INPUT_BY_STATUS[PrescriptionStatus.NO_CLAIMED] = INPUT_LIST_5
    INPUT_BY_STATUS[PrescriptionStatus.AWAITING_RELEASE_READY] = INPUT_LIST_6
    INPUT_BY_STATUS[PrescriptionStatus.REPEAT_DISPENSE_FUTURE_INSTANCE] = INPUT_LIST_7
    INPUT_BY_STATUS[PrescriptionStatus.FUTURE_DATED_PRESCRIPTION] = INPUT_LIST_6
    INPUT_BY_STATUS[PrescriptionStatus.PENDING_CANCELLATION] = [fields.FIELD_PRESCRIPTION_DATE]

    FIELD_REPEAT_DISPENSE_EXPIRY_PERIOD = "repeatDispenseExpiryPeriod"
    FIELD_PRESCRIPTION_EXPIRY_PERIOD = "prescriptionExpiryPeriod"
    FIELD_WITH_DISPENSER_ACTIVE_EXPIRY_PERIOD = "withDispenserActiveExpiryPeriod"
    FIELD_EXPIRED_DELETE_PERIOD = "expiredDeletePeriod"
    FIELD_CANCELLED_DELETE_PERIOD = "cancelledDeletePeriod"
    FIELD_NOTIFICATION_DELAY_PERIOD = "notificationDelayPeriod"
    FIELD_CLAIMED_DELETE_PERIOD = "claimedDeletePeriod"
    FIELD_NOT_DISPENSED_DELETE_PERIOD = "notDispensedDeletePeriod"
    FIELD_RELEASE_VERSION = "releaseVersion"

    def __init__(self, log_object, internal_id):
        self.log_object = EpsLogger(log_object)
        self.internal_id = internal_id

        # Map between prescription status and method for calculating index values
        self._index_map = {}
        self._index_map[PrescriptionStatus.TO_BE_DISPENSED] = self.un_dispensed
        self._index_map[PrescriptionStatus.WITH_DISPENSER] = self.un_dispensed
        self._index_map[PrescriptionStatus.WITH_DISPENSER_ACTIVE] = self.part_dispensed
        self._index_map[PrescriptionStatus.EXPIRED] = self.expired
        self._index_map[PrescriptionStatus.CANCELLED] = self.cancelled
        self._index_map[PrescriptionStatus.DISPENSED] = self.dispensed
        self._index_map[PrescriptionStatus.NO_CLAIMED] = self.completed
        self._index_map[PrescriptionStatus.NOT_DISPENSED] = self.not_dispensed
        self._index_map[PrescriptionStatus.CLAIMED] = self.completed
        self._index_map[PrescriptionStatus.AWAITING_RELEASE_READY] = self.awaiting_nominated_release
        self._index_map[PrescriptionStatus.REPEAT_DISPENSE_FUTURE_INSTANCE] = self.un_dispensed
        self._index_map[PrescriptionStatus.FUTURE_DATED_PRESCRIPTION] = self.future_dated
        self._index_map[PrescriptionStatus.PENDING_CANCELLATION] = self.awaiting_cancellation

    def next_activity_date(self, nad_status, nad_reference):
        """
        Function takes prescriptionStatus (this will be the prescriptionStatus to be
        if the function is called during an update process)
        Function takes nad_status - a dictionary of information relevant to
        next-activity-date calculation
        Function takes nad_reference - a dictionary of global variables relevant to
        next-activity-date calculation
        Function should return [nextActivity, nextActivityDate, expiryDate]
        """
        prescription_status = nad_status[fields.FIELD_PRESCRIPTION_STATUS]

        for key in NextActivityGenerator.INPUT_BY_STATUS[prescription_status]:
            if fields.FIELD_CAPITAL_D_DATE in key:
                if nad_status[key]:
                    nad_status[key] = datetime.datetime.strptime(
                        nad_status[key], TimeFormats.STANDARD_DATE_FORMAT
                    )
                elif key not in [
                    fields.FIELD_NOMINATED_DOWNLOAD_DATE,
                    fields.FIELD_DISPENSE_WINDOW_LOW_DATE,
                ]:
                    nad_status[key] = datetime.datetime.now()

        self._calculate_expiry_date(nad_status, nad_reference)
        return_value = self._index_map[prescription_status](nad_status, nad_reference)
        return return_value

    def _calculate_expiry_date(self, nad_status, nad_reference):
        """
        Calculate the expiry date to be used in subsequent Next Activity calculations
        """
        if int(nad_status[fields.FIELD_INSTANCE_NUMBER]) > 1:
            expiry_date = (
                nad_status[fields.FIELD_PRESCRIPTION_DATE]
                + nad_reference[self.FIELD_REPEAT_DISPENSE_EXPIRY_PERIOD]
            )
        else:
            expiry_date = (
                nad_status[fields.FIELD_PRESCRIPTION_DATE]
                + nad_reference[self.FIELD_PRESCRIPTION_EXPIRY_PERIOD]
            )

        nad_status[fields.FIELD_EXPIRY_DATE] = expiry_date
        expiry_date_str = expiry_date.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        nad_status[fields.FIELD_FORMATTED_EXPIRY_DATE] = expiry_date_str

    def un_dispensed(self, nad_status, _):
        """
        return [nextActivity, nextActivityDate, expiryDate] for un_dispensed prescription
        messages, covers:
        toBeDispensed
        withDispenser
        RepeatDispenseFutureInstance
        """
        next_activity = fields.NEXTACTIVITY_EXPIRE
        next_activity_date = nad_status[fields.FIELD_FORMATTED_EXPIRY_DATE]
        return [next_activity, next_activity_date, nad_status[fields.FIELD_EXPIRY_DATE]]

    def part_dispensed(self, nad_status, nad_reference):
        """
        return [nextActivity, nextActivityDate, expiryDate] for part_dispensed prescription
        messages
        """
        max_dispense_time = nad_status[fields.FIELD_LAST_DISPENSE_DATE]
        max_dispense_time += nad_reference[self.FIELD_WITH_DISPENSER_ACTIVE_EXPIRY_PERIOD]
        expiry_date = min(max_dispense_time, nad_status[fields.FIELD_EXPIRY_DATE])

        if nad_status[self.FIELD_RELEASE_VERSION] == fields.R1_VERSION:
            next_activity = fields.NEXTACTIVITY_EXPIRE
            next_activity_date = expiry_date.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        else:
            if not nad_status[fields.FIELD_LAST_DISPENSE_NOTIFICATION_MSG_REF]:
                next_activity = fields.NEXTACTIVITY_EXPIRE
                next_activity_date = expiry_date.strftime(TimeFormats.STANDARD_DATE_FORMAT)
            else:
                next_activity = fields.NEXTACTIVITY_CREATENOCLAIM
                next_activity_date = max_dispense_time.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        return [next_activity, next_activity_date, expiry_date]

    def expired(self, nad_status, nad_reference):
        """
        return [nextActivity, nextActivityDate, expiryDate] for expired prescription
        messages
        """
        deletion_date = (
            nad_status[fields.FIELD_COMPLETION_DATE]
            + nad_reference[self.FIELD_EXPIRED_DELETE_PERIOD]
        )
        next_activity = fields.NEXTACTIVITY_DELETE
        next_activity_date = deletion_date.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        return [next_activity, next_activity_date, None]

    def cancelled(self, nad_status, nad_reference):
        """
        return [nextActivity, nextActivityDate, expiryDate] for cancelled prescription
        messages
        """
        deletion_date = (
            nad_status[fields.FIELD_COMPLETION_DATE]
            + nad_reference[self.FIELD_CANCELLED_DELETE_PERIOD]
        )
        next_activity = fields.NEXTACTIVITY_DELETE
        next_activity_date = deletion_date.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        return [next_activity, next_activity_date, None]

    def dispensed(self, nad_status, nad_reference):
        """
        return [nextActivity, nextActivityDate, expiryDate] for dispensed prescription
        messages.
        Note that if a claim is not received before the notification delay period expires,
        a no claim notification is sent to the PPD.
        """
        completion_date = nad_status[fields.FIELD_COMPLETION_DATE]
        max_notification_date = (
            completion_date + nad_reference[self.FIELD_NOTIFICATION_DELAY_PERIOD]
        )
        if nad_status[self.FIELD_RELEASE_VERSION] == fields.R1_VERSION:  # noqa: SIM108
            next_activity = fields.NEXTACTIVITY_DELETE
        else:
            next_activity = fields.NEXTACTIVITY_CREATENOCLAIM
        next_activity_date = max_notification_date.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        return [next_activity, next_activity_date, None]

    def completed(self, nad_status, nad_reference):
        """
        return [nextActivity, nextActivityDate, expiryDate] for completed prescription
        messages

        Note, all reference to claim sent date removed as this now only applies to already
        claimed and no-claimed prescriptions.
        """
        deletion_date = (
            nad_status[fields.FIELD_CLAIM_SENT_DATE]
            + nad_reference[self.FIELD_CLAIMED_DELETE_PERIOD]
        )
        next_activity = fields.NEXTACTIVITY_DELETE
        next_activity_date = deletion_date.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        return [next_activity, next_activity_date, None]

    def not_dispensed(self, nad_status, nad_reference):
        """
        return [nextActivity, nextActivityDate, expiryDate] for not_dispensed prescription
        messages
        """
        deletion_date = (
            nad_status[fields.FIELD_COMPLETION_DATE]
            + nad_reference[self.FIELD_NOT_DISPENSED_DELETE_PERIOD]
        )
        next_activity = fields.NEXTACTIVITY_DELETE
        next_activity_date = deletion_date.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        return [next_activity, next_activity_date, None]

    def awaiting_nominated_release(self, nad_status, _):
        """
        return [nextActivity, nextActivityDate, expiryDate] for awaiting_nominated_release
        prescription messages
        """
        ready_date = nad_status[fields.FIELD_DISPENSE_WINDOW_LOW_DATE]

        if nad_status[fields.FIELD_NOMINATED_DOWNLOAD_DATE]:
            ready_date = nad_status[fields.FIELD_NOMINATED_DOWNLOAD_DATE]

        ready_date_string = ready_date.strftime(TimeFormats.STANDARD_DATE_FORMAT)

        if ready_date < nad_status[fields.FIELD_EXPIRY_DATE]:
            next_activity = fields.NEXTACTIVITY_READY
            next_activity_date = ready_date_string
        else:
            next_activity = fields.NEXTACTIVITY_EXPIRE
            next_activity_date = nad_status[fields.FIELD_FORMATTED_EXPIRY_DATE]
        return [next_activity, next_activity_date, nad_status[fields.FIELD_EXPIRY_DATE]]

    def future_dated(self, nad_status, _):
        """
        return [nextActivity, nextActivityDate, expiryDate] for awaiting_nominated_release
        prescription messages
        """
        if nad_status[fields.FIELD_DISPENSE_WINDOW_LOW_DATE]:
            ready_date = max(
                nad_status[fields.FIELD_DISPENSE_WINDOW_LOW_DATE],
                nad_status[fields.FIELD_PRESCRIPTION_DATE],
            )
        else:
            ready_date = nad_status[fields.FIELD_PRESCRIPTION_DATE]

        ready_date_string = ready_date.strftime(TimeFormats.STANDARD_DATE_FORMAT)

        if nad_status[fields.FIELD_NOMINATED_DOWNLOAD_DATE]:
            ready_date = nad_status[fields.FIELD_NOMINATED_DOWNLOAD_DATE]
        if ready_date < nad_status[fields.FIELD_EXPIRY_DATE]:
            next_activity = fields.NEXTACTIVITY_READY
            next_activity_date = ready_date_string
        else:
            next_activity = fields.NEXTACTIVITY_EXPIRE
            next_activity_date = nad_status[fields.FIELD_FORMATTED_EXPIRY_DATE]
        return [next_activity, next_activity_date, nad_status[fields.FIELD_EXPIRY_DATE]]

    def awaiting_cancellation(self, nad_status, nad_reference):
        """
        return [nextActivity, nextActivityDate, expiryDate] for awaiting_cancellation
        prescription messages
        """
        deletion_date = (
            nad_status[fields.FIELD_HANDLE_TIME] + nad_reference[self.FIELD_CANCELLED_DELETE_PERIOD]
        )
        next_activity = fields.NEXTACTIVITY_DELETE
        next_activity_date = deletion_date.strftime(TimeFormats.STANDARD_DATE_FORMAT)
        return [next_activity, next_activity_date, None]
