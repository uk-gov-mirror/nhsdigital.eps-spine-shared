import datetime
import re
import uuid

from eps_spine_shared.errors import EpsSystemError
from eps_spine_shared.nhsfundamentals.time_utilities import TimeFormats


class ChangeLogProcessor(object):
    """
    Keep the change log within the record

    The methods here assume that a None is never passed as change log, if necessary pass {} instead.
    """

    TIMESTAMP = "Timestamp"
    SCN = "SCN"
    SYS_SDS = "agentSystemSDS1"
    PRS_SDS = "agentPersonSDSPerson"
    UPDATES = "updatesApplied"
    XSLT = "Source XSLT"
    RSP_PARAMS = "Response Parameters"
    NOTIFICATIONS = "Notifications"
    INTERNAL_ID = "InternalID"
    INTERACTION_ID = "interactionID"
    TIME_PREPARED = "timePreparedForUpdate"
    INSTANCE = "instance"

    RECORD_SCN_REF = "SCN"
    RECORD_CHANGELOG_REF = "changeLog"

    INITIAL_SCN = 1

    DO_NOT_PRUNE = -1
    PRUNE_POINT = 12
    INVALID_SCN = -1

    @classmethod
    def log_for_general_update(cls, scn, internal_id=None, xslt=None, rsp_parameters=None):
        """
        Add a general change log update, nothing specific to a domain
        """
        if not rsp_parameters:
            rsp_parameters = {}

        logOfChange = {}
        _timeOfChange = datetime.datetime.now().strftime(TimeFormats.STANDARD_DATE_TIME_FORMAT)
        logOfChange[cls.TIMESTAMP] = _timeOfChange
        logOfChange[cls.SCN] = scn
        logOfChange[cls.INTERNAL_ID] = internal_id
        logOfChange[cls.XSLT] = xslt
        logOfChange[cls.RSP_PARAMS] = rsp_parameters
        return logOfChange

    @classmethod
    def update_change_log(cls, record, new_log, message_id, prune_point=None):
        """
        Take a change log from the record, add the new log to it, and prune to the prune
        point
        """
        if not prune_point:
            prune_point = cls.PRUNE_POINT

        change_log = record.get(cls.RECORD_CHANGELOG_REF, {})
        change_log[message_id] = new_log
        cls.prune_change_log(change_log, prune_point)

        record[cls.RECORD_CHANGELOG_REF] = change_log
        return record

    @classmethod
    def prune_change_log(cls, change_log, prune_point):
        """
        Prune to the prune point
        """
        if prune_point != cls.DO_NOT_PRUNE:
            _, highest_scn = cls.get_highest_scn(change_log)
            if highest_scn != cls.INVALID_SCN:
                scn_to_prune = highest_scn - prune_point
                prune_list = []
                for guid, change_log_entry in change_log.items():
                    entry_scn = int(change_log_entry.get(cls.SCN, cls.INVALID_SCN))
                    if entry_scn < scn_to_prune:
                        prune_list.append(guid)
                for guid in prune_list:
                    del change_log[guid]

    @classmethod
    def get_highest_scn(cls, change_log):
        """
        Return the (guid, scn) from the first change_log found with the highest SCN
        """
        (highest_guid, highest_scn) = (None, cls.INVALID_SCN)
        for guid in change_log:
            scn = int(change_log[guid].get(cls.SCN, cls.INVALID_SCN))
            if scn > highest_scn:
                highest_guid = guid
                highest_scn = scn
        return (highest_guid, highest_scn)

    @classmethod
    def get_scn(cls, change_log_entry):
        """
        Retrieve the SCN as an int from the provided change_log entry
        """
        scn_number = int(change_log_entry.get(cls.SCN, cls.INVALID_SCN))
        return scn_number

    @classmethod
    def list_scns(cls, change_log):
        """
        Performs list comprehension on the change_log dictionary to retrieve all the SCNs from change_log

        Duplicates will be present and change_log entries with no SCN will be represented with the
        INVALID_SCN constant
        """
        scn_number_list = [cls.get_scn(change_log[x]) for x in change_log]
        return scn_number_list

    @classmethod
    def get_max_scn(cls, change_log):
        """
        Return the highest SCN value from the provided change_log
        """
        scn_number_list = cls.list_scns(change_log)
        if not scn_number_list:
            return cls.INVALID_SCN
        highest_scn = max(scn_number_list)
        return highest_scn

    @classmethod
    def get_all_guids_for_scn(cls, change_log, search_scn):
        """
        For the provided SCN return the GUID Keys of all the change_log entries that have that SCN

        Usually this will be a single GUID, but in the case of tickled records there can be multiple.
        """
        search_scn = int(search_scn)
        guid_list = [k for k in change_log if cls.get_scn(change_log[k]) == search_scn]
        return guid_list

    @classmethod
    def get_max_scn_guids(cls, change_log):
        """
        Finds the highest SCN in the change_log and returns all the GUIDs that have that SCN
        """
        highest_scn = cls.get_max_scn(change_log)
        guid_list = cls.get_all_guids_for_scn(change_log, highest_scn)
        return guid_list

    @classmethod
    def get_all_guids(cls, change_log):
        """
        Return a list of all the GUID keys from the provided change_log
        """
        return list(change_log.keys())

    @classmethod
    def get_last_change_time(cls, change_log):
        """
        Returns the last change time
        """
        try:
            guid = cls.get_max_scn_guids(change_log)[0]
        except IndexError:
            return None
        return change_log[guid].get(cls.TIMESTAMP)

    @classmethod
    def set_initial_change_log(cls, record, internal_id, reason_guid=None):
        """
        If no change log is present set an initial change log on the record.  It may
        use a GUID as a key or a string explaining the reason for initiating the
        change log.
        """
        change_log = record.get(cls.RECORD_CHANGELOG_REF)
        if change_log:
            return

        scn = int(record.get(cls.RECORD_SCN_REF, cls.INITIAL_SCN))
        if not reason_guid:
            reason_guid = str(uuid.uuid4()).upper()
        change_log = {}
        change_log[reason_guid] = cls.log_for_general_update(scn, internal_id)

        record[cls.RECORD_CHANGELOG_REF] = change_log


class DemographicsChangeLogProcessor(ChangeLogProcessor):
    """
    Change Log Processor specifically for demographic records
    """

    # Demographic record uses 'serialChangeNumber' rather than the default 'SCN'
    RECORD_SCN_REF = "serialChangeNumber"

    @classmethod
    def log_for_domain_update(cls, update_context, internal_id):
        """
        Create a change log for this expected change - requires attributes to be set on
        context object
        """
        log_of_change = cls.log_for_general_update(
            update_context.pdsRecord.get(cls.RECORD_SCN_REF, cls.INITIAL_SCN),
            internal_id,
            update_context.responseDetails.get(cls.XSLT),
            update_context.responseDetails.get(cls.RSP_PARAMS),
        )

        log_of_change[cls.SYS_SDS] = update_context.agentSystem
        log_of_change[cls.PRS_SDS] = update_context.agentPerson
        log_of_change[cls.UPDATES] = update_context.updatesApplied
        log_of_change[cls.NOTIFICATIONS] = update_context.notificationsToQueue
        return log_of_change

    @staticmethod
    def get_highest_gp_links_transaction_number(change_log, sender, recipient):
        """
        Return the highest GP Links transaction number which has been included in the change log, or None (if there
        aren't any).
        """
        max_number = -1

        gp_links_key_pattern = re.compile(
            "^{}_{}_[0-9]+_[0-9]+_(?P<transactionNumber>[0-9]+)$".format(
                sender.upper(), recipient.upper()
            )
        )

        for key in change_log.keys():  # noqa: SIM118
            match = gp_links_key_pattern.match(key)
            # Ignore keys which aren't related to GP Links transactions
            if match is None:
                continue
            transaction_number = int(match.group("transactionNumber"))
            if transaction_number > max_number:
                max_number = transaction_number

        return max_number


class PrescriptionsChangeLogProcessor(ChangeLogProcessor):
    """
    Change Log Processor specifically for prescriptions records
    """

    FROM_STATUS = "fromStatus"
    TO_STATUS = "toStatus"
    INS_FROM_STATUS = "instanceFromStatus"
    INS_TO_STATUS = "instanceToStatus"
    PRE_CHANGE_STATUS_DICT = "preChangeStatusDict"
    POST_CHANGE_STATUS_DICT = "postChangeStatusDict"
    CHANGED_ISSUES_LIST = "issuesAlteredByChange"
    PRE_CHANGE_CURRENT_ISSUE = "preChangeCurrentIssue"
    POST_CHANGE_CURRENT_ISSUE = "postChangeCurrentIssue"
    TOUCHED = "touched"
    AGENT_ROLE_PROFILE_CODE_ID = "agentRoleProfileCodeId"
    AGENT_PERSON_ROLE = "agentPersonRole"
    AGENT_PERSON_ORG_CODE = "agentPersonOrgCode"

    MIN_INITIALHISTORY = 16
    MIN_RECENTHISTORY = 16
    REPEATING_ACTIONS = [
        "PORX_IN060102UK30",
        "PORX_IN060102SM30",
        "PORX_IN132004UK30",
        "PORX_IN132004SM30",
        "PORX_IN132004UK04",
        "PORX_IN100101UK31",
        "PORX_IN100101SM31",
        "PORX_IN100101UK04",
        "PORX_IN020101UK31",
        "PORX_IN020102UK31",
        "PORX_IN020101SM31",
        "PORX_IN020102SM31",
        "PORX_IN020101UK04",
        "PORX_IN020102UK04",
        "PORX_IN060102GB01",
        "PRESCRIPTION_DISPENSE_PROPOSAL_RETURN",
    ]

    REGEX_ALPHANUMERIC8 = re.compile(r"^[A-Za-z0-9\-]{1,8}$")

    @classmethod
    def log_for_domain_update(cls, update_context, internal_id):
        """
        Create a change log for this expected change - requires attribute to be set on
        context object
        """
        log_of_change = cls.log_for_general_update(
            update_context.epsRecord.get_scn(),
            internal_id,
            update_context.responseDetails.get(cls.XSLT),
            update_context.responseDetails.get(cls.RSP_PARAMS),
        )
        log_of_change = update_context.workDescriptionObject.createInitialEventLog(log_of_change)

        instance = (
            str(update_context.updateInstance)
            if update_context.updateInstance
            else str(update_context.instanceID)
        )

        log_of_change[cls.TIME_PREPARED] = update_context.handleTime.strftime(
            TimeFormats.STANDARD_DATE_TIME_FORMAT
        )

        # NOTE: FROM_STATUS and TO_STATUS seem to be legacy fields, that have been
        # superceded by the INS_FROM_STATUS and INS_TO_STATUS fields set below.
        # The only reference to TO_STATUS seems to be in PrescriptionJsonQueryResponse.cfg
        # template used by the prescription detail view web service
        log_of_change[cls.FROM_STATUS] = (
            update_context.epsRecord.return_previous_prescription_status(
                update_context.instanceID, False
            )
        )
        log_of_change[cls.TO_STATUS] = update_context.epsRecord.return_prescription_status(
            update_context.instanceID, False
        )

        # Event history lines for UI
        # **** NOTE THAT THESE ARE WRONG, THEY REFER TO THE FINAL ISSUE, WHICH MAY NOT BE THE ISSUE THAT WAS UPDATED
        log_of_change[cls.INSTANCE] = instance
        log_of_change[cls.INS_FROM_STATUS] = (
            update_context.epsRecord.return_previous_prescription_status(instance, False)
        )
        log_of_change[cls.INS_TO_STATUS] = update_context.epsRecord.return_prescription_status(
            instance, False
        )
        log_of_change[cls.AGENT_ROLE_PROFILE_CODE_ID] = update_context.agentRoleProfileCodeId
        log_of_change[cls.AGENT_PERSON_ROLE] = update_context.agentPersonRole
        org_code = update_context.agentOrganization
        has_dispenser_code = (
            hasattr(update_context, "dispenserCode") and update_context.dispenserCode
        )
        if (
            not org_code
            and has_dispenser_code
            and cls.REGEX_ALPHANUMERIC8.match(update_context.dispenserCode)
        ):
            org_code = update_context.dispenserCode
        log_of_change[cls.AGENT_PERSON_ORG_CODE] = org_code

        # To help with troubleshooting, the following change entries are added
        pre_change_issue_statuses = update_context.epsRecord.return_prechange_issue_status_dict()
        post_change_issue_statuses = update_context.epsRecord.create_issue_current_status_dict()
        log_of_change[cls.PRE_CHANGE_STATUS_DICT] = pre_change_issue_statuses
        log_of_change[cls.POST_CHANGE_STATUS_DICT] = post_change_issue_statuses
        log_of_change[cls.CHANGED_ISSUES_LIST] = update_context.epsRecord.return_changed_issue_list(
            pre_change_issue_statuses,
            post_change_issue_statuses,
            None,
            update_context.changedIssuesList,
        )
        # To help with troubleshooting, the following currentIssue values are added
        log_of_change[cls.PRE_CHANGE_CURRENT_ISSUE] = (
            update_context.epsRecord.return_prechange_current_issue()
        )
        log_of_change[cls.POST_CHANGE_CURRENT_ISSUE] = update_context.epsRecord.current_issue_number
        if hasattr(update_context, cls.TOUCHED) and update_context.touched:
            log_of_change[cls.TOUCHED] = update_context.touched

        return log_of_change

    @classmethod
    def prune_change_log(cls, change_log, prune_point):
        """
        Prune the change log where there is a series of change log entries for the same
        interactionID - and the change is neither recent nor part of the early history

        The intention is that if we get a repeating interaction we don't continue to explode the
        changeLog with all the history
        """
        inverted_change_log = {}
        max_scn = 0
        for guid, change_log_entry in change_log.items():
            scn = int(change_log_entry.get(cls.SCN, cls.INVALID_SCN))
            inverted_change_log[scn] = (guid, change_log_entry.get(cls.INTERACTION_ID))
            max_scn = max(max_scn, scn)
        if max_scn <= prune_point:
            # Don't make any changes
            return

        icl_scn_keys = list(inverted_change_log.keys())
        icl_scn_keys.sort(reverse=True)
        guids_to_prune = []
        for icl_scn in icl_scn_keys:
            if icl_scn > (max_scn - cls.MIN_RECENTHISTORY) or icl_scn < cls.MIN_INITIALHISTORY:
                continue
            this_int_id = inverted_change_log.get(icl_scn, (None, None))[1]
            (previous_guid, previous_int_id) = inverted_change_log.get(icl_scn - 1, (None, None))
            one_before_int_id = inverted_change_log.get(icl_scn - 2, (None, None))[1]
            if (
                this_int_id
                and this_int_id in cls.REPEATING_ACTIONS
                and this_int_id == previous_int_id
                and previous_int_id == one_before_int_id
            ):
                guids_to_prune.append(previous_guid)

        for guid in guids_to_prune:
            del change_log[guid]

        if len(change_log) > prune_point:
            # If we have breached the prune point but can't safely prune - stop before
            # The un-pruned record becomes an issue
            raise EpsSystemError(EpsSystemError.SYSTEM_FAILURE)
