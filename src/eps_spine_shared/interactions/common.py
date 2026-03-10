import base64
import datetime
import sys
import zlib

from dateutil import relativedelta

from eps_spine_shared.common import indexes
from eps_spine_shared.common.dynamodb_common import prescription_id_without_check_digit
from eps_spine_shared.common.dynamodb_datastore import EpsDynamoDbDataStore
from eps_spine_shared.common.prescription import fields
from eps_spine_shared.common.prescription.repeat_dispense import RepeatDispenseRecord
from eps_spine_shared.common.prescription.repeat_prescribe import RepeatPrescribeRecord
from eps_spine_shared.common.prescription.single_prescribe import SinglePrescribeRecord
from eps_spine_shared.errors import EpsSystemError
from eps_spine_shared.interactions.updates import apply_blind_update, apply_smart_update
from eps_spine_shared.logger import EpsLogger
from eps_spine_shared.nhsfundamentals.time_utilities import TimeFormats
from eps_spine_shared.spinecore.base_utilities import handle_encoding_oddities
from eps_spine_shared.spinecore.changelog import PrescriptionsChangeLogProcessor

CANCEL_INTERACTION = "PORX_IN050102UK32"
EXPECTED_DELETE_WAIT_TIME_MONTHS = 18
EXPECTED_NOMINATED_RELEASE_DELETE_WAIT_TIME_DAYS = 36
SERVICE = "urn:nhs:names:services:mm"
TEST_PRESCRIBING_SITES = ["Z99901", "Z99902"]

PRESCRIPTION_EXPIRY_PERIOD_MONTHS = 6
REPEAT_DISP_EXPIRY_PERIOD_MONTHS = 12
DATA_CLEANSE_PERIOD_MONTHS = 6
WD_ACTIVE_EXPIRY_PERIOD_DAYS = 180
EXPIRED_DELETE_PERIOD = 90
CANCELLED_DELETE_PERIOD = 180
CLAIMED_DELETE_PERIOD = 36
NOT_DISPENSED_DELETE_PERIOD = 30
NOMINATED_DOWNLOAD_LEAD_DAYS = 7
NOTIFICATION_DELAY_PERIOD = 180
PURGED_DELETE_PERIOD = 365


def check_for_replay(
    eps_record_id, eps_record_retrieved, message_id, context, internal_id, log_object: EpsLogger
):
    """
    Check a retrieved record for the existence of the message GUID within the change log
    """
    try:
        change_log = eps_record_retrieved["changeLog"]
    except Exception as e:  # noqa: BLE001
        log_object.write_log(
            "EPS0004",
            sys.exc_info(),
            {"internalID": internal_id, "epsRecordID": eps_record_id},
        )
        raise EpsSystemError("systemFailure") from e

    if message_id in change_log:
        log_object.write_log(
            "EPS0005",
            None,
            {
                "internalID": internal_id,
                "epsRecordID": eps_record_id,
                "changeLog": str(change_log),
            },
        )
        context.replayedChangeLog = change_log[message_id]
        return True

    return False


def build_working_record(context, internal_id, log_object: EpsLogger):
    """
    An epsRecord object needs to be created from the record extracted from the
    store.  The record-type should have been extracted - and this will be used to
    determine which class of object to create.

    Note that Pending Cancellation placeholders will not have a recordType, so
    default this to 'Acute' to allow processing to continue.
    """
    record_type = (
        "Acute"
        if "recordType" not in context.recordToProcess
        else context.recordToProcess["recordType"]
    )
    if record_type == "Acute":
        context.epsRecord = SinglePrescribeRecord(log_object, internal_id)
    elif record_type == "RepeatPrescribe":
        context.epsRecord = RepeatPrescribeRecord(log_object, internal_id)
    elif record_type == "RepeatDispense":
        context.epsRecord = RepeatDispenseRecord(log_object, internal_id)
    else:
        log_object.write_log(
            "EPS0133", None, {"internalID": internal_id, "recordType": str(record_type)}
        )
        raise EpsSystemError("developmentFailure")

    context.epsRecord.create_record_from_store(context.recordToProcess["value"])


def check_for_pending_cancellations(context):
    """
    Check for pending cancellations on the record, and bind them to context
    if they exist
    """
    pending_cancellations = context.epsRecord.return_pending_cancellations()
    if pending_cancellations:
        context.cancellationObjects = pending_cancellations


def prepare_document_for_store(
    context, doc_type, doc_ref_title, services_dict, deep_copy, internal_id, log_object: EpsLogger
):
    """
    For inbound messages to be stored in the datastore.
    The key for the object should be the internalID of the message.
    """
    if context.replayDetected:
        context.documentsToStore = None
        return

    if (
        hasattr(context, "prescriptionID") and context.prescriptionID
    ):  # noqa: SIM108 - More readable as is
        presc_id = context.prescriptionID
    else:
        presc_id = "NominatedReleaseRequest_" + internal_id

    document_ref = internal_id

    setattr(context, doc_ref_title, document_ref)

    document_to_store = {}
    document_to_store["key"] = document_ref
    document_to_store["value"] = extract_body_to_store(
        presc_id, doc_type, context, services_dict, deep_copy, internal_id, log_object
    )
    document_to_store["index"] = create_index_for_document(context, doc_ref_title, presc_id)
    document_to_store["vectorClock"] = None
    context.documentsToStore.append(document_to_store)
    context.documentReferences.append(document_ref)

    log_object.write_log(
        "EPS0125",
        None,
        {"internalID": internal_id, "type": doc_type, "key": document_ref, "vectorClock": "None"},
    )


def extract_body_to_store(
    prescription_id,
    doc_type,
    context,
    services_dict,
    deep_copy,
    internal_id,
    log_object: EpsLogger,
    base_document=None,
):
    """
    Extract the inbound message body and prepare as a document for the epsDocument
    store
    """
    try:
        if base_document is None:
            base_document = context.xmlBody

        deep_copy_transform = services_dict["Style Sheets"][deep_copy]
        compressed_document = zlib.compress(str(deep_copy_transform(base_document)))
        encoded_document = base64.b64encode(compressed_document)
        value = {}
        value["content"] = encoded_document
        value["content type"] = "xml"
        value["id"] = prescription_id
        value["type"] = doc_type
    except Exception as e:  # noqa: BLE001
        log_object.write_log(
            "EPS0014b", sys.exc_info(), {"internalID": internal_id, "type": doc_type}
        )
        raise EpsSystemError(EpsSystemError.MESSAGE_FAILURE) from e
    return value


def create_index_for_document(context, doc_ref_title, prescription_id):
    """
    Index required is prescriptionID should there be a need to search for document by prescription ID
    Other index is storeTimeByDocRefTitle - this allows for documents of a certain
    type to be queried by the range of the document age (e.g. searching for all
    Claim Notices which have been present for more than 48 hours)
    """
    store_time = context.handleTime.strftime(TimeFormats.STANDARD_DATE_TIME_FORMAT)

    default_delta = relativedelta(months=+EXPECTED_DELETE_WAIT_TIME_MONTHS)
    nominated_release_delta = relativedelta(days=+EXPECTED_NOMINATED_RELEASE_DELETE_WAIT_TIME_DAYS)
    delete_date_obj_delta = (
        nominated_release_delta
        if doc_ref_title == "NominatedReleaseRequestMsgRef"
        else default_delta
    )

    delete_date_obj = context.handleTime + delete_date_obj_delta
    delete_date = delete_date_obj.strftime(TimeFormats.STANDARD_DATE_FORMAT)

    index_dict = {}
    index_dict[indexes.INDEX_PRESCRIPTION_ID] = [prescription_id]
    index_dict[indexes.INDEX_STORE_TIME_DOC_REF_TITLE] = [doc_ref_title + "_" + store_time]
    index_dict[indexes.INDEX_DELETE_DATE] = [delete_date]

    return index_dict


def log_pending_cancellation_event(context, start_issue_number, internal_id, log_object: EpsLogger):
    """
    Generate a pending cancellation eventLog entry
    """
    if not hasattr(context, "responseParameters"):
        context.responseParameters = {}
        context.responseParameters["cancellationResponseText"] = "Subsequent cancellation"
        context.responseParameters["timeStampSent"] = datetime.datetime.now().strftime(
            TimeFormats.STANDARD_DATE_TIME_FORMAT
        )
        context.responseParameters["messageID"] = context.messageID

    context.responseDetails = {}
    context.responseDetails[PrescriptionsChangeLogProcessor.RSP_PARAMS] = context.responseParameters
    error_response_stylesheet = "generateHL7MCCIDetectedIssue.xsl"
    cancellation_body_xslt = "cancellationRequest_to_cancellationResponse.xsl"
    response_xslt = [error_response_stylesheet, cancellation_body_xslt]
    context.responseDetails[PrescriptionsChangeLogProcessor.XSLT] = response_xslt
    context.epsRecord.increment_scn()
    create_event_log(context, internal_id, log_object, start_issue_number)
    context.epsRecord.add_event_to_change_log(internal_id, context.eventLog)


def create_event_log(context, internal_id, log_object: EpsLogger, instance_id=None):
    """
    Create the change log for this event. Will be placed on change log in record
    under a key of the messageID
    """
    if context.replayDetected:
        return

    if not instance_id:
        if context.epsRecord:
            instance_id = context.epsRecord.return_current_instance()
        else:
            log_object.write_log("EPS0673", None, {"internalID": internal_id})
            instance_id = "NotAvailable"
    context.instanceID = instance_id

    if context.epsRecord:
        event_log = PrescriptionsChangeLogProcessor.log_for_domain_update(context, internal_id)
        context.eventLog = event_log


def apply_all_cancellations(
    context,
    internal_id,
    log_object: EpsLogger,
    was_pending=False,
    start_issue_number=None,
    send_subsequent_cancellation=True,
):
    """
    Apply all the cancellations on the context (these should normally be fetched from
    the record)
    """
    for cancellation_obj in context.cancellationObjects:
        [cancel_id, issues_updated] = context.epsRecord.apply_cancellation(
            cancellation_obj, start_issue_number
        )
        log_object.write_log(
            "EPS0266",
            None,
            {
                "internalID": internal_id,
                "prescriptionID": context.prescriptionID,
                "issuesUpdated": issues_updated,
                "cancellationID": cancel_id,
            },
        )

        if not is_death(cancellation_obj, internal_id, log_object):
            if was_pending and send_subsequent_cancellation:
                context.cancellationObjects.append(cancellation_obj)


def is_death(cancellation_obj, internal_id, log_object: EpsLogger):
    """
    Returns True if this is a Death Notification
    """
    reasons = cancellation_obj.get(fields.FIELD_REASONS)

    if not reasons:
        return False

    for reason in reasons:
        if str(handle_encoding_oddities(reason)).lower().find("notification of death") != -1:
            log_object.write_log(
                "EPS0652", None, {"internalID": internal_id, "reason": str(reason)}
            )
            return True

    return False


def prepare_record_for_store(
    context, internal_id, log_object: EpsLogger, fetched_record=False, key=None
):
    """
    Prepare the record to be stored:
    1 - Check there is a need to store (not replay)
    2 - Set the key
    3 - Add change log to record
    4 - Set the index (including calculation of nextActivity)
    5 - Set the value (from the epsRecord object)

    fetched_record indicates whether the recordToStore is based on one retrieved by
    this interactionWorker process.  If it is, there will be a vectorClock, which
    is required in order for the updateApplier to use as an optimistic 'lock'

    key if passed will be used as the key to be stored (otherwise generate from
    context.prescriptionID)
    """
    if context.replayDetected:
        context.recordToStore = None
        return

    context.recordToStore = {}

    if not key:
        presc_id = prescription_id_without_check_digit(context.prescriptionID)
        context.recordToStore["key"] = presc_id
    else:
        context.recordToStore["key"] = key

    index_dict = create_record_index(context, internal_id, log_object)
    context.recordToStore["index"] = index_dict
    context.epsRecord.add_index_to_record(index_dict)
    context.epsRecord.add_document_references(context.documentReferences)

    context.epsRecord.increment_scn()
    context.epsRecord.add_event_to_change_log(context.messageID, context.eventLog)

    context.recordToStore["value"] = context.epsRecord.return_record_to_be_stored()

    if fetched_record:
        context.recordToStore["vectorClock"] = context.recordToProcess["vectorClock"]
    else:
        context.recordToStore["vectorClock"] = None

    context.recordToStore["recordType"] = context.epsRecord.record_type

    log_object.write_log(
        "EPS0125",
        None,
        {
            "internalID": internal_id,
            "type": "prescriptionRecord",
            "key": context.recordToStore["key"],
            "vectorClock": "None",
        },
    )


def create_record_index(context, internal_id, log_object: EpsLogger):
    """
    Create the index values to be used when storing the epsRecord.
    There may be separate index terms for each individual instance
    (but only unique index terms for the prescription should be returned).
    """
    index_maker = indexes.EpsIndexFactory(
        log_object, internal_id, TEST_PRESCRIBING_SITES, get_nad_references()
    )
    return index_maker.build_indexes(context)


def get_nad_references():
    """
    Create a reference dictionary of information
    for use during next activity date calculation
    """
    return {
        "prescriptionExpiryPeriod": relativedelta(months=+PRESCRIPTION_EXPIRY_PERIOD_MONTHS),
        "repeatDispenseExpiryPeriod": relativedelta(months=+REPEAT_DISP_EXPIRY_PERIOD_MONTHS),
        "dataCleansePeriod": relativedelta(months=+DATA_CLEANSE_PERIOD_MONTHS),
        "withDispenserActiveExpiryPeriod": relativedelta(days=+WD_ACTIVE_EXPIRY_PERIOD_DAYS),
        "expiredDeletePeriod": relativedelta(days=+EXPIRED_DELETE_PERIOD),
        "cancelledDeletePeriod": relativedelta(days=+CANCELLED_DELETE_PERIOD),
        "claimedDeletePeriod": relativedelta(days=+CLAIMED_DELETE_PERIOD),
        "notDispensedDeletePeriod": relativedelta(days=+NOT_DISPENSED_DELETE_PERIOD),
        "nominatedDownloadDateLeadTime": relativedelta(days=+NOMINATED_DOWNLOAD_LEAD_DAYS),
        "notificationDelayPeriod": relativedelta(days=+NOTIFICATION_DELAY_PERIOD),
        "purgedDeletePeriod": relativedelta(days=+PURGED_DELETE_PERIOD),
    }


def apply_updates(
    context,
    failure_count,
    internal_id,
    log_object: EpsLogger,
    datastore_object: EpsDynamoDbDataStore,
):
    """
    Apply record and document updates directly
    """
    log_object.write_log("EPS0900", None, {"internalID": internal_id})

    add_documents_to_store(context, internal_id, log_object, datastore_object)
    apply_record_change_to_store(context, failure_count, internal_id, log_object, datastore_object)


def add_documents_to_store(
    context, internal_id, log_object: EpsLogger, datastore_object: EpsDynamoDbDataStore
):
    """
    Add documents to the store from the context
    """
    documents_to_store = context.documentsToStore
    if not documents_to_store:
        log_object.write_log("EPS0910", None, {"internalID": internal_id})
        return

    for document_to_store in documents_to_store:
        apply_blind_update(
            document_to_store, "epsDocument", internal_id, log_object, datastore_object
        )


def apply_record_change_to_store(
    context,
    failure_count,
    internal_id,
    log_object: EpsLogger,
    datastore_object: EpsDynamoDbDataStore,
):
    """
    Apply the record change to the store from the context
    """
    record_to_store = context.recordToStore

    if not record_to_store:
        log_object.write_log("EPS0920", None, {"internalID": internal_id})
        return

    if not record_to_store["vectorClock"]:
        apply_blind_update(record_to_store, "epsRecord", internal_id, log_object, datastore_object)
    else:
        apply_smart_update(
            record_to_store,
            failure_count,
            internal_id,
            log_object,
            datastore_object,
            context.documentsToStore,
        )
