import sys
import traceback
from datetime import datetime, timezone

from eps_spine_shared.common.dynamodb_client import EpsDataStoreError
from eps_spine_shared.common.dynamodb_common import prescription_id_without_check_digit
from eps_spine_shared.common.dynamodb_datastore import EpsDynamoDbDataStore
from eps_spine_shared.common.prescription.record import PrescriptionRecord
from eps_spine_shared.common.prescription.repeat_dispense import RepeatDispenseRecord
from eps_spine_shared.common.prescription.repeat_prescribe import RepeatPrescribeRecord
from eps_spine_shared.common.prescription.single_prescribe import SinglePrescribeRecord
from eps_spine_shared.common.prescription.types import PrescriptionTreatmentType
from eps_spine_shared.errors import (
    EpsBusinessError,
    EpsErrorBase,
    EpsSystemError,
    EpsValidationError,
)
from eps_spine_shared.interactions.common import (
    apply_all_cancellations,
    apply_updates,
    build_working_record,
    check_for_pending_cancellations,
    check_for_replay,
    create_event_log,
    log_pending_cancellation_event,
    prepare_document_for_store,
    prepare_record_for_store,
)
from eps_spine_shared.logger import EpsLogger
from eps_spine_shared.validation.common import check_mandatory_items
from eps_spine_shared.validation.create import run_validations

MANDATORY_ITEMS = [
    "agentOrganization",
    "agentRoleProfileCodeId",
    "hcplOrgCode",
    "prescribingGpCode",
    "nhsNumber",
    "prescriptionID",
    "prescriptionTime",
    "prescriptionTreatmentType",
    "signedTime",
    "birthTime",
    "agentSdsRole",
    "hl7EventID",
]

CANCELLATION_BODY_XSLT = "cancellationDocument_to_cancellationResponse.xsl"
CANCELLATION_SUCCESS_RESPONSE_TEXT = "Prescription/Item was cancelled"
CANCELLATION_SUCCESS_RESPONSE_CODE = "0001"
CANCEL_SUCCESS_RESPONSE_CODE_SYSTEM = "2.16.840.1.113883.2.1.3.2.4.17.19"
CANCELLATION_SUCCESS_STYLESHEET = "CancellationResponse_PORX_MT135201UK31.xsl"


def output_validate(context, internal_id, log_object: EpsLogger):
    """
    Validate the WDO using the local validator
    """
    try:
        check_mandatory_items(context, MANDATORY_ITEMS)
        run_validations(context, datetime.now(tz=timezone.utc), internal_id, log_object)
        log_object.write_log("EPS0001", None, {"internalID": internal_id})
    except EpsValidationError as e:
        last_log_line = traceback.format_tb(sys.exc_info()[2])
        log_object.write_log(
            "EPS0002",
            None,
            {
                "internalID": internal_id,
                "interactionID": context.interactionID,
                "errorDetails": e.supp_info,
                "lastLogLine": last_log_line,
            },
        )
        raise EpsBusinessError(EpsErrorBase.UNABLE_TO_PROCESS, e.supp_info) from e


def audit_prescription_id(prescription_id, interaction_id, internal_id, log_object: EpsLogger):
    """
    Log out the inbound prescriptionID - to help with tracing issue by prescriptionID
    """
    log_object.write_log(
        "EPS0095a",
        None,
        {
            "internalID": internal_id,
            "prescriptionID": prescription_id,
            "interactionID": interaction_id,
        },
    )


def check_for_duplicate(
    context,
    prescription_id,
    internal_id,
    log_object: EpsLogger,
    data_store_object: EpsDynamoDbDataStore,
):
    """
    Check prescription store for existence of prescription
    """
    eps_record_id = prescription_id_without_check_digit(prescription_id)

    try:
        is_present = data_store_object.is_record_present(internal_id, eps_record_id)
        if not is_present:
            log_object.write_log(
                "EPS0003", None, {"internalID": internal_id, "eps_record_id": eps_record_id}
            )
            return
    except EpsDataStoreError as e:
        log_object.write_log(
            "EPS0130",
            None,
            {"internalID": internal_id, "eps_record_id": eps_record_id, "reason": e.error_topic},
        )
        raise EpsSystemError(EpsSystemError.IMMEDIATE_REQUEUE) from e

    # Prescription present - may be a pending cancellation
    try:
        record_returned = data_store_object.return_record_for_process(internal_id, eps_record_id)
    except EpsDataStoreError as e:
        log_object.write_log(
            "EPS0130",
            None,
            {"internalID": internal_id, "eps_record_id": eps_record_id, "reason": e.error_topic},
        )
        raise EpsSystemError(EpsSystemError.IMMEDIATE_REQUEUE) from e

    check_for_late_upload_request(record_returned, internal_id, log_object)

    context.replayDetected = check_for_replay(
        eps_record_id, record_returned["value"], context.messageID, context, internal_id, log_object
    )

    if context.replayDetected:
        return

    context.recordToProcess = record_returned
    if not check_existing_record_real(eps_record_id, context, internal_id, log_object):
        log_object.write_log(
            "EPS0128a", None, {"internalID": internal_id, "prescriptionID": context.prescriptionID}
        )

        raise EpsSystemError(EpsSystemError.MESSAGE_FAILURE)


def check_for_late_upload_request(existing_record, internal_id, log_object: EpsLogger):
    """
    It is possible for a cancellation to be received and then for an upload request to follow after over six months.
    In this case, the record having a next activity of purge results in an exception upon further processing.
    """
    record = PrescriptionRecord(log_object, internal_id)
    record.create_record_from_store(existing_record["value"])

    if record.is_next_activity_purge():
        prescription_id = record.return_prescription_id()
        log_object.write_log(
            "EPS0818", None, {"prescriptionID": prescription_id, "internalID": internal_id}
        )
        raise EpsBusinessError(EpsErrorBase.EXISTS_WITH_NEXT_ACTIVITY_PURGE)


def check_existing_record_real(eps_record_id, context, internal_id, log_object: EpsLogger):
    """
    Presence of cancellation placeholder has already been confirmed, so now retrieve
    the pending cancellation for processing so that the new prescription may overwrite it.
    """
    vector_clock = context.recordToProcess["vectorClock"]
    log_object.write_log(
        "EPS0139",
        None,
        {"internalID": internal_id, "key": eps_record_id, "vectorClock": vector_clock},
    )

    build_working_record(context, internal_id, log_object)

    is_prescription = context.epsRecord.check_real()
    if is_prescription:
        log_object.write_log(
            "EPS0128",
            None,
            {"internalID": internal_id, "prescriptionID": context.prescriptionID},
        )
        raise EpsBusinessError(EpsErrorBase.DUPLICATE_PRESRIPTION)

    # Pending Cancellation
    check_for_pending_cancellations(context)
    context.cancellationPlaceholderFound = True
    context.fetchedRecord = True
    return True


def create_initial_record(context, internal_id, log_object: EpsLogger):
    """
    Create a Prescriptions Record object, and set all initial values
    """

    if context.replayDetected:
        return

    treatment_type = context.prescriptionTreatmentType
    if treatment_type == PrescriptionTreatmentType.ACUTE_PRESCRIBING:
        record_object = SinglePrescribeRecord(log_object, internal_id)
    elif treatment_type == PrescriptionTreatmentType.REPEAT_PRESCRIBING:
        record_object = RepeatPrescribeRecord(log_object, internal_id)
    elif treatment_type == PrescriptionTreatmentType.REPEAT_DISPENSING:
        record_object = RepeatDispenseRecord(log_object, internal_id)
    else:
        log_object.write_log(
            "EPS0122", None, {"internalID": internal_id, "treatmentType": treatment_type}
        )
        raise EpsSystemError("messageFailure")

    record_object.create_initial_record(context)
    context.epsRecord = record_object
    context.epsRecord.set_initial_prescription_status(context.handleTime)

    if context.cancellationPlaceholderFound:
        apply_all_cancellations(context, internal_id, log_object, was_pending=True)


def log_pending_cancellation_events(context, internal_id, log_object: EpsLogger):
    """
    Generate pending cancellation eventLog entries for all cancellations on the context
    """
    for _ in context.cancellationObjects:
        log_pending_cancellation_event(context, None, internal_id, log_object)


def prescriptions_workflow(
    context,
    prescription_id,
    interaction_id,
    doc_type,
    doc_ref_title,
    services_dict,
    deep_copy,
    failure_count,
    internal_id,
    log_object: EpsLogger,
    datastore_object: EpsDynamoDbDataStore,
):
    """
    Workflow for creating a prescription
    """
    output_validate(context, internal_id, log_object)
    audit_prescription_id(prescription_id, interaction_id, internal_id, log_object)
    check_for_duplicate(context, prescription_id, internal_id, log_object, datastore_object)
    prepare_document_for_store(
        context, doc_type, doc_ref_title, services_dict, deep_copy, internal_id, log_object
    )
    create_initial_record(context, internal_id, log_object)
    log_pending_cancellation_events(context, internal_id, log_object)
    create_event_log(context, internal_id, log_object)
    prepare_record_for_store(context, internal_id, log_object)
    apply_updates(context, failure_count, internal_id, log_object, datastore_object)
