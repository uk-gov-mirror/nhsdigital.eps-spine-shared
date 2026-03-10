from eps_spine_shared.common import indexes
from eps_spine_shared.common.dynamodb_client import EpsDataStoreError
from eps_spine_shared.common.dynamodb_datastore import EpsDynamoDbDataStore
from eps_spine_shared.common.prescription.statuses import PrescriptionStatus
from eps_spine_shared.errors import EpsSystemError
from eps_spine_shared.logger import EpsLogger


def apply_smart_update(
    object_to_store,
    failure_count,
    internal_id,
    log_object: EpsLogger,
    datastore_object: EpsDynamoDbDataStore,
    docs_to_store=None,
):
    """
    Can be used for inserting a new object, or overwriting an object where
    last_write_wins is True
    """
    key = object_to_store["key"]
    value = object_to_store["value"]
    index_dict = object_to_store.get("index")
    record_type = object_to_store.get("recordType")

    try:
        scn = value.get("SCN")
        existing_record = datastore_object.return_record_for_process(internal_id, key)

        is_pending_cancellation = False
        existing_record_indexes = existing_record["value"]["indexes"]
        prescriber_status_index = existing_record_indexes.get(indexes.INDEX_PRESCRIBER_STATUS)
        if prescriber_status_index and len(prescriber_status_index) > 0:
            is_pending_cancellation = prescriber_status_index[0].endswith(
                PrescriptionStatus.PENDING_CANCELLATION
            )

        if is_pending_cancellation:
            existing_scn = existing_record.get("value", {}).get("SCN")
            new_scn = existing_scn + 1 if existing_scn else 2
            value["SCN"] = new_scn
            scn = new_scn
    except Exception:  # noqa: BLE001
        scn = None

    try:
        datastore_object.insert_eps_record_object(
            internal_id, key, value, index_dict, record_type, is_update=True
        )
    except EpsDataStoreError as e:
        if e.error_topic == EpsDataStoreError.CONDITIONAL_UPDATE_FAILURE:
            if failure_count >= 0:
                failure_count -= 1
            if docs_to_store:
                for doc_to_store in docs_to_store:
                    doc_key = doc_to_store["key"]
                    log_object.write_log(
                        "EPS0126b", None, {"internalID": internal_id, "key": doc_key}
                    )
                    if "notification" not in doc_to_store.get("key", "").lower():
                        log_object.write_log(
                            "EPS0126d", None, {"internalID": internal_id, "key": doc_key}
                        )
                        continue
                    log_object.write_log(
                        "EPS0126c", None, {"internalID": internal_id, "key": doc_key}
                    )
                    datastore_object.delete_document(
                        internal_id, documentKey=doc_key, deleteNotification=True
                    )

        log_object.write_log(
            "EPS0126a",
            None,
            {
                "internalID": internal_id,
                "key": key,
                "scn": scn,
                "errorCode": e.error_topic,
                "vectorClock": object_to_store["vectorClock"],
            },
        )

        raise EpsSystemError(EpsSystemError.IMMEDIATE_REQUEUE) from e

    log_object.write_log(
        "EPS0127a",
        None,
        {
            "internalID": internal_id,
            "key": key,
            "scn": scn,
            "vectorClock": object_to_store["vectorClock"],
        },
    )


def apply_blind_update(
    object_to_store,
    bucket,
    internal_id,
    log_object: EpsLogger,
    datastore_object: EpsDynamoDbDataStore,
):
    """
    Can be used for inserting a new object, or overwriting an object where
    last_write_wins is True
    """

    key = object_to_store["key"]
    value = object_to_store["value"]
    index_name = object_to_store.get("index")
    record_type = object_to_store.get("recordType")

    try:
        scn = None
        if bucket == "epsRecord":
            scn = value.get("SCN")
    except Exception:  # noqa: BLE001
        scn = None

    try:
        if bucket == "epsDocument":
            datastore_object.insert_eps_document_object(internal_id, key, value, index_name)
        if bucket == "epsRecord":
            datastore_object.insert_eps_record_object(
                internal_id, key, value, index_name, record_type
            )
    except EpsDataStoreError as e:
        log_object.write_log(
            "EPS0126",
            None,
            {
                "internalID": internal_id,
                "bucket": bucket,
                "key": key,
                "scn": scn,
                "errorCode": e.error_topic,
            },
        )
        raise EpsSystemError(EpsSystemError.IMMEDIATE_REQUEUE) from e

    log_object.write_log(
        "EPS0127", None, {"internalID": internal_id, "bucket": bucket, "key": key, "scn": scn}
    )
