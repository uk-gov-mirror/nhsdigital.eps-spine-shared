import base64
import copy
import functools
import sys
import time
import zlib
from datetime import datetime, timedelta, timezone
from random import randint
from typing import List, Tuple

import simplejson
from boto3.dynamodb.types import Binary
from dateutil.relativedelta import relativedelta

from eps_spine_shared.common import indexes
from eps_spine_shared.common.dynamodb_client import EpsDataStoreError, EpsDynamoDbClient
from eps_spine_shared.common.dynamodb_common import (
    GSI,
    NEXT_ACTIVITY_DATE_PARTITIONS,
    Attribute,
    Key,
    ProjectedAttribute,
    SortKey,
    determine_release_version,
    prescription_id_without_check_digit,
    replace_decimals,
)
from eps_spine_shared.common.dynamodb_index import EpsDynamoDbIndex, PrescriptionStatus
from eps_spine_shared.common.dynamodb_query import Conditions, DynamoDbQuery
from eps_spine_shared.logger import EpsLogger
from eps_spine_shared.nhsfundamentals.time_utilities import (
    TimeFormats,
    convert_spine_date,
    time_now_as_string,
)


def timer(func):
    """
    Decorator to be used to time methods.
    """

    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        self = args[0]
        internal_id = args[1]
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time_ms = (end_time - start_time) * 1000
        run_time_ms = float(f"{run_time_ms:.2f}")
        self.log_object.write_log(
            "DDB0002",
            None,
            {
                "cls": type(self).__name__,
                "func": func.__name__,
                "duration": run_time_ms,
                "internalID": internal_id,
            },
        )
        return value

    return wrapper_timer


class EpsDynamoDbDataStore:
    """
    The prescriptions message store specific DynamoDB client.
    """

    SEPARATOR = "#"
    CLAIM_SEQUENCE_NUMBER_KEY = "claimSequenceNumber"
    NWSSP_CLAIM_SEQUENCE_NUMBER_KEY = "claimSequenceNumberNwssp"
    INDEX_CLAIMID = "claimid_bin"
    INDEX_CLAIMHANDLETIME = "claimhandletime_bin"
    INDEX_CLAIM_SEQNUMBER = "seqnum_bin"
    INDEX_CLAIM_SEQNUMBER_NWSSP = "nwsspseqnum_bin"
    INDEX_SCN = "delta_bin"
    INDEX_WORKLISTDATE = "workListDate_bin"
    NOTIFICATION_PREFIX = "Notification_"
    STORE_TIME_DOC_REF_TITLE_PREFIX = "NominatedReleaseRequestMsgRef"
    DEFAULT_EXPIRY_DAYS = 56
    MAX_NEXT_ACTIVITY_DATE = "99991231"

    def __init__(self, log_object, system_config):
        """
        Instantiate the DynamoDB client.
        """
        self.log_object = EpsLogger(log_object)
        self.client = EpsDynamoDbClient(
            log_object,
            system_config["ddb aws endpoint url"],
            system_config["datastore table name"],
            system_config["datastore role arn"],
            system_config["process name"],
            system_config["sts endpoint url"],
        )
        self.indexes = EpsDynamoDbIndex(log_object, self.client)

    def testConnection(self):
        """
        Placeholder test connection, returns constant value
        """
        return True

    def base64_decode_document_content(self, internal_id, document):
        """
        base64 decode document content in order to store as binary type in DynamoDB.
        """
        if content := document.get("content"):
            try:
                decoded = base64.b64decode(document["content"].encode("utf-8"))
                if base64.b64encode(decoded).decode("utf-8") == content:
                    document["content"] = decoded
                else:
                    raise ValueError("Document content not b64 encoded")
            except Exception as e:  # noqa: BLE001
                self.log_object.write_log(
                    "DDB0031", sys.exc_info(), {"error": str(e), "internalID": internal_id}
                )
                raise e

    def get_expire_at(self, delta, from_datetime=None):
        """
        Returns an int timestamp to be used as an expireAt attribute.
        This will determine when the item is deleted from the table.
        """
        if not from_datetime:
            from_datetime = datetime.now(timezone.utc)

        if not from_datetime.tzinfo:
            from_datetime = datetime.combine(
                from_datetime.date(), from_datetime.time(), timezone.utc
            )

        return int((from_datetime + delta).timestamp())

    def calculate_record_expire_at(
        self, next_activity, next_activity_date_str, creation_datetime_string
    ):
        """
        If the record next activity is delete or purge, use the nextActivity and nextActivityDate
        to calculate its expireAt (ttl) value, otherwise fall-back to the default of 18 months.
        """
        creation_datetime = convert_spine_date(
            creation_datetime_string, TimeFormats.STANDARD_DATE_TIME_FORMAT
        )
        creation_datetime_utc = datetime.combine(
            creation_datetime.date(), creation_datetime.time(), timezone.utc
        )
        default_expire_at = self.get_expire_at(relativedelta(months=18), creation_datetime_utc)

        if (
            next_activity.lower() not in ["delete", "purge"]
            or not next_activity_date_str
            or next_activity_date_str == self.MAX_NEXT_ACTIVITY_DATE
        ):
            return default_expire_at

        delta = relativedelta() if next_activity.lower() == "purge" else relativedelta(months=12)

        next_activity_datetime = convert_spine_date(
            next_activity_date_str, TimeFormats.STANDARD_DATE_FORMAT
        )
        next_activity_datetime_utc = datetime.combine(
            next_activity_datetime.date(), next_activity_datetime.time(), timezone.utc
        )

        next_activity_expire_at = self.get_expire_at(delta, next_activity_datetime_utc)

        return min(next_activity_expire_at, default_expire_at)

    def build_document(self, internal_id, document, index):
        """
        Build EPS Document object to be inserted into DynamoDB.
        """
        document_copy = copy.deepcopy(document)
        self.base64_decode_document_content(internal_id, document_copy)

        default_expire_at = self.get_expire_at(relativedelta(months=18))

        item = {
            Key.SK.name: SortKey.DOCUMENT.value,
            ProjectedAttribute.INDEXES.name: self.convert_index_keys_to_lower_case(index),
            ProjectedAttribute.BODY.name: document_copy,
            ProjectedAttribute.EXPIRE_AT.name: default_expire_at,
        }

        if index:
            doc_ref_title, store_time = index[indexes.INDEX_STORE_TIME_DOC_REF_TITLE][0].split("_")
            item[Attribute.DOC_REF_TITLE.name] = doc_ref_title

            if doc_ref_title == "ClaimNotification":
                item[Attribute.CLAIM_NOTIFICATION_STORE_DATE.name] = store_time[:8]

            item[Attribute.STORE_TIME.name] = store_time

            delete_date = index[indexes.INDEX_DELETE_DATE][0]
            delete_date_time = datetime.strptime(delete_date, TimeFormats.STANDARD_DATE_FORMAT)
            item[ProjectedAttribute.EXPIRE_AT.name] = int(delete_date_time.timestamp())

        return item

    @timer
    def insert_eps_document_object(self, internal_id, document_key, document, index=None):
        """
        Insert EPS Document object into the configured table.
        """
        item = self.build_document(internal_id, document, index)
        item[Key.PK.name] = document_key
        return self.client.insert_items(internal_id, [item], True)

    def convert_index_keys_to_lower_case(self, index):
        """
        Convert all keys in an index dict to lower case.
        """
        if not isinstance(index, dict):
            return index
        return {key.lower(): index[key] for key in index}

    def parse_next_activity_nad(self, indexes):
        """
        Split nextActivityNAD string into sharded next activity and its date.
        """
        next_activity_nad = indexes["nextActivityNAD_bin"][0]
        next_activity_nad_split = next_activity_nad.split("_")

        next_activity = next_activity_nad_split[0]
        next_activity_date_str = (
            next_activity_nad_split[1] if len(next_activity_nad_split) == 2 else None
        )

        shard = randint(1, NEXT_ACTIVITY_DATE_PARTITIONS)

        return next_activity, shard, next_activity_date_str

    def build_record(self, prescription_id, record, record_type, indexes):
        """
        Build EPS Record object to be inserted into DynamoDB.
        """
        record_key = prescription_id_without_check_digit(prescription_id)

        if not indexes:
            indexes = record["indexes"]
        instances = record["instances"].values()

        next_activity, shard, next_activity_date_str = self.parse_next_activity_nad(indexes)

        scn = record["SCN"]

        compressed_record = zlib.compress(simplejson.dumps(record).encode("utf-8"))

        creation_datetime_string = record["prescription"]["prescriptionTime"]

        expire_at = self.calculate_record_expire_at(
            next_activity, next_activity_date_str, creation_datetime_string
        )

        item = {
            Key.PK.name: record_key,
            Key.SK.name: SortKey.RECORD.value,
            ProjectedAttribute.BODY.name: compressed_record,
            Attribute.NEXT_ACTIVITY.name: f"{next_activity}.{shard}",
            ProjectedAttribute.SCN.name: scn,
            ProjectedAttribute.INDEXES.name: self.convert_index_keys_to_lower_case(indexes),
            ProjectedAttribute.EXPIRE_AT.name: expire_at,
        }
        if next_activity_date_str:
            item[Attribute.NEXT_ACTIVITY_DATE.name] = next_activity_date_str

        if next_activity.lower() == "purge":
            return item

        nhs_number = record["patient"]["nhsNumber"]
        prescriber_org = record["prescription"]["prescribingOrganization"]

        statuses = list(set([instance["prescriptionStatus"] for instance in instances]))
        is_ready = PrescriptionStatus.TO_BE_DISPENSED in statuses
        if PrescriptionStatus.TO_BE_DISPENSED in statuses:
            statuses.remove(PrescriptionStatus.TO_BE_DISPENSED)
            statuses.insert(0, PrescriptionStatus.TO_BE_DISPENSED)
        status = self.SEPARATOR.join(statuses)

        dispenser_orgs = []
        for instance in instances:
            org = instance.get("dispense", {}).get("dispensingOrganization")
            if org:
                dispenser_orgs.append(org)
        dispenser_org = self.SEPARATOR.join(set(dispenser_orgs))

        nominated_pharmacy = record.get("nomination", {}).get("nominatedPerformer")

        item_update = {
            Attribute.CREATION_DATETIME.name: creation_datetime_string,
            Attribute.NHS_NUMBER.name: nhs_number,
            Attribute.PRESCRIBER_ORG.name: prescriber_org,
            ProjectedAttribute.STATUS.name: status,
            Attribute.IS_READY.name: int(is_ready),
        }
        if dispenser_org:
            item[Attribute.DISPENSER_ORG.name] = dispenser_org
        if nominated_pharmacy:
            item[Attribute.NOMINATED_PHARMACY.name] = nominated_pharmacy
            if not dispenser_org:
                item[Attribute.DISPENSER_ORG.name] = nominated_pharmacy
        if record_type:
            item[ProjectedAttribute.RECORD_TYPE.name] = record_type
        item[ProjectedAttribute.RELEASE_VERSION.name] = determine_release_version(prescription_id)

        item.update(item_update)
        return item

    @timer
    def insert_eps_record_object(
        self, internal_id, prescription_id, record, index=None, record_type=None, is_update=False
    ):
        """
        Insert EPS Record object into the configured table.
        """
        item = self.build_record(prescription_id, record, record_type, index)

        return self.client.insert_items(internal_id, [item], is_update)

    @timer
    def insert_eps_work_list(self, internal_id, message_id, work_list, index=None):
        """
        Insert EPS WorkList object into the configured table.
        """
        work_list_indexes = {self.INDEX_WORKLISTDATE: [time_now_as_string()]}
        if index:
            work_list_indexes = index

        expire_at = self.get_expire_at(timedelta(days=self.DEFAULT_EXPIRY_DAYS))
        item = {
            Key.PK.name: message_id,
            Key.SK.name: SortKey.WORK_LIST.value,
            ProjectedAttribute.EXPIRE_AT.name: expire_at,
            ProjectedAttribute.BODY.name: self.compress_work_list_xml(internal_id, work_list),
            ProjectedAttribute.INDEXES.name: self.convert_index_keys_to_lower_case(
                work_list_indexes
            ),
        }
        return self.client.insert_items(internal_id, [item], True)

    @timer
    def is_record_present(self, internal_id, prescription_id) -> bool:
        """
        Returns a boolean indicating the presence of a record.
        """
        record_key = prescription_id_without_check_digit(prescription_id)
        record = self.client.get_item(
            internal_id, record_key, SortKey.RECORD.value, expect_exists=False
        )
        return True if record else False

    @timer
    def return_terms_by_nhs_number_date(self, internal_id, range_start, range_end, term_regex=None):
        """
        Return the epsRecord terms which match the supplied range and regex for the nhsNumberDate index.
        """
        return self.return_terms_by_index_date(
            internal_id, indexes.INDEX_NHSNUMBER_DATE, range_start, range_end, term_regex
        )

    @timer
    def return_terms_by_index_date(
        self, _internal_id, index, range_start, range_end=None, term_regex=None
    ):
        """
        Return the epsRecord terms which match the supplied range and regex for the supplied index.
        """
        index_map = {
            indexes.INDEX_NHSNUMBER_PRDSDATE: self.indexes.nhs_number_presc_disp_date,
            indexes.INDEX_NHSNUMBER_PRDATE: self.indexes.nhs_number_presc_date,
            indexes.INDEX_NHSNUMBER_DSDATE: self.indexes.nhs_number_disp_date,
            indexes.INDEX_NHSNUMBER_DATE: self.indexes.nhs_number_date,
            indexes.INDEX_PRESCRIBER_DSDATE: self.indexes.presc_disp_date,
            indexes.INDEX_PRESCRIBER_DATE: self.indexes.presc_date,
            indexes.INDEX_DISPENSER_DATE: self.indexes.disp_date,
            indexes.INDEX_NOMPHARM: self.indexes.nom_pharm_status,
        }
        return index_map[index](range_start, range_end, term_regex)

    @timer
    def return_terms_by_nhs_number(self, _internal_id, nhs_number):
        """
        Return the epsRecord terms which match the supplied NHS number.
        """
        return self.indexes.query_nhs_number_date(indexes.INDEX_NHSNUMBER, nhs_number)

    @timer
    def return_pids_for_nomination_change(self, internal_id, nhs_number):
        """
        Return the epsRecord list which match the supplied NHS number.
        """
        pid_list = self.return_terms_by_nhs_number(internal_id, nhs_number)

        prescriptions = []

        for pid in pid_list:
            prescriptions.append(pid[1])

        return prescriptions

    def get_nominated_pharmacy_records(self, nominated_pharmacy, batch_size, internal_id):
        """
        Run an index query to get the to-be-dispensed prescriptions for this nominated pharmacy.
        """
        key_list = self.get_nom_pharm_records_unfiltered(internal_id, nominated_pharmacy)
        discarded_key_count = max((len(key_list) - int(batch_size)), 0)
        key_list = key_list[:batch_size]
        return [key_list, discarded_key_count]

    @timer
    def get_nom_pharm_records_unfiltered(self, _internal_id, nominated_pharmacy, limit=None):
        """
        Query the nomPharmStatus index to get the unfiltered, to-be-dispensed prescriptions for the given pharmacy.
        """
        return self.indexes.query_nom_pharm_status(nominated_pharmacy, limit=limit)

    @timer
    def return_record_for_process(self, internal_id, prescription_id, expect_exists=True):
        """
        Look for and return an epsRecord object.
        """
        record_key = prescription_id_without_check_digit(prescription_id)
        item = self.client.get_item(
            internal_id, record_key, SortKey.RECORD.value, expect_exists=expect_exists
        )
        if not item:
            return {}
        body = item.get(ProjectedAttribute.BODY.name)
        if body and not isinstance(body, dict):
            body = simplejson.loads(zlib.decompress(bytes(body)))

        return self._build_record_to_return(item, body)

    def _build_record_to_return(self, item, body):
        """
        Create the record in the format expected by the calling code.
        """
        replace_decimals(body)

        record = {"value": body, "vectorClock": "vc"}

        if record_type := item.get("recordType"):
            record["recordType"] = record_type

        sharded_release_version = item.get(
            "releaseVersion", determine_release_version(item.get(Key.PK.name))
        )
        record["releaseVersion"] = sharded_release_version.split(".")[0]

        return record

    def base64_encode_document_content(self, internal_id, document_body):
        """
        base64 encode document content and convert to string, to align with return type of original datastore.
        """
        if document_body and not isinstance(document_body.get("content"), str):
            try:
                document_body["content"] = base64.b64encode(bytes(document_body["content"])).decode(
                    "utf-8"
                )
            except Exception as e:  # noqa: BLE001
                self.log_object.write_log(
                    "DDB0032", sys.exc_info(), {"error": str(e), "internalID": internal_id}
                )
                raise e

    @timer
    def return_document_for_process(self, internal_id, document_key, expect_exists=True):
        """
        Look for and return an epsDocument object.
        """
        item = self.client.get_item(
            internal_id,
            document_key,
            SortKey.DOCUMENT.value,
            expect_none=True,
            expect_exists=expect_exists,
        )
        if not item:
            return {}

        body = item.get(ProjectedAttribute.BODY.name)
        replace_decimals(body)

        if item.get(Attribute.DOC_REF_TITLE.name, "").lower() != "claimnotification":
            self.base64_encode_document_content(internal_id, body)
        elif isinstance(body.get("payload"), Binary):
            body["payload"] = body["payload"].value.decode("utf-8")

        return body

    @timer
    def return_record_for_update(self, internal_id, prescription_id):
        """
        Look for and return an epsRecord object,
        but with dataObject on self so that an update can be applied.
        """
        record_key = prescription_id_without_check_digit(prescription_id)
        item = self.client.get_item(internal_id, record_key, SortKey.RECORD.value)
        body = item.get(ProjectedAttribute.BODY.name)
        if body and not isinstance(body, dict):
            body = simplejson.loads(zlib.decompress(bytes(body)))

        self.dataObject = body
        return self._build_record_to_return(item, body)

    def get_prescription_record_data(self, internal_id, prescription_id, expect_exists=True):
        """
        Gets the prescription record from the data store and return just the data.
        :expect_exists defaulted to True. Thus we expect the key should already exist, if
        no matches are found DDB will throw a EpsDataStoreError (Missing Record).
        """
        record_key = prescription_id_without_check_digit(prescription_id)
        data_object = self.client.get_item(
            internal_id, record_key, SortKey.RECORD.value, expect_exists=expect_exists
        )

        if data_object is None:
            return None

        return data_object

    @timer
    def get_work_list(self, internal_id, message_id):
        """
        Look for and return a workList object.
        """
        item = self.client.get_item(
            internal_id, message_id, SortKey.WORK_LIST.value, expect_exists=False, expect_none=True
        )
        if item is None:
            return None

        if body := item.get(ProjectedAttribute.BODY.name):
            replace_decimals(body)
            self.decompress_work_list_xml(internal_id, body)
        return body

    @timer
    def compress_work_list_xml(self, _internal_id, work_list):
        """
        Compresses the XML contained in the work list, if present. Maintains original responseDetails on context.
        """
        work_list_deep_copy = copy.deepcopy(work_list)
        xml_bytes = work_list_deep_copy.get("responseDetails", {}).get("XML")

        if xml_bytes:
            if isinstance(xml_bytes, str):
                xml_bytes = xml_bytes.encode("utf-8")
            compressed_xml = zlib.compress(xml_bytes)
            work_list_deep_copy["responseDetails"]["XML"] = compressed_xml
        return work_list_deep_copy

    @timer
    def decompress_work_list_xml(self, _internal_id, body):
        """
        Decompresses the XML contained in the work list, if present.
        """
        compressed_xml = body.get("responseDetails", {}).get("XML")

        if compressed_xml:
            decompressed_xml = zlib.decompress(bytes(compressed_xml))
            body["responseDetails"]["XML"] = decompressed_xml

    def _fetch_next_sequence_number(self, internal_id, key, max_sequence_number, read_only=False):
        """
        Fetch the next sequence number from a given key.
        """
        item = self.client.get_item(
            internal_id, key, SortKey.SEQUENCE_NUMBER.value, expect_exists=False
        )
        is_update = True
        if not item:
            item = {
                Key.PK.name: key,
                Key.SK.name: SortKey.SEQUENCE_NUMBER.value,
                Attribute.SEQUENCE_NUMBER.name: 1,
            }
            is_update = False
        else:
            replace_decimals(item)
            sequence_number = item[Attribute.SEQUENCE_NUMBER.name]
            item[Attribute.SEQUENCE_NUMBER.name] = (
                sequence_number + 1 if sequence_number < max_sequence_number else 1
            )

        if not read_only:
            tries = 0
            while True:
                try:
                    self.client.insert_items(internal_id, [item], is_update, False)
                    break
                except EpsDataStoreError as e:
                    if e.error_topic == EpsDataStoreError.CONDITIONAL_UPDATE_FAILURE and tries < 25:
                        sequence_number = item[Attribute.SEQUENCE_NUMBER.name]
                        item[Attribute.SEQUENCE_NUMBER.name] = (
                            sequence_number + 1 if sequence_number < max_sequence_number else 1
                        )
                        tries += 1
                    else:
                        raise

        return item[Attribute.SEQUENCE_NUMBER.name]

    @timer
    def fetch_next_sequence_number(self, internal_id, max_sequence_number, read_only=False):
        """
        Fetch the next sequence number for a batch claim message.
        ONLY SINGLETON WORKER PROCESSES SHOULD CALL THIS - IT IS NOT AN ATOMIC ACTION.
        """
        return self._fetch_next_sequence_number(
            internal_id, self.CLAIM_SEQUENCE_NUMBER_KEY, max_sequence_number, read_only
        )

    @timer
    def fetch_next_sequence_number_nwssp(self, internal_id, max_sequence_number, read_only=False):
        """
        Fetch the next sequence number for a welsh batch claim message

        ONLY SINGLETON WORKER PROCESSES SHOULD CALL THIS - IT IS NOT AN ATOMIC ACTION
        """
        return self._fetch_next_sequence_number(
            internal_id, self.NWSSP_CLAIM_SEQUENCE_NUMBER_KEY, max_sequence_number, read_only
        )

    @timer
    def store_batch_claim(self, internal_id, batch_claim_original):
        """
        batchClaims need to be stored by their GUIDs with a claims sort key.
        They also require an index value for each claimID in the batch.
        A further index value is added with sequence number, for batch resend functionality.
        """
        batch_claim = copy.deepcopy(batch_claim_original)
        key = batch_claim["Batch GUID"]

        claim_id_index_terms = batch_claim["Claim ID List"]
        handle_time_index_term = batch_claim["Handle Time"]
        sequence_number = batch_claim["Sequence Number"]
        index_scn_value = f"{time_now_as_string()}|{sequence_number}"

        nwssp = "Nwssp Sequence Number" in batch_claim
        nwssp_sequence_number = batch_claim.get("Nwssp Sequence Number")
        expire_at = self.get_expire_at(timedelta(days=self.DEFAULT_EXPIRY_DAYS))

        indexes = {
            self.INDEX_CLAIMID: claim_id_index_terms,
            self.INDEX_CLAIMHANDLETIME: [handle_time_index_term],
            self.INDEX_CLAIM_SEQNUMBER: [sequence_number],
            self.INDEX_SCN: [index_scn_value],
        }
        if nwssp:
            indexes[self.INDEX_CLAIM_SEQNUMBER_NWSSP] = [nwssp_sequence_number]

        if batch_claim.get("Claim Metadata") and not batch_claim.get("Backward Incompatible"):
            batch_claim["Batch XML"] = ""

        item = {
            Key.PK.name: key,
            Key.SK.name: SortKey.CLAIM.value,
            ProjectedAttribute.BODY.name: batch_claim,
            ProjectedAttribute.EXPIRE_AT.name: expire_at,
            ProjectedAttribute.CLAIM_IDS.name: claim_id_index_terms,
            ProjectedAttribute.INDEXES.name: self.convert_index_keys_to_lower_case(indexes),
            Attribute.BATCH_CLAIM_ID.name: key,
        }
        if nwssp:
            item[Attribute.SEQUENCE_NUMBER_NWSSP.name] = nwssp_sequence_number
        else:
            item[Attribute.SEQUENCE_NUMBER.name] = sequence_number

        try:
            self.client.insert_items(internal_id, [item], True)
        except Exception:  # noqa: BLE001
            self.log_object.write_log("EPS0279", sys.exc_info(), {"internalID": key})
            return False
        return True

    def fetch_batch_claim(self, internal_id, batch_claim_id):
        """
        Retrieves the batch claim and returns the batch message for the calling application to handle.
        """
        item = self.client.get_item(
            internal_id, batch_claim_id, SortKey.CLAIM.value, expect_exists=False
        )
        if not item:
            return {}

        body = item.get(ProjectedAttribute.BODY.name)
        replace_decimals(body)
        batch_xml = body["Batch XML"]

        if not isinstance(batch_xml, str):
            try:
                body["Batch XML"] = bytes(batch_xml).decode("utf-8")
            except Exception as e:  # noqa: BLE001
                self.log_object.write_log(
                    "DDB0033", sys.exc_info(), {"error": str(e), "internalID": internal_id}
                )
                raise e

        return body

    @timer
    def delete_claim_notification(self, internal_id, claim_id):
        """
        Delete the claim notification document from the table, and return True if the deletion was successful.
        """
        try:
            self.client.delete_item(
                self.NOTIFICATION_PREFIX + str(claim_id), SortKey.DOCUMENT.value
            )
        except Exception:  # noqa: BLE001
            self.log_object.write_log(
                "EPS0289", sys.exc_info(), {"claimID": claim_id, "internalID": internal_id}
            )
            return False
        return True

    @timer
    def delete_document(self, internal_id, document_key, delete_notification=False):
        """
        Delete a document from the table. Return a boolean indicator of success.
        """
        if (
            str(document_key).lower().startswith(self.NOTIFICATION_PREFIX.lower())
            and not delete_notification
        ):
            return True

        item = self.client.get_item(
            internal_id, document_key, SortKey.DOCUMENT.value, expect_exists=False
        )

        if not item:
            self.log_object.write_log(
                "EPS0601b", None, {"documentRef": document_key, "internalID": internal_id}
            )
            return False

        self.log_object.write_log(
            "EPS0601", None, {"documentRef": document_key, "internalID": internal_id}
        )
        self.client.delete_item(document_key, SortKey.DOCUMENT.value)
        return True

    @timer
    def delete_record(self, internal_id, record_key):
        """
        Delete a record from the table.
        """
        self.log_object.write_log(
            "EPS0602", None, {"recordRef": record_key, "internalID": internal_id}
        )
        self.client.delete_item(record_key, SortKey.RECORD.value)

    @timer
    def return_pids_due_for_next_activity(
        self, _internal_id, next_activity_start, next_activity_end, shard=None
    ):
        """
        Returns all the epsRecord keys for prescriptions whose nextActivity is the same as that provided,
        and whose next activity date is within the date range provided.
        """
        return self.indexes.query_next_activity_date(next_activity_start, next_activity_end, shard)

    @timer
    def return_prescription_ids_for_nom_pharm(self, _internal_id, nominated_pharmacy_index_term):
        """
        Returns the epsRecord keys relating to the given nominated pharmacy term.
        """
        ods_code = nominated_pharmacy_index_term.split("_")[0]
        return self.indexes.query_nom_pharm_status(ods_code)

    @timer
    def return_claim_notification_ids_between_store_dates(self, internal_id, start_date, end_date):
        """
        Returns all the epsDocument keys for claim notification documents whose store dates are in the given window.
        """
        return self.indexes.query_claim_notification_store_time(internal_id, start_date, end_date)

    @timer
    def get_all_pids_by_nominated_pharmacy(self, _internal_id, nominated_pharmacy):
        """
        Run an index query to get all prescriptions for this nominated pharmacy.
        """
        return self.indexes.query_nom_pharm_status(nominated_pharmacy, True)

    @timer
    def check_item_exists(self, internal_id, pk, sk, expect_exists) -> bool:
        """
        Returns False as covered by condition expression.
        """
        item = self.client.get_item(internal_id, pk, sk, expect_exists)
        if item:
            return True
        return False

    def find_batch_claim_from_seq_number(self, sequence_number, nwssp=False):
        """
        Run a query against the sequence number index looking for the
        batch GUID (key) on the basis of sequence number.
        """
        return self.indexes.query_batch_claim_id_sequence_number(sequence_number, nwssp)

    @timer
    def return_pfp_pids_for_nhs_number(
        self, internal_id, nhs_number, start_date, end_date, limit
    ) -> Tuple[bool, List[str]]:
        """
        Returns a list of prescription IDs against a given NHS number for PfP.
        Also returns a boolean indicating if there are more results available.
        """
        key_conditions = Conditions.nhs_number_equals(
            nhs_number
        ) & Conditions.creation_datetime_range(start_date, end_date)

        filter_expressions = (
            Conditions.release_version_r2()
            & Conditions.next_activity_not_purged()
            & Conditions.record_type_not_erd()
        )

        desired_statuses = [
            PrescriptionStatus.TO_BE_DISPENSED,
            PrescriptionStatus.WITH_DISPENSER,
            PrescriptionStatus.WITH_DISPENSER_ACTIVE,
            PrescriptionStatus.DISPENSED,
            PrescriptionStatus.NOT_DISPENSED,
            PrescriptionStatus.CLAIMED,
            PrescriptionStatus.REPEAT_DISPENSE_FUTURE_INSTANCE,
        ]
        status_filters = [Conditions.status_equals(status) for status in desired_statuses]
        filter_expressions = filter_expressions & (
            functools.reduce(lambda a, b: a | b, status_filters)
        )

        query = DynamoDbQuery(
            self.client,
            self.log_object,
            internal_id,
            GSI.NHS_NUMBER_DATE_2,
            key_conditions,
            filter_expressions,
            limit,
            descending=True,
        )

        prescription_ids = list([item["pk"] for item in query])
        more_results = not query.complete
        return more_results, prescription_ids
