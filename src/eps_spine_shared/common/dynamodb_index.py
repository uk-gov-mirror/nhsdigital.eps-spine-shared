import re
from datetime import datetime, timedelta
from typing import Tuple

from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key as BotoKey

from eps_spine_shared.common import indexes
from eps_spine_shared.common.dynamodb_client import EpsDynamoDbClient
from eps_spine_shared.common.dynamodb_common import (
    GSI,
    NEXT_ACTIVITY_DATE_PARTITIONS,
    Attribute,
    Key,
    ProjectedAttribute,
    SortKey,
)
from eps_spine_shared.common.prescription.record import PrescriptionStatus
from eps_spine_shared.logger import EpsLogger
from eps_spine_shared.nhsfundamentals.time_utilities import TimeFormats


class EpsDynamoDbIndex:
    """
    The prescriptions message store specific DynamoDB client.
    """

    def __init__(self, log_object, client: EpsDynamoDbClient):
        """
        Instantiate the DynamoDB client.
        """
        self.log_object = EpsLogger(log_object)
        self.client = client

    def nhs_number_date(self, range_start, range_end, term_regex):
        """
        Query the nhsNumberDate index.
        """
        # POC - Use context in these methods, rather than range_start and range_end.
        nhs_number, start_date = range_start.split(indexes.SEPERATOR)
        end_date = range_end.split(indexes.SEPERATOR)[-1]

        return self.query_nhs_number_date(
            indexes.INDEX_NHSNUMBER_DATE, nhs_number, start_date, end_date, term_regex=term_regex
        )

    def nhs_number_presc_disp_date(self, range_start, range_end, term_regex):
        """
        Query the nhsNumberDate index, filtering on prescriber and dispenser.
        """
        nhs_number, prescriber_org, dispenser_org, start_date = range_start.split(indexes.SEPERATOR)
        end_date = range_end.split(indexes.SEPERATOR)[-1]
        filter_expression = Attr(Attribute.PRESCRIBER_ORG.name).eq(prescriber_org) & Attr(
            Attribute.DISPENSER_ORG.name
        ).contains(dispenser_org)

        return self.query_nhs_number_date(
            indexes.INDEX_NHSNUMBER_PRDSDATE,
            nhs_number,
            start_date,
            end_date,
            filter_expression,
            term_regex,
        )

    def nhs_number_presc_date(self, range_start, range_end, term_regex):
        """
        Query the nhsNumberDate index, filtering on prescriber.
        """
        nhs_number, prescriber_org, start_date = range_start.split(indexes.SEPERATOR)
        end_date = range_end.split(indexes.SEPERATOR)[-1]
        filter_expression = Attr(Attribute.PRESCRIBER_ORG.name).eq(prescriber_org)

        return self.query_nhs_number_date(
            indexes.INDEX_NHSNUMBER_PRDATE,
            nhs_number,
            start_date,
            end_date,
            filter_expression,
            term_regex,
        )

    def nhs_number_disp_date(self, range_start, range_end, term_regex):
        """
        Query the nhsNumberDate index, filtering on dispenser.
        """
        nhs_number, dispenser_org, start_date = range_start.split(indexes.SEPERATOR)
        end_date = range_end.split(indexes.SEPERATOR)[-1]
        filter_expression = Attr(Attribute.DISPENSER_ORG.name).contains(dispenser_org)

        return self.query_nhs_number_date(
            indexes.INDEX_NHSNUMBER_DSDATE,
            nhs_number,
            start_date,
            end_date,
            filter_expression,
            term_regex,
        )

    def presc_disp_date(self, range_start, range_end, term_regex):
        """
        Query the prescriberDate index, filtering on dispenser.
        """
        prescriber_org, dispenser_org, start_date = range_start.split(indexes.SEPERATOR)
        end_date = range_end.split(indexes.SEPERATOR)[-1]
        filter_expression = Attr(Attribute.DISPENSER_ORG.name).contains(dispenser_org)

        return self.query_prescriber_date(
            indexes.INDEX_PRESCRIBER_DSDATE,
            prescriber_org,
            start_date,
            end_date,
            filter_expression,
            term_regex,
        )

    def presc_date(self, range_start, range_end, term_regex):
        """
        Query the prescriberDate index.
        """
        prescriber_org, start_date = range_start.split(indexes.SEPERATOR)
        end_date = range_end.split(indexes.SEPERATOR)[-1]

        return self.query_prescriber_date(
            indexes.INDEX_PRESCRIBER_DATE,
            prescriber_org,
            start_date,
            end_date,
            term_regex=term_regex,
        )

    def disp_date(self, range_start, range_end, term_regex):
        """
        Query the dispenserDate index.
        """
        dispenser_org, start_date = range_start.split(indexes.SEPERATOR)
        end_date = range_end.split(indexes.SEPERATOR)[-1]

        return self.query_dispenser_date(
            indexes.INDEX_DISPENSER_DATE, dispenser_org, start_date, end_date, term_regex=term_regex
        )

    def nom_pharm_status(self, range_start, _, term_regex):
        """
        Query the nomPharmStatus index for terms.
        """
        ods_code, status = range_start.split("_")

        return self.query_nom_pharm_status_terms(
            indexes.INDEX_NOMPHARM, ods_code, status, term_regex=term_regex
        )

    def build_terms(self, items, index_name, term_regex):
        """
        Build terms from items returned by the index query.
        """
        terms = []
        for item in items:
            index_terms = item.get(ProjectedAttribute.INDEXES.name, {}).get(index_name.lower())
            if not index_terms:
                continue
            [
                terms.append((index_term, item[Key.PK.name]))
                for index_term in index_terms
                if ((not term_regex) or re.search(term_regex, index_term))
            ]
        return terms

    def pad_or_trim_date(self, date):
        """
        Ensure the date length is fourteen characters, if present.
        """
        if not date:
            return None

        if len(date) >= 14:
            return date[:14]

        while len(date) < 14:
            date = date + "0"
        return date

    def query_nhs_number_date(
        self,
        index,
        nhs_number,
        start_date=None,
        end_date=None,
        filter_expression=None,
        term_regex=None,
    ):
        """
        Return the epsRecord terms which match the supplied range and regex for the nhsNumberDate index.
        """
        start_date, end_date = [self.pad_or_trim_date(date) for date in [start_date, end_date]]

        pk_expression = BotoKey(Attribute.NHS_NUMBER.name).eq(nhs_number)
        sk_expression = None
        if start_date and end_date:
            [valid, sk_expression] = self._get_valid_range_condition(
                Attribute.CREATION_DATETIME.name, start_date, end_date
            )

            if not valid:
                return []
        elif start_date:
            sk_expression = BotoKey(Attribute.CREATION_DATETIME.name).gte(start_date)
        elif end_date:
            sk_expression = BotoKey(Attribute.CREATION_DATETIME.name).lte(end_date)

        key_condition_expression = (
            pk_expression if not sk_expression else pk_expression & sk_expression
        )
        items = self.client.query_index(
            GSI.NHS_NUMBER_DATE.name, key_condition_expression, filter_expression
        )

        return self.build_terms(items, index, term_regex)

    def query_prescriber_date(
        self, index, prescriber_org, start_date, end_date, filter_expression=None, term_regex=None
    ):
        """
        Return the epsRecord terms which match the supplied range and regex for the prescriberDate index.
        """
        start_date, end_date = [self.pad_or_trim_date(date) for date in [start_date, end_date]]

        pk_expression = BotoKey(Attribute.PRESCRIBER_ORG.name).eq(prescriber_org)
        [valid, sk_expression] = self._get_valid_range_condition(
            Attribute.CREATION_DATETIME.name, start_date, end_date
        )

        if not valid:
            return []

        items = self.client.query_index(
            GSI.PRESCRIBER_DATE.name, pk_expression & sk_expression, filter_expression
        )

        return self.build_terms(items, index, term_regex)

    def query_dispenser_date(
        self, index, dispenser_org, start_date, end_date, filter_expression=None, term_regex=None
    ):
        """
        Return the epsRecord terms which match the supplied range and regex for the dispenserDate index.
        """
        start_date, end_date = [self.pad_or_trim_date(date) for date in [start_date, end_date]]

        pk_expression = BotoKey(Attribute.DISPENSER_ORG.name).eq(dispenser_org)
        [valid, sk_expression] = self._get_valid_range_condition(
            Attribute.CREATION_DATETIME.name, start_date, end_date
        )

        if not valid:
            return []

        items = self.client.query_index(
            GSI.DISPENSER_DATE.name, pk_expression & sk_expression, filter_expression
        )

        return self.build_terms(items, index, term_regex)

    def query_nom_pharm_status(self, ods_code, all_statuses=False, limit=None):
        """
        Return the nomPharmStatus prescription keys which match the supplied ODS code.
        Query using the nominatedPharmacyStatus index. If all_statuses is False, only return prescriptions
        with status TO_BE_DISPENSED (0001).
        """
        key_condition_expression = BotoKey(Attribute.NOMINATED_PHARMACY.name).eq(ods_code)

        is_ready_condition = (
            BotoKey(Attribute.IS_READY.name).eq(int(True))
            if not all_statuses
            else BotoKey(Attribute.IS_READY.name).between(0, 1)
        )
        key_condition_expression = key_condition_expression & is_ready_condition

        items = self.client.query_index_with_limit(
            GSI.NOMINATED_PHARMACY_STATUS.name, key_condition_expression, None, limit
        )

        return [item[Key.PK.name] for item in items]

    def query_nom_pharm_status_terms(self, index, ods_code, status, term_regex=None):
        """
        Return the nomPharmStatus terms which match the supplied ODS code and status.
        Query using the nominatedPharmacyStatus index, with is_ready derived from the status.
        """
        is_ready = status == PrescriptionStatus.TO_BE_DISPENSED

        key_condition_expression = BotoKey(Attribute.NOMINATED_PHARMACY.name).eq(
            ods_code
        ) & BotoKey(Attribute.IS_READY.name).eq(int(is_ready))

        filter_expression = Attr(ProjectedAttribute.STATUS.name).contains(status)

        items = self.client.query_index(
            GSI.NOMINATED_PHARMACY_STATUS.name, key_condition_expression, filter_expression
        )

        return self.build_terms(items, index, term_regex)

    def query_claim_id(self, claim_id):
        """
        Search for an existing batch claim containing the given claim_id.
        """
        key_condition_expression = BotoKey(Key.SK.name).eq(SortKey.CLAIM.value)
        filter_expression = Attr(ProjectedAttribute.CLAIM_IDS.name).contains(claim_id)

        items = self.client.query_index(
            GSI.CLAIM_ID.name, key_condition_expression, filter_expression
        )

        return [item[Key.PK.name] for item in items]

    def query_next_activity_date(self, range_start, range_end, shard=None):
        """
        Yields the epsRecord keys which match the supplied nextActivity and date range for the nextActivity index.

        nextActivity is suffix-sharded with NEXT_ACTIVITY_DATE_PARTITIONS to avoid hot partitions on ddb.
        This means NEXT_ACTIVITY_DATE_PARITIONS + 1 queries are performed, one for each partition
        and one for the non-partitioned nextActivityDate index.
        """
        next_activity, start_date = range_start.split("_")
        end_date = range_end.split("_")[-1]

        [valid, sk_expression] = self._get_valid_range_condition(
            Attribute.NEXT_ACTIVITY_DATE.name, start_date, end_date
        )

        if not valid:
            return []

        if shard or shard == "":
            yield from self._query_next_activity_date_shard(next_activity, sk_expression, shard)
            return

        shards = [None] + list(range(1, NEXT_ACTIVITY_DATE_PARTITIONS + 1))

        for shard in shards:
            yield from self._query_next_activity_date_shard(next_activity, sk_expression, shard)

    def _query_next_activity_date_shard(self, next_activity, sk_expression, shard):
        """
        Return a generator for the epsRecord keys which match the supplied nextActivity and date range
        for a given pk shard.
        """
        expected_next_activity = (
            next_activity if shard is None or shard == "" else f"{next_activity}.{shard}"
        )
        pk_expression = BotoKey(Attribute.NEXT_ACTIVITY.name).eq(expected_next_activity)

        return self.client.query_index_yield(
            GSI.NEXT_ACTIVITY_DATE.name, pk_expression & sk_expression
        )

    def _get_date_range_for_query(self, start_datetime_str, end_datetime_str):
        """
        Get days included in the given range. For use in claimNotificationStoreTime index query.
        """
        start_datetime = datetime.strptime(
            start_datetime_str, TimeFormats.STANDARD_DATE_TIME_FORMAT
        )
        end_datetime = datetime.strptime(end_datetime_str, TimeFormats.STANDARD_DATE_TIME_FORMAT)

        return [
            (start_datetime + timedelta(days=d)).strftime(TimeFormats.STANDARD_DATE_FORMAT)
            for d in range((end_datetime.date() - start_datetime.date()).days + 1)
        ]

    def query_claim_notification_store_time(
        self, internal_id, start_datetime_str, end_datetime_str
    ):
        """
        Search for claim notification documents whose store times fall within the specified window.
        """
        [valid, sk_expression] = self._get_valid_range_condition(
            Attribute.STORE_TIME.name, start_datetime_str, end_datetime_str
        )

        if not valid:
            return []

        dates = self._get_date_range_for_query(start_datetime_str, end_datetime_str)
        generators = []

        for date in dates:
            pk_expression = BotoKey(Attribute.CLAIM_NOTIFICATION_STORE_DATE.name).eq(date)
            self.log_object.write_log(
                "DDB0013",
                None,
                {
                    "date": date,
                    "startTime": start_datetime_str,
                    "endTime": end_datetime_str,
                    "internalID": internal_id,
                },
            )
            generators.append(
                self.client.query_index_yield(
                    GSI.CLAIM_NOTIFICATION_STORE_TIME.name, pk_expression & sk_expression, None
                )
            )

        for generator in generators:
            yield from generator

    def _get_valid_range_condition(self, key, start, end) -> Tuple[bool, object]:
        """
        Returns a range condition if the start < end
        """
        if end == start:
            return True, BotoKey(key).eq(start)
        if end < start:
            return False, None
        else:
            return True, BotoKey(key).between(start, end)

    def query_batch_claim_id_sequence_number(self, sequence_number, nwssp=False):
        """
        Query the claimIdSequenceNumber index for batch claim IDs based on sequence number.
        """
        index_name = (
            GSI.CLAIM_ID_SEQUENCE_NUMBER_NWSSP.name if nwssp else GSI.CLAIM_ID_SEQUENCE_NUMBER.name
        )
        key_name = Attribute.SEQUENCE_NUMBER_NWSSP.name if nwssp else Attribute.SEQUENCE_NUMBER.name

        key_condition_expression = BotoKey(key_name).eq(sequence_number)

        items = self.client.query_index(index_name, key_condition_expression, None)

        return [
            item[Key.PK.name]
            for item in items
            if item[Key.PK.name] not in ["claimSequenceNumber", "claimSequenceNumberNwssp"]
        ]
