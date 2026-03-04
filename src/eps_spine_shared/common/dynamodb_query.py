"""
A class to handle DynamoDB queries, such as pagination and completion flagging.
"""

from boto3.dynamodb.conditions import Attr, ConditionExpressionBuilder, Key

import eps_spine_shared.common.prescription.fields as fields
from eps_spine_shared.common.dynamodb_client import EpsDynamoDbClient
from eps_spine_shared.common.dynamodb_common import GSI, Attribute, ProjectedAttribute, RecordType
from eps_spine_shared.logger import EpsLogger


class DynamoDbQuery:
    """
    Abstraction of a dynamoDB query, handling pagination and flagging completion.
    To be used either as a generator, or calling asList.
    self.complete indicates
    """

    def __init__(
        self,
        client: EpsDynamoDbClient,
        logger: EpsLogger,
        internal_id: str,
        index: GSI,
        key_condition_expression,
        filter_expression=None,
        limit: int = None,
        descending: bool = False,
    ) -> None:
        self._client = client
        self._logger = logger
        self._internal_id = internal_id

        condition_builder = ConditionExpressionBuilder()
        key_condition_expression, condition_attributes, condition_values = (
            condition_builder.build_expression(key_condition_expression, True)
        )
        if filter_expression:
            filter_expression, filter_attributes, filter_values = (
                condition_builder.build_expression(filter_expression, False)
            )
            condition_attributes.update(filter_attributes)
            condition_values.update(filter_values)

        query_args = {
            "TableName": client.table_name,
            "IndexName": index.name,
            "KeyConditionExpression": key_condition_expression,
            "ExpressionAttributeNames": condition_attributes,
            "ExpressionAttributeValues": client.serialise_for_dynamodb(condition_values),
        }

        if filter_expression:
            query_args["FilterExpression"] = filter_expression
        if limit:
            query_args["PaginationConfig"] = {"MaxItems": limit, "PageSize": limit}
        if descending:
            query_args["ScanIndexForward"] = False

        self._pages = iter(self._client.client.get_paginator("query").paginate(**query_args))
        self._item_iterator = self._items()
        self._is_last_page = False
        self.complete = False

    def _items(self):
        """
        Yields individual deserialised items from the query, handling pagination.
        """
        for page in self._pages:
            self._logger.write_log(
                "DDB0050",
                None,
                {
                    "itemCount": len(page["Items"]),
                    "hasLastEvaluatedKey": "LastEvaluatedKey" in page,
                    "internalID": self._internal_id,
                },
            )
            items = [self._client.deserialise_from_dynamodb(item) for item in page["Items"]]
            self._is_last_page = "LastEvaluatedKey" not in page
            for item in items:
                yield item

    def __iter__(self):
        """
        Start iteration
        """
        return self

    def __next__(self):
        """
        Return the next item. If no more items, set complete flag if on last page.
        """
        try:
            return next(self._item_iterator)
        except StopIteration:
            if self._is_last_page:
                self.complete = True
            raise


class Conditions:
    """
    Wrapper for condition expressions
    """

    @staticmethod
    def nhs_number_equals(nhs_number: str):
        """
        Condition expression for nhsNumber equality
        """

        return Key(Attribute.NHS_NUMBER.name).eq(nhs_number)

    @staticmethod
    def creation_datetime_range(start: str, end: str = None):
        """
        Condition expression for creationDatetime between start and end
        """
        if not end:
            return Key(Attribute.CREATION_DATETIME.name).gte(start)

        return Key(Attribute.CREATION_DATETIME.name).between(start, end)

    @staticmethod
    def release_version_r2():
        """
        Condition expression for releaseVersion (sharded) equals R2
        """

        return Attr(ProjectedAttribute.RELEASE_VERSION.value).contains(fields.R2_VERSION)

    @staticmethod
    def next_activity_not_purged():
        """
        Condition expression for nextActivity not equal to PURGED
        """

        return ~Attr(Attribute.NEXT_ACTIVITY.name).contains(fields.NEXTACTIVITY_PURGE)

    @staticmethod
    def record_type_not_erd():
        """
        Condition expression for recordType is not repeat dispensing
        """

        return Attr(ProjectedAttribute.RECORD_TYPE.value).ne(RecordType.REPEAT_DISPENSE.value)

    @staticmethod
    def status_equals(status: str):
        """
        Condition expression for status equality
        """

        return Attr(ProjectedAttribute.STATUS.value).eq(status)
