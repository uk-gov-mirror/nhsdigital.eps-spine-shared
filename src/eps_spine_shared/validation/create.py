from datetime import datetime

from dateutil.relativedelta import relativedelta

from eps_spine_shared.common.prescription.fields import DEFAULT_DAYSSUPPLY
from eps_spine_shared.errors import EpsValidationError
from eps_spine_shared.logger import EpsLogger
from eps_spine_shared.nhsfundamentals.time_utilities import TimeFormats
from eps_spine_shared.validation import constants, message_vocab
from eps_spine_shared.validation.common import (
    check_hl7_event_id,
    check_nhs_number,
    check_nominated_performer,
    check_organisation_and_roles,
    check_prescription_id,
    check_standard_date,
    check_standard_date_time,
)


def check_hcpl_org(context):
    """
    This is an org only found in EPS2 prescriber details
    """
    if not constants.REGEX_ALPHANUMERIC8.match(context.msgOutput[message_vocab.HCPLORG]):
        raise EpsValidationError(message_vocab.HCPLORG + " has invalid format")


def check_signed_time(context, internal_id, log_object: EpsLogger):
    """
    Signed time must be a valid date/time
    """
    check_standard_date_time(context, message_vocab.SIGNED_TIME, internal_id, log_object)


def check_days_supply(context):
    """
    daysSupply is how many days each prescription instance should cover - supports
    the calculation of nominated download dates
    """
    if not context.msgOutput.get(message_vocab.DAYS_SUPPLY):
        context.msgOutput[message_vocab.DAYS_SUPPLY] = DEFAULT_DAYSSUPPLY
    else:
        if not constants.REGEX_INTEGER12.match(context.msgOutput[message_vocab.DAYS_SUPPLY]):
            raise EpsValidationError("daysSupply is not an integer")
        days_supply = int(context.msgOutput[message_vocab.DAYS_SUPPLY])
        if days_supply < 0:
            raise EpsValidationError("daysSupply must be a non-zero integer")
        if days_supply > constants.MAX_DAYSSUPPLY:
            raise EpsValidationError("daysSupply cannot exceed " + str(constants.MAX_DAYSSUPPLY))
        # This will need to be an integer when used in the interaction worker
        context.msgOutput[message_vocab.DAYS_SUPPLY] = days_supply

    context.outputFields.add(message_vocab.DAYS_SUPPLY)


def check_repeat_dispense_window(context, handle_time: datetime):
    """
    The overall time to cover the dispense of all repeated instances

    Return immediately if not a repeat dispense, or if a repeat dispense and values
    are missing
    """
    context.outputFields.add(message_vocab.DAYS_SUPPLY_LOW)
    context.outputFields.add(message_vocab.DAYS_SUPPLY_HIGH)

    max_supply_date = handle_time + relativedelta(months=+constants.MAX_FUTURESUPPLYMONTHS)
    max_supply_date_string = max_supply_date.strftime(TimeFormats.STANDARD_DATE_FORMAT)

    if context.msgOutput[message_vocab.TREATMENTTYPE] != constants.STATUS_REPEAT_DISP:
        context.msgOutput[message_vocab.DAYS_SUPPLY_LOW] = handle_time.strftime(
            TimeFormats.STANDARD_DATE_FORMAT
        )
        context.msgOutput[message_vocab.DAYS_SUPPLY_HIGH] = max_supply_date_string
        return

    if not (
        context.msgOutput.get(message_vocab.DAYS_SUPPLY_LOW)
        and context.msgOutput.get(message_vocab.DAYS_SUPPLY_HIGH)
    ):
        supp_info = "daysSupply effective time not provided but "
        supp_info += "prescription treatment type is repeat"
        raise EpsValidationError(supp_info)

    check_standard_date(context, message_vocab.DAYS_SUPPLY_HIGH)
    check_standard_date(context, message_vocab.DAYS_SUPPLY_LOW)

    if context.msgOutput[message_vocab.DAYS_SUPPLY_HIGH] > max_supply_date_string:
        supp_info = "daysSupplyValidHigh is more than "
        supp_info += str(constants.MAX_FUTURESUPPLYMONTHS) + " months beyond current day"
        raise EpsValidationError(supp_info)
    if context.msgOutput[message_vocab.DAYS_SUPPLY_HIGH] < handle_time.strftime(
        TimeFormats.STANDARD_DATE_FORMAT
    ):
        raise EpsValidationError("daysSupplyValidHigh is in the past")
    if (
        context.msgOutput[message_vocab.DAYS_SUPPLY_LOW]
        > context.msgOutput[message_vocab.DAYS_SUPPLY_HIGH]
    ):
        raise EpsValidationError("daysSupplyValid low is after daysSupplyValidHigh")


def check_prescriber_details(context, internal_id, log_object: EpsLogger):
    """
    Validate prescriber details (not required beyond validation).
    """
    if not constants.REGEX_ALPHANUMERIC8.match(context.msgOutput[message_vocab.AGENT_PERSON]):
        log_object.write_log(
            "EPS0323a",
            None,
            {
                "internalID": internal_id,
                "prescribingGpCode": context.msgOutput[message_vocab.AGENT_PERSON],
            },
        )
        if not constants.REGEX_ALPHANUMERIC12.match(context.msgOutput[message_vocab.AGENT_PERSON]):
            raise EpsValidationError(message_vocab.AGENT_PERSON + " has invalid format")

    context.outputFields.add(message_vocab.AGENT_PERSON)


def check_patient_name(context):
    """
    Adds patient name to the context outputFields
    """
    context.outputFields.add(message_vocab.PREFIX)
    context.outputFields.add(message_vocab.SUFFIX)
    context.outputFields.add(message_vocab.GIVEN)
    context.outputFields.add(message_vocab.FAMILY)


def check_prescription_treatment_type(context):
    """
    Validate treatment type
    """
    if context.msgOutput[message_vocab.TREATMENTTYPE] not in constants.TREATMENT_TYPELIST:
        supp_info = message_vocab.TREATMENTTYPE + " is not of expected type"
        raise EpsValidationError(supp_info)
    context.outputFields.add(message_vocab.TREATMENTTYPE)


def check_prescription_type(context, internal_id, log_object: EpsLogger):
    """
    Validate the prescriptionType
    """
    presc_type = context.msgOutput.get(message_vocab.PRESCTYPE)
    if presc_type not in constants.PRESC_TYPELIST:
        log_object.write_log("EPS0619", None, {"internalID": internal_id, "prescType": presc_type})
        context.msgOutput[message_vocab.PRESCTYPE] = "NotProvided"

    context.outputFields.add(message_vocab.PRESCTYPE)


def check_repeat_dispense_instances(context, internal_id, log_object: EpsLogger):
    """
    Repeat dispense instances is an integer range found within repeat dispense
    prescriptions to articulate the number of instances.  Low must be 1!
    """
    if not (
        context.msgOutput.get(message_vocab.REPEATLOW)
        and context.msgOutput.get(message_vocab.REPEATHIGH)
    ):
        if context.msgOutput[message_vocab.TREATMENTTYPE] == constants.STATUS_ACUTE:
            return
        supp_info = message_vocab.REPEATHIGH + " and " + message_vocab.REPEATLOW
        supp_info += " values must both be provided if not Acute prescription"
        raise EpsValidationError(supp_info)

    if not constants.REGEX_INTEGER12.match(context.msgOutput[message_vocab.REPEATHIGH]):
        supp_info = message_vocab.REPEATHIGH + " is not an integer"
        raise EpsValidationError(supp_info)
    if not constants.REGEX_INTEGER12.match(context.msgOutput[message_vocab.REPEATLOW]):
        supp_info = message_vocab.REPEATLOW + " is not an integer"
        raise EpsValidationError(supp_info)

    context.msgOutput[message_vocab.REPEATLOW] = int(context.msgOutput[message_vocab.REPEATLOW])
    context.msgOutput[message_vocab.REPEATHIGH] = int(context.msgOutput[message_vocab.REPEATHIGH])
    if context.msgOutput[message_vocab.REPEATLOW] != 1:
        supp_info = message_vocab.REPEATLOW + " must be 1"
        raise EpsValidationError(supp_info)
    if context.msgOutput[message_vocab.REPEATHIGH] > constants.MAX_PRESCRIPTIONREPEATS:
        supp_info = message_vocab.REPEATHIGH + " must not be over configured "
        supp_info += "maximum of " + str(constants.MAX_PRESCRIPTIONREPEATS)
        raise EpsValidationError(supp_info)
    if context.msgOutput[message_vocab.REPEATHIGH] < context.msgOutput[message_vocab.REPEATLOW]:
        supp_info = message_vocab.REPEATLOW + " is greater than " + message_vocab.REPEATHIGH
        raise EpsValidationError(supp_info)
    if (
        context.msgOutput[message_vocab.REPEATHIGH] != 1
        and context.msgOutput[message_vocab.TREATMENTTYPE] == constants.STATUS_REPEAT
    ):
        log_object.write_log(
            "EPS0509",
            None,
            {
                "internalID": internal_id,
                "target": "Prescription",
                "maxRepeats": context.msgOutput[message_vocab.REPEATHIGH],
            },
        )

    context.outputFields.add(message_vocab.REPEATLOW)
    context.outputFields.add(message_vocab.REPEATHIGH)


def check_birth_date(context, handle_time: datetime):
    """
    Birth date must be a valid date, and must not be in the future
    """
    check_standard_date(context, message_vocab.BIRTHTIME)
    now_as_string = handle_time.strftime(TimeFormats.STANDARD_DATE_TIME_FORMAT)
    if context.msgOutput[message_vocab.BIRTHTIME] > now_as_string:
        supp_info = message_vocab.BIRTHTIME + " is in the future"
        raise EpsValidationError(supp_info)


def validate_line_items(context, internal_id, log_object: EpsLogger):
    """
    Validating line items - there are up to 32 line items

    Each line item has a GUID (ID)
    Each line item may have a repeatLow and a repeatHigh (not one but not the other)
    Result needs to be placed onto lineItems dictionary

    Fields may be presented as empty when fields are not present - so these need to be
    treated correctly as not present
    - To manage this, delete any keys from the dictionary if the result is None or ''
    """
    max_repeat_high = 1
    context.msgOutput[message_vocab.LINEITEMS] = []

    for line_number in range(constants.MAX_LINEITEMS):
        line_item = line_number + 1
        line_dict = {}

        line_item_id = message_vocab.LINEITEM_PX + str(line_item) + message_vocab.LINEITEM_SX_ID
        if context.msgOutput.get(line_item_id):
            line_dict[message_vocab.LINEITEM_DT_ORDER] = line_item
            line_dict[message_vocab.LINEITEM_DT_ID] = context.msgOutput[line_item_id]
            line_dict[message_vocab.LINEITEM_DT_STATUS] = "0007"
        else:
            break

        line_item_repeat_high = (
            message_vocab.LINEITEM_PX + str(line_item) + message_vocab.LINEITEM_SX_REPEATHIGH
        )
        line_item_repeat_low = (
            message_vocab.LINEITEM_PX + str(line_item) + message_vocab.LINEITEM_SX_REPEATLOW
        )
        if context.msgOutput.get(line_item_repeat_high):
            line_dict[message_vocab.LINEITEM_DT_MAXREPEATS] = context.msgOutput[
                line_item_repeat_high
            ]
        if context.msgOutput.get(line_item_repeat_low):
            line_dict[message_vocab.LINEITEM_DT_CURRINSTANCE] = context.msgOutput[
                line_item_repeat_low
            ]

        max_repeat_high = validate_line_item(
            context, line_item, line_dict, max_repeat_high, log_object, internal_id
        )
        context.msgOutput[message_vocab.LINEITEMS].append(line_dict)

    if len(context.msgOutput[message_vocab.LINEITEMS]) < 1:
        supp_info = "No valid line items found"
        raise EpsValidationError(supp_info)

    max_line_item = message_vocab.LINEITEM_PX
    max_line_item += str(constants.MAX_LINEITEMS + 1)
    max_line_item += message_vocab.LINEITEM_SX_ID
    if max_line_item in context.msgOutput:
        supp_info = "lineItems over expected max count of " + str(constants.MAX_LINEITEMS)
        raise EpsValidationError(supp_info)

    if (
        message_vocab.REPEATHIGH in context.msgOutput
        and max_repeat_high < context.msgOutput[message_vocab.REPEATHIGH]
    ):
        supp_info = "Prescription repeat count must not be greater than all "
        supp_info += "Line Item repeat counts"
        raise EpsValidationError(supp_info)

    context.outputFields.add(message_vocab.LINEITEMS)


def validate_line_item(
    context,
    line_item,
    line_dict,
    max_repeat_high,
    internal_id,
    log_object: EpsLogger,
):
    """
    Ensure that the GUID is valid
    Check for an appropriate combination of maxRepeats and currentInstance
    Check for an appropriate value of maxRepeats
    Check for an appropriate value for currentInstance
    """
    if not constants.REGEX_GUID.match(line_dict[message_vocab.LINEITEM_DT_ID]):
        supp_info = line_dict[message_vocab.LINEITEM_DT_ID]
        supp_info += " is not a valid GUID format"
        raise EpsValidationError(supp_info)

    if (
        message_vocab.LINEITEM_DT_MAXREPEATS not in line_dict
        and message_vocab.LINEITEM_DT_CURRINSTANCE not in line_dict
        and context.msgOutput[message_vocab.TREATMENTTYPE] == constants.STATUS_ACUTE
    ):
        return max_repeat_high

    check_for_invalid_line_item_repeat_combinations(context, line_dict, line_item)

    if not constants.REGEX_INTEGER12.match(line_dict[message_vocab.LINEITEM_DT_MAXREPEATS]):
        raise EpsValidationError(f"repeat.High for line item {line_item} is not an integer")

    repeat_high = int(line_dict[message_vocab.LINEITEM_DT_MAXREPEATS])
    if repeat_high < 1:
        raise EpsValidationError(f"repeat.High for line item {line_item} must be greater than zero")
    if repeat_high > int(context.msgOutput[message_vocab.REPEATHIGH]):
        raise EpsValidationError(
            f"repeat.High of {repeat_high} for line item {line_item} must not be greater than "
            f"{message_vocab.REPEATHIGH} of {context.msgOutput[message_vocab.REPEATHIGH]}"
        )
    if (
        repeat_high != 1
        and context.msgOutput[message_vocab.TREATMENTTYPE] == constants.STATUS_REPEAT
    ):
        log_object.write_log(
            "EPS0509",
            None,
            {
                "internalID": internal_id,
                "target": str(line_item),
                "maxRepeats": repeat_high,
            },
        )
    if not constants.REGEX_INTEGER12.match(line_dict[message_vocab.LINEITEM_DT_CURRINSTANCE]):
        raise EpsValidationError(f"repeat.Low for line item {line_item} is not an integer")
    repeat_low = int(line_dict[message_vocab.LINEITEM_DT_CURRINSTANCE])
    if repeat_low != 1:
        raise EpsValidationError(f"repeat.Low for line item {line_item} is not set to 1")

    max_repeat_high = max(max_repeat_high, repeat_high)
    return max_repeat_high


def check_for_invalid_line_item_repeat_combinations(context, line_dict, line_item):
    """
    If not an acute prescription - check the combination of repeat and instance
    information is valid
    """
    if (
        message_vocab.LINEITEM_DT_MAXREPEATS not in line_dict
        and message_vocab.LINEITEM_DT_CURRINSTANCE not in line_dict
    ):
        raise EpsValidationError(
            f"repeat.High and repeat.Low values must both be provided "
            f"for lineItem {line_item} if not acute prescription"
        )
    elif message_vocab.LINEITEM_DT_MAXREPEATS not in line_dict:
        raise EpsValidationError(
            f"repeat.Low provided but not repeat.High for line item {line_item}"
        )
    elif message_vocab.LINEITEM_DT_CURRINSTANCE not in line_dict:
        raise EpsValidationError(
            f"repeat.High provided but not repeat.Low for line item {line_item}"
        )
    elif not context.msgOutput.get(message_vocab.REPEATHIGH):
        raise EpsValidationError(
            f"Line item {line_item} repeat value provided for non-repeat prescription"
        )


def run_validations(validation_context, handle_time: datetime, internal_id, log_object: EpsLogger):
    """
    Validate elements extracted from the inbound message
    """
    check_prescriber_details(validation_context, internal_id, log_object)
    check_organisation_and_roles(validation_context, internal_id, log_object)
    check_nhs_number(validation_context)
    check_patient_name(validation_context)
    check_standard_date_time(validation_context, message_vocab.PRESCTIME, internal_id, log_object)
    check_prescription_treatment_type(validation_context)
    check_prescription_type(validation_context, internal_id, log_object)
    check_repeat_dispense_instances(validation_context, internal_id, log_object)
    check_birth_date(validation_context, handle_time)
    check_hl7_event_id(validation_context)
    validate_line_items(validation_context, internal_id, log_object)
    validation_context.outputFields.add(message_vocab.PRESCSTATUS)
    validation_context.msgOutput[message_vocab.PRESCSTATUS] = "NOT_SET_YET"

    check_hcpl_org(validation_context)
    check_nominated_performer(validation_context)
    check_prescription_id(validation_context, internal_id, log_object)
    check_signed_time(validation_context, internal_id, log_object)
    check_days_supply(validation_context)
    check_repeat_dispense_window(validation_context, handle_time)
    validation_context.outputFields.add(message_vocab.SIGNED_INFO)
    validation_context.outputFields.add(message_vocab.DIGEST_METHOD)
