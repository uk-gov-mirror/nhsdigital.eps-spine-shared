import datetime

from eps_spine_shared.common import checksum_util
from eps_spine_shared.errors import EpsValidationError
from eps_spine_shared.logger import EpsLogger
from eps_spine_shared.nhsfundamentals.mim_rules import is_nhs_number_valid
from eps_spine_shared.nhsfundamentals.time_utilities import (
    TimeFormats,
    convert_international_time,
)
from eps_spine_shared.validation import message_vocab
from eps_spine_shared.validation.constants import (
    PERFORMER_TYPELIST,
    REGEX_ALPHANUMERIC8,
    REGEX_GUID,
    REGEX_NUMERIC15,
    REGEX_PRESCRID,
    REGEX_ROLECODE,
)


def check_nominated_performer(context):
    """
    If there is nominated performer (i.e. pharmacy) information - then the format
    needs to be validated
    """
    if context.msgOutput.get(message_vocab.NOMPERFORMER) and context.msgOutput.get(
        message_vocab.NOMPERFORMER_TYPE
    ):
        if not REGEX_ALPHANUMERIC8.match(context.msgOutput.get(message_vocab.NOMPERFORMER)):
            raise EpsValidationError("nominatedPerformer has invalid format")
        if context.msgOutput.get(message_vocab.NOMPERFORMER_TYPE) not in PERFORMER_TYPELIST:
            raise EpsValidationError("nominatedPerformer has invalid type")

    if context.msgOutput.get(message_vocab.NOMPERFORMER) == "":
        raise EpsValidationError("nominatedPerformer is present but empty")

    context.outputFields.add(message_vocab.NOMPERFORMER)
    context.outputFields.add(message_vocab.NOMPERFORMER_TYPE)


def check_prescription_id(context, internal_id, log_object: EpsLogger):
    """
    Check the format of a prescription ID and that it has the correct checksum
    """
    if not REGEX_PRESCRID.match(context.msgOutput[message_vocab.PRESCID]):
        raise EpsValidationError(message_vocab.PRESCID + " has invalid format")

    valid = checksum_util.check_checksum(
        context.msgOutput[message_vocab.PRESCID], internal_id, log_object
    )
    if not valid:
        raise EpsValidationError(message_vocab.PRESCID + " has invalid checksum")

    context.outputFields.add(message_vocab.PRESCID)


def check_organisation_and_roles(context, internal_id, log_object: EpsLogger):
    """
    Check the organisation and role information is of the correct format
    Requires:
        agent_organization
        agent_role_profile_code_id
        agent_sds_role
    """
    if not REGEX_ALPHANUMERIC8.match(context.msgOutput[message_vocab.AGENTORG]):
        raise EpsValidationError(message_vocab.AGENTORG + " has invalid format")
    if not REGEX_NUMERIC15.match(context.msgOutput[message_vocab.ROLEPROFILE]):
        log_object.write_log(
            "EPS0323b",
            None,
            {
                "internalID": internal_id,
                "agent_sds_role_profile_id": context.msgOutput[message_vocab.ROLEPROFILE],
            },
        )

    if context.msgOutput[message_vocab.ROLE] == "NotProvided":
        log_object.write_log("EPS0330", None, {"internalID": internal_id})
    elif not REGEX_ROLECODE.match(context.msgOutput[message_vocab.ROLE]):
        log_object.write_log(
            "EPS0323",
            None,
            {
                "internalID": internal_id,
                "agent_sds_role": context.msgOutput[message_vocab.ROLE],
            },
        )

    context.outputFields.add(message_vocab.AGENTORG)
    context.outputFields.add(message_vocab.ROLEPROFILE)
    context.outputFields.add(message_vocab.ROLE)


def check_nhs_number(context):
    """
    Check an nhs number is of a valid format
    Requires:
        nhsNumber
    """
    if is_nhs_number_valid(context.msgOutput[message_vocab.PATIENTID]):
        context.outputFields.add(message_vocab.PATIENTID)
    else:
        supp_info = message_vocab.PATIENTID + " is not valid"
        raise EpsValidationError(supp_info)


def check_standard_date_time(context, attribute_name, internal_id, log_object: EpsLogger):
    """
    Check for a valid time
    """
    try:
        if len(context.msgOutput[attribute_name]) != 14:
            if len(context.msgOutput[attribute_name]) != 19:
                raise ValueError("Wrong String Length")
            parsed_time = convert_international_time(
                context.msgOutput[attribute_name], log_object, internal_id
            )
            context.msgOutput[attribute_name] = parsed_time
        datetime.datetime.strptime(
            context.msgOutput[attribute_name], TimeFormats.STANDARD_DATE_TIME_FORMAT
        )
    except ValueError as value_error:
        supp_info = attribute_name + " is not a valid time or in the "
        supp_info += "valid format; expected format " + TimeFormats.STANDARD_DATE_TIME_FORMAT
        raise EpsValidationError(supp_info) from value_error

    context.outputFields.add(attribute_name)


def check_standard_date(context, attribute_name):
    """
    Check for a valid date
    """
    try:
        if len(context.msgOutput[attribute_name]) != 8:
            raise ValueError("Wrong String Length")
        datetime.datetime.strptime(
            context.msgOutput[attribute_name], TimeFormats.STANDARD_DATE_FORMAT
        )
    except ValueError as value_error:
        supp_info = attribute_name + " is not a valid time or in the "
        supp_info += "valid format; expected format " + TimeFormats.STANDARD_DATE_FORMAT
        raise EpsValidationError(supp_info) from value_error

    context.outputFields.add(attribute_name)


def check_hl7_event_id(context):
    """
    Check a HL7 ID is in a valid UUID format
    Requires:
        hl7EventID
    """
    if not REGEX_GUID.match(context.msgOutput[message_vocab.HL7EVENTID]):
        raise EpsValidationError(message_vocab.HL7EVENTID + " has invalid format")
    context.outputFields.add(message_vocab.HL7EVENTID)


def check_mandatory_items(context, mandatory_extracted_items):
    """
    Check for mandatory keys in the schematron output
    """
    for mandatory_key in mandatory_extracted_items:
        if mandatory_key not in context.msgOutput:
            raise EpsValidationError("Mandatory field " + mandatory_key + " missing")
