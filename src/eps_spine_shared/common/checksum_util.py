from string import ascii_uppercase

LONGIDLENGTH_WITH_CHECKDIGIT = 37
SHORTIDLENGTH_WITH_CHECKDIGIT = 20


def calculate_checksum(prescription_id):
    """
    Generate a checksum for either R1 or R2 prescription
    """
    prsc_id = prescription_id.replace("-", "")
    prsc_id_length = len(prsc_id)

    running_total = 0
    for string_position in range(prsc_id_length - 1):
        char_mod36 = int(prsc_id[string_position], 36)
        running_total += char_mod36 * (2 ** (prsc_id_length - string_position - 1))

    check_value = (38 - running_total % 37) % 37
    if check_value == 36:
        check_value = "+"
    elif check_value > 9:
        check_value = ascii_uppercase[check_value - 10]
    else:
        check_value = str(check_value)

    return check_value


def check_checksum(prescription_id, internal_id, log_object):
    """
    Check the checksum of a Prescription ID
    :prescription_id the prescription to check
    :log_object invalid checksums will be logged
    """
    check_character = prescription_id[-1:]
    check_value = calculate_checksum(prescription_id)

    if check_value == check_character:
        return True

    log_object.write_log(
        "MWS0042",
        None,
        {
            "internalID": internal_id,
            "prescriptionID": prescription_id,
            "checkValue": check_value,
        },
    )

    return False


def remove_check_digit(prescription_id):
    """
    Takes the passed in id and determines, by its length, if it contains a checkdigit,
    returns an id without the check digit
    """
    prescription_key = prescription_id
    id_length = len(prescription_id)
    if id_length in [LONGIDLENGTH_WITH_CHECKDIGIT, SHORTIDLENGTH_WITH_CHECKDIGIT]:
        prescription_key = prescription_id[:-1]
    return prescription_key
