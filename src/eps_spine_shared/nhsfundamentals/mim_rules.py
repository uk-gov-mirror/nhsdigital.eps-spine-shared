import re

TEN_DIGIT_NUMBER_REGEX = "^[\\d]{10}$"


def is_nhs_number_valid(nhs_number):
    """
    Function to check the check digit on a standard nhsNumber
    Tenth digit of the NHS number is a check digit
    See http://www.datadictionary.nhs.uk/data_dictionary/attributes/n/nhs_number_de.asp
    Returns True if check passes, False otherwise
    """
    nhs_number_match = False

    if re.match(TEN_DIGIT_NUMBER_REGEX, nhs_number):

        total = 0
        multiplier = 10

        for i in range(9):
            total += int(nhs_number[i]) * multiplier
            multiplier -= 1

        check_digit = 11 - total % 11
        if check_digit == 11:
            check_digit = 0

        if check_digit == int(nhs_number[9]):
            nhs_number_match = True

    return nhs_number_match
