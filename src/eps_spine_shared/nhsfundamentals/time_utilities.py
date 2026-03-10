import zoneinfo
from datetime import datetime, timedelta

from eps_spine_shared.logger import EpsLogger


class TimeFormats:
    STANDARD_DATE_TIME_UTC_ZONE_FORMAT = "%Y%m%d%H%M%S+0000"
    STANDARD_DATE_TIME_FORMAT = "%Y%m%d%H%M%S"
    STANDARD_DATE_TIME_LENGTH = 14
    DATE_TIME_WITHOUT_SECONDS_FORMAT = "%Y%m%d%H%M"
    STANDARD_DATE_FORMAT = "%Y%m%d"
    STANDARD_DATE_FORMAT_YEAR_MONTH = "%Y%m"
    STANDARD_DATE_FORMAT_YEAR_ONLY = "%Y"
    HL7_DATETIME_FORMAT = "%Y%m%dT%H%M%S.%f"
    SPINE_DATETIME_MS_FORMAT = "%Y%m%d%H%M%S.%f"
    SPINE_DATE_FORMAT = "%Y%m%d"
    EBXML_FORMAT = "%Y-%m-%dT%H:%M:%S"
    SMSP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    EXTENDED_SMSP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
    EXTENDED_SMSP_PLUS_Z_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


TZ_BST = "BST"
TZ_GMT = "GMT"
TZ_BST_OFFSET = "Etc/GMT-1"
TZ_UTC = "utc"

_TIMEFORMAT_LENGTH_MAP = {
    TimeFormats.STANDARD_DATE_TIME_LENGTH: TimeFormats.STANDARD_DATE_TIME_FORMAT,
    12: TimeFormats.DATE_TIME_WITHOUT_SECONDS_FORMAT,
    8: TimeFormats.STANDARD_DATE_FORMAT,
    6: TimeFormats.STANDARD_DATE_FORMAT_YEAR_MONTH,
    4: TimeFormats.STANDARD_DATE_FORMAT_YEAR_ONLY,
    22: TimeFormats.HL7_DATETIME_FORMAT,
    21: TimeFormats.SPINE_DATETIME_MS_FORMAT,
    20: TimeFormats.SMSP_FORMAT,
    23: TimeFormats.EXTENDED_SMSP_FORMAT,
    26: TimeFormats.EXTENDED_SMSP_FORMAT,
    24: TimeFormats.EXTENDED_SMSP_PLUS_Z_FORMAT,
    27: TimeFormats.EXTENDED_SMSP_PLUS_Z_FORMAT,
}


def guess_common_datetime_format(time_string, raise_error_if_unknown=False):
    """
    Guess the date time format from the commonly used list

    Args:
        time_string (str):
            The datetime string to try determine the format of.
        raise_error_if_unknown (bool):
            Determines the action when the format cannot be determined.
            False (default) will return None, True will raise an error.
    """
    fmt = None
    if len(time_string) == 19:
        try:
            datetime.strptime(time_string, TimeFormats.EBXML_FORMAT)
            fmt = TimeFormats.EBXML_FORMAT
        except ValueError:
            fmt = TimeFormats.STANDARD_DATE_TIME_UTC_ZONE_FORMAT
    else:
        fmt = _TIMEFORMAT_LENGTH_MAP.get(len(time_string), None)

    if not fmt and raise_error_if_unknown:
        raise ValueError("Could not determine datetime format of '{}'".format(time_string))

    return fmt


def convert_spine_date(date_string, date_format=None):
    """
    Try to convert a Spine date using the passed format - if it fails - try the most
    appropriate
    """
    if date_format:
        try:
            date_object = datetime.strptime(date_string, date_format)
            return date_object
        except ValueError:
            pass

    date_format = guess_common_datetime_format(date_string, raise_error_if_unknown=True)
    return datetime.strptime(date_string, date_format)


def date_today_as_string():
    """
    Return the current date as a string in standard format
    """
    return time_now_as_string(TimeFormats.STANDARD_DATE_FORMAT)


def time_now_as_string(date_format=TimeFormats.STANDARD_DATE_TIME_FORMAT):
    """
    Return the current date and time as a string in standard format
    """
    return now().strftime(date_format)


def now():
    """
    Utility to gets the current date and time.
    The intention is for this to be easier to replace when testing.
    :returns: a datetime representing the current date and time
    """
    return datetime.now()


def convert_international_time(international_date, log_object: EpsLogger, internal_id):
    """
    Convert a HL7 offset time in BST or GMT format into a 14 digit GMT string, the
    allowable international format is: YYYYMMDDHHMMSS[+|-ZZzz], but only +|-0000 and +0100 are permitted
    """
    date_format = TimeFormats.STANDARD_DATE_TIME_FORMAT

    if international_date.endswith("+0100"):
        # International format BST detected
        logged_time_zone = TZ_BST
        formatted_date = datetime.strptime(international_date[:14], date_format)
        corrected_date = formatted_date.replace(tzinfo=zoneinfo.ZoneInfo(TZ_BST_OFFSET))
        localised_date = corrected_date.astimezone(zoneinfo.ZoneInfo(TZ_GMT))
        returned_date = localised_date.strftime(date_format)

    elif international_date.endswith("+0000") or international_date.endswith("-0000"):
        # International format GMT detected
        # specifically looking for  or - (rather than last four digits of 0000 in case
        # of non-international date being passed)
        returned_date = international_date[:14]
        logged_time_zone = TZ_GMT
    else:
        # Invalid format detected
        log_object.write_log(
            "EPS0508", None, {"internalID": internal_id, "datetime": international_date}
        )
        raise ValueError

    log_object.write_log(
        "EPS0507",
        None,
        {
            "internalID": internal_id,
            "datetime": international_date,
            "timezone": logged_time_zone,
            "convertedDateTime": returned_date,
        },
    )
    return returned_date


class StopWatch:
    """
    Class to support timing points in the code
    """

    def __init__(self):
        self.start_time = None

    def start_the_clock(self):
        """
        Start the clock
        """
        self.start_time = datetime.now()

    def stop_the_clock(self):
        """
        Stop the clock automatically resets and restarts the clock
        Use split the clock if want to keep a parent timer running
        """
        step_duration_seconds = self.split_the_clock()
        self.start_time = datetime.now()
        return step_duration_seconds

    def split_the_clock(self):
        """
        Split the clock, keeping the parent timer running
        """
        step_duration = datetime.now() - self.start_time
        step_duration_seconds = round(
            float(step_duration.seconds) + float(step_duration.microseconds) / 1000000, 3
        )
        if step_duration_seconds < 0.0005:
            step_duration_seconds = 0.000

        return step_duration_seconds

    def reset_the_clock(self, seed_time):
        """
        Reset the clock assuming a new seed time, to be used when time has
        been passed as message by string Assumed format of time is:
        %Y%m%dT%H%M%S.%3N
        """
        date_split = seed_time.split(".")
        self.start_time = datetime.strptime(date_split[0], "%Y%m%dT%H%M%S")
        self.start_time += timedelta(milliseconds=int(date_split[1]))
