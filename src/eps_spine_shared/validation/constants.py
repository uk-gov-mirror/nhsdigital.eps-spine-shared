import re

from eps_spine_shared.common.prescription import fields

REGEX_NUMERIC15 = "^[0-9]{1,15}$"
REGEX_ALPHANUMERIC12 = r"^[A-Za-z0-9\-]{1,12}$"
REGEX_ALPHANUMERIC8 = r"^[A-Za-z0-9\-]{1,8}$"
REGEX_ALPHA4 = "^[A-Za-z]{1,4}$"
REGEX_TEXT120 = "^[A-Za-z0-9 \t\r\n\v\f/-]{1,120}$"
REGEX_INTEGER12 = "^[0-9]{1,12}$"

REGEX_PRESCRID = r"^[A-F0-9]{6}\-[A-Z0-9]{6}\-[A-F0-9]{5}[A-Z0-9\+]{1}$"
REGEX_PRESCRIDR1 = (
    r"^[A-F0-9]{8}\-[A-F0-9]{4}\-[A-F0-9]{4}\-[A-F0-9]{4}\-[A-F0-9]{12}[A-Z0-9\+]{1}$"
)
REGEX_PRESCRIDR1_ALT = r"^[A-F0-9]{8}\-[A-F0-9]{4}\-[A-F0-9]{4}\-[A-F0-9]{4}\-[A-F0-9]{12}$"
REGEX_GUID = r"^[A-F0-9]{8}\-[A-F0-9]{4}\-[A-F0-9]{4}\-[A-F0-9]{4}\-[A-F0-9]{12}$"
REGEX_ROLECODE = r"^[A-Z]{1}[0-9]{4}\:[A-Z]{1}[0-9]{4}\:[A-Z]{1}[0-9]{4}$"
REGEX_ALTROLECODE = r"^[A-Z]{1}[0-9]{4}$"

REGEX_NUMERIC15 = re.compile(REGEX_NUMERIC15)
REGEX_ALPHANUMERIC12 = re.compile(REGEX_ALPHANUMERIC12)
REGEX_ALPHANUMERIC8 = re.compile(REGEX_ALPHANUMERIC8)
REGEX_ALPHA4 = re.compile(REGEX_ALPHA4)
REGEX_TEXT120 = re.compile(REGEX_TEXT120)
REGEX_INTEGER12 = re.compile(REGEX_INTEGER12)

REGEX_PRESCRID = re.compile(REGEX_PRESCRID)
REGEX_PRESCRIDR1 = re.compile(REGEX_PRESCRIDR1)
REGEX_PRESCRIDR1_ALT = re.compile(REGEX_PRESCRIDR1_ALT)
REGEX_GUID = re.compile(REGEX_GUID)
REGEX_ROLECODE = re.compile(REGEX_ROLECODE)
REGEX_ALTROLECODE = re.compile(REGEX_ALTROLECODE)

STATUS_ACUTE = "0001"
STATUS_REPEAT = "0002"
STATUS_REPEAT_DISP = "0003"

TREATMENT_TYPELIST = [STATUS_ACUTE, STATUS_REPEAT, STATUS_REPEAT_DISP]

PRESC_TYPELIST = [
    "0001",
    "0002",
    "0003",
    "0004",
    "0005",
    "0006",
    "0007",
    "0008",
    "0009",
    "0010",
    "0011",
    "0101",
    "0102",
    "0103",
    "0104",
    "0105",
    "0106",
    "0107",
    "0108",
    "0109",
    "0110",
    "0113",
    "0114",
    "0116",
    "0117",
    "0119",
    "0120",
    "0121",
    "0122",
    "0123",
    "0124",
    "0125",
    "0304",
    "0305",
    "0306",
    "0307",
    "0406",
    "0607",
    "0708",
    "0709",
    "0713",
    "0714",
    "0716",
    "0717",
    "0718",
    "0719",
    "0721",
    "0722",
    "0901",
    "0904",
    "0908",
    "0913",
    "0914",
    "0915",
    "0916",
    "1004",
    "1005",
    "1008",
    "1013",
    "1014",
    "1016",
    "1017",
    "1024",
    "1025",
    "1104",
    "1105",
    "1108",
    "1113",
    "1114",
    "1116",
    "1117",
    "1124",
    "1125",
    "1204",
    "1205",
    "1208",
    "1213",
    "1214",
    "1216",
    "1217",
    "1224",
    "1225",
    "1001",
    "1101",
    "1201",
    "0201",
    "0204",
    "0205",
    "0208",
    "0213",
    "0214",
    "0216",
    "0217",
    "0224",
    "0225",
    "2001",
    "2004",
    "2005",
    "2008",
    "2013",
    "2014",
    "2016",
    "2017",
    "2024",
    "2025",
    "0707",
    "0501",
    "0504",
    "0505",
    "0508",
    "0513",
    "0514",
    "0516",
    "0517",
    "0524",
    "0525",
    "5001",
    "5004",
    "5005",
    "5008",
    "5013",
    "5014",
    "5016",
    "5017",
    "5024",
    "5025",
]

PERFORMER_TYPELIST = ["P1", "P2", "P3"]
WITHDRAW_TYPELIST = ["LD", "AD"]
WITHDRAW_RSONLIST = ["QU", "MU", "DA", "PA", "OC", "ONC"]

R1 = "R1"
R2 = "R2"

R1_PRESCID_LENGTHS = [36, 37]
R2_PRESCID_LENGTHS = [18]

ALT_DATETIME_FORMAT = "%Y%m%d%H%M"

MAX_LINEITEMS = 4
MAX_PRESCRIPTIONREPEATS = 99
DEFAULT_DAYSSUPPLY = fields.DEFAULT_DAYSSUPPLY
MAX_DAYSSUPPLY = 366
MIN_AGE = 16
MAX_AGE = 60
MAX_FUTURESUPPLYMONTHS = 12
NOT_DISPENSED = "NotDispensedReason"
