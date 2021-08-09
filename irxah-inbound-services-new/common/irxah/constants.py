# Indicates what is the delimiter string for columns in the split file
COLUMN_DELIMITER = "|"
BLANK = ""

# Schemas available in redshift db which are utilised in executing sqls
SCHEMA_DW = "ahub_dw"
SCHEMA_STAGING = "ahub_stg"

# Tables and Views available in redshift db which are utilised in executing sqls
TABLE_JOB = "job"
TABLE_JOB_DETAIL = "job_detail"
TABLE_ACCUM_DETAIL_STAGING = "stg_accum_dtl"
TABLE_RECON_DETAIL_STAGING = "stg_accumulator_reconciliation_detail"
TABLE_ACCUM_HISTORY_DETAIL_STAGING = "stg_accum_hist_dtl"
VIEW_ACCUM_HISTORY_DETAIL_STAGING = "vw_accum_hist_dtl"
TABLE_ACCUM_HISTORY_CROSSWALK_STAGING = "stg_accum_hist_crosswalk"
TABLE_ACCUM_DETAIL_DW = "accumulator_detail"
TABLE_RECON_DETAIL_DW = "accumulator_reconciliation_detail"
TABLE_CLIENT = "client"
TABLE_CLIENT_FILES = "client_files"
TABLE_COLUMN_RULES = "column_rules"
TABLE_FILE_COLUMNS = "file_columns"
TABLE_COLUMN_ERROR_CODES = "column_error_codes"
TABLE_FILE_VALIDATION_RESULT = "file_validation_result"
TABLE_CUSTOM_TO_STANDARD_MAPPING = "custom_to_standard_mapping"
FILE_SCHEDULE = "file_schedule"
EXPORT_SETTING = "export_setting"

# S3_bukcet name and file_names used in various functions
JOB_S3_BUCKET_ARGUMENT_NAME = "S3_FILE_BUCKET"
JOB_S3_FILE_ARGUMENT_NAME = "S3_FILE_NAME"

# values used in 'validate_file_columns.py' to check if Accumulator_Balance_Qualifier fields have expected values or not
LIST_OF_ACCUM_QUALIFIERS = [
    "989:991",
    "1025:1027",
    "1061:1063",
    "1097:1099",
    "1133:1135",
    "1169:1171",
    "1229:1231",
    "1265:1267",
    "1301:1303",
    "1337:1339",
    "1373:1375",
    "1409:1411",
]

# position of column 'record_count' in trailer line of outbound file
COMMON_TRAILER_RECORD_COUNT_POSITION = "210:219"

# 'TRANSMISSION_ID' column position value and its respective error_code, error_level to check the record duplication in incoming client files
BCI_TRANSMISSION_ID_COLUMN_RULE_ID = 225
CVS_TRANSMISSION_ID_COLUMN_RULE_ID = 1225
TRANSMISSION_ID_COLUMN_POSITION = "358:407"
TRANSMISSION_ID_ERROR_CODE = 103
TRANSMISSION_ID_ERROR_LEVEL = 1

# Delimiter for getting job keys
DEFAULT_DELIMITER = ","
NULL = "NULL"

# 'NA' value is used in 'cvs_manual_marking.py' to declare few glue parmaters and set them to default 'NA'
NA = "NA"


# String literals used for getting glue jobs status while processing them in 'processflow.py'
SUCCESS = "Success"
RUNNING = "Running"
FAIL = "Fail"
INCOMPLETE = "Incomplete"

# timezone in which glue jobs run. Preferably a local timezone.
DEFAULT_TIMEZONE = "America/New_York"

# Secret name key in arguments
SECRET_NAME_KEY = "SECRET_NAME"
FILE_TIMESTAMP_FORMAT = "%y%m%d%H%M%S"


# s3 folder names
UNKNOWN_FILES = "unknown"
UNPROCESSED_FILES = "unprocessed"
