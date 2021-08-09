import sys
from irxah.jobs.process_flow import process_incoming_file_load_in_database

try:
    from awsglue.utils import getResolvedOptions
except ImportError:
    pass

args = getResolvedOptions(
    sys.argv,
    [
        "SECRET_NAME",
        "S3_FILE",
        "S3_FILE_BUCKET",
        "UNZIP_JOB_DETAIL_KEY",
        "CLIENT_FILE_ID",
    ],
)
if args["UNZIP_JOB_DETAIL_KEY"] == "0":  # retrieved from custom to standard file conversion activity
    args["UNZIP_JOB_DETAIL_KEY"] = None
process_incoming_file_load_in_database(**args)
