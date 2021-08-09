import sys
from irxah.jobs.export import export_client_file
# while running lambdas we encounter import error as awsglue is inbuilt module for Glue but not for lambda
try:
    from awsglue.utils import getResolvedOptions
except ImportError:
    pass

args = getResolvedOptions(
    sys.argv,
    [
        "CLIENT_FILE_ID",
        "SECRET_NAME",
        "S3_FILE_BUCKET",
        "FILE_SCHEDULE_ID",
        "UPDATE_FILE_SCHEDULE_STATUS",
    ],
)
export_client_file(**args)
