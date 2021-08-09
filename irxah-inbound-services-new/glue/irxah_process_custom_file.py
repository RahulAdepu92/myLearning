import sys
from irxah.jobs.process_flow import custom_to_standard

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
custom_to_standard(**args)