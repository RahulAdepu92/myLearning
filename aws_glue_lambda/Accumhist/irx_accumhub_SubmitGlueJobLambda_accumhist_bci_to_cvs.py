"""
Author : Debashis Mohanty
Description : This python Script will trigger the glue job "irx_accumhub_unload_merge_accumhist_bci_to_cvs"
  based on s3 event trigger that will load Redshift "stg_accum_dtl" table,
  unload it to output files and merge them into single CVS file.
"""


import os
import boto3

glue = boto3.client("glue")


def lambda_handler(event, context):
    gluejobname = os.environ["GLUEJOB_NAME"]

    response = glue.start_job_run(
        JobName=gluejobname,
        Arguments={
            "--IAM_ARN": os.environ["IAM_ARN"],
            "--S3_ACCUMHUB_BUCKET": os.environ["S3_ACCUMHUB_BUCKET"],
            "--REDSHIFT_TABLE_NAME_ACCUMHIST_DTL": os.environ["REDSHIFT_TABLE_NAME_ACCUMHIST_DTL"],
            "--REDSHIFT_DB_SCHEMA": os.environ["REDSHIFT_DB_SCHEMA"],
            "--ENVIRONMENT": os.environ["ENVIRONMENT"],
            "--S3_MERGE_TRAILER_FILE_NAME": os.environ["S3_MERGE_TRAILER_FILE_NAME"],
            "--S3_OUTBOUND_FILE_PREFIX": os.environ["S3_OUTBOUND_FILE_PREFIX"],
            "--S3_OUTBOUND_PATH": os.environ["S3_OUTBOUND_PATH"],
            "--S3_MERGE_DETAIL_FILE_NAME": os.environ["S3_MERGE_DETAIL_FILE_NAME"],
            "--S3_OUTBOUND_TEMP_PATH": os.environ["S3_OUTBOUND_TEMP_PATH"],
            "--S3_MERGE_HEADER_FILE_NAME": os.environ["S3_MERGE_HEADER_FILE_NAME"],
            "--REDSHIFT_TABLE_NAME_CROSSWALK": os.environ["REDSHIFT_TABLE_NAME_CROSSWALK"],
            "--S3_INBOUND_PATH": os.environ["S3_INBOUND_PATH"],
            "--S3_INBOUND_DETAIL_PATH": os.environ["S3_INBOUND_DETAIL_PATH"],
            "--S3_INBOUND_ACCUM_CROSSWALK_FILE_NAME": os.environ[
                "S3_INBOUND_ACCUM_CROSSWALK_FILE_NAME"
            ],
            "--S3_INBOUND_ACCUMHIST_FILE_NAME_PREFIX": os.environ[
                "S3_INBOUND_ACCUMHIST_FILE_NAME_PREFIX"
            ],
            "--S3_INBOUND_TEMP_PATH": os.environ["S3_INBOUND_TEMP_PATH"],
            "--S3_OUTBOUND_DETAIL": os.environ["S3_OUTBOUND_DETAIL"],
            "--S3_OUTBOUND_TRAILER": os.environ["S3_OUTBOUND_TRAILER"],
            "--S3_INBOUND_TRAILER_PATH": os.environ["S3_INBOUND_TRAILER_PATH"],
        },
    )

    jobId = response["JobRunId"]
    status = glue.get_job_run(JobName=gluejobname, RunId=jobId)
