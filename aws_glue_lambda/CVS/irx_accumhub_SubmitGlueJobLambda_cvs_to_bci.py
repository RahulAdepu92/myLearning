"""
Author : Rahul Adepu
Description : This python script will trigger the glue job "irx_accumhub_load_cvs_to_bci"
  based on s3 event trigger that will load Redshift "stg_accum_dtl" table.
"""


import os
import boto3

glue = boto3.client("glue")


def lambda_handler(event, context):
    gluejobname = os.environ["GLUEJOB_NAME"]

    response = glue.start_job_run(
        JobName=gluejobname,
        Arguments={
            "--S3_ACCUMHUB_BUCKET": os.environ["S3_ACCUMHUB_BUCKET"],
            "--S3_INBOUND_DETAIL_PATH": os.environ["S3_INBOUND_DETAIL_PATH"],
            "--S3_INBOUND_DETAIL_FILE_PREFIX": os.environ["S3_INBOUND_DETAIL_FILE_PREFIX"],
            "--REDSHIFT_DB_SCHEMA": os.environ["REDSHIFT_DB_SCHEMA"],
            "--REDSHIFT_TABLE": os.environ["REDSHIFT_TABLE"],
            "--IAM_ARN": os.environ["IAM_ARN"],
            "--ENVIRONMENT": os.environ["ENVIRONMENT"],
            "--S3_INBOUND_TRAILER_TEMP_FILE_PATH": os.environ["S3_INBOUND_TRAILER_TEMP_FILE_PATH"],
            "--S3_INBOUND_DETAIL_TEMP_FILE_PATH": os.environ["S3_INBOUND_DETAIL_TEMP_FILE_PATH"],
            "--S3_INBOUND_HEADER_TEMP_FILE_PATH": os.environ["S3_INBOUND_HEADER_TEMP_FILE_PATH"],
        },
    )

    jobId = response["JobRunId"]
    status = glue.get_job_run(JobName=gluejobname, RunId=jobId)
