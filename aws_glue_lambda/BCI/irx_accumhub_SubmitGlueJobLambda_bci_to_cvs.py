"""
Author : Rahul Adepu
Description : This python Script will trigger the glue job "irx_accumhub_load_unload_merge_bci_to_cvs"
based on s3 event trigger that will load Redshift "stg_accum_dtl" table,
unload it to output files and merge them into single CVS file.
"""


import os
import boto3
import time

glue = boto3.client("glue")


def lambda_handler(event, context):
    gluejobname = os.environ["GLUEJOB_NAME"]

    response = glue.start_job_run(
        JobName=gluejobname,
        Arguments={
            "--S3_ACCUMHUB_BUCKET": os.environ["S3_ACCUMHUB_BUCKET"],
            "--S3_INBOUND_DETAIL_PATH": os.environ["S3_INBOUND_DETAIL_PATH"],
            "--S3_INBOUND_DETAIL_FILE_PREFIX": os.environ["S3_INBOUND_DETAIL_FILE_PREFIX"],
            "--S3_OUTBOUND_PATH": os.environ["S3_OUTBOUND_PATH"],
            "--S3_OUTBOUND_DETAIL_FILE_NAME": os.environ["S3_OUTBOUND_DETAIL_FILE_NAME"],
            "--S3_OUTBOUND_HEADER_FILE_NAME": os.environ["S3_OUTBOUND_HEADER_FILE_NAME"],
            "--S3_OUTBOUND_TRAILER_FILE_NAME": os.environ["S3_OUTBOUND_TRAILER_FILE_NAME"],
            "--REDSHIFT_DB_SCHEMA": os.environ["REDSHIFT_DB_SCHEMA"],
            "--REDSHIFT_TABLE": os.environ["REDSHIFT_TABLE"],
            "--S3_MERGE_DETAIL_FILE_NAME": os.environ["S3_MERGE_DETAIL_FILE_NAME"],
            "--S3_MERGE_HEADER_FILE_NAME": os.environ["S3_MERGE_HEADER_FILE_NAME"],
            "--S3_MERGE_TRAILER_FILE_NAME": os.environ["S3_MERGE_TRAILER_FILE_NAME"],
            "--S3_MERGE_OUTPUT_PATH": os.environ["S3_MERGE_OUTPUT_PATH"],
            "--S3_MERGE_OUTPUT_SUFFIX": os.environ["S3_MERGE_OUTPUT_SUFFIX"],
            "--IAM_ARN": os.environ["IAM_ARN"],
            "--ENVIRONMENT": os.environ["ENVIRONMENT"],
            "--S3_INBOUND_TRAILER_TEMP_FILE_PATH": os.environ["S3_INBOUND_TRAILER_TEMP_FILE_PATH"],
            "--S3_INBOUND_DETAIL_TEMP_FILE_PATH": os.environ["S3_INBOUND_DETAIL_TEMP_FILE_PATH"],
            "--S3_INBOUND_HEADER_TEMP_FILE_PATH": os.environ["S3_INBOUND_HEADER_TEMP_FILE_PATH"],
            "--S3_OUTBOUND_TEMP_FILE_PATH": os.environ["S3_OUTBOUND_TEMP_FILE_PATH"],
            "--FILE_TYPE": os.environ["FILE_TYPE"],
            "--TRANSMISSION_FILE_TYPE": os.environ["TRANSMISSION_FILE_TYPE"],
            "--INPUT_SENDER_ID": os.environ["INPUT_SENDER_ID"],
            "--OUTPUT_SENDER_ID": os.environ["OUTPUT_SENDER_ID"],
            "--OUTPUT_RECEIVER_ID": os.environ["OUTPUT_RECEIVER_ID"],
        },
    )

    jobId = response["JobRunId"]
    status = glue.get_job_run(JobName=gluejobname, RunId=jobId)
