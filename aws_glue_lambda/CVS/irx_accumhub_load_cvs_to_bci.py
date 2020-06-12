"""
Author : Manikandan
Description : This python script will copy s3 detail inbound split file to Redshift "stg_accum_dtl" table.
  This code resides in a glue job "irx_accumhub_load_cvs_to_bci" and is called
  by lambda "irx_accumhub_SubmitGlueJobLambda_cvs_to_bci".
"""

import boto3
import json
import logging
import logging.config
import os
import pg
import sys
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions

# environment variables

args = getResolvedOptions(
    sys.argv,
    [
        "S3_ACCUMHUB_BUCKET",
        "S3_INBOUND_DETAIL_PATH",
        "S3_INBOUND_DETAIL_FILE_PREFIX",
        "REDSHIFT_DB_SCHEMA",
        "REDSHIFT_TABLE",
        "S3_INBOUND_TRAILER_TEMP_FILE_PATH",
        "S3_INBOUND_DETAIL_TEMP_FILE_PATH",
        "S3_INBOUND_HEADER_TEMP_FILE_PATH",
        "IAM_ARN",
        "ENVIRONMENT",
    ],
)

s3_accumhub_bucket = args["S3_ACCUMHUB_BUCKET"]
iam_arn = args["IAM_ARN"]
environment = args["ENVIRONMENT"]
redshift_db_schema = args["REDSHIFT_DB_SCHEMA"]
redshift_table = args["REDSHIFT_TABLE"]
s3_inbound_detail_file = os.path.join(
    args["S3_INBOUND_DETAIL_PATH"], args["S3_INBOUND_DETAIL_FILE_PREFIX"]
)
s3_inbound_trailer_temp_file_path = args["S3_INBOUND_TRAILER_TEMP_FILE_PATH"]
s3_inbound_detail_temp_file_path = args["S3_INBOUND_DETAIL_TEMP_FILE_PATH"]
s3_inbound_header_temp_file_path = args["S3_INBOUND_HEADER_TEMP_FILE_PATH"]


# end of environment variables section

s3obj = boto3.client("s3")
s3resource = boto3.resource("s3")


def get_secret():
    """
    Function to fetch the credentials for the Redshift Service
    :return:
    """
    secret = ""
    secret_name = f"irx-ahub-{environment}-redshift-cluster"
    # secret_name = "idw-" + environment + "-redshift-cluster"
    region_name = "us-east-1"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    logging.debug("Opening Session")
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        logging.debug("Getting Secret value response")
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as error:
        if error.response["Error"]["Code"] == "DecryptionFailureException":
            raise error
        if error.response["Error"]["Code"] == "InternalServiceErrorException":
            raise error
        if error.response["Error"]["Code"] == "InvalidParameterException":
            raise error
        if error.response["Error"]["Code"] == "InvalidRequestException":
            raise error
        if error.response["Error"]["Code"] == "ResourceNotFoundException":
            raise error
    else:
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]

    return secret


def create_redshift_connection(host, port, db_name, user, password):
    """
    Function to establish a connection with Redshift using psycopg2
    :param host: Host (IP) of the Redshift cluster
    :param port: Port of the Redshift database
    :param user: Name of the Redshift master user
    :param pw: Password for the Redshift master password
    :param database: Name of the Redshift database
    :return: Returns a Redshift connection object
   """

    conn_string = "host={} port={} dbname={} user={} password={}".format(
        host, port, db_name, user, password,
    )
    conn = pg.connect(dbname=conn_string)
    return conn


# ********SECTION 1**********


def load_detail_table(cursor, db_schema, detail_table, s3_in_bucket, s3_in_detail_file, iam_role):
    """Function to load detail file records to redshift detail table"""
    copy_detail_query = (
        "COPY {}.{} FROM 's3://{}/{}' "
        "iam_role '{}' "
        "delimiter '|';".format(db_schema, detail_table, s3_in_bucket, s3_in_detail_file, iam_role)
    )
    cursor.query(copy_detail_query)
    # logging.debug(copy_dtl)


def inbound_tempfolder_files_delete(
    s3_in_bucket,
    s3_in_trailer_temp_file,
    s3_in_detail_temp_file,
    s3_in_header_temp_file,
    iam_role,
    s3resource,
    s3obj,
):
    """Function to remove the inbound temporary s3 files"""
    folder_list = [s3_in_trailer_temp_file, s3_in_detail_temp_file, s3_in_header_temp_file]

    s3bucket = s3resource.Bucket(s3_in_bucket)
    for folder_name in folder_list:
        for key in s3bucket.objects.filter(Prefix=folder_name, Delimiter="/"):
            if str(key).lower().endswith(".txt"):
                s3obj.delete_object(Bucket=s3_in_bucket, Key=key.key)


# ********SECTION 1 ends**********


def main():
    secret = get_secret()
    secret = json.loads(secret)
    redshift_user = secret.get("username")
    # redshift_user=" "
    redshift_password = secret.get("password")
    # redshift_password = " "
    redshift_host = secret.get("host")
    # redshift_host ="irx-bci-accum.crpqir2xaogw.us-east-1.redshift.amazonaws.com"
    redshift_port = secret.get("port")
    # redshift_port="5439"
    redshift_database = secret.get("dbname")
    # redshift_database ="devdb"
    conn = create_redshift_connection(
        redshift_host, redshift_port, redshift_database, redshift_user, redshift_password
    )
    cursor = conn
    load_detail_table(
        cursor,
        redshift_db_schema,
        redshift_table,
        s3_accumhub_bucket,
        s3_inbound_detail_file,
        iam_arn,
    )
    logging.info("copy for detail completed")
    inbound_tempfolder_files_delete(
        s3_accumhub_bucket,
        s3_inbound_trailer_temp_file_path,
        s3_inbound_detail_temp_file_path,
        s3_inbound_header_temp_file_path,
        iam_arn,
        s3resource,
        s3obj,
    )
    logging.info("inbound temp folder file clean up process is completed")


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime) s - %(levelname)s:%(message)s", level=logging.DEBUG)
    main()
