"""
Author : Debashis Mohanty
Description : This Python script will compare the old PBM accumulation history file
  with crosswalk file and replace the old PBM carrier,
  account and group with IRX carrier, account and group respectively
"""

import boto3
import datetime
import json
import logging
import logging.config
import pg
import sys
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
import dateutil.tz

# environment variables

args = getResolvedOptions(
    sys.argv,
    [
        "IAM_ARN",
        "S3_ACCUMHUB_BUCKET",
        "REDSHIFT_TABLE_NAME_ACCUMHIST_DTL",
        "REDSHIFT_DB_SCHEMA",
        "ENVIRONMENT",
        "S3_MERGE_TRAILER_FILE_NAME",
        "S3_OUTBOUND_FILE_PREFIX",
        "S3_OUTBOUND_PATH",
        "S3_MERGE_DETAIL_FILE_NAME",
        "S3_OUTBOUND_TEMP_PATH",
        "S3_INBOUND_TEMP_PATH",
        "S3_MERGE_HEADER_FILE_NAME",
        "REDSHIFT_TABLE_NAME_CROSSWALK",
        "S3_INBOUND_PATH",
        "S3_INBOUND_DETAIL_PATH",
        "S3_INBOUND_ACCUM_CROSSWALK_FILE_NAME",
        "S3_INBOUND_ACCUMHIST_FILE_NAME_PREFIX",
        "S3_OUTBOUND_TRAILER",
        "S3_OUTBOUND_DETAIL",
        "S3_INBOUND_TRAILER_PATH",
    ],
)
s3_accumhub_bucket = args["S3_ACCUMHUB_BUCKET"]
s3_inbound_crosswalk_file = args["S3_INBOUND_PATH"] + args["S3_INBOUND_ACCUM_CROSSWALK_FILE_NAME"]
s3_inbound_accumhist_file = (
    args["S3_INBOUND_DETAIL_PATH"] + args["S3_INBOUND_ACCUMHIST_FILE_NAME_PREFIX"] + "DTL.TXT"
)
s3_outbound_detail_file = args["S3_OUTBOUND_TEMP_PATH"] + args["S3_MERGE_DETAIL_FILE_NAME"]
s3_outbound_header_file = args["S3_INBOUND_TEMP_PATH"] + args["S3_MERGE_HEADER_FILE_NAME"]
s3_outbound_trailer_file = args["S3_OUTBOUND_TEMP_PATH"] + args["S3_MERGE_TRAILER_FILE_NAME"]
s3_inbound_trailer_temp_file_path = args["S3_INBOUND_TRAILER_PATH"]
s3_inbound_detail_temp_file_path = args["S3_INBOUND_DETAIL_PATH"]
s3_inbound_header_temp_file_path = args["S3_INBOUND_TEMP_PATH"]
db_schema = args["REDSHIFT_DB_SCHEMA"]
redshift_table_crosswalk = args["REDSHIFT_TABLE_NAME_CROSSWALK"]
redshift_table_accum_hist_dtl = args["REDSHIFT_TABLE_NAME_ACCUMHIST_DTL"]
iam_arn = args["IAM_ARN"]
environment = args["ENVIRONMENT"]
detail = s3_outbound_detail_file
header = s3_outbound_header_file
trailer = s3_outbound_trailer_file
outbound_path = args["S3_OUTBOUND_PATH"]
s3_outbound_trailer_unload_file = args["S3_OUTBOUND_TEMP_PATH"] + args["S3_OUTBOUND_TRAILER"]
s3_outbound_detail_unload_file = args["S3_OUTBOUND_TEMP_PATH"] + args["S3_OUTBOUND_DETAIL"]
s3_outbound_temp_file_path = args["S3_OUTBOUND_TEMP_PATH"]

eastern = dateutil.tz.gettz("US/Eastern")
timestamp_file = datetime.datetime.now(tz=eastern).strftime("%y%m%d%H%M%S")
s3_merge_file_name = args["S3_OUTBOUND_FILE_PREFIX"] + f"{timestamp_file}.TXT"

# end of environment variables section

s3client = boto3.client("s3")
s3resource = boto3.resource("s3")


def get_secret():
    secret = ""
    secret_name = f"irx-ahub-{environment}-redshift-cluster"
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


def cleanup_table(cursor, db_schema, redshift_table):
    """Function to remove the records from REDHSIFT DTL table before current process starts"""
    truncate = "truncate table {}.{} ".format(db_schema, redshift_table)
    cursor.query(truncate)
    # logging.info(truncate)


def load_file_into_table(cursor, db_schema, redshift_table, s3_in_bucket, s3_in_file, iam_role):
    """Function to copy crosswalk and accumhist files from S3 bucket and load into respective redshift tables"""
    copy_query = (
        "COPY {}.{} FROM 's3://{}/{}' "
        "iam_role '{}' "
        "delimiter '|';".format(db_schema, redshift_table, s3_in_bucket, s3_in_file, iam_role)
    )
    cursor.query(copy_query)
    # logging.info(load_file_into_table)


def execute_query(cursor, query):
    """function to unload redshift table into outbound folder and save it into a given file"""
    cursor.query(query)


def records_merge(s3_bucket, detail, header, trailer):
    """Function to merge detail,header and trailer files in outbound folder"""
    approved_objects = s3client.get_object(Bucket=s3_bucket, Key=detail)
    approved_details = approved_objects["Body"].read().decode(encoding="utf-8", errors="ignore")
    header_objects = s3client.get_object(Bucket=s3_bucket, Key=header)
    header_details = (
        header_objects["Body"].read().decode(encoding="utf-8", errors="ignore").replace("|", "")
    )
    trailer_objects = s3client.get_object(Bucket=s3_bucket, Key=trailer)
    trailer_details = trailer_objects["Body"].read().decode(encoding="utf-8", errors="ignore")

    detail_val = []
    header_val = []
    trailer_val = []
    for approved_line in approved_details.splitlines():
        detail_val.append(approved_line)
    for header_line in header_details.splitlines():
        header_val.append(header_line)
    for trailer_line in trailer_details.splitlines():
        trailer_val.append(trailer_line)
    result = "\n".join(header_val) + "\n" + "\n".join(detail_val) + "\n" + "\n".join(trailer_val) + "\n"

    s3client.put_object(Body=result, Bucket=s3_bucket, Key=outbound_path + s3_merge_file_name)


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


def outbound_tempfolder_files_delete(
    s3resource, s3obj, s3_outbound_temp_file_path, s3_accumhub_bucket
):
    """Function to remove the outbound temporary s3 files"""
    s3bucket = s3resource.Bucket(s3_accumhub_bucket)
    for key in s3bucket.objects.filter(Prefix=s3_outbound_temp_file_path, Delimiter="/"):
        if str(key).lower().endswith(".txt"):
            s3obj.delete_object(Bucket=s3_accumhub_bucket, Key=key.key)


def main():
    secret = get_secret()
    secret = json.loads(secret)
    redshift_user = secret.get("username")
    redshift_password = secret.get("password")
    redshift_host = secret.get("host")
    redshift_port = secret.get("port")
    redshift_database = secret.get("dbname")
    conn = create_redshift_connection(
        redshift_host, redshift_port, redshift_database, redshift_user, redshift_password
    )
    cursor = conn

    unload_detail_query = (
        "UNLOAD('select dtl.recd_typ,dtl.member_id,stg.irx_carrier_nbr as carrier_nbr,stg.irx_account_nbr as account,"
        "stg.irx_group_id as group_id,dtl.adj_typ,dtl.adj_amt,dtl.accum_cd,dtl.adj_dt,dtl.adj_cd,dtl.plan_cd,"
        "dtl.care_facility,dtl.member_state_cd,dtl.patient_first_nm,dtl.patient_last_nm,dtl.patient_DOB,"
        "dtl.Patient_relationship_cd ,dtl.RESERVED_SP "
        "from {}.{} dtl "
        "left join "
        "(select carrier_nbr, account_nbr, cvs_group_id,eff_dt,term_dt,cvs_plan,irx_carrier_nbr,irx_account_nbr,"
        "irx_group_id,row_num "
        "from (SELECT carrier_nbr,account_nbr, cvs_group_id,eff_dt,term_dt,cvs_plan,irx_carrier_nbr,irx_account_nbr,"
        "irx_group_id,ROW_NUMBER() OVER (PARTITION BY carrier_nbr,account_nbr,cvs_group_id ORDER BY term_dt desc) "
        "row_num from {}.{})hist where hist.row_num=1)stg on dtl.carrier_nbr=stg.carrier_nbr AND "
        "dtl.account_nbr=stg.account_nbr AND dtl.group_id=stg.cvs_group_id') "
        "TO 's3://{}/{}' iam_role '{}' "
        "FIXEDWIDTH "
        "'0:1,1:18,2:9,3:15,4:15,5:1,6:12,7:10,8:7,9:10,10:10,11:6,12:2,13:15,14:25,15:8,16:1,17:135'"
        "ALLOWOVERWRITE "
        "parallel off;".format(
            db_schema,
            redshift_table_accum_hist_dtl,
            db_schema,
            redshift_table_crosswalk,
            s3_accumhub_bucket,
            s3_outbound_detail_unload_file,
            iam_arn,
        )
    )

    unload_trailer_query = (
        "UNLOAD('select ''9'' as recd_typ,''Caremark'' as member_id,(select lpad(count(*),9,''0'') from (select "
        "dtl.recd_typ,dtl.member_id,stg.irx_carrier_nbr as carrier,stg.irx_account_nbr as account,stg.irx_group_id as "
        "group,dtl.adj_typ,dtl.adj_amt,dtl.accum_cd,dtl.adj_dt,dtl.adj_cd,dtl.plan_cd,dtl.care_facility,"
        "dtl.member_state_cd,dtl.patient_first_nm,dtl.patient_last_nm,dtl.patient_DOB,dtl.Patient_relationship_cd ,"
        "dtl.RESERVED_SP from {}.{} dtl "
        "left join(select carrier_nbr, account_nbr, cvs_group_id,eff_dt,term_dt,cvs_plan,irx_carrier_nbr,"
        "irx_account_nbr,irx_group_id,row_num "
        "from(SELECT carrier_nbr,account_nbr, cvs_group_id,eff_dt,term_dt,cvs_plan,irx_carrier_nbr,irx_account_nbr,"
        "irx_group_id,ROW_NUMBER() OVER (PARTITION BY carrier_nbr,account_nbr,cvs_group_id ORDER BY term_dt desc) "
        "row_num FROM {}.{})hist where hist.row_num=1)stg on dtl.carrier_nbr=stg.carrier_nbr AND "
        "dtl.account_nbr=stg.account_nbr AND dtl.group_id=stg.cvs_group_id)) as carrier_nbr,'' '' as account_nbr "
        "from(select * from (select dtl.recd_typ,dtl.member_id,stg.irx_carrier_nbr as carrier_nbr,stg.irx_account_nbr "
        "as account,stg.irx_group_id as group_id,dtl.adj_typ,dtl.adj_amt,dtl.accum_cd,dtl.adj_dt,dtl.adj_cd,"
        "dtl.plan_cd,dtl.care_facility,dtl.member_state_cd,dtl.patient_first_nm,dtl.patient_last_nm,dtl.patient_DOB,"
        "dtl.Patient_relationship_cd ,dtl.RESERVED_SP "
        "from {}.{} dtl left join(select carrier_nbr, account_nbr, cvs_group_id,eff_dt,term_dt,cvs_plan,"
        "irx_carrier_nbr,irx_account_nbr,irx_group_id,row_num "
        "from (SELECT carrier_nbr,account_nbr, cvs_group_id,eff_dt,term_dt,cvs_plan,irx_carrier_nbr,irx_account_nbr,"
        "irx_group_id,ROW_NUMBER() OVER (PARTITION BY carrier_nbr,account_nbr,cvs_group_id ORDER BY term_dt desc) "
        "row_num FROM {}.{})hist where hist.row_num=1)stg on dtl.carrier_nbr=stg.carrier_nbr AND "
        "dtl.account_nbr=stg.account_nbr AND dtl.group_id=stg.cvs_group_id)limit 1)') "
        "TO 's3://{}/{}' iam_role '{}' "
        "FIXEDWIDTH '0:1,1:9,2:10,3:280' "
        "ALLOWOVERWRITE "
        "parallel off; ".format(
            db_schema,
            redshift_table_accum_hist_dtl,
            db_schema,
            redshift_table_crosswalk,
            db_schema,
            redshift_table_accum_hist_dtl,
            db_schema,
            redshift_table_crosswalk,
            s3_accumhub_bucket,
            s3_outbound_trailer_unload_file,
            iam_arn,
        )
    )

    cleanup_table(cursor, db_schema, redshift_table_crosswalk)
    logging.info("cleanup of crosswalk table is completed")
    cleanup_table(cursor, db_schema, redshift_table_accum_hist_dtl)
    logging.info("cleanup of accums history table is completed")
    load_file_into_table(
        cursor,
        db_schema,
        redshift_table_crosswalk,
        s3_accumhub_bucket,
        s3_inbound_crosswalk_file,
        iam_arn,
    )
    logging.info("Copying from S3 bucket cross walk file to red shift table")
    load_file_into_table(
        cursor,
        db_schema,
        redshift_table_accum_hist_dtl,
        s3_accumhub_bucket,
        s3_inbound_accumhist_file,
        iam_arn,
    )
    logging.info("Copying from S3 bucket accums history file to red shift table")
    execute_query(cursor, unload_detail_query)
    logging.info("unloading completed for HISTORY,")
    execute_query(cursor, unload_trailer_query)
    logging.info("unloading completed for TLR!")
    records_merge(s3_accumhub_bucket, detail, header, trailer)
    logging.info("Merging completed!!")
    inbound_tempfolder_files_delete(
        s3_accumhub_bucket,
        s3_inbound_trailer_temp_file_path,
        s3_inbound_detail_temp_file_path,
        s3_inbound_header_temp_file_path,
        iam_arn,
        s3resource,
        s3client,
    )
    outbound_tempfolder_files_delete(
        s3resource, s3client, s3_outbound_temp_file_path, s3_accumhub_bucket
    )
    logging.info("outbound temp folder file clean up process is completed")


if __name__ == "__main__":
    main()
