"""
Author : Manikandan
Description : This python script will unload Redshift "stg_accum_dtl" table to header, detail
  and trailer output files followed by merging all of them and produce approved and rejected records files.
  This code resides in a glue job "irx_accumhub_unload_merge_cvs_to_bci"
  and is executed by scheduled trigger "irx-accumhub-unload-merge-cvs-to-bci-scheduled-trigger".
"""

import boto3
import datetime
import dateutil.tz
import json
import logging
import logging.config
import os
import pg
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions

# environment variables
s3_accumhub_bucket = os.getenv("S3_ACCUMHUB_BUCKET ", "irx-accumhub-dev-data")
s3_outbound_temp_path = os.getenv("S3_OUTBOUND_TEMP_PATH ", "outbound/bci/temp")
s3_outbound_dq_details_file_name = os.getenv(
    "S3_OUTBOUND_DQ_DETAILS_FILE_NAME ", "ACCDLYINT_TST_BCI_DTL_DQ.txt"
)
s3_outbound_dr_details_file_name = os.getenv(
    "S3_OUTBOUND_DR_DETAILS_FILE_NAME ", "ACCDLYERR_TST_BCI_DTL_DR.txt"
)
s3_outbound_dq_header_file_name = os.getenv(
    "S3_OUTBOUND_DQ_HEADER_FILE_NAME ", "ACCDLYINT_TST_BCI_HDR_DQ.txt"
)
s3_outbound_dr_header_file_name = os.getenv(
    "S3_OUTBOUND_DR_HEADER_FILE_NAME ", "ACCDLYINT_TST_BCI_HDR_DR.txt"
)
s3_outbound_dq_trailer_file_name = os.getenv(
    "S3_OUTBOUND_DQ_TRAILER_FILE_NAME ", "ACCDLYINT_TST_BCI_TLR_DQ.txt"
)
s3_outbound_dr_trailer_file_name = os.getenv(
    "S3_OUTBOUND_DR_TRAILER_FILE_NAME ", "ACCDLYERR_TST_BCI_TLR_DR.txt"
)
s3_merge_temp_path = os.getenv("S3_MERGE_TEMP_PATH ", "outbound/bci/temp")
s3_merge_dq_detail_file_name = os.getenv(
    "S3_MERGE_DQ_DETAIL_FILE_NAME ", "ACCDLYINT_TST_BCI_DTL_DQ.txt000"
)
s3_merge_dr_detail_file_name = os.getenv(
    "S3_MERGE_DR_DETAIL_FILE_NAME ", "ACCDLYERR_TST_BCI_DTL_DR.txt000"
)
s3_merge_dq_header_file_name = os.getenv(
    "S3_MERGE_DQ_HEADER_FILE_NAME ", "ACCDLYINT_TST_BCI_HDR_DQ.txt000"
)
s3_merge_dr_header_file_name = os.getenv(
    "S3_MERGE_DR_HEADER_FILE_NAME ", "ACCDLYINT_TST_BCI_HDR_DR.txt000"
)
s3_merge_dq_trailer_file_name = os.getenv(
    "S3_MERGE_DQ_TRAILER_FILE_NAME ", "ACCDLYINT_TST_BCI_TLR_DQ.txt000"
)
s3_merge_dr_trailer_file_name = os.getenv(
    "S3_MERGE_DR_TRAILER_FILE_NAME ", "ACCDLYERR_TST_BCI_TLR_DR.txt000"
)
s3_outbound_path = os.getenv("S3_OUTBOUND_PATH ", "outbound/bci")
s3_outbound_dq_file_prefix = os.getenv("S3_OUTBOUND_DQ_FILE_PREFIX ", "ACCDLYINT_TST_BCI_")
s3_outbound_dr_file_prefix = os.getenv("S3_OUTBOUND_DR_FILE_PREFIX ", "ACCDLYERR_TST_BCI_")
redshift_db_schema = os.getenv("REDSHIFT_DB_SCHEMA ", "ahub_stg")
redshift_table_name = os.getenv("REDSHIFT_TABLE_NAME ", "stg_accum_dtl")
iam_arn = os.getenv("IAM_ARN ", "arn:aws:iam::795038802291:role/irx-accum-phi-redshift-glue")
environment = os.getenv("ENVIRONMENT", "dv")
s3_outbound_temp_file_path = os.getenv("S3_OUTBOUND_TEMP_FILE_PATH", "outbound/bci/temp/")
file_type = os.getenv("FILE_TYPE", "T")
transmission_file_typ_dq = os.getenv("TRANSMISSION_FILE_TYP_DQ", "DQ")
transmission_file_typ_dr = os.getenv("TRANSMISSION_FILE_TYP_DR", "DR")
transmission_file_type_dq = os.getenv("TRANSMISSION_FILE_TYPE_DQ", "T")
transmission_file_type_dr = os.getenv("TRANSMISSION_FILE_TYPE_DR", "R")

input_sender_id = os.getenv("INPUT_SENDER_ID ", "00990CAREMARK")
output_sender_id = os.getenv("OUTPUT_SENDER_ID ", "00489INGENIORX")
output_receiver_id = os.getenv("OUTPUT_SENDER_ID ", "20500BCIDAHO")
eastern = dateutil.tz.gettz("US/Eastern")
timestamp_file = datetime.datetime.now(tz=eastern).strftime("%y%m%d%H%M%S")
timestamp_process_routing = datetime.datetime.now(tz=eastern).strftime("%m%d%Y%H%M%S")
process_routing_timestamp = output_sender_id + f"{timestamp_process_routing}"
# print(process_routing_timestamp)
timestamp_src_creation = datetime.datetime.now(tz=eastern).strftime("%H%M")

# end of environment variables section

s3_temp_dq_records_file_path = os.path.join(s3_merge_temp_path, s3_merge_dq_detail_file_name)
s3_temp_dr_records_file_path = os.path.join(s3_merge_temp_path, s3_merge_dr_detail_file_name)
s3_temp_dq_header_file_path = os.path.join(s3_merge_temp_path, s3_merge_dq_header_file_name)
s3_temp_dr_header_file_path = os.path.join(s3_merge_temp_path, s3_merge_dr_header_file_name)
s3_temp_dq_trailer_file_path = os.path.join(s3_merge_temp_path, s3_merge_dq_trailer_file_name)
s3_temp_dr_trailer_file_path = os.path.join(s3_merge_temp_path, s3_merge_dr_trailer_file_name)

s3_outbound_file_detail_dq = os.path.join(s3_outbound_temp_path, s3_outbound_dq_details_file_name)
s3_outbound_file_detail_dr = os.path.join(s3_outbound_temp_path, s3_outbound_dr_details_file_name)
s3_outbound_file_header_dq = os.path.join(s3_outbound_temp_path, s3_outbound_dq_header_file_name)
s3_outbound_file_header_dr = os.path.join(s3_outbound_temp_path, s3_outbound_dr_header_file_name)
s3_outbound_file_trailer_dq = os.path.join(s3_outbound_temp_path, s3_outbound_dq_trailer_file_name)
s3_outbound_file_trailer_dr = os.path.join(s3_outbound_temp_path, s3_outbound_dr_trailer_file_name)

s3client = boto3.client("s3")
s3obj = boto3.client("s3")
s3resource = boto3.resource("s3")


def get_secret():
    """
    Fetches the credentials for the Redshift Service
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
    Establishes a connection with Redshift using psycopg2
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


def unload_detail(
    cursor,
    timestamp,
    output_sender,
    output_receiver,
    db_schema,
    table_name,
    input_sender,
    transmission_file_typ,
    s3_out_bucket,
    s3_file_path,
    iam_role,
):
    """Unloads records from redshift detail table to outbound folder detail file in fixedwidth format"""
    unload_detail_query = (
        "unload ('SELECT ''{}'' AS PRCS_ROUT_ID,RECD_TYPE,TRANSMISSION_FILE_TYP,VER_RELEASE_NBR,''{}'' AS SENDER_ID,"
        "''{}'' AS RECEIVER_ID,SUBMISSION_NBR,TRANS_RESP_STS,REJECT_CD,REC_LGTH,RESERVED_SP1,TRANS_DT,TRANS_TS,"
        "DATE_OF_SVIC,SERVICE_PROVIDER_ID_QFR,SERVICE_PROVIDER_ID,DOC_REF_ID_QFR,DOC_REF_ID,TRANSMISSION_ID,"
        "BENEFIT_TYP,IN_NETWORK_IND,FORMULARY_STS,ACCUM_ACTION_CD,SENDER_REFERENCE_NBR,INSURANCE_CD,"
        "ACCUM_BAL_BENEFIT_TYP,BENEFIT_EFF_DT,BENEFIT_TERM_DT,ACCUM_CHG_SRCE_CD,TRANS_ID,TRANS_ID_CROSS_REF,"
        "ADJUSTMENT_REASON_CD,ACCUM_REF_TIME_STMP,RESERVED_SP2,CARDHOLDER_ID,GROUP_ID,PATIENT_FIRST_NM,"
        "MIDDLE_INITIAL,PATIENT_LAST_NM,PATIENT_RELATIONSHIP_CD,DATE_OF_BIRTH,PATIENT_GENDER_CD,"
        "PATIENT_STATE_PROVINCE_ADDR,CARDHOLDER_LAST_NM,CARRIER_NBR,ACCOUNT_NBR,CLIENT_PASS_THROUGH,FAMILY_ID_NBR,"
        "CARDHOLDER_ID_ALTN,GROUP_ID_ALTN,PATIENT_ID,PERSON_CD,RESERVED_SP3,ACCUM_BAL_CNT,ACCUM_SPECIFIC_CATG,"
        "RESERVED_SP4,ACCUM_BAL_QFR_1,ACCUM_NETWORK_IND_1,ACCUM_APPLIED_AMT_1,APPLIED_ACTION_CD_1,"
        "ACCUM_BENEFIT_PD_AMT_1,BENFIT_ACTION_CD_1,ACCUM_REMAINING_BAL_1, "
        "REMAINING_ACTION_CD_1,ACCUM_BAL_QFR_2,ACCUM_NETWORK_IND_2,ACCUM_APPLIED_AMT_2,APPLIED_ACTION_CD_2,"
        "ACCUM_BENEFIT_PD_AMT_2,BENFIT_ACTION_CD_2,ACCUM_REMAINING_BAL_2,REMAINING_ACTION_CD_2,ACCUM_BAL_QFR_3,"
        "ACCUM_NETWORK_IND_3,ACCUM_APPLIED_AMT_3,APPLIED_ACTION_CODE_3,ACCUM_BENEFIT_PD_AMT_3,BENFIT_ACTION_CD_3,"
        "ACCUM_REMAINING_BAL_3,REMAINING_ACTION_CD_3,ACCUM_BAL_QFR_4,ACCUM_NETWORK_IND_4,ACCUM_APPLIED_AMT_4,"
        "APPLIED_ACTION_CD_4,ACCUM_BENEFIT_PD_AMT_4,BENFIT_ACTION_CD_4,ACCUM_REMAINING_BAL_4,REM_ACTION_CD_4,"
        "'' '' AS RESERVED_SP5 from (select *,ROW_NUMBER() over (partition by TRANSMISSION_ID, PATIENT_ID, "
        "DATE_OF_BIRTH, TRANS_ID, ACCUM_SPECIFIC_CATG, ACCUM_BAL_QFR_1, "
        "ACCUM_BAL_QFR_2, ACCUM_BAL_QFR_3, ACCUM_BAL_QFR_4,APPLIED_ACTION_CD_1, APPLIED_ACTION_CD_2, "
        "APPLIED_ACTION_CODE_3, APPLIED_ACTION_CD_4, ACCUM_APPLIED_AMT_1, ACCUM_APPLIED_AMT_2, ACCUM_APPLIED_AMT_3, "
        "ACCUM_APPLIED_AMT_4 order by TRANS_DT) RowNumber FROM {}.{} where sender_id = ''{}'' and "
        "transmission_file_typ =''{}'') a where a.RowNumber = 1 ') "
        "TO 's3://{}/{}' iam_role '{}' "
        "FIXEDWIDTH "
        "'0:200,1:2,2:2,3:2,4:30,5:30,6:4,7:1,8:3,9:5,10:20,11:8,12:8,13:8,14:2,15:15,16:2,17:15,18:50,19:1,20:1,21:1,22:2,23:30,24:20,25:1,26:8,27:8,28:1,29:30,30:30,31:1,32:26,33:13,34:20,35:15,36:25,37:1,38:35,39:1,40:8,41:1,42:2,43:35,44:9,45:15,46:50,47:20,48:20,49:15,50:20,51:3,52:90,53:2,54:2,55:20,56:2,57:1,58:10,59:1,60:10,61:1,62:10,63:1,64:2,65:1,66:10,67:1,68:10,69:1,70:10,71:1,72:2,73:1,74:10,75:1,76:10,77:1,78:10,79:1,80:2,81:1,82:10,83:1,84:10,85:1,86:10,87:1,88:567' "
        "ALLOWOVERWRITE "
        "parallel off;".format(
            timestamp,
            output_sender,
            output_receiver,
            db_schema,
            table_name,
            input_sender,
            transmission_file_typ,
            s3_out_bucket,
            s3_file_path,
            iam_role,
        )
    )
    cursor.query(unload_detail_query)
    # logging.debug(unl_dtl_dq)


# ********SECTION 1 ends**********

# ********SECTION 2**********


def unload_header(
    cursor,
    timestamp,
    transmission_file_type,
    timestamp_src,
    output_sender,
    output_receiver,
    file_type,
    s3_out_bucket,
    s3_file_path,
    iam_role,
):
    """Exports DQ records in outbound folder header file in fixedwidth format"""
    unload_header = (
        "UNLOAD ('select ''{}'' AS PRCS_ROUT_ID,''HD'' AS RECD_TYPE,''{}'' AS TRANSMISSION_FILE_TYP,"
        "to_char(sysdate, ''YYYYMMDD'') AS SRC_CREATE_DT,''{}'' AS SRC_CREATE_TS,"
        "''{}'' AS SENDER_ID,''{}'' AS RECEIVER_ID,''0000001'' AS BATCH_NBR,''{}'' AS FILE_TYP,"
        "''10'' AS VER_RELEASE_NBR,'' '' AS RESERVED_SP') "
        "TO 's3://{}/{}' iam_role '{}' "
        "FIXEDWIDTH "
        "'0:200,1:2,2:1,3:8,4:4,5:30,6:30,7:7,8:1,9:2,10:1415' "
        "ALLOWOVERWRITE "
        "parallel off;".format(
            timestamp,
            transmission_file_type,
            timestamp_src,
            output_sender,
            output_receiver,
            file_type,
            s3_out_bucket,
            s3_file_path,
            iam_role,
        )
    )
    cursor.query(unload_header)
    # logging.debug(unl_hdr)


# ********SECTION 2 ends**********

# ********SECTION 3**********


def unload_trailer(
    cursor,
    timestamp,
    db_schema,
    table_name,
    input_sender,
    transmission_file_typ,
    s3_out_bucket,
    s3_file_path,
    iam_role,
):
    """Unloads records in outbound folder trailer file in fixedwidth format"""
    unload_trailer_query = (
        "UNLOAD ('select ''{}'' AS PRCS_ROUT_ID,''TR'' AS RECD_TYPE,''0000001'' AS BATCH_NBR,lpad(c.cnt,10,''0'') AS "
        "REC_CNT,'' '' AS MSG_TXT,'' ''AS RESERVED_SP from (select cast(count(1) as varchar)cnt from (SELECT * from ("
        "select *,ROW_NUMBER() over (partition by TRANSMISSION_ID, PATIENT_ID, DATE_OF_BIRTH, TRANS_ID, "
        "ACCUM_SPECIFIC_CATG, ACCUM_BAL_QFR_1, ACCUM_BAL_QFR_2, ACCUM_BAL_QFR_3, ACCUM_BAL_QFR_4,APPLIED_ACTION_CD_1, "
        "APPLIED_ACTION_CD_2, APPLIED_ACTION_CODE_3, APPLIED_ACTION_CD_4, ACCUM_APPLIED_AMT_1, ACCUM_APPLIED_AMT_2, "
        "ACCUM_APPLIED_AMT_3, ACCUM_APPLIED_AMT_4 order by TRANS_DT) RowNumber from {}.{} where sender_id = ''{}'' "
        "and transmission_file_typ =''{}'')a where a.RowNumber = 1 )b)c') "
        "TO 's3://{}/{}' iam_role '{}' "
        "FIXEDWIDTH "
        "'0:200,1:2,2:7,3:10,4:80,5:1401' "
        "ALLOWOVERWRITE "
        "parallel off;".format(
            timestamp,
            db_schema,
            table_name,
            input_sender,
            transmission_file_typ,
            s3_out_bucket,
            s3_file_path,
            iam_role,
        )
    )
    cursor.query(unload_trailer_query)
    # logging.debug(unl_tlr_dq)


# ********SECTION 3 ends**********

# ********SECTION 4**********


def merge_records(s3_merge_bucket, records, header, trailer, s3_out_path, s3_out_file_prefix):
    """Merges detail,header and trailer files in outbound folder"""
    approved_objects = s3client.get_object(Bucket=s3_merge_bucket, Key=records)
    approved_details = approved_objects["Body"].read().decode(encoding="utf-8", errors="ignore")
    header_objects = s3client.get_object(Bucket=s3_merge_bucket, Key=header)
    header_details = header_objects["Body"].read().decode(encoding="utf-8", errors="ignore")
    trailer_objects = s3client.get_object(Bucket=s3_merge_bucket, Key=trailer)
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

    if len(detail_val) >= 1:
        result = (
            "\n".join(header_val)
            + "\n"
            + "\n".join(detail_val)
            + "\n"
            + "\n".join(trailer_val)
            + "\n"
        )
    elif len(detail_val) == 0:
        result = "\n".join(header_val) + "\n" + "\n".join(trailer_val) + "\n"

    s3client.put_object(
        Body=result,
        Bucket=s3_merge_bucket,
        Key=os.path.join(s3_out_path, f"{s3_out_file_prefix}{timestamp_file}.txt"),
    )


# ********SECTION 4 ends**********

# ********SECTION 5 begin**********


def cleanup_detail_table(cursor, db_schema, table_name, input_sender):
    """Removes the records from REDHSIFT DTL table before the process starts"""
    truncate = "DELETE FROM {}.{} WHERE sender_id='{}'".format(db_schema, table_name, input_sender)
    cursor.query(truncate)
    # logging.debug(truncate)


# ********SECTION 5 ends**********

# ********SECTION 6 begin**********


def outbound_tempfolder_files_delete(s3resource, s3obj, s3_out_temp_file, s3_in_bucket):
    """Removes the outbound temporary s3 files"""
    s3bucket = s3resource.Bucket(s3_in_bucket)
    for key in s3bucket.objects.filter(Prefix=s3_out_temp_file, Delimiter="/"):
        if ".txt" in str(key):
            s3obj.delete_object(Bucket=s3_in_bucket, Key=key.key)


# ********SECTION 6 ends**********


def main():
    secret = get_secret()
    secret = json.loads(secret)
    redshift_user = secret.get("username")
    # REDSHIFT_USER=" "
    redshift_password = secret.get("password")
    # REDSHIFT_PASSWORD =" "
    redshift_host = secret.get("host")
    # REDSHIFT_HOST ="irx-accumhub.clyedcpeuy3o.us-east-1.redshift.amazonaws.com"
    redshift_port = secret.get("port")
    # REDSHIFT_PORT="5439"
    redshift_database = secret.get("dbname")
    # REDSHIFT_DATABASE ="devdb"
    conn = create_redshift_connection(
        redshift_host, redshift_port, redshift_database, redshift_user, redshift_password
    )
    cursor = conn
    unload_detail(
        cursor,
        process_routing_timestamp,
        output_sender_id,
        output_receiver_id,
        redshift_db_schema,
        redshift_table_name,
        input_sender_id,
        transmission_file_typ_dq,
        s3_accumhub_bucket,
        s3_outbound_file_detail_dq,
        iam_arn,
    )
    logging.info("Unloading detail dq records completed")
    unload_detail(
        cursor,
        process_routing_timestamp,
        output_sender_id,
        output_receiver_id,
        redshift_db_schema,
        redshift_table_name,
        input_sender_id,
        transmission_file_typ_dr,
        s3_accumhub_bucket,
        s3_outbound_file_detail_dr,
        iam_arn,
    )
    logging.info("Unloading detail dr records completed")
    unload_header(
        cursor,
        process_routing_timestamp,
        transmission_file_type_dq,
        timestamp_src_creation,
        output_sender_id,
        output_receiver_id,
        file_type,
        s3_accumhub_bucket,
        s3_outbound_file_header_dq,
        iam_arn,
    )
    logging.info("Unloading header dq records completed")
    unload_header(
        cursor,
        process_routing_timestamp,
        transmission_file_type_dr,
        timestamp_src_creation,
        output_sender_id,
        output_receiver_id,
        file_type,
        s3_accumhub_bucket,
        s3_outbound_file_header_dr,
        iam_arn,
    )
    logging.info("Unloading header dr records completed")
    unload_trailer(
        cursor,
        process_routing_timestamp,
        redshift_db_schema,
        redshift_table_name,
        input_sender_id,
        transmission_file_typ_dq,
        s3_accumhub_bucket,
        s3_outbound_file_trailer_dq,
        iam_arn,
    )
    logging.info("Unloading trailer dq records completed")
    unload_trailer(
        cursor,
        process_routing_timestamp,
        redshift_db_schema,
        redshift_table_name,
        input_sender_id,
        transmission_file_typ_dr,
        s3_accumhub_bucket,
        s3_outbound_file_trailer_dr,
        iam_arn,
    )
    logging.info("Unloading trailer dr records completed")
    merge_records(
        s3_accumhub_bucket,
        s3_temp_dq_records_file_path,
        s3_temp_dq_header_file_path,
        s3_temp_dq_trailer_file_path,
        s3_outbound_path,
        s3_outbound_dq_file_prefix,
    )
    logging.info("Merge approved records completed!")
    merge_records(
        s3_accumhub_bucket,
        s3_temp_dr_records_file_path,
        s3_temp_dr_header_file_path,
        s3_temp_dr_trailer_file_path,
        s3_outbound_path,
        s3_outbound_dr_file_prefix,
    )
    logging.info("Merge rejected records completed!")
    cleanup_detail_table(cursor, redshift_db_schema, redshift_table_name, input_sender_id)
    logging.info("Cleanup completed for CVS data from RedShift Accum Detail Stage Table")
    outbound_tempfolder_files_delete(
        s3resource, s3obj, s3_outbound_temp_file_path, s3_accumhub_bucket
    )
    logging.info("outbound temp folder file clean up process is completed")


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime) s - %(levelname)s:%(message)s", level=logging.DEBUG)
    main()
