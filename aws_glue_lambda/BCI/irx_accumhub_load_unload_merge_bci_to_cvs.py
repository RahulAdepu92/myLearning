"""
Complete and Ready
Author : Rahul Adepu
Description : This Python script will copy s3 detail inbound split file to Redshift "stg_accum_dtl" table
  and unload it to header, detail and trailer output files followed by merging all of them.
  This code resides in a glue job "irx_accumhub_load_unload_merge_bci_to_cvs"
  and is called by lambda "irx_accumhub_SubmitGlueJobLambda_bci_to_cvs".
"""

import boto3
import datetime
import dateutil.tz
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
        "S3_OUTBOUND_PATH",
        "S3_OUTBOUND_DETAIL_FILE_NAME",
        "S3_OUTBOUND_HEADER_FILE_NAME",
        "S3_OUTBOUND_TRAILER_FILE_NAME",
        "REDSHIFT_DB_SCHEMA",
        "REDSHIFT_TABLE",
        "S3_MERGE_DETAIL_FILE_NAME",
        "S3_MERGE_HEADER_FILE_NAME",
        "S3_MERGE_TRAILER_FILE_NAME",
        "S3_MERGE_OUTPUT_PATH",
        "S3_MERGE_OUTPUT_SUFFIX",
        "S3_INBOUND_TRAILER_TEMP_FILE_PATH",
        "S3_INBOUND_DETAIL_TEMP_FILE_PATH",
        "S3_INBOUND_HEADER_TEMP_FILE_PATH",
        "S3_OUTBOUND_TEMP_FILE_PATH",
        "IAM_ARN",
        "ENVIRONMENT",
        "FILE_TYPE",
        "TRANSMISSION_FILE_TYPE",
        "INPUT_SENDER_ID",
        "OUTPUT_SENDER_ID",
        "OUTPUT_RECEIVER_ID",
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
print ("s3_inbound_detail_file  " , s3_inbound_detail_file)
s3_outbound_detail_file = os.path.join(
    args["S3_OUTBOUND_PATH"], args["S3_OUTBOUND_DETAIL_FILE_NAME"]
)
s3_outbound_header_file = os.path.join(
    args["S3_OUTBOUND_PATH"], args["S3_OUTBOUND_HEADER_FILE_NAME"]
)
s3_outbound_trailer_file = os.path.join(
    args["S3_OUTBOUND_PATH"], args["S3_OUTBOUND_TRAILER_FILE_NAME"]
)
s3_merge_detail_file = os.path.join(args["S3_OUTBOUND_PATH"], args["S3_MERGE_DETAIL_FILE_NAME"])
s3_merge_header_file = os.path.join(args["S3_OUTBOUND_PATH"], args["S3_MERGE_HEADER_FILE_NAME"])
s3_merge_trailer_file = os.path.join(args["S3_OUTBOUND_PATH"], args["S3_MERGE_TRAILER_FILE_NAME"])
s3_merge_output_path = args["S3_MERGE_OUTPUT_PATH"]
s3_inbound_trailer_temp_file_path = args["S3_INBOUND_TRAILER_TEMP_FILE_PATH"]
s3_inbound_detail_temp_file_path = args["S3_INBOUND_DETAIL_TEMP_FILE_PATH"]
s3_inbound_header_temp_file_path = args["S3_INBOUND_HEADER_TEMP_FILE_PATH"]
s3_outbound_temp_file_path = args["S3_OUTBOUND_TEMP_FILE_PATH"]
file_type = args["FILE_TYPE"]
transmission_file_type = args["TRANSMISSION_FILE_TYPE"]
input_sender_id = args["INPUT_SENDER_ID"]
output_sender_id = args["OUTPUT_SENDER_ID"]
output_receiver_id = args["OUTPUT_RECEIVER_ID"]

eastern = dateutil.tz.gettz("US/Eastern")
timestamp_file = datetime.datetime.now(tz=eastern).strftime("%y%m%d%H%M%S")
# print(timestamp_file)
timestamp_process_routing = datetime.datetime.now(tz=eastern).strftime("%m%d%Y%H%M%S")
process_routing_timestamp = output_sender_id + f"{timestamp_process_routing}"
# print(process_routing_timestamp)
timestamp_src_creation = datetime.datetime.now(tz=eastern).strftime("%H%M")
s3_merge_file_name = args["S3_MERGE_OUTPUT_SUFFIX"] + f"{timestamp_file}.TXT"

# end of environment variables section


s3client = boto3.client("s3")
s3resource = boto3.resource("s3")


def get_secret():
    """
    Fetches the credentials for the Redshift Service
    :return:
    """
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
    logging.info("Redshift connection is established")
    return conn


# ********SECTION 1**********


def cleanup_detail_table(cursor, db_schema, table_to_clean, input_sender):
    """Removes the records from RedShift DTL table before current process starts"""
    truncate = "DELETE FROM {}.{} WHERE sender_id='{}'".format(
        db_schema, table_to_clean, input_sender
    )
    cursor.query(truncate)
    # logging.debug(truncate)


def load_detail_table(cursor, db_schema, detail_table, s3_in_bucket, s3_in_detail_file, iam_role):
    """Loads detail file records to RedShift detail table"""
    copy_detail_query = (
        "COPY {}.{} FROM 's3://{}/{}' "
        "iam_role '{}' "
        "delimiter '|';".format(db_schema, detail_table, s3_in_bucket, s3_in_detail_file, iam_role)
    )
    print ("copy_detail_query  " , copy_detail_query)
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
    """Removes the inbound temporary s3 files"""
    folder_list = [s3_in_trailer_temp_file, s3_in_detail_temp_file, s3_in_header_temp_file]

    s3bucket = s3resource.Bucket(s3_in_bucket)
    for folder_name in folder_list:
        for key in s3bucket.objects.filter(Prefix=folder_name, Delimiter="/"):
            if str(key).lower().endswith(".txt"):
                s3obj.delete_object(Bucket=s3_in_bucket, Key=key.key)


# ********SECTION 1 ends**********

# ********SECTION 2**********


def unload_detail_table(
    cursor,
    timestamp,
    output_sender,
    output_receiver,
    db_schema,
    detail_table,
    input_sender,
    s3_out_bucket,
    s3_out_detail_file,
    iam_role,
):
    """Unloads records from RedShift detail table to outbound folder detail file in fixed-width format"""
    unload_detail_query = (
        "UNLOAD ('SELECT ''{}'' AS PRCS_ROUT_ID,RECD_TYPE,TRANSMISSION_FILE_TYP,VER_RELEASE_NBR,''{}'' AS SENDER_ID,"
        "''{}'' AS RECEIVER_ID,SUBMISSION_NBR,TRANS_RESP_STS,REJECT_CD,REC_LGTH,RESERVED_SP1,TRANS_DT,TRANS_TS,"
        "DATE_OF_SVIC,SERVICE_PROVIDER_ID_QFR,SERVICE_PROVIDER_ID,DOC_REF_ID_QFR,DOC_REF_ID,TRANSMISSION_ID,"
        "BENEFIT_TYP,IN_NETWORK_IND,FORMULARY_STS,ACCUM_ACTION_CD,SENDER_REFERENCE_NBR,INSURANCE_CD,"
        "ACCUM_BAL_BENEFIT_TYP,BENEFIT_EFF_DT,BENEFIT_TERM_DT,ACCUM_CHG_SRCE_CD,TRANS_ID,TRANS_ID_CROSS_REF,"
        "ADJUSTMENT_REASON_CD,ACCUM_REF_TIME_STMP,RESERVED_SP2,CARDHOLDER_ID,GROUP_ID,PATIENT_FIRST_NM,"
        "MIDDLE_INITIAL,PATIENT_LAST_NM,PATIENT_RELATIONSHIP_CD,DATE_OF_BIRTH,PATIENT_GENDER_CD,"
        "PATIENT_STATE_PROVINCE_ADDR,CARDHOLDER_LAST_NM,CARRIER_NBR,ACCOUNT_NBR,CLIENT_PASS_THROUGH,FAMILY_ID_NBR,"
        "CARDHOLDER_ID_ALTN,GROUP_ID_ALTN,PATIENT_ID,PERSON_CD,RESERVED_SP3,ACCUM_BAL_CNT,ACCUM_SPECIFIC_CATG,"
        "RESERVED_SP4,ACCUM_BAL_QFR_1,ACCUM_NETWORK_IND_1,ACCUM_APPLIED_AMT_1,APPLIED_ACTION_CD_1,"
        "ACCUM_BENEFIT_PD_AMT_1,BENFIT_ACTION_CD_1,ACCUM_REMAINING_BAL_1,REMAINING_ACTION_CD_1,ACCUM_BAL_QFR_2,"
        "ACCUM_NETWORK_IND_2,ACCUM_APPLIED_AMT_2,APPLIED_ACTION_CD_2,ACCUM_BENEFIT_PD_AMT_2,BENFIT_ACTION_CD_2,"
        "ACCUM_REMAINING_BAL_2,REMAINING_ACTION_CD_2,ACCUM_BAL_QFR_3,ACCUM_NETWORK_IND_3,ACCUM_APPLIED_AMT_3,"
        "APPLIED_ACTION_CODE_3,ACCUM_BENEFIT_PD_AMT_3,BENFIT_ACTION_CD_3,ACCUM_REMAINING_BAL_3,REMAINING_ACTION_CD_3,"
        "ACCUM_BAL_QFR_4,ACCUM_NETWORK_IND_4,ACCUM_APPLIED_AMT_4,APPLIED_ACTION_CD_4,ACCUM_BENEFIT_PD_AMT_4,"
        "BENFIT_ACTION_CD_4,ACCUM_REMAINING_BAL_4,REM_ACTION_CD_4,'' '' as ACCUM_BAL_QFR_5,'' '' as "
        "ACCUM_NETWORK_IND_5,''0000000000'' as ACCUM_APPLIED_AMT_5,'' '' as APPLIED_ACTION_CD_5,''0000000000'' as "
        "ACCUM_BENEFIT_PD_AMT_5,'' '' as BENFIT_ACTION_CD_5,''0000000000'' as ACCUM_REMAINING_BAL_5,"
        "'' '' as REMAINING_ACTION_CD_5,'' '' as ACCUM_BAL_QFR_6,'' '' as ACCUM_NETWORK_IND_6,"
        "''0000000000'' as ACCUM_APPLIED_AMT_6,'' '' as APPLIED_ACTION_CD_6,''0000000000'' as ACCUM_BENEFIT_PD_AMT_6,"
        "'' '' as BENFIT_ACTION_CD_6,''0000000000'' as ACCUM_REMAINING_BAL_6,'' '' as REMAINING_ACTION_CD_6,'' '' as RESERVED_SP5, "
        "'' '' as ACCUM_BAL_QFR_7,'' '' as ACCUM_NETWORK_IND_7,''0000000000'' as ACCUM_APPLIED_AMT_7,"
        "'' '' as APPLIED_ACTION_CD_7,''0000000000'' as ACCUM_BENEFIT_PD_AMT_7,'' '' as BENFIT_ACTION_CD_7,"
        "''0000000000'' as ACCUM_REMAINING_BAL_7,'' '' as REMAINING_ACTION_CD_7,'' '' as ACCUM_BAL_QFR_8,"
        "'' '' as ACCUM_NETWORK_IND_8,''0000000000'' as ACCUM_APPLIED_AMT_8,'' '' as APPLIED_ACTION_CD_8,"
        "''0000000000'' as ACCUM_BENEFIT_PD_AMT_8,'' '' as BENFIT_ACTION_CD_8,''0000000000'' as ACCUM_REMAINING_BAL_8,"
        "'' '' as REMAINING_ACTION_CD_8,'' '' as ACCUM_BAL_QFR_9,'' '' as ACCUM_NETWORK_IND_9,"
        "''0000000000'' as ACCUM_APPLIED_AMT_9,'' '' as APPLIED_ACTION_CD_9,''0000000000'' as ACCUM_BENEFIT_PD_AMT_9,"
        "'' '' as BENFIT_ACTION_CD_9,''0000000000'' as ACCUM_REMAINING_BAL_9,'' '' as REMAINING_ACTION_CD_9,"
        "'' '' as ACCUM_BAL_QFR_10,'' '' as ACCUM_NETWORK_IND_10,''0000000000'' as ACCUM_APPLIED_AMT_10,"
        "'' '' as APPLIED_ACTION_CD_10,''0000000000'' as ACCUM_BENEFIT_PD_AMT_10,'' '' as BENFIT_ACTION_CD_10,"
        "''0000000000'' as ACCUM_REMAINING_BAL_10,'' '' as REMAINING_ACTION_CD_10,'' '' as ACCUM_BAL_QFR_11,"
        "'' '' as ACCUM_NETWORK_IND_11,''0000000000'' as ACCUM_APPLIED_AMT_11,'' '' as APPLIED_ACTION_CD_11,"
        "''0000000000'' as ACCUM_BENEFIT_PD_AMT_11,'' '' as BENFIT_ACTION_CD_11,''0000000000'' as ACCUM_REMAINING_BAL_11,"
        "'' '' as REMAINING_ACTION_CD_11,'' '' as ACCUM_BAL_QFR_12,'' '' as ACCUM_NETWORK_IND_12,"
        "''0000000000'' as ACCUM_APPLIED_AMT_12,'' '' as APPLIED_ACTION_CD_12,''0000000000'' as ACCUM_BENEFIT_PD_AMT_12,"
        "'' '' as BENFIT_ACTION_CD_12,''0000000000'' as ACCUM_REMAINING_BAL_12,'' '' as REMAINING_ACTION_CD_12,"
        "'' '' as Optional_Data_Indicator,''0000000000'' as Total_Amount_Paid,"
        "'' '' as Action_Code,''0000000000'' as Amount_of_Copay,'' '' as Action_Code,''0000000000'' as "
        "Patient_Pay_Amount,'' '' as Action_Code,''0000000000'' as Amount_Attributed_to_Product_Selection_Brand,"
        "'' '' as Action_Code,''0000000000'' as Amount_Attributed_to_Sales_Tax,'' '' as Action_Code,''0000000000'' as "
        "Amount_Attributed_to_Processor_Fee,'' '' as Action_Code,''0000000000'' as Gross_Amount_Due,"
        "'' '' as Action_Code,''0000000000'' as Invoiced_Amount,'' '' as Action_Code,''0000000000'' as "
        "Penalty_Amount,'' '' as Action_Code,'' '' as Reserved_SP6,'' '' as Product_Service_ID_Qualifier,"
        "'' '' as Product_Service_ID,''000'' as Days_Supply,''0000000000'' as Quantity_Dispensed,"
        "'' '' as Product_Service_Name,'' '' as Brand_Generic_Indicator,'' '' as Therapeutic_Class_Code_Qualifier,"
        "'' '' as Therapeutic_Class_Code,'' '' as Dispensed_As_Written_DAW_Product_Selection,'' '' as Reserved_SP7 "
        "FROM {}.{} WHERE "
        "sender_id=''{}'' ') "
        "TO 's3://{}/{}' iam_role '{}' "
        "FIXEDWIDTH "
        "'0:200,1:2,2:2,3:2,4:30,5:30,6:4,7:1,8:3,9:5,10:20,11:8,12:8,13:8,14:2,15:15,16:2,17:15,18:50,19:1,20:1,21:1,22:2,23:30,24:20,25:1,26:8,27:8,28:1,29:30,30:30,31:1,32:26,33:13,34:20,35:15,36:25,37:1,38:35,39:1,40:8,41:1,42:2,43:35,44:9,45:15,46:50,47:20,48:20,49:15,50:20,51:3,52:90,53:2,54:2,55:20,56:2,57:1,58:10,59:1,60:10,61:1,62:10,63:1,64:2,65:1,66:10,67:1,68:10,69:1,70:10,71:1,72:2,73:1,74:10,75:1,76:10,77:1,78:10,79:1,80:2,81:1,82:10,83:1,84:10,85:1,86:10,87:1,88:2,89:1,90:10,91:1,92:10,93:1,94:10,95:1,96:2,97:1,98:10,99:1,100:10,101:1,102:10,103:1,104:24,105:2,106:1,107:10,108:1,109:10,110:1,111:10,112:1,113:2,114:1,115:10,116:1,117:10,118:1,119:10,120:1,121:2,122:1,123:10,124:1,125:10,126:1,127:10,128:1,129:2,130:1,131:10,132:1,133:10,134:1,135:10,136:1,137:2,138:1,139:10,140:1,141:10,142:1,143:10,144:1,145:2,146:1,147:10,148:1,149:10,150:1,151:10,152:1,153:1,154:10,155:1,156:10,157:1,158:10,159:1,160:10,161:1,162:10,163:1,164:10,165:1,166:10,167:1,168:10,169:1,170:10,171:1,172:23,173:2,174:19,175:3,176:10,177:30,178:1,179:1,180:17,181:1,182:48' "
        "ALLOWOVERWRITE "
        "parallel off;".format(
            timestamp,
            output_sender,
            output_receiver,
            db_schema,
            detail_table,
            input_sender_id,
            s3_out_bucket,
            s3_out_detail_file,
            iam_role,
        )
    )
    cursor.query(unload_detail_query)
    # logging.debug(unl_dtl)


# ********SECTION 2 ends**********

# ********SECTION 3**********


def unload_header_table(
    cursor,
    timestamp,
    transmission_file_type,
    timestamp_src,
    output_sender,
    output_receiver,
    s3_out_bucket,
    s3_out_header_file,
    iam_role,
):
    """Exports records to outbound folder to a header file in fixed-width format"""
    unload_header_query = (
        "UNLOAD ('select ''{}'' AS PRCS_ROUT_ID,''HD'' AS RECD_TYPE,''{}'' AS TRANSMISSION_FILE_TYP,to_char(sysdate, "
        "''YYYYMMDD'') AS SRC_CREATE_DT,''{}'' AS SRC_CREATE_TS,''{}'' AS SENDER_ID,"
        "''{}'' AS RECEIVER_ID,''0000001'' AS BATCH_NBR,''{}'' AS FILE_TYP,''10'' AS VER_RELEASE_NBR,"
        "'' '' AS RESERVED_SP') "
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
            s3_out_header_file,
            iam_role,
        )
    )
    cursor.query(unload_header_query)
    # logging.debug(unl_hdr)


# ********SECTION 3 ends**********

# ********SECTION 4**********


def unload_trailer_table(
    cursor,
    timestamp,
    db_schema,
    trailer_table,
    input_sender,
    s3_out_bucket,
    s3_out_trailer_file,
    iam_role,
):
    """Exports records to outbound folder to a trailer file in fixed-width format"""
    unload_trailer_query = (
        "UNLOAD ('select ''{}'' AS PRCS_ROUT_ID,''TR'' AS RECD_TYPE,''0000001'' AS BATCH_NBR,lpad(a.cnt,10,''0'') AS "
        "REC_CNT,'' '' AS MSG_TXT,'' ''AS RESERVED_SP from (select cast(count(1) as varchar)cnt from {}.{} WHERE "
        "sender_id=''{}'')a ') "
        "TO 's3://{}/{}' iam_role '{}' "
        "FIXEDWIDTH "
        "'0:200,1:2,2:7,3:10,4:80,5:1401' "
        "ALLOWOVERWRITE "
        "parallel off;".format(
            timestamp,
            db_schema,
            trailer_table,
            input_sender,
            s3_out_bucket,
            s3_out_trailer_file,
            iam_role,
        )
    )
    cursor.query(unload_trailer_query)
    # logging.debug(unl_tlr)


# ********SECTION 4 ends**********

# ********SECTION 5**********


def files_merge(merge_bucket, merge_detail_file, merge_header_file, merge_trailer_file):
    """Merges detail, header and trailer files in outbound folder"""
    approved_objects = s3client.get_object(Bucket=merge_bucket, Key=merge_detail_file)
    approved_details = approved_objects["Body"].read().decode(encoding="utf-8", errors="ignore")
    header_objects = s3client.get_object(Bucket=merge_bucket, Key=merge_header_file)
    header_details = header_objects["Body"].read().decode(encoding="utf-8", errors="ignore")
    trailer_objects = s3client.get_object(Bucket=merge_bucket, Key=merge_trailer_file)
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

    # s3_merge_output_path, s3_merge_detail_file_name are variables from out of scope
    s3client.put_object(
        Body=result, Bucket=merge_bucket, Key=os.path.join(s3_merge_output_path, s3_merge_file_name)
    )


# ********SECTION 5 ends**********

# ********SECTION 6 begin**********


def outbound_tempfolder_files_delete(s3resource, s3obj, s3_out_temp_file, s3_in_bucket):
    """Removes the outbound temporary s3 files"""
    s3bucket = s3resource.Bucket(s3_in_bucket)
    for key in s3bucket.objects.filter(Prefix=s3_out_temp_file, Delimiter="/"):
        if str(key).lower().endswith(".txt"):
            s3obj.delete_object(Bucket=s3_in_bucket, Key=key.key)


# ********SECTION 6 ends**********


def main():
    secret = get_secret()
    # cursor = None
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
    cleanup_detail_table(cursor, redshift_db_schema, redshift_table, input_sender_id)
    logging.info("cleanup completed for detail table")
    load_detail_table(
        cursor,
        redshift_db_schema,
        redshift_table,
        s3_accumhub_bucket,
        s3_inbound_detail_file,
        iam_arn,
    )
    logging.info("loading completed for detail table")
    unload_detail_table(
        cursor,
        process_routing_timestamp,
        output_sender_id,
        output_receiver_id,
        redshift_db_schema,
        redshift_table,
        input_sender_id,
        s3_accumhub_bucket,
        s3_outbound_detail_file,
        iam_arn,
    )
    logging.info("unloading completed for detail table")
    inbound_tempfolder_files_delete(
        s3_accumhub_bucket,
        s3_inbound_trailer_temp_file_path,
        s3_inbound_detail_temp_file_path,
        s3_inbound_header_temp_file_path,
        iam_arn,
        s3resource,
        s3client,
    )
    logging.info("inbound temp folder file clean up process is completed")
    unload_header_table(
        cursor,
        process_routing_timestamp,
        transmission_file_type,
        timestamp_src_creation,
        output_sender_id,
        output_receiver_id,
        s3_accumhub_bucket,
        s3_outbound_header_file,
        iam_arn,
    )
    logging.info("unloading completed for header table")
    unload_trailer_table(
        cursor,
        process_routing_timestamp,
        redshift_db_schema,
        redshift_table,
        input_sender_id,
        s3_accumhub_bucket,
        s3_outbound_trailer_file,
        iam_arn,
    )
    logging.info("unloading completed for trailer table")
    files_merge(
        s3_accumhub_bucket, s3_merge_detail_file, s3_merge_header_file, s3_merge_trailer_file
    )
    logging.info("Merging completed for cvs file.")
    outbound_tempfolder_files_delete(
        s3resource, s3client, s3_outbound_temp_file_path, s3_accumhub_bucket
    )
    logging.info("outbound temp folder file clean up process is completed")


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime) s - %(levelname)s:%(message)s", level=logging.DEBUG)
    main()
