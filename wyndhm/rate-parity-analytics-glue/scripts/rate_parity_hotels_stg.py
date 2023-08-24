import json
import pytz
import logging
import os
import sys
import boto3
import datetime
import time
import re
from datetime import datetime
from datetime import date
from datetime import timedelta
from dateutil import tz
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.functions import lit, substring, col, upper, trim, md5, concat_ws, when, to_date, round, date_format, \
    instr, length, expr, input_file_name, udf, regexp_replace, regexp_extract, split, isnull
from pyspark.sql.types import TimestampType, DoubleType, StructType, StructField, StringType, IntegerType, LongType, \
    FloatType, DecimalType, DateType, ArrayType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
job_name = args['JOB_NAME']

################################### Enabling access to the arguments ##################################
job = Job(glueContext)
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME', 'ScriptBucketName', 'source_bucket', 'blue_bucket', 'gold_bucket', 'sns', 'rsdb',
                           'redshiftconnstage', 'redshiftconndw', 'stagetablename', 'rs_schema_name_dw', 'audittable',
                           'errortable', 'load_error_view', 'env', 'schemanamestg'])
job.init(args['JOB_NAME'], args)

rsdb = args['rsdb']
redshiftconnstage = args['redshiftconnstage']
redshiftconndw = args['redshiftconndw']
source_bucket = args['source_bucket']
blue_bucket = args['blue_bucket']
gold_bucket = args['gold_bucket']
ScriptBucketName = args['ScriptBucketName']
stg_table_name = args['stagetablename']
errortable = args['errortable']
audittable = args['audittable']
rs_schema_name_dw = args['rs_schema_name_dw']
schemanamestg = args['schemanamestg']
env = args['env']
sns_Notify = args['sns']
job_run = True
load_error_view = args['load_error_view']
s3_file_read_path = "otai/disparity/summary/parity-analytics/hotels/"

################################### Setting up logger for event logging ##################################
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

################################
loadtime_utc = datetime.now()
loadtime_utc_str = loadtime_utc.strftime("%Y-%m-%d-%H-%M-%S")
from_zone = tz.tzutc()
to_zone = tz.gettz('US/Eastern')
newloadtime = loadtime_utc.replace(tzinfo=from_zone)
s3upload_local = newloadtime.astimezone(to_zone)
s3upload_local_str = s3upload_local.strftime("%Y-%m-%d %H:%M:%S")
s3upload_local_string = s3upload_local.strftime("%Y%m%d%H%M%S")

datesplit = s3upload_local_str.split("-")
# print(datesplit)
YR = datesplit[0]
MO = datesplit[1]
DY = datesplit[2].split(" ")[0]
datepath = YR + '-' + MO + '-' + DY
redshift_tmp_dir = f"s3://{blue_bucket}/otai/disparity/summary/parity-analytics/redshift/dataload/{job_name}/{s3upload_local_string}/"  # this is used as dummy parameter when load is skipped for any reason
s3_filename = ""

s3 = boto3.resource('s3')
s3Client = boto3.client('s3', region_name='us-west-2')
s3resource = boto3.resource('s3', region_name='us-west-2')
glue = boto3.client('glue')

try:
    sns_client = boto3.client('sns', region_name='us-west-2')
    logger.info("Enabled sns Notification")
except Exception as e:
    logger.error(str(e))
    logger.info(" Unable to enable sns, failed with error: {}".format(str(e)))
    logger.info("Continuing with load without sns notification")


def notifymsg(sub, msg):
    sns_client.publish(TopicArn=sns_Notify, Message=msg, Subject=sub)
    logger.info("**************** [INFO] sns Notification Sent: {} *************************".format(job_name))


############ Retrieving connection information from Glue connections for Redshift DW  #############
try:
    logger.info("Getting Redshift Connection Information for DW")
    rs_connection_dest = glueContext.extract_jdbc_conf(redshiftconndw)
    rs_url_dest = rs_connection_dest["url"]
    rs_user_dest = rs_connection_dest["user"]
    rs_pwd_dest = rs_connection_dest["password"]
    rs_url_db_dest = rs_url_dest + '/' + rsdb
except Exception as e:
    f_msg = f" Unable to connect to Redshift while processing the glue job {job_name} , failed with error: {str(e)}  "
    f_sub = f"Rate parity Summary Hotels Glue job {job_name} failed"
    notifymsg(f_sub, f_msg)
    logger.info(f" Exiting with error: {str(e)}")
    raise SystemExit(e)

# ############ Retrieving connection information from Glue connections for Redshift DWSTG  #############
try:
    logger.info("Getting Redshift Connection Information for stage")
    redshiftconnstage_dest = glueContext.extract_jdbc_conf(redshiftconnstage)
    rs_url_stg_dest = redshiftconnstage_dest["url"]
    rs_user_stg_dest = redshiftconnstage_dest["user"]
    rs_pwd_stg_dest = redshiftconnstage_dest["password"]
    rs_url_db_stg_dest = rs_url_stg_dest + '/' + rsdb
except Exception as e:
    f_msg = f" Unable to connect to Redshift while processing the glue job {job_name} , failed with error: {str(e)}  "
    f_sub = f"Rate parity Summary Hotels Glue job {job_name} failed"
    notifymsg(f_sub, f_msg)
    logger.info(f" Exiting with error: {str(e)}")
    raise SystemExit(e)

########################### Read Rate Parity Summary Hotels s3 file ###################################

def extract_basename(file_path):
    return os.path.basename(file_path)


def extract_date(filename):
    return filename[filename.find("_"):filename.find(".csv")].replace("_", "")


udf_extract_basename = udf(extract_basename, StringType())
udf_extract_date = udf(extract_date,StringType())  # get the arrival date from name of the stage file created via lambda process (
# ex: hotels_20230531.csv)


def search_s3_bucket(S3_BUCKET_NAME, prefix, suffix):
    result = []
    paginator = s3Client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix)
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(suffix):
                result.append(key)
    return result


def create_timestamp_est():
    utc_now = datetime.now()
    est = pytz.timezone('US/Eastern')
    est_now = utc_now.astimezone(est).strftime('%Y-%m-%d %H:%M:%S.%f')
    return est_now


########################### SQL for prcs_ctrl_log table ###################################

def auditload(redshift_tmp_dir, fileName, filePath, create_ts_val, file_count):
    logger.info("Finding any rejected data in stl_load_errors_view table based on filename and and the latest starttime..")
    rs_error_count = 0
    try:
        rs_error_sql = "select distinct colname,filename,line_number,raw_field_value,err_reason from {0} " \
                       "where filename like '%{1}%' and starttime in (select max(starttime) from {0} where filename " \
                       "like '%{1}%') " \
            .format(schemanamestg + "." + load_error_view, redshift_tmp_dir)

        rs_error_df = spark.read.format("com.databricks.spark.redshift").option("url",
                                                                                rs_url_db_dest + '?user=' +
                                                                                rs_user_dest + '&password=' +
                                                                                rs_pwd_dest) \
            .option("query", rs_error_sql) \
            .option("forward_spark_s3_credentials", "true") \
            .option("tempdir",
                    f"s3://{blue_bucket}/otai/disparity/summary/parity-analytics/temp/glue/errortable/{job_name}/") \
            .load()

        rs_error_count = rs_error_df.count()
        # rs_error_df.show()
        if rs_error_count != 0:
            logger.info("Count of errored records: " + str(rs_error_count))
            msg = f"{rs_error_count} error record(s) found for the file '{filePath}'. " \
                  f"Refer {schemanamestg}.{errortable} table for error details.."
            sub = "Error Report for Glue job {0} {1}".format(job_name, env)
            notifymsg(sub, msg)

        ########################### Mark the status in case to Failed if the count is zero for otherwise mark it as
        # Completed  ###################################
        rs_load_count = (file_count - rs_error_count)
        if rs_load_count == 0:
            src_sts = 'F'
        else:
            src_sts = 'C'

    except Exception as e:
        logger.error(str(e))
        logger.error(
            "**************** [ERROR] Error while fetching count from Redshift, failed with : {} ***********".format(
                str(e)))
        f_msg = " Error while fetching count from Redshift for the job {0} and the file {1}, failed with error: {2}  " \
                "".format(job_name, filePath, str(e))
        f_sub = "Rate Parity Summary Hotels Reject record alert: Glue Job - " + job_name + " failed"
        notifymsg(f_sub, f_msg)
        exit(1)
    ########################## Insert the error records to Stg_glue_error. This is used to track the records that
    # failed to load to  table #####################################

    if rs_error_count != 0:
        try:
            rs_error_df.createOrReplaceTempView("rs_error_temp")

            rs_error_load_sql = spark.sql(""" select 'OTAI' as sub_area, 'rate_parity_summary_hotels' as prcs_nm, '{0}' as feed_nm, 
            '{1}' as file_pth, 'Inbound' as feed_typ, filename as err_file_nm, cast(line_number as string) as file_line_num,
             colname as err_clumn_nm, raw_field_value as actl_field_value, err_reason as err_rsn, cast('{2}' as timestamp) as create_ts,
             'Glue' as create_usr from rs_error_temp """.format(fileName, filePath, create_ts_val))
            rs_error_load_dyf = DynamicFrame.fromDF(rs_error_load_sql, glueContext, "rs_error_load_dyf")

            rs_error_load = glueContext.write_dynamic_frame.from_jdbc_conf(frame=rs_error_load_dyf,
                                                                           catalog_connection=redshiftconnstage,
                                                                           connection_options={
                                                                               "url": rs_url_db_stg_dest,
                                                                               "database": rsdb,
                                                                               "user": rs_user_stg_dest,
                                                                               "password": rs_pwd_stg_dest,
                                                                               "dbtable": schemanamestg + '.' + errortable},
                                                                           redshift_tmp_dir=f"s3://{blue_bucket}/otai/disparity/summary/parity-analytics/redshift/errorload/{job_name}/")
            logger.info("Error data load complete!!")
        except Exception as e:
            logger.error(str(e))
            logger.error(
                "**************** [ERROR] Error while loading to stg_glue_error table : {} ***********".format(str(e)))
            f_msg = "Error while loading to stg_glue_error for the job {0} and the file {1}, failed with error: {2}  ".format(
                job_name, filePath, str(e))
            f_sub = "Rate Parity Summary Hotels Redshift record Process Alert: Glue Job - " + job_name + " failed"
            notifymsg(f_sub, f_msg)
            exit(1)

    ########################## Insert the error records to prcs_crl_log. This is used to track the file loading  to  table #####################################
    logger.info("Getting audit data to load into prcs_cntrl_log table..")
    try:
        s3upload_local = create_timestamp_est()
        audit_df = sqlContext.createDataFrame(
            [('OTAI', 'rate_parity_summary_hotels', fileName, filePath, 'Inbound', file_count, src_sts, 'Glue', s3upload_local,
              rs_error_count, s3upload_local)],
            ["sub_area", "prcs_nm", "feed_nm", "file_pth", "feed_typ", "tot_cnt", "src_sts", "create_by",
             "src_insert_ts", "src_fail_cnt", "prcs_strt_ts"])
        # audit_df.show()
        audit_df.createTempView("auditview")
        audit_df2 = spark.sql(
            """select cast(sub_area as string) as sub_area,cast(prcs_nm as string) as prcs_nm, cast(feed_nm as string) as feed_nm ,
            cast (file_pth as string )  as file_pth, cast (feed_typ as string )  as feed_typ,cast(tot_cnt as integer ) as tot_cnt,
            cast(tot_cnt as integer ) as src_sucss_cnt,cast(src_fail_cnt as integer ) as src_fail_cnt,cast(src_sts as string ) as src_sts,
             cast(create_by as string) as create_by,cast(src_insert_ts as timestamp) as src_insert_ts, cast(prcs_strt_ts as timestamp) as prcs_strt_ts from auditview """)
        audit_dyf = DynamicFrame.fromDF(audit_df2, glueContext, "audit_dyf")
        # audit_dyf.printSchema()
        # audit_dyf.show()
        # print(rs_url_db_stg_dest, rs_pwd_dest, schemanamestg, redshiftconnstage)
        audit_load = glueContext.write_dynamic_frame.from_jdbc_conf(frame=audit_dyf,
                                                                    catalog_connection=redshiftconnstage,
                                                                    connection_options={"url": rs_url_db_stg_dest,
                                                                                        "database": rsdb,
                                                                                        "user": rs_user_stg_dest,
                                                                                        "password": rs_pwd_stg_dest,
                                                                                        "dbtable": schemanamestg + '.' + audittable,
                                                                                        "extracopyoptions": "MAXERROR 100"},
                                                                    redshift_tmp_dir=f"s3://{blue_bucket}/otai/disparity/summary/parity-analytics/redshift/auditload/{job_name}/")

        spark.catalog.dropTempView("auditview")
        logger.info("Audit data load complete!!")

    except Exception as e:
        logger.error(str(e))
        logger.error(
            "**************** [ERROR] Error while loading to prcs_ctrl_log table : {} ***********".format(str(e)))
        f_msg = "Error while fetching count from Redshift for the file {0}, failed with error: {1}  ".format(fileName,
                                                                                                               str(e))
        f_sub = "Rate Parity Summary Hotels Glue job " + job_name + " failed"
        notifymsg(f_sub, f_msg)
        exit(1)


def move_file_within_s3_bucket(src_bucket_name, src_key, dest_bucket_name, dest_key):
    try:
        src_bucket = s3.Bucket(src_bucket_name)
        src_obj = src_bucket.Object(src_key)
        dest_bucket = s3.Bucket(dest_bucket_name)
        dest_obj = dest_bucket.Object(dest_key)

        # Copy the object to the new path
        dest_obj.copy_from(CopySource={"Bucket": src_bucket_name, "Key": src_key})
        # Delete the original object
        src_obj.delete()
        logger.info(f"Successfully moved 's3://{src_bucket_name}/{src_key}' to 's3://{dest_bucket_name}/{dest_key}'")

    except Exception as e:
        logger.error(f"Error moving 's3://{src_bucket_name}/{src_key}' to 's3://{dest_bucket_name}/{dest_key}'. Error is: {e}")
        sns_Notify(f"Error moving '{src_key}' to '{dest_key}'. Error Message: {e}  {job_name} Failed")
        raise SystemExit(e)


#################################### get_input_s3_path ##########################################
# This function will frame the input s3 path.
# By default, it will take all the files in the current date
def get_input_s3_path():
    s3_path = []
    key_list = []
    s3_prefix = s3_file_read_path  # path prefix from where the file has to be picked.
    s3_path.append(s3_prefix)
    rt_suffix = '.csv'
    obj = search_s3_bucket(source_bucket, s3_prefix, rt_suffix)
    logger.info(f"object(s) found: {obj}")
    if len(obj) < 1:
        logger.error("*********** File Not Found ***********")
        f_msg = f"Rate Parity Summary Hotels file not received for '{datepath}'"
        f_sub = f"Rate Parity Summary Hotels file not received for '{datepath}'"
        notifymsg(f_sub, f_msg)
        raise SystemExit("src file not found!")
    else:
        for path in s3_path:
            result = s3Client.list_objects(Bucket=source_bucket, Prefix=path)
            # print(result)
            if result.get("Contents") is not None:
                for obj in result.get("Contents"):
                    key = obj["Key"]
                    # print(key)
                    key_lower = key.lower()
                    if key_lower.endswith(".csv"):
                        key_list.append("s3://" + source_bucket + "/" + key)
        logger.info(f"List of files in the S3 Path: {key_list}")
    return key_list


create_ts = create_timestamp_est()
try:
    logger.info("Reading S3 file..")
    input_datasource_s3 = get_input_s3_path()
    if len(input_datasource_s3) != 0:
        # checking for empty file if any..
        for file in input_datasource_s3:
            src_filename = extract_basename(file)
            src_file_key = f"{s3_file_read_path}{src_filename}"
            dest_file_key = f"otai/disparity/summary/archive/parity-analytics/{src_filename}"
            response = s3Client.get_object(Bucket=gold_bucket, Key=src_file_key)
            content = response['Body'].read().decode(encoding="ascii", errors="ignore")
            data = content.splitlines()  # splitting each line into string and making a list

            if len(data) <= 1 or (len(data) > 1 and len(data[1].split(",")) <= 1):  # empty file/sent with just header only
                f_msg = f" Empty file received for Rate Parity Summary Hotels for '{datepath}' "
                f_sub = f" Empty file received for Rate Parity Summary Hotels for '{datepath}' "
                notifymsg(f_sub, f_msg)
                # moving that empty file to archive folder. Else, that file will never be moved from that place.
                move_file_within_s3_bucket(source_bucket, src_file_key, source_bucket, dest_file_key)
                raise SystemExit("empty file received")

        # considering the file for processing if it is valid/not empty..
        hotels_s3_df = glueContext.create_dynamic_frame.from_options(connection_type="s3",
                                                                     connection_options={
                                                                         "paths": input_datasource_s3,
                                                                         'recurse': True},
                                                                     format="csv",
                                                                     format_options={
                                                                         'withHeader': True,
                                                                         "separator": ","
                                                                     }, transformation_ctx='hotels_s3_read_dyf')
        hotels_s3_read_dyf = hotels_s3_df.toDF()
        hotels_s3_read_dyf.printSchema()
        create_ts = create_timestamp_est()
        # filename = os.path.basename(s3_filename) # gets only the file name
        # file_arrival_dt = filename[filename.find("_"):filename.find(".csv")].replace("_", "")
        hotels_df = hotels_s3_read_dyf \
            .withColumn('htl_nm', when(col("Hotel").rlike("^\s*$"), None).
                        otherwise(col("Hotel"))) \
            .withColumn('site_id', when(col("Brand code").rlike("^\s*$"), None).
                        otherwise(lpad(col("Brand code"),5,'0'))) \
            .withColumn('cntry_cd', when(col("Country").rlike("^\s*$"), None).
                        otherwise(col("Country"))) \
            .withColumn('loss_pct', when(col("Loss %").rlike("^\s*$"), None).
                        otherwise(col("Loss %"))) \
            .withColumn('shp_loss_pct', when(col("Loss shops").rlike("^\s*$"), None).
                        otherwise(col("Loss shops"))) \
            .withColumn('meet_pct', when(col("Meet %").rlike("^\s*$"), None).
                        otherwise(col("Meet %"))) \
            .withColumn('shp_meet_pct', when(col("Meet shops").rlike("^\s*$"), None).
                        otherwise(col("Meet shops"))) \
            .withColumn('win_pct', when(col("Win %").rlike("^\s*$"), None).
                        otherwise(col("Win %"))) \
            .withColumn('shp_win_pct', when(col("Win shops").rlike("^\s*$"), None).
                        otherwise(col("Win shops"))) \
            .withColumn('no_of_shps', when(col("Shops").rlike("^\s*$"), None).
                        otherwise(col("Shops"))) \
            .withColumn('src_file', input_file_name()) \
            .withColumn('src_file_nm', udf_extract_basename(col('src_file'))) \
            .withColumn('as_of_dt', udf_extract_date(col('src_file_nm'))) \
            .withColumn('create_ts', lit(create_ts)) \
            .withColumn('created_by', lit('GLUE'))

        logger.info("s3 read data-frame count: {0}".format(hotels_s3_read_dyf.count()))

        # selecting only needed columns from above created df
        hotels_dyf = hotels_df.select(col('htl_nm'), col('site_id'), col('cntry_cd'), col('loss_pct'),
                                      col('shp_loss_pct'), col('meet_pct'), col('shp_meet_pct'),
                                      col('win_pct'), col('shp_win_pct'), col('no_of_shps'), col('as_of_dt'),
                                      col('src_file'), col('src_file_nm'), col('create_ts'), col('created_by'))

        hotels_dyf.printSchema()
        hotels_dyf.show(2)
        #######################################DQ Checks #####################################################################

        logger.info("starting dq process..")
        hotels_dq_dyf = hotels_dyf. \
            withColumn('prcs_flg', when(
            isnull(col('htl_nm')) | isnull(col('site_id')) | isnull(col('loss_pct')) | isnull(col('meet_pct')) |
            isnull(col('win_pct')), 'E').
                       when(
            isnull(col('cntry_cd')) | isnull(col('shp_loss_pct')) | isnull(col('shp_meet_pct')) | isnull(
                col('shp_win_pct')) | isnull(col('no_of_shps')), 'Y').otherwise(None)) \
            .withColumn("err_desc",
                        concat_ws("_", when(isnull(col('htl_nm')) | isnull(col('site_id')) | isnull(col('loss_pct')) |
                                            isnull(col('meet_pct')) | isnull(col('win_pct')), 'H'),
                                  when(isnull(col("htl_nm")), "missing_htl_nm"),
                                  when(isnull(col("site_id")), "missing_site_id"),
                                  when(isnull(col("loss_pct")), "missing_loss_pct"),
                                  when(isnull(col("meet_pct")), "missing_meet_pct"),
                                  when(isnull(col("win_pct")), "missing_win_pct"),
                                  when(isnull(col('cntry_cd')) | isnull(col('shp_loss_pct')) | isnull(
                                      col('shp_meet_pct')) |
                                       isnull(col('shp_win_pct')) | isnull(col('no_of_shps')), 'S'),
                                  when(isnull(col("cntry_cd")), "missing_cntry_cd"),
                                  when(isnull(col("shp_loss_pct")), "missing_shp_loss_pct"),
                                  when(isnull(col("shp_meet_pct")), "missing_shp_meet_pct"),
                                  when(isnull(col("shp_win_pct")), "missing_shp_win_pct"),
                                  when(isnull(col("no_of_shps")), "missing_no_of_shps")))

        # hotels_dq_dyf.select(col('as_of_dt'), col('prcs_flg'), col('err_desc')).show(truncate=False)
        # logger.info("s3 read data-frame count {0}".format(hotels_dq_dyf.count()))
        hotels_dq_dyf.createOrReplaceTempView("s3_read_temp")

        file_count_sql = spark.sql(
            """ select *, SUBSTRING_INDEX(src_file,'/',-1) as fileName from (select src_file, count(*) as 
            s3_file_record_count from  s3_read_temp group by src_file) """)

        file_count_sql.show(10, False)

        s3_file_list = [
            {'filePath': x['src_file'],
             'fileName': x['fileName'],
             's3_file_record_count': x['s3_file_record_count']}
            for x in file_count_sql.rdd.collect()
        ]
        logger.info("s3_file_list: {0}".format(s3_file_list))

        # s3_filename = hotels_dq_dyf.select(col("src_file_nm")).distinct().head()[0]
        # logger.info("s3_filename: {0}".format(s3_filename))
        for row in s3_file_list:
            file_name = row["fileName"]
            file_path = row["filePath"]
            s3_file_record_count = row['s3_file_record_count']
            if s3_file_record_count > 0:
                src_file_nm = file_name
                logger.info("current processing file name: {0}".format(src_file_nm))
                final_df = hotels_dq_dyf.select(substring(col('htl_nm'), 1, 255).alias('htl_nm'), # accepting only 255 chars as per business req. If substring is not given then the record will get rejected when more than 255 char arrives.
                                                col('site_id'),
                                                substring(col('cntry_cd'), 1, 255).alias('cntry_cd'),
                                                col('loss_pct').cast(DecimalType(25, 5)),
                                                col('shp_loss_pct').cast(DecimalType(25, 5)),
                                                col('meet_pct').cast(DecimalType(25, 5)),
                                                col('shp_meet_pct').cast(DecimalType(25, 5)),
                                                col('win_pct').cast(DecimalType(25, 5)),
                                                col('shp_win_pct').cast(DecimalType(25, 5)),
                                                col('no_of_shps').cast(DecimalType(10)), col('prcs_flg'),
                                                col('err_desc'),
                                                to_date(col('as_of_dt'), "yyyy-MM-dd").cast(DateType()).alias(
                                                    'as_of_dt'),
                                                col('src_file_nm'), col('create_ts').cast(TimestampType()),
                                                col('created_by')) \
                    .where(hotels_dq_dyf.src_file_nm == file_name)

                final_df.show(2)
                final_dynf = DynamicFrame.fromDF(final_df, glueContext, "final_dynf")
                final_dynf.printSchema()

                logger.info(f"loading the file '{file_name}' into stg table..")
                redshift_load_tmp_dir = f"s3://{blue_bucket}/otai/disparity/summary/parity-analytics/redshift/dataload/{job_name}/{file_name}/"

                redshift_load = glueContext.write_dynamic_frame.from_jdbc_conf(
                    frame=final_dynf,
                    catalog_connection=redshiftconnstage,
                    connection_options={"url": rs_url_db_stg_dest,
                                        "database": rsdb,
                                        "user": rs_user_stg_dest,
                                        "password": rs_pwd_stg_dest,
                                        "dbtable": schemanamestg + '.' + stg_table_name,
                                        "extracopyoptions": "MAXERROR 100000"},
                    redshift_tmp_dir= redshift_load_tmp_dir)

                logger.info(f"loading the audit data for '{file_name}' file into audit table..")
                auditload(redshift_load_tmp_dir, file_name, file_path, create_ts, s3_file_record_count)

            else:
                # below auditing will never be used as files with less than count of 1 will be rejected initially only.
                # but just left as is in the code as no harm..
                auditload(redshift_tmp_dir, file_name, file_path, create_ts, s3_file_record_count)

    else:
        f_msg = "No Files for processing for Rate Parity Summary Hotels"
        f_sub = f"Rate Parity Summary Hotels Stage Loading Failed '{job_name}'"
        notifymsg(f_sub, f_msg)


except Exception as e:
    logger.error(
        "************ {} [ERROR] Exception while loading to redshift *************".format(
            str(datetime.now())))
    logger.error("*********** [ERROR] Failing with error:  {} **************".format(str(e)))
    f_msg = f"Error while loading {schemanamestg}.{stg_table_name}, failed with error: {e}"
    f_sub = f"Rate Parity Summary Hotels Stage Loading Failed -- {job_name}"
    notifymsg(f_sub, f_msg)
    filePath = get_input_s3_path()
    auditload(redshift_tmp_dir, s3_filename, filePath, create_ts, 0)
    raise SystemExit(e)


finally:
    # archiving all the available files in staging
    for file in input_datasource_s3:
        filename = extract_basename(file)
        logger.info(f"moving the file '{filename}' to archive folder..")
        src_key = f"otai/disparity/summary/parity-analytics/hotels/{filename}"
        dest_key = f"otai/disparity/summary/archive/parity-analytics/hotels/{filename}"
        move_file_within_s3_bucket(source_bucket, src_key, source_bucket, dest_key)

logger.info(" ************** {} End of Load process for S3 Read *********************** ".format(
    str(datetime.now())))
job.commit()
logger.info("JOB COMPLETE!!")