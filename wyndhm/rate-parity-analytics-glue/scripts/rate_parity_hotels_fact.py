import time

import pytz
import datetime
import logging
import sys
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3
import pytz
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from dateutil import tz
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.functions import lit, col, md5, concat_ws, when, udf, row_number
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

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
                           'redshiftconnstage', 'redshiftconndw', 'facttablename','stagetablename', 'rs_schema_name_dw', 'audittable',
                           'errortable', 'load_error_view', 'env', 'schemanamestg', 'sender', 'recipientlistforstatus', 'recipientlistforreport'])
job.init(args['JOB_NAME'], args)

rsdb = args['rsdb']
redshiftconnstage = args['redshiftconnstage']
redshiftconndw = args['redshiftconndw']
source_bucket = args['source_bucket']
blue_bucket = args['blue_bucket']
gold_bucket = args['gold_bucket']
ScriptBucketName = args['ScriptBucketName']
stg_table_name = args['stagetablename']
facttablename = args['facttablename']
errortable = args['errortable']
audittable = args['audittable']
rs_schema_name_dw = args['rs_schema_name_dw']
schemanamestg = args['schemanamestg']
recipientlistforstatus = args['recipientlistforstatus']
recipientlistforreport = args['recipientlistforreport']
sender = args['sender']
env = args['env']
sns_Notify = args['sns']
job_run = True
load_error_view = args['load_error_view']

max_retries = 5
retry_delay_in_seconds = 10

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


################################### Create low level reosurce service client for S3 ans sns  ##################################


s3Client = boto3.client('s3', region_name='us-west-2')
s3resource = boto3.resource('s3', region_name='us-west-2')
glueclient = boto3.client('glue', region_name='us-west-2')
sesClient = boto3.client("sesv2", region_name="us-west-2")

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


################################### Retriving connection information from Glue connections for Redshift  ##################################
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
    f_sub = f"Glue job {job_name} failed"
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
    f_sub = f"Glue job {job_name} failed"
    notifymsg(f_sub, f_msg)
    logger.info(f" Exiting with error: {str(e)}")
    raise SystemExit(e)


def create_timestamp_est():
    utc_now = datetime.now()
    est = pytz.timezone('US/Eastern')
    est_now = utc_now.astimezone(est).strftime('%Y-%m-%d %H:%M:%S')
    return est_now


def extract_date(filename):
    return filename[filename.find("_"):filename.find(".csv")].replace("_", "")


def convert_string_as_msg(strng):
    return strng.replace("_H", " | H").replace("_S", " | S")


def convert_string_as_col(strng):
    val = convert_string_as_msg(strng)
    return val.replace("H_missing_","").replace("S_missing_","").replace("_missing_"," | ")


udf_convert_string_as_msg = udf(convert_string_as_msg, StringType())
udf_convert_string_as_col = udf(convert_string_as_col, StringType())

########################### Send Email ###################################


def send_mail(subject, recipientslist, sender, body, date=None, df=None):
    to_emails = recipientslist
    # logger.info(to_emails)
    message = {
    'Subject': {'Data': subject},
    'Body': {'Html': {'Data': body}}
    }
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = to_emails[0]
    msg['To'] = to_emails[0]
    sender = sender
    to_add= to_emails
    # what a recipient sees if they don't use an email reader
    msg.preamble = 'Multipart message.\n'
    part1 = MIMEText(body, 'html')
    msg.attach(part1)
    # for attachment
    if df is not None:
        filename = f"Hotels_error_report_{date}.csv"
        df.toPandas().to_csv(filename, index=False)
        part = MIMEApplication(open(filename, 'rb').read())
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(part)

    response = sesClient.send_email(
            FromEmailAddress=sender,
            Destination={
                'ToAddresses' :to_add
            },
           Content=
            {
                'Raw': {
                    'Data': msg.as_string()
                }
            }
        )
    # logger.info(response)


def load_rs_table():
    post_query = """begin;
                --STEP 1: updating audit table when load is completed and successful
                    update {0}.{1} set edw_sts ='C', edw_sucss_cnt ={2}, edw_fail_cnt=0, 
                    tot_cnt={3}, update_by='Glue', edw_insert_ts=to_timestamp('{4}', 'YYYY-MM-DD HH24:MI:SS'), 
                    prcs_end_ts=to_timestamp('{5}', 'YYYY-MM-DD HH24:MI:SS'),
                    rmrks='Summary Hotels loading completed' where feed_nm ='{6}' and 
                    src_insert_ts> '{7}';
                --STEP 2: updating fact table with appropriate cur_rec_ind as per data
                    update {8}.{9} set curr_rec_ind ='D' where (rec_key_hash,create_ts) in 
                    (select rec_key_hash, create_ts from (select rec_key_hash, create_ts,
                    row_number() over(partition by rec_key_hash order by create_ts desc) rnum 
                    from {8}.{9} where curr_rec_ind <> 'D') where rnum >1 and curr_rec_ind <> 'D');
                --STEP 3: updating fact table with available site_key for null data
                    update dw.fact_otai_rt_prty_htls rph
                    set site_key = ls.site_key
                    from
                    (select ls.site_id, ls.site_key from dw.lkup_site ls where ls.cur_site_flg ='Y') ls
                    where rph.site_id = ls.site_id
                    and rph.site_key = '-9999';
                 end;""" \
        .format(schemanamestg, audittable, loaded_count, file_count, create_ts, create_ts, file_name, est_date,
                rs_schema_name_dw, facttablename)
    logger.info(post_query)

    rs_hotels_load_final = DynamicFrame.fromDF(rs_hotels_fact_load, glueContext, "rs_hotels_load_final")
    final_load = glueContext.write_dynamic_frame.from_jdbc_conf(frame=rs_hotels_load_final,
                                                                catalog_connection=redshiftconndw,
                                                                connection_options={"postactions": post_query,
                                                                                    "url": rs_url_db_dest,
                                                                                    "database": rsdb,
                                                                                    "user": rs_user_dest,
                                                                                    "password": rs_pwd_dest,
                                                                                    "dbtable":
                                                                                        rs_schema_name_dw +
                                                                                        '.' + facttablename},
                                                                redshift_tmp_dir=f"s3://"
                                                                                 f"{blue_bucket}/otai/disparity/summary/parity-analytics/glue/redshift/load/{job_name}/")
    logger.info(f"Summary Hotels Fact Loading Completed")

#################################### get latest job run timestamp ##########################################

try:
    response = glueclient.get_job_runs(
                    JobName=args['JOB_NAME'],
                    MaxResults=15
                )
    job_runs = response['JobRuns']
    start_time=None
    for run in job_runs:
        if run['JobRunState'] != 'SUCCEEDED':
            continue
        else:
            start_time = run['StartedOn']
            break

    # Extract the start time of the last successful run or set it to None
    logger.info(start_time)
    # logger.info the start time
    rs_load_sql=""
    if start_time is None:
        logger.info(f"The job {job_name} has not been run before.")
        rs_load_sql = " select  * from {0} ".format(schemanamestg+'.'+stg_table_name)
        est_date = '1111-11-11'  # assuming default time for first job when no job is run before..
    else:
        date_str = str(start_time)
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")
        output_string = str(start_time)[:19]
        logger.info(output_string)
        dtt = datetime.fromisoformat(formatted_date)

        utc = pytz.timezone('UTC')
        # Create a timezone object for EST
        est = pytz.timezone('US/Eastern')
        # Convert the input string to a datetime object in UTC timezone
        utc_dt = utc.localize(datetime.strptime(str(dtt), '%Y-%m-%d %H:%M:%S'))
        # Convert the UTC datetime object to EST timezone
        est_dt = utc_dt.astimezone(est)
        # Format the EST datetime object as a string
        est_date = est_dt.strftime('%Y-%m-%d %H:%M:%S')
        # logger.info the output string
        logger.info(est_date)
        logger.info(f"The last successful run of job {job_name} started at '{est_date}'.")
        rs_load_sql = " select  * from {0} where create_ts >'{1}' ".format(schemanamestg+'.'+stg_table_name, est_date)
        logger.info(rs_load_sql)

    rs_hotels_df = spark.read.format("com.databricks.spark.redshift")\
        .option("url", rs_url_db_stg_dest+'?user='+rs_user_stg_dest+'&password='+rs_pwd_stg_dest)\
        .option("query", rs_load_sql)\
        .option("forward_spark_s3_credentials", "true")\
        .option("tempdir", "s3://"+blue_bucket+'/otai/disparity/summary/parity-analytics/'+job_name+'/').load()
    rs_hotels_df.printSchema()
    src_count=rs_hotels_df.count()

    result_df = rs_hotels_df.groupBy(col("src_file_nm")).agg(count("*").alias('src_count'))
    result_df = result_df.select("src_file_nm", "src_count")

    rs_site_lkp_sql= f"""select ls.site_id as ls_site_id ,ls.site_key from dw.lkup_site ls 
                         where ls.cur_site_flg ='Y'"""
    rs_site_lkp_df = spark.read.format("com.databricks.spark.redshift")\
        .option("url", rs_url_db_dest+'?user='+rs_user_dest+'&password='+rs_pwd_dest)\
        .option("query", rs_site_lkp_sql)\
        .option("forward_spark_s3_credentials", "true")\
        .option("tempdir", "s3://"+blue_bucket+'/otai/disparity/summary/parity-analytics/'+job_name+'/').load()
    # rs_site_lkp_df.printSchema()
    rs_site_lkp_df = rs_site_lkp_df.dropDuplicates()

    joined_df = rs_hotels_df.join(rs_site_lkp_df, rs_hotels_df.site_id == rs_site_lkp_df.ls_site_id, "left")
    # Assuming rs_expedia_df is already defined
    # Create a new dataframe with curr_rec_ind column
    hotels_fact_df = joined_df.withColumn("curr_rec_rank",row_number().over(Window.partitionBy("site_id", "as_of_dt", "src_file_nm").orderBy(concat_ws('site_id', 'as_of_dt','src_file_nm')))) \
                                .withColumn("rec_key_hash", md5(concat_ws("", 'site_id', 'as_of_dt')))\

    # Mark duplicate records as curr_rec_ind = 'D'
    final_hotels_fact_df = hotels_fact_df.withColumn("curr_rec_ind", when(col("curr_rec_rank") != 1, "D").otherwise("Y"))
    final_hotels_fact_df.show(2)

    result_hotels_fact_df = final_hotels_fact_df.groupBy(col("src_file_nm")).agg(count("*").alias('loaded_count'))
    result_hotels_fact_df = result_hotels_fact_df.select(col("src_file_nm").alias('loaded_src_file_nm'), "loaded_count")

    full_df = result_df.join(result_hotels_fact_df, result_df.src_file_nm == result_hotels_fact_df.loaded_src_file_nm, "inner")
    full_final_df = full_df.select(col("src_file_nm"), col("src_count"), col("loaded_count"))
    full_final_df.show()

    s3_file_list = [
        {'fileName': x['src_file_nm'], 'src_count': x['src_count'], 'loaded_count': x['loaded_count']} for x in full_final_df.rdd.collect()]

    # Final Loading
    # final_hotels_fact_df.printSchema()
    final_hotels_fact_df.createOrReplaceTempView("rs_hotels_fact_temp")
    count_of_records=final_hotels_fact_df.count()
    logger.info(f"num of records to load to fact table: {count_of_records}")
    create_ts=create_timestamp_est()

    for file_dict in s3_file_list:
        file_name = file_dict["fileName"]
        processing_file_date = extract_date(file_name)  # to get date
        datesplit = processing_file_date.split("-")
        YR = datesplit[0]
        MO = datesplit[1]
        DY = datesplit[2]
        logger.info(f"file_name: {file_name}")
        file_count = file_dict["src_count"]
        logger.info(f"file_count: {file_count}")
        loaded_count = file_dict["loaded_count"]
        logger.info(f"loaded_count: {loaded_count}")
        sql_str = f""" SELECT htl_nm, cast(site_id as string) as site_id, cast(coalesce(site_key, -9999) as integer) as site_key,
                  cast(rec_key_hash as string) as rec_key_hash, cntry_cd,
                  cast(loss_pct as decimal(25, 5)) as loss_pct, cast(shp_loss_pct as decimal(25, 5)) as shp_loss_pct,
                  cast(meet_pct as decimal(25, 5)) as meet_pct, cast(shp_meet_pct as decimal(25, 5)) as shp_meet_pct,
                  cast(win_pct as decimal(25, 5)) as win_pct, cast(shp_win_pct as decimal(25, 5)) as shp_win_pct,
                  cast(no_of_shps as decimal(10)) as no_of_shps, prcs_flg, err_desc, cast(as_of_dt as date) as as_of_dt,
                  src_file_nm, cast('{create_ts}' as timestamp) as create_ts, created_by, curr_rec_ind 
                  from rs_hotels_fact_temp WHERE (src_file_nm = '{file_name}')"""
        rs_hotels_fact_load = spark.sql(sql_str)
        rs_hotels_fact_load.printSchema()

        # loading the stage table..
        attempt = 1
        while attempt <= max_retries:
            try:
                logger.info(f"attempt: {attempt}")
                load_rs_table()
                break
            except Exception as e:  # to cover 'Serializable isolation violation on table' error
                if "Serializable" in str(e).split(" "):
                    pass    # not doing anything but just passing the loop for another try
                else:
                    raise
            attempt += 1
            time.sleep(retry_delay_in_seconds * attempt)

        ######################### Excel Report ########################################################
        report_excel_sql = f"""select site_id, err_desc, curr_rec_ind from dw.fact_otai_rt_prty_htls 
                                where src_file_nm = '{file_name}' and (prcs_flg in ('E', 'Y') or curr_rec_ind = 'D') """
        report_excel_df = spark.read.format("com.databricks.spark.redshift").option("url",
                                                                                    rs_url_db_dest + '?user=' +
                                                                                    rs_user_dest + '&password=' +
                                                                                    rs_pwd_dest) \
            .option("query", report_excel_sql) \
            .option("forward_spark_s3_credentials", "true") \
            .option("tempdir", "s3://" + blue_bucket + '/disparity/summary/parity-analytics/glue/redshift/report/' + job_name + '/') \
            .load()
        report_excel_nullability_df = report_excel_df.filter(col('err_desc').isNotNull()).\
            withColumn('Missing field',udf_convert_string_as_col('err_desc')).withColumn('Error Message',udf_convert_string_as_msg('err_desc'))

        """
        report_excel_duplicacy_df = report_excel_df.filter(col('curr_rec_ind').isin('D')). \
            withColumn('Missing field', lit('')).withColumn('Error Message', lit('Duplicate'))
            
        report_excel_df_final = report_excel_nullability_df.union(report_excel_duplicacy_df).select(col('site_id'), col('Missing field'), col('Error Message'))
        """

        report_excel_df_final = report_excel_nullability_df.select(col('site_id'), col('Missing field'), col('Error Message'))
        error_rprt_df = report_excel_df_final.distinct()
        error_rprt_df.show(2)

        logger.info("sending email alert for delivery status..")
        sub_for_status = f'Rate Parity Summary Hotels Data Delivery Status – as of {MO}/{DY}: COMPLETED'
        # notifymsg(sub, msg)
        border = 'border: 1px solid #000000;'
        htmlbody = f"""
            Please find below Summary for Rate Parity data delivery..\n

            <html>
            <head>
            <style>
            table, th, td {
        f"{border}"
        }</style>
        </head>
        <body>
          <table  border="1" cellpadding="4"
               cellspacing="5">
          <thead><tr><th class="tg-0lax" colspan="3">Rate Parity Data Delivery</th>
        </tr>
           </thead>
            <tbody>
            <tr>
              <th>Job</th>
              <th>Status</th>
              <th>Completion Time</th>
            </tr>
             <tr>
              <td >Summary Hotels data delivery to Redshift database for Birst/Tableau report processing</td >
              <td >Completed</td >
              <td >{create_timestamp_est()}</td >
            </tr >
          </tbody>
        </table>
        </body>
        </html>


            Number of records Received: {file_count}\n
            Number of records Loaded:  {loaded_count}\n
            Number of records Rejected: 0

        """
        recipientlist_for_status = recipientlistforstatus.split(',')
        send_mail(sub_for_status, recipientlist_for_status, sender, htmlbody)

        logger.info("sending email alert for error report..")
        sub_for_report = f"Error report for '{file_name}' as of {MO}/{DY}"
        body = f"""Please find the attached error report for Hotels."""
        recipientlist_for_report = recipientlistforreport.split(',')
        send_mail(sub_for_report, recipientlist_for_report, sender, body, processing_file_date, error_rprt_df)


except Exception as e:
    logger.error("*********** [ERROR] Failing with error:  {} **************".format(str(e)))
    f_msg = f"Error while loading {rs_schema_name_dw}.{facttablename}, failed with error: {e}"
    f_sub = f"Summary Hotels Fact Loading Failed -- {job_name}"
    notifymsg(f_sub, f_msg)
    logger.info("JOB FAILED!!")
    raise SystemExit(e)

logger.info(" ************** {} End of Load process for S3 Read *********************** ".format(str(datetime.now())))
job.commit()
logger.info("JOB COMPLETE!!")