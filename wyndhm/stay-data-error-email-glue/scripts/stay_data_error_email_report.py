import logging
import sys
import pytz
from datetime import datetime
from datetime import timedelta
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
import boto3
import xlsxwriter
import pandas as pd

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)

################################### Enabling access to the arguments ##################################
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SNS', 'rsdb', 'blue_bucket',
                                     'redshiftconnection', 'env',
                                     'schemaname'])
job.init(args['JOB_NAME'], args)

################################### Setting up logger for event logging ##################################
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

################################### Assigning job arguments to variables  ##################################

rsdb = args['rsdb']
job_name = args['JOB_NAME']
sns_notify = args['SNS']
env = args['env']
redshiftconnection = args['redshiftconnection']
blue_bucket = args['blue_bucket']
src_schema_name = "dwstg"

today = datetime.now().strftime('%Y/%m/%d')
yesterday = (datetime.now() - timedelta(1)).strftime('%Y/%m/%d')

                      
################################### Create low level reosurce service client for S3 ans SNS  ##################################

s3Client = boto3.client('s3', region_name='us-west-2')
s3resource = boto3.resource('s3', region_name='us-west-2')
glueClient = boto3.client('glue', region_name='us-west-2')

#job_title = " ".join(trgt_table_name.split("_")[:]).title()  # used to print it in logs but nothing else.

try:
    sns_client = boto3.client('sns', region_name='us-west-2')
    logger.info("Enabled SNS Notification")
except Exception as e:
    logger.error(str(e))
    logger.info(" Unable to enable SNS, failed with error: {}".format(str(e)))
    logger.info("Continuing with load without SNS notification")


############# Notification Function ################################################
def notifymsg(sub, msg):
    sns_client.publish(TopicArn = sns_notify, Message = msg , Subject= sub)
    logger.info("**************** [INFO] SNS Notification Sent: {} *************************".format(job_name))


################## Create Timestamp ############################

def create_timestamp_est():
    utc_now = datetime.now()
    est = pytz.timezone('US/Eastern')
    est_now = utc_now.astimezone(est).strftime('%Y-%m-%d %H:%M:%S.%f')
    return est_now
    
def get_job_last_run_ts():
    try:
        job_runs = glueClient.get_job_runs(JobName=job_name)
        job_run_count = len(job_runs["JobRuns"])
        last_job_start_time = None
        # iterate through the job till we find the first successful job
        for res in job_runs['JobRuns']:
            if res.get("JobRunState") == "SUCCEEDED":
                logger.info("found a last successful job run")
                last_job_start_time_utc = res.get("StartedOn")  # 2021-06-21 08:08:33.525000+00:00
                est = pytz.timezone('US/Eastern')
                last_job_start_time = last_job_start_time_utc.astimezone(est).strftime('%Y-%m-%d %H:%M:%S.%f')
                break
        logger.info("Count of the job run is : {}".format(str(job_run_count)))
        if last_job_start_time:
            logger.info("Start time of the last job run is : {}".format(str(last_job_start_time)))
        else:
            logger.info("There are no previous job run")
        return last_job_start_time
    except Exception as e:
        logger.error(str(e))
        logger.info(" Unable to fetch glue job runs, failed with error: {}".format(str(e)))
        exit(1)
    
try:
   logger.info("Getting Redshift Connection Information")    
   rs_connection_dest = glueContext.extract_jdbc_conf(redshiftconnection)
   rs_url_dest = rs_connection_dest["url"]
   rs_user_dest = rs_connection_dest["user"]
   rs_pwd_dest = rs_connection_dest["password"]
   rs_url_db_dest = rs_url_dest+'/'+rsdb
except Exception as e:
   logger.error(str(e))
   f_msg=" Unable to connect to Redshift while processing the glue job {0} , failed with error: {1}  ".format(job_name,str(e))
   f_sub = "stay_data_error_Report Process Alert: Glue Job - "+job_name+" failed" 
   notifymsg(f_sub, f_msg)
   logger.info(" Exiting with error: {}".format(str(e)))
   exit(1)  

try:
   #read_redshift_table_to_dataframe(src_schema_name, "stay_data_errors", rs_connection_dwstg)
   
   last_job_run_ts = get_job_last_run_ts()
   logger.info('last_job_run_ts : ' + str(last_job_run_ts))
   
   est_now = create_timestamp_est()
   utc_now_str = datetime.now().strftime('%Y-%m-%d')
   utc_now_str_custom = datetime.now().strftime('%Y%m%d')

   if last_job_run_ts != None:
      sql_query = (""" select site_id,pms_typ_cd,stay_file_cnsmr_id,src_column_name,src_column_value,error_message,batch_num,insert_ts,brand_id,dataval_id,pros_flag,corrected_val,reprocess_flag,
                          updated_ts,pms_conf_num,stay_dt,evnt_strt_dt,evnt_end_dt,table_name from dwstg.stay_data_errors where insert_ts >= '{}' """.format(last_job_run_ts))
   else:
        sql_query = (""" select site_id,pms_typ_cd,stay_file_cnsmr_id,src_column_name,src_column_value,error_message,batch_num,insert_ts,brand_id,dataval_id,pros_flag,corrected_val,reprocess_flag,
                            updated_ts,pms_conf_num,stay_dt,evnt_strt_dt,evnt_end_dt,table_name from dwstg.stay_data_errors """)
    
   logger.info("sql query : " + str(sql_query))
    
   stay_data_error_df = spark.read.format("com.databricks.spark.redshift").option("url", rs_url_db_dest+'?user='+rs_user_dest+'&password='+rs_pwd_dest)\
                            .option("query", sql_query)\
                            .option("forward_spark_s3_credentials", "true")\
                            .option("tempdir", "s3://"+blue_bucket+'/stay_data_error_process/logs/'+'{}/'.format(job_name))\
                            .load()
    
   #stay_data_error_df = spark.sql(sql_query)
   stay_data_error_count_df = stay_data_error_df.count()
   stay_data_error_df.show()
   
except Exception as e:
   logger.error(str(e))
   f_msg="Error while reading data from Redshift. Failed with error : {} ".format(str(e))
   f_sub = "Stay Data Error Report Glue job "+job_name+" failed" 
   notifymsg(f_sub, f_msg)
   logger.info("  [ERROR] Error while reading data from S3, failed with : {}  ".format(str(e)))
   exit(1)

        
if stay_data_error_count_df < 1:
    logger.info('No Data present to process for Stay Data Error Report Job for %s run', str(datetime.now()))
    msg = f"No Data to Process for Stay Data Error Report Glue Job - {job_name} for run {str(datetime.now())}"
    sub = f"No Data to Process for Stay Data Error ReportGlue Job - {job_name}"
    notifymsg(sub, msg)
else:
     try:
        #################### generating stay Data Error records report into excel ########################
        # converting spark df into pandas dataframe for excel file generatrion
        pandasDF = stay_data_error_df.toPandas()
        # 'to_excel' works only when the file is written to tmp directory first
        # then you need to save this temp file into a permanent file in exact folder
        tmp_file_name = '/tmp/' + 'stay_data_error_report_today.xlsx'
        pandasDF.to_excel(tmp_file_name, index=False)
        # saving above created tmp file to a permanent folder
        output_file_path = f"pms_stay/stay_data_error_table/stay_data_error_report/stay_data_error_report_" \
                           f"{utc_now_str_custom}.xlsx"
                           
        s3resource.Bucket(blue_bucket).upload_file(tmp_file_name, output_file_path)
        logger.info("Final Excel report generated!")
        
     except Exception as e:
        logger.error(str(e))
        f_msg="Error while generating stay Data Error records report into excel. Failed with error : {} ".format(str(e))
        f_sub = "Stay Data Error Report Glue job "+job_name+" failed" 
        notifymsg(f_sub, f_msg)
        logger.info("  [ERROR] Error while reading data from S3, failed with : {}  ".format(str(e)))
        exit(1)
        
       
    
logger.info(
    f" *********** %s: End of Glue process for '{job_name}' *********** ", str(datetime.now()))
job.commit()
logger.info("JOB COMPLETE!!")