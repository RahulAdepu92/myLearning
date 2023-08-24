import sys
from datetime import datetime
from datetime import timedelta
import pytz
import json
import time
import pandas
from awsglue.transforms import *
from pytz import timezone
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SQLContext
import boto3
import logging
from pyspark.sql import functions as f
from pyspark.sql.functions import col, when, lit, input_file_name, substring, year, month, \
    dayofmonth, concat
from pyspark.sql.types import TimestampType, DoubleType, StructType, StructField, StringType, \
    IntegerType, LongType, FloatType, DecimalType

################################### Setting up Spark environment and enabling Glue to interact with Spark Platform ##################################
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)

################################### Enabling access to the arguments ##################################
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'blue_bucket', 'SNS', 'rsdb', 'redshiftconnection',
                                     'tablename', 'schemaname', 'ScriptBucketName', 'configfile','rules_tablename','dq_tablename','redshift_dw_connection','DQESLogGroup'])
job.init(args['JOB_NAME'], args)

################################### Setting up logger for event logging ##################################
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

################################### Assigning job arguments to variables  ##################################

rsdb = args['rsdb']
redshiftconnection = args['redshiftconnection']
redshift_dw_connection = args['redshift_dw_connection']
blue_bucket = args['blue_bucket']
file_path_gen = 's3://{}/pms_stay/processing/'.format(blue_bucket)
job_name = args['JOB_NAME']
tablename = args['tablename']
schemaname = args['schemaname']
sns_notify = args['SNS']
MARKER_FILE = "pms_stay/glue-workflow-marker-triggers/" + args["JOB_NAME"]
seqno_path = "s3://{}/pms_stay/seqno/".format(blue_bucket)
job_run = True
s3_lku_site = 's3://{}/pms_stay/lkup-site/'.format(blue_bucket)
rules_tablename = args['rules_tablename']
dq_tablename = args['dq_tablename']
s3_mbr_num ='s3://{}/pms_stay/config/mbr_num/'.format(blue_bucket)
dq_es_log_group_nm = args['DQESLogGroup']

################################### Create low level reosurce service client for S3 ans SNS  ##################################

s3Client = boto3.client('s3', region_name='us-west-2')
s3resource = boto3.resource('s3', region_name='us-west-2')
glueClient = boto3.client('glue', region_name='us-west-2')

try:
    sns_client = boto3.client('sns', region_name='us-west-2')
    logger.info("Enabled SNS Notification")
except Exception as e:
    logger.error(str(e))
    logger.info(" Unable to enable SNS, failed with error: {}".format(str(e)))
    logger.info("Continuing with load without SNS notification")


def notifymsg(sub, msg):
    env=job_name.split('-')[-1]
    msg = "SD Team Please create a ticket \n Priority: {0} \n Assignment Team: {1} \n Impacted System: {2} \n Error Details: {3} \n AWS Component: {4} \n".format("P3 Medium", "PSS AWS", "Stay glue job failure ",msg,job_name)
    sub = 'P3 Medium ' + sub + ' in '+env
    sns_client.publish(TopicArn=sns_notify, Message=msg, Subject=sub)
    logger.info(
        "**************** [INFO] SNS Notification Sent: {} *************************".format(
            job_name))


################## Create Timestamp ############################

def create_timestamp_est():
    utc_now = datetime.now()
    est = pytz.timezone('US/Eastern')
    est_now = utc_now.astimezone(est).strftime('%Y-%m-%d %H:%M:%S.%f')
    return est_now

def create_ts_es():
    
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return now
    
log_process_start_datetime = create_ts_es()
######## Check if this is rerun based on the marker file, if so the job execution is skipped ###################################
try:
    response = s3Client.head_object(Bucket=blue_bucket, Key=MARKER_FILE)
    print(response)
    print("*******************[INFO] JOB ALREADY EXECUTED ********************")
    job_run = False
except Exception as HeadObjectException:
    ##############################################################################################
    # No Marker file present, then this is first time execution
    # Will read the file based on get_input_s3_path response
    ##############################################################################################
    print(HeadObjectException)


dq_log_stream_n = 'DQEventGlueLogs'
ts = datetime.now()
ts_est = timezone('US/Eastern').localize(ts)
ts_est_str = ts_est.strftime('%Y-%m-%d-%H-%M-%S')
dq_log_stream = '{}-{}'.format(dq_log_stream_n,ts_est_str)
dq_log_group = dq_es_log_group_nm


#########################################Create DQESLogGroup stream################################### 

try:
    logs = boto3.client('logs', region_name = 'us-west-2')
    logs.create_log_stream(logGroupName=dq_log_group, logStreamName=dq_log_stream)
    logger.info("Connected to Log Stream")
    # print('DQLogStream  {} created successfully!'.format(logStreamName))
except Exception as e:
    logger.error(str(e))
    # print('DQLogStream  {} already exist.'.format(logStreamName))
    
    
def generateDQESLogs(job_name,rule_id,rule_name,log_process_start_datetime,source_count,dq_count):
    
    DQ_logs_json_ = json.dumps({"source_system":"Sabre PMS","domain":"Stay","subdomain":"","process_name":"Stay Event Job","process_arn":"{}".format(job_name),
       "task_name":"DQ Check","rule_id":"{}".format(rule_id),"rule_name":"{}".format(rule_name),"source_name":"","design_pattern":"Batch","source_system_timestamp":"{}".format(log_process_start_datetime),"original_file_name":"",
       "process_start_datetime":"{}".format(log_process_start_datetime),"process_end_datetime":"{}".format(create_ts_es()),"batch_id":"","source_record_count":source_count,
       "dq_error_count":dq_count,"unique_id":""})
     
    return DQ_logs_json_
    
def dqstreaminfo(msg):
    
        timestamp = int(round(time.time() * 1000))
        response = logs.describe_log_streams(
            logGroupName=dq_log_group,
            logStreamNamePrefix=dq_log_stream
        )
        event_log = {
            'logGroupName':dq_log_group,
            'logStreamName':dq_log_stream,
            'logEvents':[
                {
                    'timestamp': timestamp,
                    'message': msg
                }
            ]
        }
        
        if 'uploadSequenceToken' in response['logStreams'][0]:
            event_log.update({'sequenceToken': response['logStreams'][0]['uploadSequenceToken']})
        
        try:
         response = logs.put_log_events(**event_log)
        
        except Exception as e:
         logger.info(" Retrying log_stream in exception")
         timestamp = int(round(time.time() * 1000))
         response = logs.describe_log_streams(
            logGroupName=dq_log_group,
            logStreamNamePrefix=dq_log_stream
         )
         event_log = {
            'logGroupName':dq_log_group,
            'logStreamName':dq_log_stream,
            'logEvents':[
                {
                    'timestamp': timestamp,
                    'message': msg
                }
            ]
         }
        
         if 'uploadSequenceToken' in response['logStreams'][0]:
            event_log.update({'sequenceToken': response['logStreams'][0]['uploadSequenceToken']})
            
         response = logs.put_log_events(**event_log)


if job_run:

    ################################### Retriving connection information from Glue connections for Redshift dwstg ##################################
    try:
        logger.info("Getting Redshift dwstg Connection Information")
        rs_connection_dest = glueContext.extract_jdbc_conf(redshiftconnection)
        rs_url_dest = rs_connection_dest["url"]
        rs_user_dest = rs_connection_dest["user"]
        rs_pwd_dest = rs_connection_dest["password"]
        rs_url_db_dest = rs_url_dest + '/' + rsdb
    except Exception as e:
        logger.error(str(e))
        f_msg = " Unable to connect to DWSTG Redshift while processing the glue job {0} , failed with error: {1}  ".format(
            job_name, str(e))
        f_sub = "Stay Evnt Glue job " + job_name + " failed"
        notifymsg(f_sub, f_msg)
        logger.info(" Exiting with error: {}".format(str(e)))
        raise SystemExit(e)
        
    ################################### Retriving connection information from Glue connections for Redshift dw ##################################
    try:
        logger.info("Getting Redshift dw Connection Information")
        rs_connection_dw = glueContext.extract_jdbc_conf(redshift_dw_connection)
        rs_url_dw = rs_connection_dw["url"]
        rs_user_dw = rs_connection_dw["user"]
        rs_pwd_dw = rs_connection_dw["password"]
        rs_url_db_dw = rs_url_dw + '/' + rsdb
    except Exception as e:
        logger.error(str(e))
        f_msg = " Unable to connect to DW Redshift while processing the glue job {0} , failed with error: {1}  ".format(
            job_name, str(e))
        f_sub = "Stay Evnt Glue job " + job_name + " failed"
        notifymsg(f_sub, f_msg)
        logger.info(" Exiting with error: {}".format(str(e)))
        raise SystemExit(e)

        ############################## Reading Data from Processing Folder for latest RSVs #################################################
    try:
        logger.info(" Reading Stay Evnt Data from JSON ")
        stay_s3_read_dyf = glueContext.create_dynamic_frame.from_options(connection_type="s3",
                                                                        connection_options={
                                                                            "paths":
                                                                                [file_path_gen],
                                                                            'recurse': True,
                                                                            'groupFiles': 'inPartition'},
                                                                        format="json",
                                                                        format_options={
                                                                            "jsonPath": "$.stg_stay_evnt[*]"})

        stay_df = stay_s3_read_dyf.toDF()
        stay_df.show()
        stay_df_count = stay_df.count()
        source_count = stay_df.count()
        print('Total Count DF: {}'.format(stay_df_count))
        stay_df.printSchema()
        # stay_df.show(10, False)
        
        ############################## Reading Data from lkup-site file for latest RSVs #################################################
        logger.info(" Reading lku-site Data from JSON ")
        stay_s3_read_lkup_site_dyf = glueContext.create_dynamic_frame.from_options(connection_type="s3", 
                    connection_options={"paths": [s3_lku_site],'recurse':True, 'groupFiles': 'inPartition'},
                    format="json")
                    
        stay_df_lkup_site = stay_s3_read_lkup_site_dyf.toDF()
        stay_df_lkup_site_count = stay_df_lkup_site.count()
        print('Total lkup-site Count DF: {}'.format(stay_df_lkup_site_count))
        stay_df_lkup_site.printSchema()
        
        new_column_name_list = list(map(lambda x:'lkup_'+x,stay_df_lkup_site.columns))
        stay_df_lkup_site_lkup = stay_df_lkup_site.toDF(*new_column_name_list)
        
        print('ok1')
        stay_df_site_id_list = ([data[0] for data in stay_df_lkup_site_lkup.select('lkup_site_id').collect()])
        stay_df_site_id_list_dis = list(set(stay_df_site_id_list))
        print('ok2')
        
        
        """
        print('ok3')
        stay_df_crs_site_id_list = ([data[0] for data in stay_df_lkup_site_lkup.select('lkup_crs_site_id').collect()])
        stay_df_crs_site_id_list_dis = list(set(stay_df_crs_site_id_list))
        print('ok4')
        """
        
        
        
        
        ############################## Reading Data from config  file for latest member num #################################################
        logger.info(" Reading mbr_num Data from txt ")
        stay_df_mbr_num = spark.read.option("header","true").text(s3_mbr_num).selectExpr("value as mbr_num")
        stay_df_mbr_num.show()
        
        stay_df_mbr_num_list=([data[0] for data in stay_df_mbr_num.select('mbr_num').collect()])
        print(stay_df_mbr_num_list)
        


    except Exception as e:
        logger.error(
            "************ {} [ERROR] Exception while reading the file from S3 *************".format(
                str(datetime.now())))
        logger.error("*********** [ERROR] Failing with error:  {} **************".format(str(e)))
        f_msg = "  Error while reading Stay Evnt data from S3 for the job {0} , failed with error: {1}  ".format(
            job_name, str(e))
        f_sub = "Stay Evnt Glue job " + job_name + " failed"
        notifymsg(f_sub, f_msg)
        raise SystemExit(e)

    if stay_df_count == 0:
        logger.info(
            'No Data to process for Stay Evnt for {} run'.format(str(datetime.now())))
        f_msg = "No Data to Process for Stay Evnt Glue Job - {} for run {}".format(job_name,
                                                                                           str(datetime.now()))
        f_sub = "No Data to Process for Stay Evnt Glue Job - {}".format(job_name)
        notifymsg(f_sub, f_msg)
        # exit(0)
    else:    
        logger.info(" Reading from dwstg.rsv_data_valrules")
        ################## Data read from redshift to df for rsv_data_valrules#################################
        try:
            data_valrules_df = glueContext.create_dynamic_frame.from_options(
                                            connection_type = "redshift", 
                                            connection_options = {
                                                            "url": rs_url_db_dest,
                                                            "database": rsdb, 
                                                            "user": rs_user_dest, 
                                                            "password": rs_pwd_dest, 
                                                            "dbtable" : schemaname+'.'+rules_tablename ,  
                                                            "redshiftTmpDir" : "s3://"+blue_bucket+'/sabre/reservation/dataread/rsv_data_valrules/{}/{}/'.format(job_name,str(datetime.now()).replace(" ","_"))
                                                        }                                                
                                                )
            res_df_data_valrules = data_valrules_df.toDF()
            res_df_data_valrules_count = res_df_data_valrules.count()
            print('Total lkup-site Count DF: {}'.format(res_df_data_valrules_count))
            
            logger.info(" Data Read from rsv_data_valrules to df complete")
        
            new_column_name_list = list(map(lambda x:'val_'+x,res_df_data_valrules.columns))
            res_df_data_valrules_val = res_df_data_valrules.toDF(*new_column_name_list)
            
        except Exception as e:
            logger.error(str(e))
            logger.info("  [ERROR] Error while loading to df from rsv_data_valrules table, failed with : {} ".format(str(e)))
            f_msg="  Error while loading to df from rsv_data_valrules table for the job {}, failed with error: {} ".format(job_name,str(e))
            f_sub = "Stay Evnt Glue job Glue job "+job_name+" failed" 
            notifymsg(f_sub, f_msg)
            exit(1)
            
            
        logger.info(" Reading from dw.lkup_brand")
        ################## Data read from redshift to df for lkup_brand#################################
        try:
        
            data_lkup_brand_df = glueContext.create_dynamic_frame.from_options(
                                            connection_type = "redshift", 
                                            connection_options = {
                                                            "url": rs_url_db_dw,
                                                            "database": rsdb, 
                                                            "user": rs_user_dw, 
                                                            "password": rs_pwd_dw, 
                                                            "dbtable" : 'dw'+'.'+'lkup_brand' ,  
                                                            "redshiftTmpDir" : "s3://"+blue_bucket+'/sabre/reservation/dataread/lkup_brand/{}/{}/'.format(job_name,str(datetime.now()).replace(" ","_"))
                                                        }                                                
                                                )
            stay_df_lkup_brand = data_lkup_brand_df.toDF()
            stay_df_lkup_brand_count = stay_df_lkup_brand.count()
            print('Total lkup_brand Count DF: {}'.format(stay_df_lkup_brand_count))
            
            logger.info(" Data Read from lkup_brand to df complete")
        
            new_column_name_list = list(map(lambda x:'lkup_brand_'+x,stay_df_lkup_brand.columns))
            stay_df_lkup_brand_val = stay_df_lkup_brand.toDF(*new_column_name_list)
            stay_df_lkup_brand_id_val_list=([data[0] for data in stay_df_lkup_brand_val.select('lkup_brand_brand_id').collect()])
            print(stay_df_lkup_brand_id_val_list)
                        
            
        except Exception as e:
            logger.error(str(e))
            logger.info("  [ERROR] Error while loading to df from lkup_brand table, failed with : {} ".format(str(e)))
            f_msg="  Error while loading to df from lkup_brand table for the job {}, failed with error: {} ".format(job_name,str(e))
            f_sub = "Stay Evnt Glue job Glue job "+job_name+" failed" 
            notifymsg(f_sub, f_msg)
            exit(1)
            
        logger.info(" Reading from dw.dim_mkt_seg")
        ################## Data read from redshift to df for dim_mkt_seg#################################
        try:
            data_dim_mkt_seg_df = glueContext.create_dynamic_frame.from_options(
                                            connection_type = "redshift", 
                                            connection_options = {
                                                            "url": rs_url_db_dw,
                                                            "database": rsdb, 
                                                            "user": rs_user_dw, 
                                                            "password": rs_pwd_dw, 
                                                            "dbtable" : 'dw'+'.'+'dim_mkt_seg' ,  
                                                            "redshiftTmpDir" : "s3://"+blue_bucket+'/sabre/reservation/dataread/dim_mkt_seg/{}/{}/'.format(job_name,str(datetime.now()).replace(" ","_"))
                                                        }                                                
                                                )
            stay_df_dim_mkt_seg = data_dim_mkt_seg_df.toDF()
            stay_df_dim_mkt_seg_count = stay_df_dim_mkt_seg.count()
            print('Total dim_mkt_seg Count DF: {}'.format(stay_df_dim_mkt_seg_count))
            
            logger.info(" Data Read from dim_mkt_seg to df complete")
        
            new_column_name_list = list(map(lambda x:'mk_'+x,stay_df_dim_mkt_seg.columns))
            stay_df_dim_mkt_seg_val = stay_df_dim_mkt_seg.toDF(*new_column_name_list)
            stay_df_dim_mkt_seg_cd_val_list=([data[0] for data in stay_df_dim_mkt_seg_val.select('mk_mkt_seg_cd').collect()])
            print(stay_df_dim_mkt_seg_cd_val_list)
            
        except Exception as e:
            logger.error(str(e))
            logger.info("  [ERROR] Error while loading to df from dim_mkt_seg table, failed with : {} ".format(str(e)))
            f_msg="  Error while loading to df from dim_mkt_seg table for the job {}, failed with error: {} ".format(job_name,str(e))
            f_sub = "Stay Evnt Glue job Glue job "+job_name+" failed" 
            notifymsg(f_sub, f_msg)
            exit(1)
            
        ################## Data Transformation Step #################################
        try:
            logger.info('Mapping Field Name with Redshift Column')

            # Filling in blank values with Null values
            for x in stay_df.columns:
                stay_df = stay_df.withColumn(x, when(col(x) != '', col(x)).otherwise(None))
            
            # creating temp view for the seq file created while s3 read job
            seq_id_df = glueContext.read.format("csv").option("inferSchema", "false") \
                .option("header", "true").load(seqno_path)
            logger.info(seq_id_df.show())
            seq_id_df.createTempView("id_temp")
            # logger.info("id_temp is created")
            
            
            
            if seq_id_df.count() == 0:
                logger.info(
                    f"No Seq file data present to process Stay Evnt job %s for run %s",
                    job_name, str(datetime.now()))
                msg = f"No Seq file data present to process Stay Evnt Job {job_name} for " \
                      f"run " \
                      f"{str(datetime.now())}"
                sub = f"No Seq file data to Process Stay Evnt Job {job_name}"
                notifymsg(sub, msg)

            else:
                ################## Data Quality Checks #################################
            
                logger.info('Start of DQ checks')
            
                ################## Rule 201 Data Check No Consumer Sequence ID #################################
                logger.info(' Start of DQ check for Rule 201')
                dq_check_rule_201 = stay_df.withColumn('Qid', when(stay_df.stay_file_cnsmr_id.isNull(),'201').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when(stay_df.stay_file_cnsmr_id.isNull(),None).otherwise('NOACTION'))
                
                
                df_DQ_check =  dq_check_rule_201.filter(dq_check_rule_201.Qid != 'NOACTION')
            
                logger.info('DQ Check for Rule 201 Completed ')
            
                ################## Rule 203 Data Check No Brand ID #################################
            
                logger.info('Start of DQ check for Rule 203')
                dq_check_rule_203 = stay_df.withColumn('Qid', when(stay_df.brand_id.isNull(),'203').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when(stay_df.brand_id.isNull(),None).otherwise('NOACTION'))
    
                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_203.filter(dq_check_rule_203.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 203 Completed ')
            
                ################## Rule 205 Data Check No SITE ID #################################
            
                logger.info('Start of DQ check for Rule 205')
                dq_check_rule_205 = stay_df.withColumn('Qid', when(stay_df.site_id.isNull(),'205').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when(stay_df.site_id.isNull(),None).otherwise('NOACTION'))
    
                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_205.filter(dq_check_rule_205.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 205 Completed ')
            
                ################## Rule 212 Data Check No evnt_ref and book_ref #################################
            
                logger.info('Start of DQ check for Rule 212')
                dq_check_rule_212 = stay_df.withColumn('Qid', when((stay_df.evnt_ref.isNull()) & (stay_df.book_ref.isNull()),'212').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((stay_df.evnt_ref.isNull()) & (stay_df.book_ref.isNull()),None).otherwise('NOACTION'))
                
                
                        
                df_DQ_check =  df_DQ_check.union(dq_check_rule_212.filter(dq_check_rule_212.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 212 Completed ')
            
                ################## Rule 213 Data Check No book_ref #################################
            
                logger.info('Start of DQ check for Rule 213')
                dq_check_rule_213 = stay_df.withColumn('Qid', when(stay_df.book_ref.isNull(),'213').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when(stay_df.book_ref.isNull(),None).otherwise('NOACTION'))
    
                
                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_213.filter(dq_check_rule_213.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 213 Completed ')
            
                ################## Rule 214 Data Check No evnt_ref #################################
            
                logger.info('Start of DQ check for Rule 214')
                dq_check_rule_214 = stay_df.withColumn('Qid', when(stay_df.evnt_ref.isNull(),'214').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when(stay_df.evnt_ref.isNull(),None).otherwise('NOACTION'))
    
                                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_214.filter(dq_check_rule_214.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 214 Completed ')
            
                ################## Rule 215 Data Check No mkt_seg_cd #################################
            
                logger.info('Start of DQ check for Rule 215')
                dq_check_rule_215 = stay_df.withColumn('Qid', when(stay_df.mkt_seg_cd.isNull(),'215').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when(stay_df.mkt_seg_cd.isNull(),None).otherwise('NOACTION'))
    
                                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_215.filter(dq_check_rule_215.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 215 Completed ')
            
            
                ################## Rule 218 Data Check No Sale Value #################################
            
                logger.info('Start of DQ check for Rule 218')
                dq_check_rule_218 = stay_df.withColumn('Qid', when(stay_df.actl_psrv_amt.isNull(),'218').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when(stay_df.actl_psrv_amt.isNull(),None).otherwise('NOACTION'))
    
                                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_218.filter(dq_check_rule_218.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 218 Completed ')
            
                ################## Rule 217 Data Check Commodity Code is not HGT #################################
            
                logger.info('Start of DQ check for Rule 217')
                dq_check_rule_217 = stay_df.withColumn('Qid', when(stay_df.cmdy_cd != 'HGT','217').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when(stay_df.cmdy_cd != 'HGT',stay_df.cmdy_cd).otherwise('NOACTION'))
    
                                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_217.filter(dq_check_rule_217.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 217 Completed')
            
            
                ################## Rule 207 Data Check evnt_sts_cd  is not Cancel, Complete, No Show #################################
            
                logger.info('Start of DQ check for Rule 207')
                dq_check_rule_207 = stay_df.withColumn('Qid', when((~stay_df.evnt_sts_cd.isin(['C','X','N'])),'207').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((~stay_df.evnt_sts_cd.isin(['C','X','N'])),stay_df.evnt_sts_cd).otherwise('NOACTION'))
    
                                        
                df_DQ_check =  df_DQ_check.union(dq_check_rule_207.filter(dq_check_rule_207.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 207 Completed')
            
            
                ################## Rule 210 Data Check evnt_strt_dt timestamp is not correct Format #################################
            
                logger.info('Start of DQ check for Rule 210')
                dq_check_rule_210 = stay_df.withColumn('Qid', when((stay_df.evnt_strt_dt.isNotNull()) & (f.to_timestamp(( f.concat( f.trim(f.col('evnt_strt_dt').cast(StringType())).substr(1, 8)) ),'yyyyMMdd').isNull()),'210').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((stay_df.evnt_strt_dt.isNotNull()) & (f.to_timestamp(( f.concat( f.trim(f.col('evnt_strt_dt').cast(StringType())).substr(1, 8)) ),'yyyyMMdd').isNull()),f.col('evnt_strt_dt')).otherwise('NOACTION'))
    
                                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_210.filter(dq_check_rule_210.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 210 Completed')
            
                ################## Rule 211 Data Check evnt_end_dt timestamp is not correct Format #################################
            
                logger.info('Start of DQ check for Rule 211')
                dq_check_rule_211 = stay_df.withColumn('Qid', when((stay_df.evnt_end_dt.isNotNull()) & (f.to_timestamp(( f.concat( f.trim(f.col('evnt_end_dt').cast(StringType())).substr(1, 8)) ),'yyyyMMdd').isNull()),'211').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((stay_df.evnt_end_dt.isNotNull()) & (f.to_timestamp(( f.concat( f.trim(f.col('evnt_end_dt').cast(StringType())).substr(1, 8)) ),'yyyyMMdd').isNull()),f.col('evnt_end_dt')).otherwise('NOACTION'))
    
                                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_211.filter(dq_check_rule_211.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 211 Completed')
            
                        
                ################## Rule 204 Data Check Brand ID is invalid #################################
            
                logger.info('Start of DQ check for Rule 204')
                dq_check_rule_204 = stay_df.withColumn('Qid', when((~stay_df.brand_id.isin(stay_df_lkup_brand_id_val_list)),'204').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((~stay_df.brand_id.isin(stay_df_lkup_brand_id_val_list)),stay_df.brand_id).otherwise('NOACTION'))
    
                                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_204.filter(dq_check_rule_204.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 204 Completed')
            
                        
                ################## Rule 216 Data Check Market Segment is invalid #################################
            
                logger.info('Start of DQ check for Rule 216')
                dq_check_rule_216 = stay_df.withColumn('Qid', when((~stay_df.mkt_seg_cd.isin(stay_df_dim_mkt_seg_cd_val_list)),'216').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((~stay_df.mkt_seg_cd.isin(stay_df_dim_mkt_seg_cd_val_list)),stay_df.mkt_seg_cd).otherwise('NOACTION'))
    
                                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_216.filter(dq_check_rule_216.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 216 Completed')
            
                      
            
                ################## Rule 209 Data Check mbr_num is a test member #################################
            
                logger.info('Start of DQ check for Rule 209')
                dq_check_rule_209 = stay_df.withColumn('Qid', when((stay_df.mbr_num.isin(stay_df_mbr_num_list)),'209').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((stay_df.mbr_num.isin(stay_df_mbr_num_list)),stay_df.mbr_num).otherwise('NOACTION'))
    
                                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_209.filter(dq_check_rule_209.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 209 Completed')
                
                
                
                ################## Rule 206 Data Check site_id is invalid #################################
            
                logger.info('Start of DQ check for Rule 206')
                dq_check_rule_206 = stay_df.withColumn('Qid', when((stay_df.site_id.isNotNull()) & ~(stay_df.site_id.isin(stay_df_site_id_list_dis)),'206').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((stay_df.site_id.isNotNull()) & ~(stay_df.site_id.isin(stay_df_site_id_list_dis)),stay_df.site_id).otherwise('NOACTION'))
    
                #dq_check_rule_206.show(100, False)             
                df_DQ_check =  df_DQ_check.union(dq_check_rule_206.filter(dq_check_rule_206.Qid != 'NOACTION'))
                
                logger.info('DQ Check for Rule 206 Completed')
            
                logger.info(" Data checks completed")
            
                df_DQ_error = df_DQ_check.filter(df_DQ_check.Qid != 'NOACTION')
                ################## Join Dq Val rules table to df_DQ_error data frame based on Qid value #################################
                df_DQ_valrules_join = df_DQ_error.join(res_df_data_valrules_val,(df_DQ_error.Qid == res_df_data_valrules_val.val_id),how='inner')
            
            
                df_DQ_valrules_join.createOrReplaceTempView("DQ_final_ins_view")
                est_now = create_timestamp_est()
                df_DQ_final_ins = spark.sql(f""" select distinct site_id as site_id,data_src_nm as pms_typ_cd,stay_file_cnsmr_id as stay_file_cnsmr_id, val_src_colname as src_column_name,
                                                   src_col_value as src_column_value,val_data_quality_validation_rule as error_message,cast('{est_now}' as timestamp) as insert_ts,brand_id as brand_id,val_id as dataval_id,evnt_ref as pms_conf_num,
                                                    cast(dqsq.job_run_id as bigint) as batch_num,'{tablename}' as table_name, val_quarantine_flag as pros_flag, evnt_strt_dt as evnt_strt_dt,evnt_end_dt as evnt_end_dt from DQ_final_ins_view dqf
                                                    left join id_temp dqsq 
                                                    on SUBSTRING_INDEX(SUBSTRING_INDEX(dqf.src_file_nm,'/',-1),'.json',1) 
                                                    = SUBSTRING_INDEX(SUBSTRING_INDEX(dqsq.filepath,'/',-1),'.json',1)""")
                 
                                         
            
                dyf_DQ_final = DynamicFrame.fromDF(df_DQ_final_ins, glueContext,'dyf_DQ_final')
                df_DQ_final_ins.createTempView("StayDQerror")
                
                # creating temp view for the staging table
                stay_df.createTempView("stg_stay_temp")
                # logger.info("stg_stay_temp is created")
                df_DQ_final_ins.show()
                
                stg_sql = (f""" SELECT st.stay_file_cnsmr_id, st.book_ref, st.evnt_ref, st.evnt_sts_cd, st.evnt_sts_dt,
                            st.bus_unt_cd, st.brand_id, st.site_id, st.evnt_strt_dt, st.evnt_end_dt, st.evnt_ld_dy,
                            st.evnt_dur_dy, st.book_orig_dt, st.book_orig_typ_cd, st.book_orig_sys_nm, st.book_orig_cd,
                            st.corp_cli_id, st.agent_id, st.mbr_cd, st.mbr_num, st.mkt_seg_cd, st.orig_prmt_cd,
                            st.actl_prmt_cd, cast(st.pty_tot_cnt as decimal(10)), cast(st.adult_tot_cnt as decimal(10)),
							cast(st.chld_tot_cnt as decimal(10)), st.chld_pres_ind, st.cmdy_cd,
							cast(st.actl_num_of_rms as decimal(10)), cast(st.actl_psrv_amt as decimal(15,4)),
							st.orig_rt_pln_cd, st.actl_rt_pln_cd, st.prdct_cls_cd, st.pmt_meth_cd, st.pmt_ref,
							cast(sq.job_run_id as bigint) as btch_num, st.data_src_nm, st.src_file_nm, st.cur_rec_ind,
                            st.job_sess_id, sq.job_run_id, st.usr_cmnt, st.chkin_tm, st.chkout_tm, st.chkin_clrk_id,
							st.chkin_clrk_nm, st.chkout_clrk_id, st.chkout_clrk_nm, st.shr_wth_ind, st.pms_corp_acct_nm,
							st.trvl_agnt_nm, st.wyndhm_drct_num, st.wyndhm_drct_ponbr, st.wyndhm_drct_cmpny_nm,
							'Glue' as create_by, cast('{est_now}' as timestamp) as create_ts, 
                            sq.filepath as file_nm, dq.val_quarantine_flag as pros_flag 
                            FROM stg_stay_temp st left join id_temp sq 
                            on SUBSTRING_INDEX(SUBSTRING_INDEX(st.src_file_nm,'/',-1),'.json',1) 
                            = SUBSTRING_INDEX(SUBSTRING_INDEX(sq.filepath,'/',-1),'.json',1)
							left join
							(select AA.stay_file_cnsmr_id,AA.val_quarantine_flag from 
							(select stay_file_cnsmr_id,val_quarantine_flag, ROW_NUMBER() OVER (PARTITION BY stay_file_cnsmr_id ORDER BY val_quarantine_flag) row_num from DQ_final_ins_view) AA where AA.row_num = 1) dq
							on st.stay_file_cnsmr_id = dq.stay_file_cnsmr_id """)

                stg = spark.sql(stg_sql)
                stg.show()
                stg_df = DynamicFrame.fromDF(stg, glueContext, "stg_df")

                stg_table_map = ResolveChoice.apply(
                    frame=stg_df,
                    choice="make_cols",
                    transformation_ctx="stg_table_map"
                )
                stg.createTempView("stg_transformed")

        except Exception as e:
            logger.error(
                "************ {} [ERROR] Exception while doing Data Transformtion on Stay Evnt Data *************".format(
                    str(datetime.now())))
            logger.error(
                "*********** [ERROR] Failing with error:  {} **************".format(str(e)))
            f_msg = "  Error while doing Data Transformtion on Stay Evnt Data for Glue Job {0} , failed with error: {1}  ".format(
                    job_name, str(e))
            f_sub = "Stay Evnt Glue job " + job_name + " failed"
            notifymsg(f_sub, f_msg)
            raise SystemExit(e)
            
        if dyf_DQ_final.count() != 0:
            logger.info(" Starting Data load to DQ Error table")
            try:
                dyf_DQ_final = DropNullFields.apply(frame = dyf_DQ_final, transformation_ctx = "dyf_DQ_final")
                redshift_load = glueContext.write_dynamic_frame.from_jdbc_conf(
                                        frame = dyf_DQ_final, 
                                        catalog_connection = redshiftconnection, 
                                        connection_options = {
                                                            "url": rs_url_db_dest,
                                                            "database": rsdb, 
                                                            "user": rs_user_dest, 
                                                            "password": rs_pwd_dest, 
                                                            "dbtable" : schemaname+'.'+dq_tablename ,
                                                            "extracopyoptions":"MAXERROR 100000"},  
                                        redshift_tmp_dir = "s3://"+blue_bucket+'/pms_stay/dqerrorload/{}/{}/'.format(job_name,str(datetime.now()).replace(" ","_"))
                                            )
                logger.info(" Data Load Process for Stay Data Error to redshift complete")
            except Exception as e:
                logger.error(str(e))
                logger.info("  [ERROR] Error while loading to DQ Error table, failed with : {} ".format(str(e)))
                f_msg="  Error while loading to DQ Error table for the job {}, failed with error: {} ".format(job_name,str(e))
                f_sub = "Stay Evnt Glue job "+job_name+" failed" 
                notifymsg(f_sub, f_msg)
                exit(1)
                
            ########################### Read stay_data_errors_counts for ES logs  ###################################
            logger.info("{}: Reading DQ Data from DQ Load Error Table for Stay_Evnt".format(str(datetime.now())))
            try:
                Errorcount_sql = 'select dataval_id,error_message,count(1) as dq_count from StayDQerror group by dataval_id,error_message'
                print("okay1")
                Errorcount_df = spark.sql(Errorcount_sql)
                print("okay2")
        
                pandDF = Errorcount_df.select(Errorcount_df.dataval_id,Errorcount_df.error_message,Errorcount_df.dq_count).toPandas()
                print(pandDF)
                print("okay3")
                dataval_id_c = list(pandDF['dataval_id'])
                print(dataval_id_c)
                error_message_c = list(pandDF['error_message'])
                print(error_message_c)
                dq_count_c = list(pandDF['dq_count'])
                print(dq_count_c)
                print("okay4")
                rule_id_received = len(dataval_id_c)
                i=0
                while i < rule_id_received:
    
                # Generating logs for ES
                 dqstreaminfo(generateDQESLogs(job_name,dataval_id_c[i],error_message_c[i],log_process_start_datetime,source_count,dq_count_c[i]))
                 i = i+1
                print("okay5")
            except Exception as e:
                logger.error(str(e))
                logger.error("**************** [ERROR] Error reading StayDQerror counts Table, failed with error: {} ***********".format(str(e)))
                f_msg = "**************** [ERROR] Error reading StayDQerror Table Counts, failed with error: {} ***********".format(str(e))
                f_sub = "Stay Evnt Glue job - {} Failed".format(job_name)
                notifymsg(f_sub,f_msg)
                exit(1)
          
            
    #################### Dataload to Redshift ########################
        logger.info(" Starting Data load to Staging table")
        try:
            redshift_load = glueContext.write_dynamic_frame.from_jdbc_conf(
                frame=stg_table_map,
                catalog_connection=redshiftconnection,
                connection_options={
                    "url": rs_url_db_dest,
                    "database": rsdb,
                    "user": rs_user_dest,
                    "password": rs_pwd_dest,
                    "dbtable": schemaname + '.' + tablename,
                    "extracopyoptions": "MAXERROR 100000"},
                redshift_tmp_dir="s3://" + blue_bucket + '/pms_stay/dataload/{}/{}/'.format(
                    job_name, str(datetime.now()).replace(" ", "_"))
            )
            logger.info(" Data Load Process for Stay Evnt to redshift complete")
        except Exception as e:
            logger.error(str(e))
            logger.info(
                "[ERROR] Error while loading to Redshift Staging table, failed with : {} ".format(
                    str(e)))
            f_msg = "  Error while loading to Staging table for the job {}, failed with error: {} ".format(
                job_name, str(e))
            f_sub = "Stay Evnt Glue job " + job_name + " failed"
            notifymsg(f_sub, f_msg)
            raise SystemExit(e)

         
    
                 
                
        ####################### Creating a Marker File ###########################
        try:
            response = s3Client.put_object(Bucket=blue_bucket, Body="Completed", Key=MARKER_FILE)
        except Exception as e:
            logger.error(
                "************ {} [ERROR] Exception while writing the marker file to S3 for Stay Evnt Glue Job************".format(
                    str(datetime.now())))
            logger.error(
                "*********** [ERROR] Failing with error: {} **************".format(str(e)))
            f_msg = "  Error while writing the marker file to S3 for Stay Evnt Glue Job {}, failed with Error: {}".format(
                job_name, str(e))
            f_sub = "Error writing Marker File for Glue Job - {}".format(job_name)
            notifymsg(f_sub, f_msg)
            raise SystemExit(e)

logger.info(
    " ************** {} End of Load process for Stay Evnt *********************** ".format(
        str(datetime.now())))
job.commit()
logger.info("JOB COMPLETE!!")