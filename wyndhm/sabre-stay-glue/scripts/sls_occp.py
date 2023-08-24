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
s3_pms_src_nm ='s3://{}/pms_stay/config/pms_src_nm/'.format(blue_bucket)
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
    

dq_log_stream_n = 'DQSLSGlueLogs'
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
    
    DQ_logs_json_ = json.dumps({"source_system":"Sabre PMS","domain":"Stay","subdomain":"","process_name":"Stay Sls Occp Job","process_arn":"{}".format(job_name),
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
        logger.info(" Reading Stay Sls Occp Data from JSON ")
        stay_s3_read_dyf = glueContext.create_dynamic_frame.from_options(connection_type="s3",
                                                                        connection_options={
                                                                            "paths":
                                                                                [file_path_gen],
                                                                            'recurse': True,
                                                                            'groupFiles': 'inPartition'},
                                                                        format="json",
                                                                        format_options={
                                                                            "jsonPath": "$.stg_stay_sls_occp[*]"})

        stay_df = stay_s3_read_dyf.toDF()
        stay_df.show()
        stay_df_count = stay_df.count()
        source_count = stay_df.count()
        print('Total Cound DF: {}'.format(stay_df_count))
        # stay_df.printSchema()
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
        #print(stay_df_site_id_list_dis)
        print('ok2')
        
        """
        print('ok3')
        stay_df_crs_site_id_list = ([data[0] for data in stay_df_lkup_site_lkup.select('lkup_crs_site_id').collect()])
        stay_df_crs_site_id_list_dis = list(set(stay_df_crs_site_id_list))
        #print(stay_df_crs_site_id_list_dis)
        print('ok4')
        """
        
        
        
        ############################## Reading Data from config  file for latest pms_src_nm #################################################
        logger.info(" Reading pms_src_nm Data from txt ")
        stay_df_pms_src_nm = spark.read.option("header","true").text(s3_pms_src_nm).selectExpr("value as pms_src_nm")
        stay_df_pms_src_nm.show()
        
        stay_df_pms_src_nm_list=([data[0] for data in stay_df_pms_src_nm.select('pms_src_nm').collect()])
        print(stay_df_pms_src_nm_list)
   


    except Exception as e:
        logger.error(
            "************ {} [ERROR] Exception while reading the file from S3 *************".format(
                str(datetime.now())))
        logger.error("*********** [ERROR] Failing with error:  {} **************".format(str(e)))
        f_msg = "  Error while reading Stay Sls Occp data from S3 for the job {0} , failed with error: {1}  ".format(
            job_name, str(e))
        f_sub = "Stay Sls Occp Glue job " + job_name + " failed"
        notifymsg(f_sub, f_msg)
        raise SystemExit(e)

    if stay_df_count == 0:
        logger.info(
            'No Data to process for Stay Sls Occp for {} run'.format(str(datetime.now())))
        f_msg = "No Data to Process for Stay Sls Occp Glue Job - {} for run {}".format(job_name,
                                                                                           str(datetime.now()))
        f_sub = "No Data to Process for Stay Sls Occp Glue Job - {}".format(job_name)
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
            f_sub = "SStay Sls Occp Glue job "+job_name+" failed" 
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
            f_sub = "Stay Sls Occp Glue job "+job_name+" failed" 
            notifymsg(f_sub, f_msg)
            exit(1)
        logger.info(" Reading from dw.dim_mkt_seg")

           
        ################## Data Transformation Step #################################
        try:
            logger.info('Mapping Field Name with Redshift Column')

            # Filling in blank values with Null values
            for x in stay_df.columns:
                stay_df = stay_df.withColumn(x, when(col(x) != '', col(x)).otherwise(None))
            # creating temp view for the staging table
            stay_df.createTempView("stg_stay_temp")
            # logger.info("stg_stay_temp is created")
            # creating temp view for the seq file created while s3 read job
            seq_id_df = glueContext.read.format("csv").option("inferSchema", "false") \
                .option("header", "true").load(seqno_path)
            logger.info(seq_id_df.show())
            seq_id_df.createTempView("id_temp")
            # logger.info("id_temp is created")
            if seq_id_df.count() == 0:
                logger.info(
                    f"No Seq file data present to process Stay Sls Occp job %s for run %s",
                    job_name, str(datetime.now()))
                msg = f"No Seq file data present to process Stay Sls Occp Job {job_name} for " \
                      f"run " \
                      f"{str(datetime.now())}"
                sub = f"No Seq file data to Process Stay Sls Occp Job {job_name}"
                notifymsg(sub, msg)

            else:
                ################## Data Quality Checks #################################
            
                logger.info('Start of DQ checks')
                ################## Rule 202 Data Check No Data Source #################################
                logger.info(' Start of DQ check for Rule 202')
                dq_check_rule_202 = stay_df.withColumn('Qid', when(stay_df.data_src_nm.isNull(),'202').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when(stay_df.data_src_nm.isNull(),None).otherwise('NOACTION'))
                df_DQ_check =  dq_check_rule_202.filter(dq_check_rule_202.Qid != 'NOACTION')
                logger.info('DQ Check for Rule 202 Completed ')
            
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
            
                ################## Rule 219 Data Check No curr_cd #################################
                logger.info('Start of DQ check for Rule 219')
                dq_check_rule_219 = stay_df.withColumn('Qid', when(stay_df.curr_cd.isNull() ,'219').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when(stay_df.curr_cd.isNull(),None).otherwise('NOACTION'))
                df_DQ_check =  df_DQ_check.union(dq_check_rule_219.filter(dq_check_rule_219.Qid != 'NOACTION'))
                logger.info('DQ Check for Rule 219 Completed ')
            
                ################## Rule 220 Data Check No rm_sld_cnt #################################
                logger.info('Start of DQ check for Rule 220')
                dq_check_rule_220 = stay_df.withColumn('Qid', when(stay_df.rm_sld_cnt.isNull(),'220').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when(stay_df.rm_sld_cnt.isNull(),None).otherwise('NOACTION'))
                df_DQ_check =  df_DQ_check.union(dq_check_rule_220.filter(dq_check_rule_220.Qid != 'NOACTION'))
                logger.info('DQ Check for Rule 220 Completed ')
            
                ################## Rule 221 Data Check No rm_rvnu_tot_amt #################################
                logger.info('Start of DQ check for Rule 221')
                dq_check_rule_221 = stay_df.withColumn('Qid', when((stay_df.rm_sld_cnt != 0) & ((stay_df.rm_rvnu_tot_amt.isNull()) | (stay_df.rm_rvnu_tot_amt.isin(['0','0.0000']))),'221').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((stay_df.rm_sld_cnt != 0) & ((stay_df.rm_rvnu_tot_amt.isNull()) | (stay_df.rm_rvnu_tot_amt.isin(['0','0.0000']))),stay_df.rm_rvnu_tot_amt).otherwise('NOACTION'))
                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_221.filter(dq_check_rule_221.Qid != 'NOACTION'))
                logger.info('DQ Check for Rule 221 Completed ')
                                               
                ################## Rule 222 Data Check No rm_rvnu_adj_amt #################################
                logger.info('Start of DQ check for Rule 222')
                dq_check_rule_222 = stay_df.withColumn('Qid', when(stay_df.rm_rvnu_adj_amt < 0 ,'222').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when(stay_df.rm_rvnu_adj_amt < 0,stay_df.rm_rvnu_adj_amt).otherwise('NOACTION'))
                df_DQ_check =  df_DQ_check.union(dq_check_rule_222.filter(dq_check_rule_222.Qid != 'NOACTION'))
                logger.info('DQ Check for Rule 222 Completed ')
            
                ################## Rule 224 Data Check No fns_excl_rm_occp_pct #################################
                logger.info('Start of DQ check for Rule 224')
                dq_check_rule_224 = stay_df.withColumn('Qid', when((stay_df.fns_occp_rm_cnt != 0) & ((stay_df.fns_excl_rm_occp_pct.isin(['0','0.0000']) | (stay_df.fns_excl_rm_occp_pct.isNull()))) ,'224').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((stay_df.fns_occp_rm_cnt != 0) & ((stay_df.fns_excl_rm_occp_pct.isin(['0','0.0000']) & (stay_df.fns_excl_rm_occp_pct.isNull()))),stay_df.fns_excl_rm_occp_pct).otherwise('NOACTION'))
                df_DQ_check =  df_DQ_check.union(dq_check_rule_224.filter(dq_check_rule_224.Qid != 'NOACTION'))
                logger.info('DQ Check for Rule 224 Completed ')
            
                ################## Rule 225 fns_rm_reim #################################
                logger.info('Start of DQ check for Rule 225')
                dq_check_rule_225 = stay_df.withColumn('Qid', when((stay_df.fns_occp_rm_cnt != 0) & ((stay_df.fns_rm_reim.isin(['0','0.0000'])) | (stay_df.fns_rm_reim.isNull())) ,'225').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((stay_df.fns_occp_rm_cnt != 0) & ((stay_df.fns_rm_reim.isin(['0','0.0000'])) | (stay_df.fns_rm_reim.isNull())),stay_df.fns_rm_reim).otherwise('NOACTION'))
                df_DQ_check =  df_DQ_check.union(dq_check_rule_225.filter(dq_check_rule_225.Qid != 'NOACTION'))
                logger.info('DQ Check for Rule 225 Completed')
            
                ################## Rule 226 fns_rm_reim_wthout_tx_amt #################################
                logger.info('Start of DQ check for Rule 226')
                dq_check_rule_226 = stay_df.withColumn('Qid', when((stay_df.fns_occp_rm_cnt != 0) & ((stay_df.fns_rm_reim_wthout_tx_Amt.isin(['0','0.0000'])) | (stay_df.fns_rm_reim_wthout_tx_Amt.isNull())) ,'226').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((stay_df.fns_occp_rm_cnt != 0) & ((stay_df.fns_rm_reim_wthout_tx_Amt.isin(['0','0.0000'])) | (stay_df.fns_rm_reim_wthout_tx_Amt.isNull())),stay_df.fns_rm_reim_wthout_tx_Amt).otherwise('NOACTION'))
                df_DQ_check =  df_DQ_check.union(dq_check_rule_226.filter(dq_check_rule_226.Qid != 'NOACTION'))
                logger.info('DQ Check for Rule 226 Completed')
                
                ################## Rule 227 loylty_adr #################################
                logger.info('Start of DQ check for Rule 227')
                dq_check_rule_227 = stay_df.withColumn('Qid', when((stay_df.fns_occp_rm_cnt != 0) & ((stay_df.loylty_adr.isin(['0','0.0000']))|(stay_df.loylty_adr.isNull())) ,'227').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((stay_df.fns_occp_rm_cnt != 0) & ((stay_df.loylty_adr.isin(['0','0.0000']))|(stay_df.loylty_adr.isNull())),stay_df.loylty_adr).otherwise('NOACTION'))
                
                df_DQ_check =  df_DQ_check.union(dq_check_rule_227.filter(dq_check_rule_227.Qid != 'NOACTION'))
                logger.info('DQ Check for Rule 227 Completed')
                
                ################## Rule 204 Data Check Brand ID is invalid #################################
            
                logger.info('Start of DQ check for Rule 204')
                dq_check_rule_204 = stay_df.withColumn('Qid', when((~stay_df.brand_id.isin(stay_df_lkup_brand_id_val_list)),'204').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((~stay_df.brand_id.isin(stay_df_lkup_brand_id_val_list)),stay_df.brand_id).otherwise('NOACTION'))
    
                        
                df_DQ_check =  df_DQ_check.union(dq_check_rule_204.filter(dq_check_rule_204.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 204 Completed')
                
                ################## Rule 208 Data Check PMS_TYPE is a test member #################################
            
                logger.info('Start of DQ check for Rule 208')
                dq_check_rule_208 = stay_df.withColumn('Qid', when((~stay_df.data_src_nm.isin(stay_df_pms_src_nm_list)),'208').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((~stay_df.data_src_nm.isin(stay_df_pms_src_nm_list)),stay_df.data_src_nm).otherwise('NOACTION'))
    
                               
                df_DQ_check =  df_DQ_check.union(dq_check_rule_208.filter(dq_check_rule_208.Qid != 'NOACTION'))
            
                logger.info('DQ Check for Rule 208 Completed')
                
                ################## Rule 206 Data Check site_id is invalid #################################
            
                logger.info('Start of DQ check for Rule 206')
                dq_check_rule_206 = stay_df.withColumn('Qid', when((stay_df.site_id.isNotNull()) & ~(stay_df.site_id.isin(stay_df_site_id_list_dis)),'206').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when((stay_df.site_id.isNotNull()) & ~(stay_df.site_id.isin(stay_df_site_id_list_dis)),stay_df.site_id).otherwise('NOACTION'))
    
                               
                df_DQ_check =  df_DQ_check.union(dq_check_rule_206.filter(dq_check_rule_206.Qid != 'NOACTION'))
                
                logger.info('DQ Check for Rule 206 Completed')
                
                ################## Rule 223 loylty_adr #################################
                logger.info('Start of DQ check for Rule 223')
                dq_check_rule_223 = stay_df.withColumn('Qid', when(stay_df.rm_at_prop_cnt == 0 ,'223').otherwise('NOACTION')) \
                                        .withColumn('src_col_value', when(stay_df.rm_at_prop_cnt == 0,stay_df.loylty_adr).otherwise('NOACTION'))
                df_DQ_check =  df_DQ_check.union(dq_check_rule_223.filter(dq_check_rule_223.Qid != 'NOACTION'))
                logger.info('DQ Check for Rule 223 Completed')
                
               

                logger.info(" Data checks completed")
                
                df_DQ_error = df_DQ_check.filter(df_DQ_check.Qid != 'NOACTION')
                ################## Join Dq Val rules table to df_DQ_error data frame based on Qid value #################################
                df_DQ_valrules_join = df_DQ_error.join(res_df_data_valrules_val,(df_DQ_error.Qid == res_df_data_valrules_val.val_id),how='inner')

                df_DQ_valrules_join.createOrReplaceTempView("DQ_final_ins_view")
            
                est_now = create_timestamp_est()
                df_DQ_final_ins = spark.sql(f"""select distinct site_id as site_id,data_src_nm as pms_typ_cd, val_src_colname as src_column_name,
                                                   src_col_value as src_column_value,val_data_quality_validation_rule as error_message,cast('{est_now}' as timestamp) as insert_ts,brand_id as brand_id,val_id as dataval_id,
                                                    cast(dqsq.job_run_id as bigint) as batch_num,'{tablename}' as table_name, val_quarantine_flag as pros_flag,to_date(bus_dt,'yyyyMMdd') as stay_dt from DQ_final_ins_view dqf
                                                    left join id_temp dqsq 
                                                    on SUBSTRING_INDEX(SUBSTRING_INDEX(dqf.src_file_nm,'/',-1),'.json',1) 
                                                    = SUBSTRING_INDEX(SUBSTRING_INDEX(dqsq.filepath,'/',-1),'.json',1)""")
                                                    
                dyf_DQ_final = DynamicFrame.fromDF(df_DQ_final_ins, glueContext,'dyf_DQ_final')
                df_DQ_final_ins.createTempView("StayDQerror")
                
                df_DQ_final_ins.show()
                print(df_DQ_final_ins.count())
                stg_sql = (f""" SELECT st.site_id, st.brand_id, st.bus_dt, cast(st.rm_sld_cnt as integer),
                            cast(st.dy_use_rm_sld_cnt as integer), cast(st.cmplmt_rm_sld_cnt as integer), 
                            cast(st.rm_rvnu_tot_amt as decimal(15,4)), cast(st.rm_rvnu_adj_amt as decimal(15,4)),
                            cast(st.rm_at_prop_cnt as integer), cast(st.rm_off_mkt_cnt as integer), 
                            cast(st.crs_orig_rsrv_no_show_cnt as integer), cast(st.prop_orig_rsrv_no_show_cnt as integer), 
                            cast(st.crs_orig_rsrv_canc_cnt as integer), cast(st.prop_orig_rsrv_canc_cnt as integer),
                            cast(st.trn_dn as decimal(15,4)), cast(st.fns_occp_rm_cnt as integer),
                            cast(st.fns_incl_rm_occp_pct as decimal(15,4)), cast(st.fns_excl_rm_occp_pct as decimal(15,4)), 
                            cast(st.fns_excl_rm_sld_cnt as integer), cast(st.fns_incl_avg_dly_rt as decimal(15,4)),
                            cast(st.fns_excl_avg_dly_rt as decimal(15,4)), cast(st.fns_rm_reim as decimal(15,4)),
                            cast(st.rm_rvnu_adj_allow_amt as decimal(15,4)), cast(st.rm_rvnu_adj_disall_amt as decimal(15,4)), 
                            st.fix_ind, st.reltv_rm_rt_ind, st.pms_typ_cd, st.pms_ver, st.dy_use_pd_rm_cnt, st.dy_use_unpd_rm_cnt,
                            cast(st.dy_use_rm_rvnu as decimal(15,4)), st.no_show_pd_rm_cnt, st.no_show_unpd_rm_cnt, 
                            cast(st.no_show_rm_rvnu as decimal(15,4)), st.othr_pd_rm_cnt, cast(st.othr_rm_rvnu as decimal(15,4)),
                            st.in_house_pd_rm_cnt, cast(st.in_house_rm_rvnu as decimal(15,4)), st.tot_rm_pd_cnt,
                            cast(sq.job_run_id as bigint) as btch_num, st.data_src_nm, st.src_file_nm, st.cur_rec_ind,
                            st.job_sess_id, sq.job_run_id, st.usr_cmnt, cast(st.zero_rt_rms as decimal(15,4)),
                            cast(st.loylty_adr as decimal(15,4)), cast(st.adr_wth_fns as decimal(15,4)),
                            cast(st.adr_wthout_fns as decimal(15,4)), cast(st.avg_rev_per_rm as decimal(15,4)),
                            cast(st.rev_per_avlbl_rm as decimal(15,4)), cast(st.tot_trnsctn as integer),
                            cast(st.tot_walkins as integer), cast(st.tot_no_show as integer), cast(st.tot_rsrv_cxcl as integer),
                            cast(st.tot_early_chkins as integer), cast(st.tot_erly_chkouts as integer),
                            cast(st.tot_txble_rm_rev as decimal(15,4)), cast(st.tot_non_txble_rm_rev as decimal(15,4)),
                            cast(st.tot_partly_txble_rm_rev as decimal(15,4)), cast(st.tot_food_bvrg_rev as decimal(15,4)),
                            cast(st.tot_tax_rev as decimal(15,4)), cast(st.tot_othr_rev as decimal(15,4)),
                            cast(st.tot_rev as decimal(15,4)), cast(st.tot_csh_paymnts as decimal(15,4)),
                            cast(st.tot_crdt_crd_paymnts as decimal(15,4)), cast(st.tot_paymnts as decimal(15,4)),
                            cast(st.tot_drct_bill_trnsfrs as decimal(15,4)), st.curr_cd, cast(st.out_of_invntry_rms as integer),
                            cast(st.out_of_ordr_rms as integer), cast(st.fns_rm_reim_wthout_tx_Amt as decimal(15,4)),
                            'Glue' as create_by, cast('{est_now}' as timestamp) as create_ts,
                            sq.filepath as file_nm, dq.val_quarantine_flag as pros_flag 
                            FROM stg_stay_temp st left join id_temp sq 
                            on SUBSTRING_INDEX(SUBSTRING_INDEX(st.src_file_nm,'/',-1),'.json',1) 
                            = SUBSTRING_INDEX(SUBSTRING_INDEX(sq.filepath,'/',-1),'.json',1)
                            left join
							(select AA.site_id,AA.brand_id,AA.bus_dt,AA.val_quarantine_flag from 
							(select site_id,brand_id,bus_dt,val_quarantine_flag, ROW_NUMBER() OVER (PARTITION BY site_id,brand_id,bus_dt ORDER BY val_quarantine_flag) row_num from DQ_final_ins_view) AA where AA.row_num = 1) dq
							on st.site_id = dq.site_id and st.brand_id = dq.brand_id and st.bus_dt = dq.bus_dt""")

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
                "************ {} [ERROR] Exception while doing Data Transformtion on Stay Sls Occp Data *************".format(
                    str(datetime.now())))
            logger.error(
                "*********** [ERROR] Failing with error:  {} **************".format(str(e)))
            f_msg = "  Error while doing Data Transformtion on Stay Sls Occp Data for Glue Job {0} , failed with error: {1}  ".format(
                    job_name, str(e))
            f_sub = "Stay Sls Occp Glue job " + job_name + " failed"
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
                logger.info(" Data Load Process for Stay Sls Occp DQ Error table to redshift complete")
            except Exception as e:
                logger.error(str(e))
                logger.info("  [ERROR] Error while loading to DQ Error table, failed with : {} ".format(str(e)))
                f_msg="  Error while loading to DQ Error table for the job {}, failed with error: {} ".format(job_name,str(e))
                f_sub = "Stay Sls Occp Glue job "+job_name+" failed" 
                notifymsg(f_sub, f_msg)
                exit(1)
                
            ########################### Read stay_data_errors_counts for ES logs  ###################################
            logger.info("{}: Reading DQ Data from DQ Load Error Table for SLS_OCCP".format(str(datetime.now())))
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
                f_sub = "SLS OCCP Glue job - {} Failed".format(job_name)
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
            logger.info(" Data Load Process for Stay Sls Occp to redshift complete")
        except Exception as e:
            logger.error(str(e))
            logger.info(
                "[ERROR] Error while loading to Redshift Staging table, failed with : {} ".format(
                    str(e)))
            f_msg = "  Error while loading to Staging table for the job {}, failed with error: {} ".format(
                job_name, str(e))
            f_sub = "Stay Sls Occp Glue job " + job_name + " failed"
            notifymsg(f_sub, f_msg)
            raise SystemExit(e)
        
        
    
                
        ####################### Creating a Marker File ###########################
        try:
            response = s3Client.put_object(Bucket=blue_bucket, Body="Completed", Key=MARKER_FILE)
        except Exception as e:
            logger.error(
                "************ {} [ERROR] Exception while writing the marker file to S3 for Stay Sls Occp Glue Job************".format(
                    str(datetime.now())))
            logger.error(
                "*********** [ERROR] Failing with error: {} **************".format(str(e)))
            f_msg = "  Error while writing the marker file to S3 for Stay Sls Occp Glue Job {}, failed with Error: {}".format(
                job_name, str(e))
            f_sub = "Error writing Marker File for Glue Job - {}".format(job_name)
            notifymsg(f_sub, f_msg)
            raise SystemExit(e)

logger.info(
    " ************** {} End of Load process for Stay Sls Occp *********************** ".format(
        str(datetime.now())))
job.commit()
logger.info("JOB COMPLETE!!")
