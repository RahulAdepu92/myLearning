import sys
from datetime import date, datetime
from datetime import timedelta
import pytz
import json
import time
import pandas
from pytz import timezone
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SQLContext
import boto3
import logging
from requests.utils import requote_uri
from pyspark.sql.functions import col, when, lit, input_file_name, substring_index

################################### Setting up Spark environment and enabling Glue to interact with Spark Platform ##################################
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)

################################### Enabling access to the arguments ##################################
job = Job(glueContext)
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME', 'blue_bucket', 'error_bucket', 'SNS', 'rsdb', 'redshiftconnection', 'schemaname',
                           'audittable', 'ScriptBucketName', 'configfile', 'glueerrortable', 'ESLogGroup','gold_bucket'])
job.init(args['JOB_NAME'], args)

################################### Setting up logger for event logging ##################################
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

################################### Assigning job arguments to variables  ##################################

rsdb = args['rsdb']
redshiftconnection = args['redshiftconnection']
blue_bucket = args['blue_bucket']
file_path_gen = 's3://{}/pms_stay/processing/'.format(blue_bucket)
error_bucket = args['error_bucket']
gold_bucket = args['gold_bucket']
job_name = args['JOB_NAME']
schemaname = args['schemaname']
audittable = args['audittable']
glueerrortable = args['glueerrortable']
sns_notify = args['SNS']
# MARKER_FILE = "/Stay/glue-workflow-marker-triggers/" +args["JOB_NAME"]
seqno_path = "s3://{}/pms_stay/seqno/".format(blue_bucket)
REDSHIFT_PATH = "s3://{}/pms_stay/redshift/".format(blue_bucket)
es_log_group = args['ESLogGroup']
# setting path for stl_load_errors file
today = datetime.now().strftime('%Y/%m/%d')
stl_load_errors_file_path = f"s3://{blue_bucket}/pms_stay/stl_load_errors/{today}/"

################################### Create low level reosurce service client for S3 ans SNS  ##################################

s3Client = boto3.client('s3', region_name='us-west-2')
s3resource = boto3.resource('s3', region_name='us-west-2')
glueClient = boto3.client('glue', region_name='us-west-2')

########## Structuring Log Stream for ES Logs ############
log_stream_n = 'GlueLogs'
ts = datetime.now()
ts_est = timezone('US/Eastern').localize(ts)
ts_est_str = ts_est.strftime('%Y-%m-%d-%H-%M-%S')
log_stream =  '{}-{}'.format(log_stream_n,ts_est_str)


#########################################Create ESLogGroup stream################################### 
try:
    logs = boto3.client('logs', region_name = 'us-west-2')
    logs.create_log_stream(logGroupName=es_log_group, logStreamName=log_stream)
    logger.info("Connected to Log Stream {}".format(es_log_group))
except Exception as e:
    logger.error(str(e))

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
    logger.info("**************** [INFO] SNS Notification Sent: {} *************************".format(job_name))

def create_timestamp_est():
    utc_now = datetime.now()
    est = pytz.timezone('US/Eastern')
    est_now = utc_now.astimezone(est).strftime('%Y-%m-%d %H:%M:%S.%f')
    return est_now

def create_trigger_file_ts():
    now = datetime.now()
    
    est = pytz.timezone('US/Eastern')
    now_est = now.astimezone(est).strftime('%Y-%m-%d %H:%M:%S.%f')
    return now_est

################## Generate ES Logs #################
def generateESLogs(prc_arn,sts,err_desc,src_sys_ts,prc_start_dtm,prc_end_dtm,src_cnt,tgt_success_cnt,tgt_failed_cnt):
    
    _logs_json = json.dumps({
        "source_system": "PMS",
        "domain": "Stay",
        "subdomain": None,
        "process_name": "Stay Batch Load to Redshift Stage",
        "process_arn": "{}".format(prc_arn),
        "task_name": "Glue Batch Load",
        "rule_id": None,
        "rule_name": None,
        "status": sts,
        "source_file_name": None,
        "target_name": None,
        "unique_id": None,
        "unique_value_1": None,
        "unique_value_2": None,
        "unique_value_3": None,
        "error_desc": err_desc,
        "design_pattern": "Batch",
        "source_system_timestamp": src_sys_ts,
        "business_date": None,
        "orginal_file_name": None,
        "process_start_datetime": prc_start_dtm,
        "process_end_datetime": prc_end_dtm,
        "batch_id": None,
        "source_file_count": src_cnt,
        "target_success_count": tgt_success_cnt,
        "target_failed_count": tgt_failed_cnt
    })
    
    return _logs_json

######### Generate CW Log Events ############
def streaminfo(msg):
        timestamp = int(round(time.time() * 1000))
        response = logs.describe_log_streams(
            logGroupName=es_log_group,
            logStreamNamePrefix=log_stream
        )
        event_log = {
            'logGroupName':es_log_group,
            'logStreamName':log_stream,
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
            logGroupName=es_log_group,
            logStreamNamePrefix=log_stream
         )
         event_log = {
            'logGroupName':es_log_group,
            'logStreamName':log_stream,
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
         
################################### Retrieving connection information from Glue connections for Redshift  ##################################
try:
    logger.info("Getting Redshift Connection Information")
    rs_connection_dest = glueContext.extract_jdbc_conf(redshiftconnection)
    rs_url_dest = rs_connection_dest["url"]
    rs_user_dest = rs_connection_dest["user"]
    rs_pwd_dest = rs_connection_dest["password"]
    rs_url_db_dest = rs_url_dest + '/' + rsdb
except Exception as e:
    logger.error(str(e))
    f_msg = " Unable to connect to Redshift while processing the glue job {0} , failed with error: {1}  ".format(
        job_name, str(e))
    f_sub = "Stay Btch Hdr Glue job " + job_name + " failed"
    notifymsg(f_sub, f_msg)
    logger.info(" Exiting with error: {}".format(str(e)))
    streaminfo(generateESLogs(job_name,"ERROR",str(e),create_timestamp_est(),create_timestamp_est(),create_timestamp_est(),0,0,0))
    raise SystemExit(e)

############################## Reading Data from Processing Folder for latest RSVs #################################################
try:
    logger.info(" Reading Stay Btch Hdr Data from JSON ")
    stay_s3_read_dyf = glueContext.create_dynamic_frame.from_options(connection_type="s3",
                                                                        connection_options={
                                                                            "paths":
                                                                                [file_path_gen],
                                                                            'recurse': True,
                                                                            'groupFiles': 'inPartition'},
                                                                        format="json",
                                                                        format_options={
                                                                            "jsonPath": "$.stg_btch_hdr[*]"})

    stay_df = stay_s3_read_dyf.toDF()
    stay_df.show()
    stay_df_count = stay_df.count()
    print('Total Cound DF: {}'.format(stay_df_count))
    # stay_df.printSchema()
    # stay_df.show(10, False)

except Exception as e:
    logger.error(
        "************ {} [ERROR] Exception while reading the file from S3 *************".format(
                str(datetime.now())))
    logger.error("*********** [ERROR] Failing with error:  {} **************".format(str(e)))
    f_msg = "  Error while reading Stay Btch Hdr data from S3 for the job {0} , failed with error: {1}  ".format(
            job_name, str(e))
    f_sub = "Stay Btch Hdr Glue job " + job_name + " failed"
    notifymsg(f_sub, f_msg)
    streaminfo(generateESLogs(job_name,"ERROR",str(e),create_timestamp_est(),create_timestamp_est(),create_timestamp_est(),0,0,0))
    raise SystemExit(e)

if stay_df_count == 0:
    logger.info(
        'No Data to process for Stay Btch Hdr for {} run'.format(str(datetime.now())))
    f_msg = "No Data to Process for Stay Btch Hdr Glue Job - {} for run {}".format(job_name,
                                                                                str(datetime.now()))
    f_sub = "No Data to Process for Stay Btch Hdr Glue Job - {}".format(job_name)
    notifymsg(f_sub, f_msg)
    # exit(0)
else:
    ################## Data Transformation Step #################################
    try:
        logger.info('Mapping Field Name with Redshift Column')

        # Filling in blank values with Null values
        for x in stay_df.columns:
            stay_df = stay_df.withColumn(x, when(col(x) != '', col(x)).otherwise(None))
        # creating temp view for the staging table
        stay_df.createTempView("stg_stay_temp")
        logger.info("stg_stay_temp is created")

    except Exception as e:
        logger.error(
            "************ {} [ERROR] Exception while doing Data Transformtion on Stay Btch Hdr Data *************".format(
                str(datetime.now())))
        logger.error(
            "*********** [ERROR] Failing with error:  {} **************".format(str(e)))
        f_msg = "  Error while doing Data Transformtion on Stay Btch Hdr Data for Glue Job {0} , failed with error: {1}  ".format(
                    job_name, str(e))
        f_sub = "Stay Btch Hdr Glue job " + job_name + " failed"
        notifymsg(f_sub, f_msg)
        streaminfo(generateESLogs(job_name,"ERROR",str(e),create_timestamp_est(),create_timestamp_est(),create_timestamp_est(),int(stay_df_count),0,0))
        raise SystemExit(e)

############ Read the seqfile from s3, written by s3read job initially

    try:
        seqno_df = glueContext.read \
            .format("csv") \
            .option("inferSchema", "false") \
            .option("header", "true") \
            .load(seqno_path)
        # print(seqno_df.show())
        source_count = seqno_df.count()
        seqno_df.createTempView("id_temp")
        logger.info("id_temp is created")

    except Exception as e:
        logger.error(str(e))
        logger.error("**************** [ERROR] Error reading seqno file, failed with error: {} ***********".format(str(e)))
        f_msg = "**************** [ERROR] Error reading seqno file, failed with error: {} ***********".format(str(e))
        f_sub = "Stay Btch Hdr Job - {} Failed".format(job_name)
        notifymsg(f_sub, f_msg)
        streaminfo(generateESLogs(job_name,"ERROR",str(e),create_timestamp_est(),create_timestamp_est(),create_timestamp_est(),int(stay_df_count),0,0))
        raise SystemExit(e)

########################### Read STL_LOAD_ERRORS ###################################

    logger.info("{}: Getting Data from Load Error Table for Audit".format(str(datetime.now())))

    try:
        RSerror_df = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "url": rs_url_db_dest,
                "database": rsdb,
                "user": rs_user_dest,
                "password": rs_pwd_dest,
                "dbtable": schemaname + ".{}".format(glueerrortable),
                "redshiftTmpDir": REDSHIFT_PATH + "stl_load_errors/" + str(datetime.now()).replace(" ", "_") + "/"
            }
        )

        RSerror_df.toDF().createTempView("RSerror")
        logger.info("RSerror tempview is created")
    except Exception as e:
        logger.error(str(e))
        logger.error(
            "**************** [ERROR] Error reading STL_LOAD_ERRORS Table, failed with error: {} ***********".format(
                str(e)))
        f_msg = "**************** [ERROR] Error reading STL_LOAD_ERRORS Table, failed with error: {} ***********".format(
            str(e))
        f_sub = "Stay Btch Hdr Job - {} Failed".format(job_name)
        notifymsg(f_sub, f_msg)
        streaminfo(generateESLogs(job_name,"ERROR",str(e),create_timestamp_est(),create_timestamp_est(),create_timestamp_est(),int(stay_df_count),0,0))
        raise SystemExit(e)

########### setting start_time param to get fetch latest errors from stl_load_errors table #############

    starttime_df = spark.sql(""" select max(starttime) as starttime from id_temp """)
    starttime = starttime_df.first()['starttime']

########################### Write STL_LOAD_ERRORS to S3 path ###################################
    try:
        stl_load_errors_sql = \
            (f""" SELECT rs.starttime, rs.filename, rs.line_number, rs.col_length, rs.colname, rs.position,
            rs.raw_line, rs.raw_field_value, rs.err_reason 
            from RSerror rs, id_temp sq
            where SUBSTRING_INDEX(SUBSTRING_INDEX(trim(rs.raw_line),'/',-1),'.json',1) =
            SUBSTRING_INDEX(SUBSTRING_INDEX(sq.filepath,'/',-1),'.json',1)
            and rs.starttime > '{starttime}' """)

        logger.info("stl_load_errors_sql: %s", stl_load_errors_sql)
        stl_load_errors_df = spark.sql(stl_load_errors_sql)
        # stl_load_errors_df.show()

        if stl_load_errors_df.count() > 0:
            logger.info("Found errors while loading file. Hence, writing errors from stl_load_errors table to s3 path")
            stl_load_errors_df.repartition(1).write.csv(stl_load_errors_file_path, mode='append', header='true')
        else:
            logger.info("No data errors found in stl_load_errors table")

    except Exception as e:
        logger.error(
            f"**************** [ERROR] Error writing STL_LOAD_ERRORS Table to S3 path, failed with error: {str(e)} ***********")
        f_msg = f"**************** [ERROR] Error reading STL_LOAD_ERRORS Table, failed with error: {str(e)} ***********"
        f_sub = f"Stay Btch Hdr Job - {job_name} Failed"
        notifymsg(f_sub, f_msg)
        # systemexit is not needed as writing load errors to a file is independent task and shouldn't halt ongoing task


########################### SQL for Btch Hdr table ###################################

    logger.info("{}: Starting Audit Process".format(str(datetime.now())))
    try:

        est_now = create_timestamp_est()

        Audit_sql = (f""" SELECT cast(sq.job_run_id as string) as btch_num, st.src_nm, st.btch_create_dt, 
        st.btch_create_tm, st.dt_from, st.dt_thru, '{est_now}' as stg_load_strt_ts, 
        '{est_now}' as stg_load_cmpl_ts, st.std_load_strt_ts, st.std_load_cmpl_ts,
        st.mdm_load_strt_ts, st.mdm_load_cmpl_ts, case when err.error_token is null then 'New' else 'Failed'
        end as btch_sts, '{est_now}' as btch_sts_ts, 'LOAD_STAGE' as btch_stg, 
        st.src_file_nm, st.cur_rec_ind, case when err.error_token is null then 'New' else 'Failed'
        end as rs_btch_sts, st.updt_usr, cast(st.updt_ts as timestamp), cast(st.job_sess_id as bigint), 
        cast(sq.job_run_id as bigint), st.usr_cmnt, 'Glue' as create_by,
        cast('{est_now}' as timestamp) as create_ts, sq.filepath as file_nm
        FROM stg_stay_temp st 
        left join 
        id_temp sq on SUBSTRING_INDEX(SUBSTRING_INDEX(st.src_file_nm,'/',-1),'.json',1) 
        = SUBSTRING_INDEX(SUBSTRING_INDEX(sq.filepath,'/',-1),'.json',1)
        left join 
        (select distinct SUBSTRING_INDEX(SUBSTRING_INDEX(raw_line,'/',-1),'.json',1) as error_token
        from RSerror where starttime > '{starttime}') err 
        on SUBSTRING_INDEX(SUBSTRING_INDEX(sq.filepath,'/',-1),'.json',1) = err.error_token """)

        logger.info("Audit_sql: %s", Audit_sql)
        Audit = spark.sql(Audit_sql)

        Audit_dyf = DynamicFrame.fromDF(Audit, glueContext, "Audit_dyf")

        Audit_table_map = ResolveChoice.apply(
            frame=Audit_dyf,
            choice="make_cols",
            transformation_ctx = "Audit_table_map"
        )
        Audit.createTempView("btch_stats")
        
        ##### Getting Success Records Count ########
        success_records = spark.sql(""" select count(btch_sts) as cnt from btch_stats where btch_sts = 'New' """)
        success_records_cnt = success_records.first()['cnt']
        
        ##### Getting Error Records Count ########
        error_records = spark.sql(""" select count(btch_sts) as cnt from btch_stats where btch_sts = 'Failed' """)
        error_records_cnt = error_records.first()['cnt']

    except Exception as e:
        logger.error(str(e))
        logger.error("**************** [ERROR] Error with Audit process, failed with error: {} ***********".format(str(e)))
        f_msg = "**************** [ERROR] Error with Audit process, failed with error: {} ***********".format(str(e))
        f_sub = "Stay Btch Hdr Glue Job - {} Failed".format(job_name)
        notifymsg(f_sub ,f_msg)
        streaminfo(generateESLogs(job_name,"ERROR",str(e),create_timestamp_est(),create_timestamp_est(),create_timestamp_est(),int(stay_df_count),0,0))
        raise SystemExit(e)

    try:
        ########################### Write to Btch Hdr table ###################################

        logger.info("{}: Updating Btch Hdr table with status ".format(str(datetime.now())))

        erroredTokensAudit = glueContext.write_dynamic_frame.from_jdbc_conf(
            frame = Audit_table_map,
            catalog_connection = redshiftconnection,
            connection_options = {
                "url": rs_url_db_dest,
                "database": rsdb,
                "user": rs_user_dest,
                "password": rs_pwd_dest,
                "dbtable" : schemaname + ".{}".format(audittable)
            },
            redshift_tmp_dir = REDSHIFT_PATH +"BtchHdr/" +str(datetime.now()).replace(" " ,"_" ) +"/"
        )
        streaminfo(generateESLogs(job_name,"SUCCESS",None,create_timestamp_est(),create_timestamp_est(),create_timestamp_est(),int(stay_df_count),int(success_records_cnt),int(error_records_cnt)))

    except Exception as e:
        logger.error(str(e))
        logger.error("**************** [ERROR] Error with Audit process, failed with error: {} ***********".format(str(e)))
        f_msg = "**************** [ERROR] Error with Audit process, failed with error: {} ***********".format(str(e))
        f_sub = "Stay Btch Hdr Glue Job - {} Failed".format(job_name)
        notifymsg(f_sub, f_msg)
        streaminfo(generateESLogs(job_name,"ERROR",str(e),create_timestamp_est(),create_timestamp_est(),create_timestamp_est(),int(stay_df_count),0,0))
        raise SystemExit(e)


##############- Delete Marker files #################################
bucket = s3resource.Bucket(blue_bucket)

try:
    objects = bucket.objects.filter(Prefix='pms_stay/glue-workflow-marker-triggers/WHG-Glue-UW2-Stay')
    logger.info(" Deleting the marker files")
    objects.delete()    
except Exception as e:
    logger.error(" Error while deleting the marker files, failed with error: {}".format(str(e)))
    f_msg = "Error while deleting the marker files, failed with error: {}".format(str(e))
    f_sub = "ERROR Deleting Marker Files for Stay Btch Hdr Job - ".format(job_name)
    notifymsg(f_sub, f_msg)
    raise SystemExit(e)

try:
    objects = bucket.objects.filter(Prefix='pms_stay/processing/')
    logger.info(" Deleting the processed files")
    objects.delete()    
except Exception as e:
    logger.error(" Error while deleting the processed files, failed with error: {}".format(str(e)))
    f_msg = "Error while deleting the processed files, failed with error: {}".format(str(e))
    f_sub = "ERROR Deleting processed Files for Stay Btch Hdr Job - ".format(job_name)
    notifymsg(f_sub, f_msg)
    raise SystemExit(e)
    
try:
    objects = bucket.objects.filter(Prefix='pms_stay/seqno/')
    logger.info(" Deleting the seqno files")
    objects.delete()    
except Exception as e:
    logger.error(" Error while deleting the seqno files, failed with error: {}".format(str(e)))
    f_msg = "Error while deleting the seqno files, failed with error: {}".format(str(e))
    f_sub = "ERROR Deleting seqno Files for Stay Btch Hdr Job - ".format(job_name)
    notifymsg(f_sub, f_msg)
    raise SystemExit(e)

####################### generating an ETL Trigger File ###########################
try:
    etl_trigger_key = 'trigger_files/stay/stay_fact_ongoing.trg'
    etl_trigger_body = json.dumps({"stay": "/whgdata/Facts/TriggerFiles/Stay/stay_fact_ongoing.trg",
                                   "timestamp": "{}".format(create_trigger_file_ts())})
    response = s3Client.put_object(Bucket=gold_bucket, Body=etl_trigger_body, Key=etl_trigger_key)
    print(response)
except Exception as e:
    logger.error(
        "************ {} [ERROR] Exception while writing the trigger file to S3 for Btch Hdr Glue Job************".format(
            str(datetime.now())))
    logger.error("*********** [ERROR] Failing with error: {} **************".format(str(e)))
    f_msg = "Error while writing the trigger file from Glue Job {}, failed with Error: {}".format(job_name, str(e))
    f_sub = "Error writing Trigger File for Glue Job - {}".format(job_name)
    notifymsg(f_sub, f_msg)
    raise SystemExit(e)

logger.info(" End of Btch Hdr job run ")
logger.info("JOB COMPLETE!!")
