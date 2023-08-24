import sys 
from datetime import datetime
from datetime import timedelta
from typing import Optional

import pytz
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SQLContext
import boto3
import logging
from pyspark.sql.functions import col, when, lit, input_file_name, substring_index, explode, \
    monotonically_increasing_id, row_number
from pyspark.sql.types import TimestampType, DoubleType, StructType, StructField, StringType, \
    IntegerType, LongType, FloatType, DecimalType
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)

################################### Enabling access to the arguments ##################################
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'blue_bucket', 'sns', 'configfile', 'rsdb',
                                     'redshift_connection','ScriptBucketName', 'env'])
job.init(args['JOB_NAME'], args)

################################### Setting up logger for event logging ##################################
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

################################### Assigning job arguments to variables  ##################################

rsdb = args['rsdb']
blue_bucket = args['blue_bucket']
job_name = args['JOB_NAME']
sns_notify = args['sns']
write_s3_path = 's3://{}/pms_stay/processing/'.format(blue_bucket)
MARKER_FILE = "pms_stay/glue-workflow-marker-triggers/" + args["JOB_NAME"]
seqno_path = "s3://{}/pms_stay/seqno/".format(blue_bucket)
config_file = "pms_stay/{}".format(args['configfile'])
env = args['env']
redshiftconnection = args['redshift_connection']
schema_name = "dwstg"
btch_table_name = "stg_btch_hdr"
WRITE_S3_DynDB_PATH ='s3://{}/pms_stay/lkup-site/'.format(blue_bucket)

# Variables for DynamoDB Source Table
dim_site_lookup_table_name = "WHG-DynamoDB-USW2-LkupSite-{0}".format(env)

################################### Create low level reosurce service client for S3 ans SNS  ##################################

s3Client = boto3.client('s3', region_name='us-west-2')
s3resource = boto3.resource('s3', region_name='us-west-2')
glueClient = boto3.client('glue', region_name='us-west-2')

job_run = True

try:
    sns_client = boto3.client('sns', region_name='us-west-2')
    logger.info("Enabled SNS Notification")
except Exception as e:
    logger.error(str(e))
    logger.info(" Unable to enable SNS, failed with error: {}".format(str(e)))
    logger.info("Continuing with load without SNS notification")

############# Notification Function ################################################
def notifymsg(subprefix: str, msg: str, action: Optional[str] = "Failed"):
    env=job_name.split('-')[-1]
    sub = f"""{subprefix} Glue job '{job_name}' {action}"""
    msg = "SD Team Please create a ticket \n Priority: {0} \n Assignment Team: {1} \n Impacted System: {2} \n Error Details: {3} \n AWS Component: {4} \n".format("P3 Medium", "PSS AWS", "Stay glue job failure ",msg,job_name)
    sub = 'P3 Medium ' + sub + ' in '+env
    sns_client.publish(TopicArn=sns_notify, Message=msg, Subject=sub)
    logger.info(
        "**************** [INFO] SNS Notification Sent: {} *************************".format(
            job_name))

utc_now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

def create_timestamp_est():
    utc_now = datetime.now()
    est = pytz.timezone('US/Eastern')
    est_now = utc_now.astimezone(est).strftime('%Y-%m-%d %H:%M:%S.%f')
    return est_now

def read_table_to_dataframe(glueContext, source_table_name):
    """
    This method used to write the spark dataframe from DynamoDB Table.
    If record does not exist, create brand new record, if record exist, it overwrites

    :param glueContext: Glue Job Context
    :param source_table_name: Table from where to read the records
    :return : Spark Dataframe
    """
    try:
        logger.info("#### Getting initial set of records from DynamoDB Table : " + source_table_name)

        # Step:1 - Read the Data from DynamoDB Table to Glue Dynamic Frame
        dynamodb_dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="dynamodb",
            connection_options={
                "dynamodb.input.tableName": source_table_name,
                "dynamodb.throughput.read.percent": "1.0",
                "dynamodb.splits": "100"
            }
        )
        logger.info("#### Count of successfully loaded records: " + str(dynamodb_dynamic_frame.count()))

        # Step:2 - Translate Glue Dynamic Frame to Spark DataFrame and return
        return DynamicFrame.toDF(dynamodb_dynamic_frame)

    except Exception as e:
        logger.error("Unable to read from DynamoDB tables, to place into Dataframe for query %s. "
                     "Error is %s" % (source_table_name, str(e)))

        logger.info(" Exiting with error: {}".format(str(e)))


#################################### get_input_s3_path ##########################################
# This function will frame the input s3 path.
# By default, it will take all the files in the current date
def get_input_s3_path():
    logger.info("*********** Framing S3 Path ***********")
    s3_path = []
    s3_prefix = "s3://{}/pms_stay/intermediate/".format(blue_bucket)
    try:
        obj = s3Client.get_object(Bucket=blue_bucket, Key=config_file)
    except Exception:
        obj = None
        logger.info("*********** NO config file present to check ReRun dates ***********")
    if obj is not None:
        logger.info("checking for dates to ReRun from config file ")
        json_obj = json.loads(obj['Body'].read())
        rerun_dates = [x.strip() for x in json_obj["rerun"].split(',')]
        if len(rerun_dates) > 0 and rerun_dates != ['']:
            for date in rerun_dates:
                s3_path.append(s3_prefix + date + '/')
        else:
            logger.info("NO files to ReRun")

    logger.info("*********** Now retrieving files from Intermediate path ***********")
    try:
        # Creating Datetime Folder
        today = datetime.now().strftime('%Y/%m/%d')
        yesterday = (datetime.now() - timedelta(1)).strftime('%Y/%m/%d')

        logger.info("Today: {}".format(today))
        logger.info("Yesterday: {}".format(yesterday))

        s3_path.append(s3_prefix + today + '/')
        # Add yesterday files if the last execution is previous day
        client = boto3.client('glue', region_name='us-west-2')
        response = client.get_job_runs(
            JobName=args['JOB_NAME'],
            MaxResults=10
        )
        job_runs = response['JobRuns']
        start_time = None
        for run in job_runs:
            if run['JobRunState'] != 'SUCCEEDED':
                continue
            else:
                start_time = run['StartedOn']
                break
        logger.info("start_time of latest successful job is is: %s", start_time)
        # logger.info("UTC_TIME is: %s", datetime.now())
        if start_time and start_time.date() < datetime.today().date():
            logger.info(
                "adding yesterday file(s) if left over any for processing.")
        # whether previous day file processed or not will be detected by glue's job-bookmark option
            s3_path.append(s3_prefix + yesterday + '/')
        logger.info("input s3 path: %s", s3_path)
    except Exception as e:
        logger.error(
            "****************** [ERROR] Unable to frame s3 Input Path : {} **************".format(
                str(e)))
        f_msg = "Unable to Frame S3 Input Path, Error Details: {}".format(str(e))
        notifymsg("S3 Read", f_msg)
        raise SystemExit(e)

    return s3_path

   

####### Check if this is rerun based on the marker file, if so the job execution is skipped ###################################
try:
    response = s3Client.head_object(Bucket=blue_bucket, Key=MARKER_FILE)
    logger.info(response)
    logger.info("*******************[INFO] JOB ALREADY EXECUTED ********************")
    # make job_run as TRUE when you want to run same job on same day because marker file will be
    # created already when you execute this job alone instead of glueworkflow while unit testing.
    job_run = False
except Exception as HeadObjectException:
    ##############################################################################################
    # No Marker file present, then this is first time execution
    # Will read the file based on get_input_s3_path response
    ##############################################################################################
    logger.info(HeadObjectException)

if job_run:
    ########################### Reading Res JSON Data ####################################
    try:

        logger.info(" Reading S3 file")
        stay_s3_read_dyf = glueContext.create_dynamic_frame.from_options(connection_type="s3",
                                                                         connection_options={
                                                                             "paths": get_input_s3_path(),
                                                                             'recurse': True},
                                                                         format="json",
                                                                         transformation_ctx='stay_s3_read_dyf')

        stay_df = stay_s3_read_dyf.toDF()
        stay_df.show()
        stay_df_count = stay_df.count()
        logger.info('Total Cound DF (no.of input files): {}'.format(stay_df.count()))
        # stay_df.printSchema()

    except Exception as e:
        logger.error(
            "************ {} [ERROR] Exception while reading the file from S3 *************".format(
                str(datetime.now())))
        logger.error("*********** [ERROR] Failing with error:  {} **************".format(str(e)))
        f_msg = "  Error while reading data from S3 for the job {0} , failed with error: {1}  ".format(
            job_name, str(e))
        notifymsg("S3 Read", f_msg)
        raise SystemExit(e)

    if stay_df_count == 0:
        logger.info('No Data to process for S3 Read Job for {} run'.format(str(datetime.now())))
        f_msg = "No Data to Process for S3 Read Glue Job - {} for run {}".format(job_name,
                                                                                 str(datetime.now()))
        notifymsg("S3 Read", f_msg, "has NO data to run")
        # exit(0)
    else:
        ############# Writing seqno for Audit Job Validation #################################

        # Retrieving connection information from 'Glue connections' for Redshift
        try:
            logger.info("Getting Redshift Connection Information..")
            rs_connection_dest = glueContext.extract_jdbc_conf(redshiftconnection)
            rs_url_dest = rs_connection_dest["url"]
            rs_user_dest = rs_connection_dest["user"]
            rs_pwd_dest = rs_connection_dest["password"]
            rs_url_db_dest = rs_url_dest + '/' + rsdb
            logger.info("Redshift Connection details retrieved successfully")
        except Exception as e:
            f_msg = f"Unable to connect to Redshift while processing the glue job {job_name}, " \
                  f"failed with error: {e}"
            notifymsg("S3 Read", f_msg)
            logger.info(" Exiting with error: %s", str(e))
            raise SystemExit(e)

        logger.info("%s: Getting Data from Batch Header Table for Audit", str(datetime.now()))
        try:
            btch_hdr_df = glueContext.create_dynamic_frame.from_options(
                connection_type="redshift",
                connection_options={
                    "url": rs_url_db_dest,
                    "database": rsdb,
                    "user": rs_user_dest,
                    "password": rs_pwd_dest,
                    "dbtable": schema_name + f".{btch_table_name}",
                    "redshiftTmpDir": "s3://" + blue_bucket + '/pms_stay/batch_hdr/{}/{}/'.format(
                    job_name, str(datetime.now()).replace(" ", "_"))
                }
            )

            btch_hdr_df.toDF().createTempView("btch_hdr")
        except Exception as e:
            logger.error(
                " Error reading BATCH_HDR Table, failed with error: {0}".format(str(e)))
            f_msg = f"Error reading BATCH_HDR Table. Failed with following error: \n {str(e)}"
            notifymsg("S3 Read", f_msg)
            raise SystemExit(e)

        ############## writing Seqno File(.csv) for Audit Process #########################
        try:
            # adding filepath and start time column to the csv file
            stay_seqno_df = stay_df.withColumn('filepath', input_file_name()) \
                .withColumn('starttime', lit(utc_now_str))
            # adding autoincrement column to the above df
            stay_run_id_gen_df = stay_seqno_df.withColumn("run_id", monotonically_increasing_id())
            w = Window.orderBy("run_id")

            run_id_df = spark.sql(
                f""" select coalesce(max(cast(job_run_id as bigint)), 0) as job_run_id from btch_hdr """)
            run_id_df_count = run_id_df.first()['job_run_id']
            logger.info("run_id_df_count (max batch_hdr count)= %s", run_id_df_count)

            if run_id_df_count == 0:  # in the very first load, job_run_id will be zero as there
                # are no previous runs
                count = 0
            else:
                count = run_id_df_count  # gets the max batch id from previous runs till now
            stay_run_id_df = stay_run_id_gen_df.withColumn("job_run_id",
                                                           (count + (row_number().over(w))))

            stay_run_id_df_final = stay_run_id_df.select(col('filepath'), col('starttime'),
                                                         col('job_run_id')).distinct()
            stay_run_id_df_final.show()
            logger.info("Writing seq IDs to seqno folder..")
            stay_run_id_df_final.repartition(1).write.csv(seqno_path, mode="overwrite",
                                                          header='true')

        except Exception as e:
            logger.error(
                "*********** Error while writing data to Seq Folder."
                " Failing with error:  {} **************".format(str(e)))
            f_msg = "  Error while writing data to Seq Folder on S3 for the job {0} ," \
                    " failed with error: {1}  ".format(job_name, str(e))
            notifymsg("S3 Read", f_msg)
            raise SystemExit(e)

        ############## writing JSON Res Files for Processing Stage #########################

        try:
            logger.info("Writing S3 Files to Processing Folder")
            stay_processing_df = stay_seqno_df.drop("starttime")
            stay_processing_df.write.format('json').mode("overwrite").option("header", "true").save(
                write_s3_path)
        except Exception as e:
            logger.error(
                "*********** Error while writing data to Processing Folder."
                " Failing with error:  {} **************".format(str(e)))
            f_msg = "  Error while writing data to Processing Folder on S3 for the job {0} ," \
                    " failed with error: {1}  ".format(job_name, str(e))
            notifymsg("S3 Read", f_msg)
            raise SystemExit(e)
            
        ####### Extract data from Dynamodb table and load data to s3 ###################################
        try:
            lkup_site_df = read_table_to_dataframe(glueContext,dim_site_lookup_table_name)
            lkup_site_df_new = lkup_site_df.filter(lkup_site_df.cur_site_flg == "Y")
            lkup_site_df_new.repartition(1).write.format('json').mode("overwrite").option("header","true").save(WRITE_S3_DynDB_PATH) 
    
        except Exception as e:
            logger.error("************ {} [ERROR] Exception while writing the lkup-site file to S3 *************".format(str(datetime.now())))
            logger.error("*********** [ERROR] Failing with error:  {} **************".format(str(e)))
            f_msg="  Error while writing data to lkup-site Folder on S3 for the job {0} , failed with error: {1}  ".format(job_name,str(e))
            f_sub = "S3 Read Glue job "+job_name+" failed" 
            notifymsg(f_sub, f_msg)
            exit(1)

        ####################### Creating a Marker File ###########################
        try:
            response = s3Client.put_object(Bucket=blue_bucket, Body="Completed", Key=MARKER_FILE)
            logger.info(response)
        except Exception as e:
            logger.error(
                "************ {} [ERROR] Exception while writing the marker file to S3 for S3 Read Glue Job************".format(
                    str(datetime.now())))
            logger.error(
                "*********** [ERROR] Failing with error: {} **************".format(str(e)))
            f_msg = "Error while writing the marker file to S3 Read Glue Job {}, failed with Error: {}".format(
                job_name, str(e))
            notifymsg("S3 Read", f_msg)
            raise SystemExit(e)

logger.info(" ************** {} End of Load process for S3 Read *********************** ".format(
    str(datetime.now())))
job.commit()
logger.info("JOB COMPLETE!!")

