import logging
import sys
import pytz
from datetime import datetime
import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)

################################### Enabling access to the arguments ##################################
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SNS', 'rsdb', 'blue_bucket',
                                     'redshiftconnection', 'env', 'tablename'])
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
src_schema_name = "dw"
trgt_schema_name = "dwstg"
# Variables for DynamoDB Source Table
transaction_ddb_table = f"WHG-DynamoDB-USW2-MissingStayFileTrans-{env}"
trgt_table_name = args['tablename']

################################### Create low level reosurce service client for S3 ans SNS  ##################################

s3Client = boto3.client('s3', region_name='us-west-2')
s3resource = boto3.resource('s3', region_name='us-west-2')
glueClient = boto3.client('glue', region_name='us-west-2')

job_run = True

job_title = " ".join(trgt_table_name.split("_")[1:]).title()  # used to print it in logs but nothing else.

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


def read_dynamo_table_to_dataframe(source_table_name):
    """
    This method used to write the spark dataframe from DynamoDB Table.
    If record does not exist, create brand new record, if record exist, it overwrites
    :param glue_context: Glue Job Context
    :param source_table_name: Table from where to read the records
    :return : Spark Dataframe
    """
    try:
        logger.info(f" Getting initial set of records from DynamoDB Table '{source_table_name}' ")

        # Step 1: Read the Data from DynamoDB Table to Glue Dynamic Frame
        dynamodb_dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="dynamodb",
            connection_options={
                "dynamodb.input.tableName": source_table_name,
                "dynamodb.throughput.read.percent": "1.0",
                "dynamodb.splits": "100"
            }
        )
        logger.info(f"Count of records read from dynamo table: {str(dynamodb_dynamic_frame.count())}" )

        # Step 2: Translate Glue Dynamic Frame to Spark DataFrame and return
        dynamodb_df = dynamodb_dynamic_frame.toDF()
        # dynamodb_df.show()
        # Step 3:
        #dynamodb_temp = dynamodb_df.createTempView("transaction_temp")
        #dynamodb_temp.show()
        logger.info(f"'transaction_temp' df temp view is created")
        return dynamodb_df.createTempView("transaction_temp")

    except Exception as e:
        logger.error("Unable to read from DynamoDB tables, to place into Dataframe for table %s. "
                     "Error is %s" % (source_table_name, str(e)))
        logger.info(" Exiting with error: {}".format(str(e)))
        raise SystemExit(e)

########### retrieving connection information from Glue connections for Redshift  ##########
try:
    logger.info("Getting Redshift Connection Information")
    rs_connection_dest = glueContext.extract_jdbc_conf(redshiftconnection)
    rs_url_dest = rs_connection_dest["url"]
    rs_user_dest = rs_connection_dest["user"]
    rs_pwd_dest = rs_connection_dest["password"]
    rs_url_db_dest = rs_url_dest + '/' + rsdb
except Exception as e:
    logger.error(str(e))
    f_msg = f"Unable to connect to Redshift while processing the glue job '{job_name}' , failed with error: {str(e)}"
    f_sub = f"'{job_title}' Glue job '{job_name}' failed"
    notifymsg(f_sub, f_msg)
    logger.info(f"Exiting with error: {str(e)}")
    raise SystemExit(e)


def read_redshift_table_to_dataframe(schema_name:str, table_name: str,):
    """
    this function reads a redshift table and creates a temp view to help querying via spark sql
    :param schema_name: table schema
    :param table_name: table name
    :return: Spark Dataframe
    """
    logger.info(f"%s: Getting Data from '{table_name}' Table", str(datetime.now()))
    try:
        rs_table_df = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "url": rs_url_db_dest,
                "database": rsdb,
                "user": rs_user_dest,
                "password": rs_pwd_dest,
                "dbtable": f"{schema_name}.{table_name}",
                "redshiftTmpDir": f"""s3://{blue_bucket}/pms_stay/missing_stay_file/{job_name}/{str(datetime.now()).replace(' ', '_')}/"""
            }
        )

        rs_table_temp = rs_table_df.toDF().createTempView(f"{table_name}")
        logger.info(f"'{table_name}' df temp view is created")
        return rs_table_temp
    except Exception as e:
        logger.error(
            f" Error reading '{table_name}' Table, failed with error: {str(e)}")
        f_msg = f"Error reading '{table_name}' Table. Failed with following error: \n {str(e)}"
        notifymsg("S3 Read", f_msg)
        raise SystemExit(e)


transaction_table = read_dynamo_table_to_dataframe(transaction_ddb_table)
dim_site_ref = read_redshift_table_to_dataframe(src_schema_name, "dim_site_ref")
lkup_site = read_redshift_table_to_dataframe(src_schema_name, "lkup_site")
# lkup_calendar = read_redshift_table_to_dataframe(src_schema_name, "lkup_calendar")
est_now = create_timestamp_est()
utc_now_custom = datetime.now().strftime('%Y%m%d')


missing_stay_file_df = \
    spark.sql(f""" select brand_id, site_id, bus_dt,
              case when missng_flg = 'N' then rcvd_dt
              end as rcvd_dt,
              '{utc_now_custom}' as expctd_dt , '{utc_now_custom}' as sys_dt, missng_flg, 
              '{est_now}' as insert_ts from
              (select tr.brand_id, tr.site_id,  tr.bus_dt, tr.rcvd_dt , 
              case when
              tr.bus_dt = date_add(to_date(tr.rcvd_dt,"yyyyMMdd"),-1) then 'N'
              else 'Y' end as missng_flg 
              from 
              transaction_table tr
              join
              (select distinct a.site_id,b.site_sts_cd,a.pm_sys from
              dim_site_ref a , lkup_site b			
              where stay_file_prcsng ='EDW - Active' and b.site_id =a.site_id and b.cur_site_flg ='Y'			
              and site_sts_cd ='7' ) sf
              on tr.site_id = sf.site_id
              where tr.rcvd_dt = sys_dt
              )src """)
missing_stay_file_df.show()

if missing_stay_file_df.count() == 0:
    logger.info(f"No data present to insert data to RS table '{job_title}' %s for run %s",
                            job_name, str(datetime.now()))
    msg = f"No data present to process '{job_title}' Job '{job_name}' for run " \
                      f"{str(datetime.now())}"
    sub = f"No data to Process '{job_title}' Job '{job_name}'"
    notifymsg(sub, msg)
else:
    missing_stay_file_dynf = DynamicFrame.fromDF(missing_stay_file_df, glueContext, "missing_stay_file_df")
    missing_stay_file_table_map = ResolveChoice.apply(
        frame=missing_stay_file_dynf,
        choice="make_cols",
        transformation_ctx="missing_stay_file_map"
    )

    #################### Dataload to Redshift ########################
    logger.info(" Starting Data load to Staging table")
    try:
        redshift_load = glueContext.write_dynamic_frame.from_jdbc_conf(
                frame=missing_stay_file_table_map,
                catalog_connection=redshiftconnection,
                connection_options={
                        "url": rs_url_db_dest,
                        "database": rsdb,
                        "user": rs_user_dest,
                        "password": rs_pwd_dest,
                        "dbtable": trgt_schema_name + '.' + trgt_table_name,
                        "extracopyoptions": "MAXERROR 100000"},
                    redshift_tmp_dir=
                    f"""s3://{blue_bucket}/pms_stay/dataload/{job_name}/{str(datetime.now()).replace(" ", "_")}/"""
                )
        logger.info(f" Data Load Process for '{job_title}' to redshift Complete!")
    except Exception as e:
            logger.error(
                    f"Error while loading to Redshift Staging table, failed with : {str(e)}")
            f_msg = f"Error while loading to Staging table for the job '{job_name}', " \
                        f"failed with error: {str(e)}"
            f_sub = f"'{job_title}' Glue job '{job_name}' failed"
            notifymsg(f_sub, f_msg)
            raise SystemExit(e)

logger.info(
    f" *********** %s: End of Glue process for '{job_title}' *********** ", str(datetime.now()))
job.commit()
logger.info("JOB COMPLETE!!")
