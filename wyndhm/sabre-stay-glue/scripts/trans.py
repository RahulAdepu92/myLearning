import sys
from datetime import datetime
from datetime import timedelta
import pytz
from awsglue.transforms import *
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
                                     'tablename', 'schemaname', 'ScriptBucketName', 'configfile'])
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
job_name = args['JOB_NAME']
tablename = args['tablename']
schemaname = args['schemaname']
sns_notify = args['SNS']
MARKER_FILE = "pms_stay/glue-workflow-marker-triggers/" + args["JOB_NAME"]
seqno_path = "s3://{}/pms_stay/seqno/".format(blue_bucket)
job_run = True

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

if job_run:

    ################################### Retriving connection information from Glue connections for Redshift  ##################################
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
        f_sub = "Stay Trans Glue job " + job_name + " failed"
        notifymsg(f_sub, f_msg)
        logger.info(" Exiting with error: {}".format(str(e)))
        raise SystemExit(e)

        ############################## Reading Data from Processing Folder for latest RSVs #################################################
    try:
        logger.info(" Reading Stay Trans Data from JSON ")
        stay_s3_read_dyf = glueContext.create_dynamic_frame.from_options(connection_type="s3",
                                                                        connection_options={
                                                                            "paths":
                                                                                [file_path_gen],
                                                                            'recurse': True,
                                                                            'groupFiles': 'inPartition'},
                                                                        format="json",
                                                                        format_options={
                                                                            "jsonPath": "$.stg_stay_trans[*]"})

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
        f_msg = "  Error while reading Stay Trans data from S3 for the job {0} , failed with error: {1}  ".format(
            job_name, str(e))
        f_sub = "Stay Trans Glue job " + job_name + " failed"
        notifymsg(f_sub, f_msg)
        raise SystemExit(e)

    if stay_df_count == 0:
        logger.info(
            'No Data to process for Stay Trans for {} run'.format(str(datetime.now())))
        f_msg = "No Data to Process for Stay Trans Glue Job - {} for run {}".format(job_name,
                                                                                           str(datetime.now()))
        f_sub = "No Data to Process for Stay Trans Glue Job - {}".format(job_name)
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
            # creating temp view for the seq file created while s3 read job
            seq_id_df = glueContext.read.format("csv").option("inferSchema", "false") \
                .option("header", "true").option("footer", "false").load(seqno_path)
            logger.info(seq_id_df.count())
            logger.info(seq_id_df.show())
            seq_id_df.createTempView("id_temp")
            # logger.info("id_temp is created")
            if seq_id_df.count() == 0:
                logger.info(
                    f"No Seq file data present to process Stay Trans job %s for run %s",
                    job_name, str(datetime.now()))
                msg = f"No Seq file data present to process Stay Trans Job {job_name} for " \
                      f"run " \
                      f"{str(datetime.now())}"
                sub = f"No Seq file data to Process Stay Trans Job {job_name}"
                notifymsg(sub, msg)

            else:
                est_now = create_timestamp_est()
                stg_sql = (f""" SELECT st.stay_file_cnsmr_id, st.book_ref, st.evnt_ref, st.sls_ref_num,
                            st.trans_dt, st.trans_tm, st.cmdy_cd, st.prdct_cls_cd, cast(st.quot_qty as decimal(15,4)), 
                            cast(st.quot_amt as decimal(15,4)), cast(st.sls_qty as decimal(15,4)), 
                            cast(st.sls_amt as decimal(15,4)), cast(st.rm_num as decimal(15)), st.crncy_cd,
                            cast(st.crncy_rt as decimal(15,4)), st.actl_prmt_cd, st.actl_rt_pln_cd, 
                            cast(sq.job_run_id as bigint) as btch_num, st.data_src_nm, st.src_file_nm,
                            st.cur_rec_ind, st.job_sess_id, sq.job_run_id, st.usr_cmnt, st.orig_rt_pln,
                            st.hskp_prsn_nm, st.rm_typ_dscr, st.rt_pln_nm, 'Glue' as create_by, 
                            cast('{est_now}' as timestamp) as create_ts, sq.filepath as file_nm
                            FROM stg_stay_temp st left join id_temp sq 
                            on SUBSTRING_INDEX(SUBSTRING_INDEX(st.src_file_nm,'/',-1),'.json',1) 
                            = SUBSTRING_INDEX(SUBSTRING_INDEX(sq.filepath,'/',-1),'.json',1) """)

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
                "************ {} [ERROR] Exception while doing Data Transformtion on Stay Trans Data *************".format(
                    str(datetime.now())))
            logger.error(
                "*********** [ERROR] Failing with error:  {} **************".format(str(e)))
            f_msg = "  Error while doing Data Transformtion on Stay Trans Data for Glue Job {0} , failed with error: {1}  ".format(
                    job_name, str(e))
            f_sub = "Stay Trans Glue job " + job_name + " failed"
            notifymsg(f_sub, f_msg)
            raise SystemExit(e)


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
            logger.info(" Data Load Process for Stay Trans to redshift complete")
        except Exception as e:
            logger.error(str(e))
            logger.info(
                "[ERROR] Error while loading to Redshift Staging table, failed with : {} ".format(
                    str(e)))
            f_msg = "  Error while loading to Staging table for the job {}, failed with error: {} ".format(
                job_name, str(e))
            f_sub = "Stay Trans Glue job " + job_name + " failed"
            notifymsg(f_sub, f_msg)
            raise SystemExit(e)


        ####################### Creating a Marker File ###########################
        try:
            response = s3Client.put_object(Bucket=blue_bucket, Body="Completed", Key=MARKER_FILE)
        except Exception as e:
            logger.error(
                "************ {} [ERROR] Exception while writing the marker file to S3 for Stay Trans Glue Job************".format(
                    str(datetime.now())))
            logger.error(
                "*********** [ERROR] Failing with error: {} **************".format(str(e)))
            f_msg = "  Error while writing the marker file to S3 for Stay Trans Glue Job {}, failed with Error: {}".format(
                job_name, str(e))
            f_sub = "Error writing Marker File for Glue Job - {}".format(job_name)
            notifymsg(f_sub, f_msg)
            raise SystemExit(e)

logger.info(
    " ************** {} End of Load process for Stay Trans *********************** ".format(
        str(datetime.now())))
job.commit()
logger.info("JOB COMPLETE!!")