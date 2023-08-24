import logging
import os
import shutil
from datetime import datetime
from zipfile import ZipFile
import boto3

# setting loggers
logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_name = os.environ["AWS_LAMBDA_FUNCTION_NAME"]
RED_BUCKET_NAME = os.environ["redBucket"]
SOURCE_BUCKET_NAME = os.environ["redBucket"]
GOLD_BUCKET_NAME = os.environ["goldBucket"]
ERROR_BUCKET_NAME = os.environ["errorBucket"]
SNSTopicArn = os.environ["SNSTopicArn"]
ENV = os.environ["env"]
files_to_extract = os.environ["filesToExtract"] # ex: major-issues, hotels
files_to_extract_list = files_to_extract.replace(", ", ",").split(",") # converts above param into this --> ["major-issues", "hotels"]
glue_workflows_to_run = os.environ["glueWorkflowsToRun"]
glue_workflows_to_run_list = glue_workflows_to_run.replace(", ", ",").split(",")
# ["WHG-Glue-WorkFlow-USW2-Rate-Parity-MajorIssue-" + ENV, "WHG-Glue-WorkFlow-USW2-Rate-Parity-Hotels-" + ENV]

loadtime_utc = datetime.now()
loadtime_utc_str = loadtime_utc.strftime("%Y-%m-%d-%H-%M-%S")
datesplit = loadtime_utc_str.split("-")
YR = datesplit[0]
MO = datesplit[1]
DY = datesplit[2].split(" ")[0]
datepath = YR + "/" + MO + "/" + DY
datepath2 = YR + "-" + MO + "-" + DY
S3_BUCKET_PREFIX = "otai/disparity/summary/"
S3_RESPONSE_SUFFIX = datepath2 + ".zip"


start_job = None

s3client = boto3.client("s3")
s3 = boto3.resource("s3")
snsclient = boto3.client("sns", region_name="us-west-2")
glueClient = boto3.client("glue")

try:
    sns_client = boto3.client("sns", region_name="us-west-2")
    logger.info("Enabled SNS Notification")
except Exception as e:
    logger.error(e)
    logger.info(" Unable to enable SNS, failed with error: {}".format(str(e)))
    logger.info("Continuing with Lambda execution without SNS notification")


def notifyFalure(filePath, error):
    sns_client.publish(
        TopicArn=SNSTopicArn,
        Message="Lambda to trigger glue job is failed for the file {0} with error: {1}".format(
            lambda_name, error
        ),
        Subject="Rate Parity Detail feed Lambda Process failed",
    )


def notifyFalure2(subject, body):
    sns_client.publish(TopicArn=SNSTopicArn, Message=body, Subject=subject)


########## Trigger Handler ################


def getLambdaEventSource(event):
    if "source" in event and event["source"] == "aws.events":
        return "isScheduledEvent"
    elif (
        "Records" in event
        and len(event["Records"]) > 0
        and "eventSource" in event["Records"][0]
        and event["Records"][0]["eventSource"] == "aws:s3"
    ):
        return "s3"


def search_s3_bucket(S3_BUCKET_NAME, prefix, suffix):
    result = []
    paginator = s3client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix)
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(suffix):
                result.append(key)
    return result


def download_s3_object(bucket_name, object_key, file_path):
    obj = s3.Object(bucket_name, object_key)
    with open(file_path, "wb") as f:
        obj.download_fileobj(f)
        logger.info(obj)


def upload_file_to_s3(bucket_name, file_path, object_key):
    bucket = s3.Bucket(bucket_name)
    bucket.upload_file(file_path, object_key)


def start_glue_workflow(glue_workflow_name):
    """triggers the given workflow"""
    glue_response = get_glue_workflow(glue_workflow_name)
    if glue_response is not None:
        execute_response = glueClient.start_workflow_run(Name=glue_workflow_name)
        logger.info(execute_response)
        httpcode = execute_response["ResponseMetadata"]["HTTPStatusCode"]
        if httpcode == 200:
            runid = execute_response["RunId"]
            logger.info(f"Glue flow '{glue_workflow_name}' successfully triggered! Jobid is: '{runid}'")


def get_glue_workflow(glue_workflow_name):
   """gets the details of given workflow if found else returns none"""
   try:
      response = glueClient.get_workflow(Name=glue_workflow_name)
      # logger.info(f"response for '{glue_workflow_name}': {response}")
      return response
   except Exception as e:
      logger.info(f"Error found while getting workflow '{glue_workflow_name}': {str(e)}")
      return None


########## Lambda Handler ################
def main(event, context):
    extraction_folder = ""
    try:
        findEventSource = getLambdaEventSource(event)
        logger.info(f"Event is: '{findEventSource}'")
        if findEventSource == "isScheduledEvent":
            logger.info("To Check the availability of source file")
            logger.info(S3_BUCKET_PREFIX)
            try:
                lobj = search_s3_bucket(
                    SOURCE_BUCKET_NAME, S3_BUCKET_PREFIX, S3_RESPONSE_SUFFIX
                )
                logger.info(lobj)
                lobj_c = len(lobj)
                if lobj_c < 1:
                    logger.info(
                        "*********** File not received processing for today ***********"
                    )
                    Subject = " Rate Parity zip File not received for -" + datepath2
                    Body = " Rate Parity zip File not received for -" + datepath2
                    notifyFalure2(Subject, Body)

                else:
                    logger.info(
                        "*********** File  received for processing for this month ***********"
                    )

            except Exception as e:
                error = str(e)
                logger.info(error)
                logger.info(
                    "lambda job -{0} for failed with error : {1}".format(
                        lambda_name, error
                    )
                )
                notifyFalure(lambda_name, str(e))
                exit(1)

        elif findEventSource == "s3":
            logger.info("reading s3 path..")
            try:
                source_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
                source_file_key = event["Records"][0]["s3"]["object"]["key"]
                # Extract the prefix from the object key
                logger.info(source_file_key)

                prefix = source_file_key[: source_file_key.rindex("/") + 1]
                logger.info(f"prefix is: '{prefix}'")

                if prefix == "otai/disparity/summary/":
                    source_file_path = os.path.dirname(source_file_key)
                    filename = os.path.basename(source_file_key)
                    filename_wthout_tmstmp = filename.split(".")[0]
                    download_path = "/tmp/" + filename_wthout_tmstmp
                    logger.info(f"download path: '{download_path}'")
                    date_part = filename_wthout_tmstmp.split("_")[-1]
                    date_YR = str(date_part.split("-")[0])
                    date_MO = str(date_part.split("-")[1])
                    date_DY = str(date_part.split("-")[2])
                    ##s3.download_file(source_bucket_name, source_file_key, download_path)
                    download_s3_object(
                        source_bucket_name, source_file_key, download_path
                    )

                    # # Extract the .zip file
                    extraction_folder = "/tmp/extract"
                    os.makedirs(extraction_folder)
                    with ZipFile(download_path, "r") as zObject:
                        zObject.extractall(extraction_folder)

                    extracted_subfolder = os.listdir(extraction_folder)
                    logger.info(f"extracted subfolders: '{extracted_subfolder}'")
                    for extracted_sub in extracted_subfolder:
                        extracted_files_folder = os.sep.join(
                            [extraction_folder, extracted_sub]
                        )
                        dtl_files = os.listdir(extracted_files_folder)
                        logger.info(f"extracted files: '{dtl_files}'")

                        # original_file_name=extracted_files[0]
                        # # Move the extracted CSV file to the destination folder in S3
                        for file_nm in dtl_files:
                            file_nm_wthot_tmstmp = file_nm.split(".")[0]
                            if file_nm_wthot_tmstmp in files_to_extract_list:
                                csv_file_name = (
                                file_nm_wthot_tmstmp + "_" + date_part + ".csv"
                                )
                                logger.info(f"csv file name: '{csv_file_name}'")
                                csv_file_path = os.sep.join(
                                    [
                                        source_file_path,
                                        extracted_sub,
                                        file_nm_wthot_tmstmp,
                                        csv_file_name,
                                    ]
                                )
                                logger.info(f"csv file path: '{csv_file_path}'")
                                file_key = os.sep.join([extracted_files_folder, file_nm])
                                logger.info(f"file_key: '{file_key}'")
                                ## Upload the renamed file to S3
                                upload_file_to_s3(
                                    GOLD_BUCKET_NAME,
                                    file_key,
                                    csv_file_path,
                                )

                    ## Remove the temporary files
                    os.remove(download_path)
                    shutil.rmtree(extraction_folder)

                # Trigger the respective glue workflow
                for glue in glue_workflows_to_run_list:
                    logger.info(f"triggering the workflow: '{glue}'")
                    start_glue_workflow(glue)

            except Exception as e:
                error = str(e)
                logger.info(error)
                subject = "Lambda Failed while processing the file"
                notifyFalure(subject, str(e))
                # os.remove(download_path)
                shutil.rmtree(extraction_folder)
                exit(1)

        else:
            logger.info("Event source not found. Triggered manually. Hence exiting...")
            exit(1)

    except Exception as e:
        error = str(e)
        logger.info(error)
        sub = "lambda job -{0} for failed with error : {1}".format(lambda_name, error)
        notifyFalure(sub, str(e))
        exit(1)
