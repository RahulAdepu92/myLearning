import json
import logging
import os
from datetime import datetime
from typing import Dict

import boto3
from botocore.exceptions import ParamValidationError

from ..common import convert_utc_to_local, get_file_name_pattern
from ..constants import DEFAULT_TIMEZONE
from ..database.client_files import ClientFileRepo
from ..database.dbi import AhubDb
from ..glue import get_glue_logger
from ..jobs.sla_processor import process_sla
from ..model import GlueFailureEvent, ClientFile
from ..notifications import send_ahub_email_notification
from ..s3 import get_file_from_s3_trigger, get_first_file_path

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)


def _test_handler(e, c):
    logger.info("_test_handler from irxah.s3. You called me with e=%s, c=%s", e, c)


def _test_dbi(e, c):
    secret_name = os.environ["SECRET_NAME"]
    db = AhubDb(secret_name=secret_name)
    cf = db.get_one("select * from ahub_dw.client_files where client_file_id=:id", id=4)
    logger.info("Client File 4: %r", cf)


def make_arguments_from_database(
    s3_inbound_file: str, s3_bucket_name: str
) -> (Dict[str, str], str):
    """Constructs glue job arguments from only existing environment variable 'SECRET_NAME' and rest of the variables are
    fetched from db through ClientFile object. Optionally, adds the specified key-value pairs.
    For example make_arguments_from_environment(SOURCE_FILE_NAME="foo.txt"), will add the variable "SOURCE_FILE_NAME" to
    the list of job arguments with the value "foo.txt")
    :param s3_inbound_file: incoming file path retrieved from s3 trigger event i.e, inbound/txtfiles/filename.txt
    :param s3_bucket_name: s3 bucket name in which incoming file is located
    :returns: a tuple of dict and string elements. Glue job arguments are returned in Dict format whereas glue job name is string.
             For ex: ({'--SECRET_NAME': 'irx-ahub-dv-redshift-cluster', '--S3_FILE_NAME': 'inbound/bci/txtfiles/ACCDLYINT_TST_BCIINRX_200417143525_remove2.txt', '--S3_FILE_BUCKET': 'irx-accumhub-dev-data', '--CLIENT_FILE_ID': '1', '--PROCESS_NAME': 'BCI Integrated file to CVS', '--FILE_TYPE': 'INBOUND'},
                     'irxah_process_incoming_bci_file')
    """
    # 'secret_name' is the only existing environment variable for lambda function
    secret_name = os.environ["SECRET_NAME"]

    incoming_file_name = s3_inbound_file.split("/")[-1]
    logger.info(f"incoming_file: %s", incoming_file_name)
    # gets full string before 3rd '_' character in incoming file and checks if any name_pattern exists in db
    incoming_file_name_pattern = get_file_name_pattern(incoming_file_name)
    logger.info(f"incoming_file_name_pattern: %s", incoming_file_name_pattern)

    client_file: ClientFile = ClientFileRepo(secret_name=secret_name).get_client_file_from_name_pattern(incoming_file_name_pattern)
    logger.info(f"client_file : %s", client_file)

    if client_file is not None:
        logger.info(
            "incoming file name '%s' matches with file name pattern '%s' in client files table ",
            incoming_file_name,
            client_file.name_pattern,
        )

        job_args = {
            # preparing arguments for glue job 'irxah_process_incoming_<client_name>_file'
            "--SECRET_NAME": os.environ["SECRET_NAME"],  # for connecting db (retrieved as lambda environment variable)
            "--S3_FILE": s3_inbound_file,  # file_name with path (retrieved from s3 trigger event)
            "--S3_FILE_BUCKET": s3_bucket_name,  # s3_bucket_name (retrieved from s3 trigger event)
            "--CLIENT_FILE_ID": str(client_file.client_file_id),  # unique id of client file (retrieved from db)
            # we are passing a dummy job key as generation of standard file (GILA) from custom file (BCBAZ) will not have a unique unzip key.
            # by this, we are avoiding 'irxah_process_incoming_file' glue job failure
            "--UNZIP_JOB_DETAIL_KEY": "0",
        }

        glue_job_name = client_file.glue_job_name

        return job_args, glue_job_name

    logger.info(
        "Incoming file '%s' name doesn't match with any of the expected file name pattern in db. "
        "Hence, it will not be processed further. It remains as is in 'inbound/txtfiles' folder.",
        incoming_file_name,
    )
    return None


def glue_process_handler(event, context) -> None:
    """This handler is for AWS Lambda to process any incoming BCI/CVS file.
    Its purpose is only to kick the specific glue job('process_incoming_<bci/cvs>_file') to process the respective incoming file.
    Environment variable "glue_job_name" will decides which glue job to run. S3bukcet name and incoming file name are retrieved at run time.
    Glue run dependent variables are called from db.
    :param event: lambda event
    :param context: lambda context
    :return: None
    """
    s3_bucket_name = ""
    incoming_file = ""
    inbound_folder = ""
    glue_job = boto3.client("glue")

    logger.debug("## ENVIRONMENT VARIABLES: %s", os.environ)
    logger.debug("## EVENT: %s", event)

    try:
        logger.debug(" Entering in to Try Block")
        (s3_bucket_name, s3_file) = get_file_from_s3_trigger(event)
        if s3_bucket_name is None or s3_file is None:
            logger.error("Exiting. Could not retrieve a file name from event: %r", event)
            return

        inbound_folder = os.path.dirname(s3_file)
        incoming_file = s3_file
        logger.info(
            "From Try Block : s3_bucket_name = %s, incoming_file = %s",
            s3_bucket_name,
            incoming_file,
        )
    except:
        # when your approach is to run lambda manually and not through s3 trigger event, this will be helpful.
        # Currently below hardcoded variables are not defined in function, but while unit testing you can do so if you wish and test.
        s3_bucket_name = os.environ["INBOUND_BUCKET_NAME"]
        inbound_folder = os.environ["INBOUND_FOLDER"]
        incoming_file = get_first_file_path(s3_bucket_name, inbound_folder)
        logger.error(
            "From Exception Block : s3_bucket_name = %s, incoming_file = %s, inbound_folder = %s",
            s3_bucket_name,
            incoming_file,
            inbound_folder,
        )

    if len(incoming_file) == 0 or None: # handle None output (incoming file= '') from event
        logger.error(
            "In S3 Bucket : %s, there is a no file to process in the inbound folder : %s , program will EXIT now..",
            s3_bucket_name,
            inbound_folder,
        )
        return

    result = make_arguments_from_database(incoming_file, s3_bucket_name)

    if result is not None:
        job_args = result[0]
        # earlier it was an environment variable in lambda but now retrived from database
        glue_job_name = result[1]
        logger.info("Calling Glue Job:: %s, with arguments %r", glue_job_name, job_args)

        response = ""

        try:
            response = glue_job.start_job_run(JobName=glue_job_name, Arguments=job_args)
        except ParamValidationError:
            pass

        if "JobRunId" in response:
            jobId = response["JobRunId"]
            logger.info("%s job run with id %s", glue_job_name, response["JobRunId"])
            _ = glue_job.get_job_run(JobName=glue_job_name, RunId=jobId)


def schedule_handler(event, context) -> None:
    """Process the sla requirement and if SLA requirement is not met then email is sent.

    Arguments:
        arguments {List[str]} -- List of parameters supplied by Glue Job
    """
    now = datetime.utcnow()
    logger.info(
        " Current UTC time is : %s,   Current UTC Hour is : %d",
        now,
        now.hour,
    )

    # args = getResolvedOptions(arguments, ["SECRET_NAME", "CLIENT_FILE_ID", "ENVIRONMENT"])
    secret_name = os.environ["SECRET_NAME"]
    s3_bucket_name = os.environ["S3_BUCKET_NAME"]
    process_sla(secret_name, s3_bucket_name)


def glue_failure_handler(event, context) -> None:
    """
    Monitors the given glue jobs in cloudwatch and sends an email if it is failed.
    """

    # get lambda variables
    environment_level = os.environ["ENVIRONMENT"]
    sns_topic_name = os.environ["PROCESSING_NOTIFICATION_SNS"]

    # fetches the response in dict received from eventbridge rule when 'event pattern' matches
    # here failing a glue job is an event pattern. this is already stated in event rule 'glue_failure_rule'
    # the output from below will be like --> {"version": "0", "id": "26c9301e-6aec-872d-4739-1aa6ebc66853", "detail-type": "Glue Job State Change", "source": "aws.glue", "account": "795038802291", "time": "2021-03-24T07:37:07Z", "region": "us-east-1", "resources": [], "detail": {"jobName": "rahul_create_warm_pool", "severity": "ERROR", "state": "FAILED", "jobRunId": "jr_bdb3f0d56b0f842c7337be54927deb6ddb78a1f37956d9817b0f11710602139f", "message": "Command failed with exit code 1"}}
    event_response = json.dumps(event)

    # calling GlueFailureEvent class
    glue_failure_event = GlueFailureEvent(event_response)

    # get failure time (event occurrence)
    event_occurrence_time = glue_failure_event.time # returns ISO UTC time (ex: '2021-03-24T15:52:37Z')
    # convert ISO UTC time to regular UTC time and then to ET local time
    converted_time = event_occurrence_time.replace("T"," ").replace("Z",".0")
    _converted_time = datetime.strptime(converted_time, "%Y-%m-%d %H:%M:%S.%f")
    local_time = convert_utc_to_local(_converted_time, DEFAULT_TIMEZONE)

    if glue_failure_event.detail['jobRunId'].endswith("attempt_1"):
        # we dont want get notified for attempt1 run failure because this will always be failure in real time.
        # so its an attempt to avoid 2 failure alerts for each failed glue job event
        return None
    # form subject, message required for publishing SNS
    subject = f"{environment_level}: Glue job Failure"
    message = (
        f"Glue job '{glue_failure_event.detail['jobName']}' has {glue_failure_event.detail['state']} at {local_time}. Below are the details:{os.linesep}{os.linesep}"
        f"Error : {glue_failure_event.detail['message']}{os.linesep}"
        f"{os.linesep}"
        f"JobRunId : {glue_failure_event.detail['jobRunId']}"
    )

    send_ahub_email_notification(sns_topic_name, subject, message)
