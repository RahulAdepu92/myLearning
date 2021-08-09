import logging
import os
from typing import Optional

import boto3

from datetime import datetime

from .common import convert_utc_to_local
from .constants import DEFAULT_TIMEZONE
from .glue import get_glue_logger
from .model import ClientFile, FileSchedule, Job

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)

session = boto3.session.Session()
region_name = session.region_name
account_id = boto3.client("sts").get_caller_identity()[
    "Account"
]  # gives the account number as per environment like for SIT it is "795038802291"


def send_ahub_email_notification(sns_topic_name: str, subject: str, message: str):
    """This function sends email notification to the subscribed receipents.
    sns_arn  : ARN for SNS Topic
    subject  : Subject of the email
    message     : body of the email

    conditions:
    1. Topic should be created at AWS SNS ( Topic : AHUB_EMAIL_NOTIF)
    2. Its ARN should be configured as  Environment Variable 'ARN_AHUB_EMAIL_NOTIF'
    3. The receipents should subscibe to the topic


    """
    response = None
    logger.info("in notifications.send_ahub_email_notification")
    sns = boto3.client("sns")

    # sns.publish do not support more than 100 character in a subject
    if len(subject) > 100:
        subject = subject[0:100]

    # Dynamic region ARN
    full_arn = f"arn:aws:sns:{region_name}:{account_id}:{sns_topic_name}"

    try:
        response = sns.publish(
            TopicArn=full_arn,
            Message=message,
            Subject=subject,
        )
        logger.info(
            " Sent notification email to arn: %s, message: '%s'",
            full_arn,
            message,
        )

    except ValueError as em:
        logger.exception("executing in except block with error - %s", em)

    return response


def notify_file_structure_error(
    client_file: ClientFile, error_file_path: str, error_message: str
):
    """
    This function forms the subject and body of the email to be sent as a notification upon failure structural validation.
    conditions:
    1. Topic should be created at AWS SNS ( Topic : AHUB_EMAIL_NOTIF)
    2. The receipents should subscibe to the topic
    :param client_file: The Client_File object which should be populated from the irxah/database/client_files.py functions.
    :param error_file_path: The S3 path to the errored file.
    :param error_message: The error message that occured during processing.
    """
    file_category = f"{client_file.file_category} " if client_file.file_category else ""

    subject = f"{client_file.environment_level}: Structural Error- Error occurred while processing {file_category}{client_file.client_abbreviated_name} file "
    message = (
        f"Unable to process {file_category}{client_file.client_abbreviated_name} file '{error_file_path}' file because of the following error(s):"
        f" \n\n '{error_message}'"
    )

    response = send_ahub_email_notification(
        client_file.data_error_notification_sns, subject, message
    )

    logger.info(
        "Sent notification email to arn: %s, message: '%s'",
        client_file.data_error_notification_sns,
        message,
    )

    return response


def notify_file_data_errors(
    client_file: ClientFile, error_records_count: int, file_name: str
):
    """
    This function forms the subject and body of the email to be sent as a notification upon errors in field validations exceeding a threshold.
    conditions:
    1. Topic should be created at AWS SNS ( Topic : AHUB_EMAIL_NOTIF)
    2. The receipents should subscibe to the topic
    :param client_file: The client file object, it should be retrieved from database/client_files.py methods.
    :param error_records_count: The number of error records.
    :param file_name: The file name that was sent inbound.
    """
    file_category = f"{client_file.file_category} " if client_file.file_category else ""

    subject = f"{client_file.environment_level}: Data Errors- Error records found while processing {file_category} {client_file.client_abbreviated_name} file"
    message = (
        f"AccumHub encountered {error_records_count} records with data errors (notification threshold= {client_file.data_error_threshold}) "
        f"while processing '{file_name}' file."
    )

    response = send_ahub_email_notification(
        client_file.data_error_notification_sns, subject, message
    )
    return response


def notify_file_loading_error(
    file_object: object,
    file_name: str,
    sns: str,
    error_message: Optional[str] = "Error Occurred while loading incoming file to AccumHub database",
):
    """
    this function is used by accumhist file and other inbound files while sending SLA alerts. As accumhist doesnt have file schedule id,
    file_object is generalized so that it can be used either by id from client file / file schedule.
    """
    subject = f"{file_object.environment_level}: Ahub Failed to load {file_object.file_category} file from {file_object.client_abbreviated_name}"
    message = (
        f"Unable to process {file_object.client_abbreviated_name} file '{file_name}' file because of the following error(s):"
        f"{os.linesep}{os.linesep} '{error_message}'"
        f"{os.linesep}{os.linesep} However, now the system will try to reprocess the file automatically and a successful inbound processing alert would be received in next couple of minutes."
    )
    response = send_ahub_email_notification(
        sns, subject, message
    )
    return response


def send_email_notification_for_process_duration(client_file: ClientFile, job_key, elapsed_time):
    """
    This function forms the subject and body of the email to be sent as a notification upon failure of unzipping.
    conditions:
    1. Topic should be created at AWS SNS ( Topic : AHUB_EMAIL_NOTIF)
    2. The receipents should subscibe to the topic
    :param client_file: The client file object, it should be retrieved from database/client_files.py methods.
    :param job_name: The general job name.
    :param job_key: The job key
    """
    if client_file.processing_notification_sns:
        subject = f"{client_file.environment_level}: Process Duration Threshold Exceeded!"
        m, s = divmod(elapsed_time, 60)
        message = (
            f"Processing took {int(m)} minutes {s} seconds to run, which exceeded the threshold of {client_file.process_duration_threshold} seconds."
            f"{os.linesep}{os.linesep}"
            f"The respective job key is {job_key}."
        )

        response = send_ahub_email_notification(
            client_file.processing_notification_sns, subject, message
        )
        return response


def send_email_notification_sla_inbound(file_schedule: FileSchedule, inbound_file: str) -> None:
    """Sends an alert for  non receipt of an inbound file.

    Args:
        file_schedule (FileSchedule): File Schedule
        inbound_file (str): Inbound File Name ( only pattern )
    """
    subject = f"{file_schedule.environment_level}: SLA Alert for incoming {file_schedule.file_category} file from {file_schedule.client_abbreviated_name} "
    message = (
        f"A {file_schedule.file_description} from {file_schedule.client_abbreviated_name} was expected to arrive "
        f"between {file_schedule.current_sla_start_time} and {file_schedule.current_sla_end_time} and it failed to do so by {file_schedule.current_sla_end_time}."
        f"{os.linesep}{os.linesep}"
        f"Expected Inbound File Name : {inbound_file} ( HH is hour in 24 hour format, MM is minute and SS is second){os.linesep}{os.linesep}"
        f"Please validate with the upstream systems and the originator. "
    )
    _ = send_ahub_email_notification(file_schedule.notification_sns, subject, message)


def send_email_notification_when_failure_executing_sla(
    file_schedule: FileSchedule, error_message: str
) -> None:
    """Sends an alert for  unable to process the file.

    Args:
        file_schedule (FileSchedule): File Schedule
        error_message (str): REason for failure ( only pattern )
    """
    subject = f"{file_schedule.environment_level}: Error Occured while executing the SLA for {file_schedule.file_description} "
    message = (
        f"AccumHub was unable to process the SLA because of the following reason: "
        f"{os.linesep}{os.linesep}"
        f"Reason : {error_message}"
        f"{os.linesep}{os.linesep}"
        f"AccumHub will not process any SLA for the {file_schedule.file_description} until above error is reoslved."
    )
    _ = send_ahub_email_notification(file_schedule.notification_sns, subject, message)


def send_email_notification_sla_outbound(file_schedule: FileSchedule, outbound_file: str) -> None:
    """When AHUB does not generate the outbound file timely the following email template is used to
        send an email.

    Args:
        file_schedule (FileSchedule): File Schedule
        outbound_files (str): Name of the outbound file
    """
    subject = f"{file_schedule.environment_level}: SLA Alert for outgoing {file_schedule.file_category} file to {file_schedule.client_abbreviated_name}"
    message = (
        f"A {file_schedule.file_description} to {file_schedule.client_abbreviated_name} was expected to depart by {file_schedule.current_sla_end_time} "
        f"and it failed to do so by {file_schedule.current_sla_end_time}."
        f"{os.linesep}{os.linesep}"
        f"Expected Outbound File Name : {outbound_file} (HH is hour in 24 hour format, MM is minute and SS is second) {os.linesep}{os.linesep}"
        f"Please notify recipient of delay in dispatch."
    )

    _ = send_ahub_email_notification(file_schedule.notification_sns, subject, message)


def send_email_notification_cvs(file_schedule: FileSchedule, actual_file_count: int) -> None:
    """Sends email notification related to CVS when required number of files ( 12) are not received by AccumHub

    Args:
        file_schedule (FileSchedule): File Schedule
        actual_file_count (int): Number of file received in last 24 hours
    """
    subject = f"{file_schedule.environment_level}: SLA Alert for not receiving {file_schedule.total_files_per_day} files from {file_schedule.client_abbreviated_name}"
    message = (
        f"AccumHub was expecting total {file_schedule.total_files_per_day}  {file_schedule.file_description} from {file_schedule.client_abbreviated_name}"
        f" between {file_schedule.current_sla_start_time} and {file_schedule.current_sla_end_time}."
        f"{os.linesep}{os.linesep}"
        f" AccumHub received {actual_file_count} files. Corresponding outbound files to client will not be generated until this issue is resolved manually."
        f"{os.linesep}{os.linesep}"
        f" Please validate with the upstream systems and the originator."
    )
    _ = send_ahub_email_notification(file_schedule.notification_sns, subject, message)


def send_email_notification_when_file_received_outside_sla(
        file_schedule: FileSchedule, inbound_file: str, inbound_file_time: str, file_record_count: int,
) -> None:
    """Sends email notification using the content of the client_file to craft the Subject and Message

    Arguments:
        client_file {ClientFile} -- Non-empty client_file object
        inbound_file {str} -- Name of incoming file
        inbound_file_time {str} -- Time in EDT/EST the file was received..
    """
    subject = f"{file_schedule.environment_level}: Ahub received {file_schedule.file_category} file from {file_schedule.client_abbreviated_name} outside SLA window "
    message = (
        f"AccumHub received the following {file_schedule.file_description} and it has been processed by {inbound_file_time}.{os.linesep * 2}"
        f"File Name : {inbound_file} ({file_record_count} records){os.linesep}"
        f"{os.linesep}"
        f"File Frequency : {file_schedule.frequency_type}{os.linesep}"
        f"Expected Arrival : Between {file_schedule.current_sla_start_time} and {file_schedule.current_sla_end_time} {os.linesep * 2}"
        f"Please note that {file_schedule.client_abbreviated_name} failed to send the file as per SLA."
    )
    _ = send_ahub_email_notification(file_schedule.notification_sns, subject, message)


## Ahub_666 ##

# Template 1:
# Send an email when an inbound file arrives and it matches with a file pattern ( prefix) but the underlying file is set to in active ( in active = False).

def notify_inactive_inbound_file(
        client_file: ClientFile, file_schedule: FileSchedule, inbound_file: str, file_path: str, file_arrival_time: str
) -> None:
    """Sends email notification when an Inactive file (is_active = false) is landed into s3 inbound/srcfiles folder
    :param client_file: The client file object, it should be retrieved from database/client_files.py methods
    :param inbound_file: file received by Ahub
    :param file_path: file path to where the unknown file will be moved
    :param file_arrival_time: the local time file received by Ahub
    """
    subject = f"{file_schedule.environment_level}: Error occurred while processing Incoming {file_schedule.file_category} file from {file_schedule.client_abbreviated_name}"
    message = (
        f"Unable to process below incoming {file_schedule.file_description} received on {file_arrival_time} because :{os.linesep}"
        f"AccumHub recognized that the file status is INACTIVE (is_active = FALSE){os.linesep * 2}"
        f"File Name : {inbound_file}{os.linesep}"
        f"File Frequency : {file_schedule.frequency_type}{os.linesep}"
        f"Expected Arrival : Between {file_schedule.current_sla_start_time} and {file_schedule.current_sla_end_time}{os.linesep * 2}"
        f"This file stays as is in '{file_path}' folder and it will not be processed further."
    )

    response = send_ahub_email_notification(
        client_file.file_extraction_error_notification_sns, subject, message
    )


# Template 2:
#  Send an email when an inbound file arrives and it does not match any file pattern (prefix), so in short a complete strange file.

def notify_unknown_inbound_file_arrival(
        environment_level: str,
        sns_name: str,
        inbound_file: str,
        file_arrival_time: str,
        file_path: str
) -> None:
    """Sends email notification when a file of prefix pattern which is not recognized by db is landed into s3 inbound/srcfiles folder
    :param environment_level: environment to be specified in the subject of an alert
    :param sns_name: sns topic name by which alert will be sent notifying the unknown file arrival
    :param inbound_file: file received by Ahub
    :param file_arrival_time: the local time file received by Ahub
    :param file_path: file path to where the unknown file will be moved
    """
    subject = f"{environment_level}: Error occurred while processing Incoming file"
    message = (
        f"Unable to process incoming file '{inbound_file}' received on {file_arrival_time} because :{os.linesep}"
        f"AccumHub did not recognize the file name pattern in reference to the expected Inbound file name patterns in db.{os.linesep * 2}"
        f"This file is moved to '{file_path}' folder and it will not be processed further."
    )

    response = send_ahub_email_notification(
        sns_name, subject, message
    )


# Template 3:
# Send an email when an inbound file arrives and it matches with a file pattern ( prefix) but the underlying file is missing extension   or extension is neither .zip nor .txt.

def notify_inbound_file_extraction_error(client_file: ClientFile, file_schedule: FileSchedule, incoming_file: str,
                                 file_arrival_time, error_string: str) -> dict:
    """This function forms the subject and body of the email to be sent as
    a notification upon failure of unzipping.
    conditions:
    1. Topic should be created at AWS SNS ( Topic : AHUB_EMAIL_NOTIF)
    2. Its ARN should be configured as  Environment Variable 'ARN_AHUB_EMAIL_NOTIF'
    3. The recipients should subscribe to the topic

    :param client_file: s3 bucket name.
    :param file_schedule: File Schedule object, it should be retrieved from database/file_schedule.py method
    :param incoming_file: s3 inbound file name.
    :param file_arrival_time: the local time file received by Ahub
    :param error_string: the error message.
    :returns: response for publish action

    """
    subject = f"{file_schedule.environment_level}: Error extracting Incoming {file_schedule.file_category} file from {file_schedule.client_abbreviated_name}"
    message = (
        f"Error encountered extracting below {file_schedule.file_description} received on {file_arrival_time}.{os.linesep * 2}"
        f"Error : {error_string}{os.linesep}"
        f"{os.linesep}"
        f"File Name : {incoming_file}{os.linesep}"
        f"File Frequency : {file_schedule.frequency_type}{os.linesep}"
        f"Expected Arrival : Between {file_schedule.current_sla_start_time} and {file_schedule.current_sla_end_time}{os.linesep * 2}"
        f"This file will be moved to '{client_file.error_folder}' folder and cannot be processed further."
    )

    response = send_ahub_email_notification(
        client_file.file_extraction_error_notification_sns,
        subject,
        message,
    )
    return response


# Template 4:
# Send an email when an inbound file arrives and it matches with a file pattern ( prefix)  and the  underlying file has a valid extension ( .zip or .txt).

def notify_inbound_file_arrival(
        job: Job,
        file_schedule: FileSchedule,
        file_arrival_time: str,
        sns: str
) -> None:
    """Sends email notification when a file of prefix pattern which is not recognized by db is landed into s3 inbound/srcfiles folder
    :param job: Non-empty Job object
    :param file_schedule: Non-empty file_schedule object
    :param file_arrival_time: the local time file received by Ahub
    """
    subject = f"{file_schedule.environment_level}: Ahub received {file_schedule.file_category} file from {file_schedule.client_abbreviated_name}"
    message = (
        f"AccumHub received the following {file_schedule.file_description} and it has been processed by {file_arrival_time}.{os.linesep * 2}"
        f"File Name : {job.incoming_file_name} ({job.file_record_count} records){os.linesep}"
        f"{os.linesep}"
        f"File Frequency : {file_schedule.frequency_type}{os.linesep}"
        f"Expected Arrival : Between {file_schedule.current_sla_start_time} and {file_schedule.current_sla_end_time}"
    )

    response = send_ahub_email_notification(
        sns,
        subject,
        message,
    )


# Template 5 & 6:
# Send an email when an outbound file is generated but not delivered to the respective client ( because deliver_files_to_client=False) and outbound_successful_acknowledgement = True
# Send an email when an outbound file is generated  and  delivered to the  respective client ( because deliver_files_to_client=True)

def notify_outbound_file_delivery_to_cfx(
        client_file: ClientFile,
        file_object: object,
        export_file_name: str,
        file_origin: str,
        file_departure_time: str,
        file_record_count: str,
        cfx_path: str,
        cfx_delivery_message: str,
        cfx_error_occurred: Optional[bool] = False
) -> None:
    """Sends email notification using the content of the client_file and job to craft the Subject and Message
    :param client_file: Non-empty client_file object
    :param file_object: Non-empty file_schedule/ClientFile object
    :param export_file_name: file name of outgoing file
    :param file_origin: to know whether file in outbound folder is processed by ahub or place manually
    :param file_departure_time: the local time at which file got generated by Ahub
    :param file_record_count: a full string that has outbound file name with total number of records in it.
    :param cfx_path: cfx s3 path to where file has to be copied
    :param cfx_delivery_message: success/failure message while copying file to cfx
    :param cfx_error_occurred: boolean value if error has been occurred while copying file to cfx
    """

    # When AHUB generates an outbound file , it is in .txt format and at the time of the delivery , system queries that whether it needs to be zipped or not.
    # The process that delivers the file to the respective client is different ( handled through a lambda 'irxah_zip_outgoing_file_and_send_to_cfx’ ).
    # In that process we do not OPEN up the file , do any record count etc.. this is the only place and the process where we are able to include the record count.
    # Although the delivery in the outbound/txtfiles means the file generation is successful but as the name suggests it is in TXT file, we don't know whether going outbound it will be zipped or not.
    # The only way to know is to use the .zip_file property to identify that whether the outbound file will be zipped or delivered as is ( in .TXT) format. Hence , the client_file.zip_file property is used here".

    if client_file.zip_file is False:  # zipping not required, extension remains as .TXT
        file_name_displayed = export_file_name
    else:  # zipping is required, extension changes to .ZIP
        file_name_displayed = export_file_name.replace('.TXT', '.ZIP')

    if file_object == client_file:
        # client file that doesnt have file_schedule id doesnt fetch scedule info. So defaulting it to blank.
        schedule_info = ""
    else:
        schedule_info = (
            f"File Frequency : {file_object.frequency_type}{os.linesep}"
            f"Expected Departure : Between {file_object.current_sla_start_time} and {file_object.current_sla_end_time}"
        )

    message = (
        f"AccumHub {file_origin} the following {file_object.file_description} on {file_departure_time}.{os.linesep * 2}"
        f"File Name : {file_name_displayed} {file_record_count}{os.linesep}"
        f"{os.linesep}"
        f"{schedule_info}"
        f"{os.linesep * 2}"
        f"{cfx_delivery_message}{os.linesep}"
        f"'{cfx_path}'"
    )
    # when no error occurs while copying file to cfx
    if not cfx_error_occurred:
        # Ahub exported file to cfx successfully
        if client_file.deliver_files_to_client is True:
            subject = f"{file_object.environment_level}: Ahub delivered {file_object.file_category} file to {file_object.client_abbreviated_name} Successfully"
        # Ahub holded file export to cfx
        else:
            subject = f"{file_object.environment_level}: Ahub held delivery of {file_object.file_category} file to {file_object.client_abbreviated_name}"
    # when error occurs while copying file to cfx
    else:
        subject = f"{file_object.environment_level}: Ahub failed to deliver {file_object.file_category} file to {file_object.client_abbreviated_name}"

    _ = send_ahub_email_notification(
        client_file.outbound_file_generation_notification_sns, subject, message
    )


def notify_inbound_file(file_schedule: FileSchedule, job: Job, client_file: ClientFile) -> None:
    """
    function used in load_in_db.py and cvs.py to send an alert for inbound file arrival
    :param file_schedule: Non-empty file_schedule object
    :param job: The job object which contains the characteristics of a job.
    """
    if file_schedule is None:
        # Processing Gila standard file (client_file_id=23) is a special case which doesnt have file_schedule_id.
        # However its corresponding alert will be sent while processing the Inbound Custom file (client_file_id=16)
        logger.info("file_schedule_id for client_file_id = %s is not found. Hence, inbound processing alert will not be sent",
                    job.client_file_id)
    else:
        try:
            job_end_timestamp = datetime.strptime(job.job_end_timestamp, "%Y-%m-%d %H:%M:%S.%f")

            logger.info("inbound_successful_acknowledgement is set to '%s'.",
                        client_file.inbound_successful_acknowledgement)
            if not client_file.inbound_successful_acknowledgement:
                logger.info("Hence, inbound file process alert will not be sent")
            else:
                logger.info("Hence, attempting to send inbound file process alert")
                notify_inbound_file_arrival(job,
                                            file_schedule,
                                            convert_utc_to_local(job_end_timestamp, file_schedule.notification_timezone),
                                            client_file.inbound_file_arrival_notification_sns,
                                            )

        except Exception as e:
            logging.exception(e)
            logger.info("Error occurred: %s. Not able to send the notification for Inbound file arrival", e)


# Template 7:
#  Send an email when client sends more than one inbound file on same day

def notify_multiple_file_arrival(job: Job, file_schedule: FileSchedule, sns: str) -> None:
    """
    function used in process_flow.py to send an alert if multiple inbound file received from a client on same day
    :param file_schedule: Non-empty file_schedule object
    :param job: The job object which contains the characteristics of a job.
    """
    if file_schedule is None:
        # Processing Gila standard file (client_file_id=23) is a special case which doesnt have file_schedule_id.
        # However its corresponding alert will be sent while processing the Inbound Custom file (client_file_id=16)
        logger.info(
            "file_schedule_id for client_file_id = %s is not found. Hence, inbound processing alert will not be sent",
            job.client_file_id)
    else:
        try:
            job_start_timestamp = datetime.strptime(job.job_start_timestamp, "%Y-%m-%d %H:%M:%S.%f")
            logger.info("As file arrived more than once for current day, sending Multiple client file arrival alert")

            notify_multiple_file(job,
                                 file_schedule,
                                 convert_utc_to_local(job_start_timestamp, DEFAULT_TIMEZONE,),
                                 sns,)

        except Exception as e:
            logging.exception(e)
            logger.info("Error occurred: %s. Not able to send the notification for Inbound file arrival", e)


def notify_multiple_file(
        job: Job,
        file_schedule: FileSchedule,
        file_arrival_time: str,
        sns: str,
) -> None:
    """Sends email notification when more than one file is received from same client on the same day
    :param job: Non-empty Job object
    :param file_schedule: Non-empty file_schedule object
    :param file_arrival_time: the local time file received by Ahub
    """
    subject = f"{file_schedule.environment_level}: Multiple {file_schedule.file_category} file received from {file_schedule.client_abbreviated_name}"
    message = (
        f"Unable to process the following {file_schedule.file_description} received at {file_arrival_time} because: {os.linesep}"
        f"AccumHub received {file_schedule.client_abbreviated_name} file more than once on same day.{os.linesep * 2}"
        f"File Name : {job.incoming_file_name} ({job.file_record_count} records){os.linesep * 2}"
        f"This file has been moved to the 'inbound/unprocessed' folder. It will not be processed again unless the 'process_multiple_file' flag is set to TRUE and the file is moved to 'srcfiles' folder."
    )

    response = send_ahub_email_notification(
        sns, subject, message
    )


def notify_validation_flag_off(
    client_file: ClientFile,
    job: Job,
    flag: str,
) -> None:
    """Sends email notification when more than one file is received from same client on the same day
    :param client_file: ClientFile object
    :param job: Non-empty Job object
    :param flag: the validation column name
    """
    file_category = f"{client_file.file_category} " if client_file.file_category else ""
    job_start_timestamp = datetime.strptime(job.job_start_timestamp, "%Y-%m-%d %H:%M:%S.%f")
    received_time = convert_utc_to_local(job_start_timestamp, DEFAULT_TIMEZONE, )

    subject = f"{client_file.environment_level}: Unexpected Validation configuration setting for {client_file.client_abbreviated_name} {file_category} file"
    message = (
        f"Unable to process the file '{job.incoming_file_name}' received at {received_time} because: {os.linesep}"
        f"The validation flag '{flag}' for {client_file.client_abbreviated_name} (client_file_id= {client_file.client_file_id}) is set to 'FALSE' {os.linesep * 2}"
        f"This file has been moved to 'inbound/unprocessed' folder. It will not be processed until the '{flag}' flag is to TRUE and the file is moved to 'srcfiles' folder."
    )

    response = send_ahub_email_notification(
        client_file.processing_notification_sns, subject, message
    )