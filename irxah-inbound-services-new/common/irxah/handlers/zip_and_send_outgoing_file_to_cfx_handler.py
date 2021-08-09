import logging
import logging.config
import os

import boto3

from datetime import datetime

from ..common import convert_utc_to_local, get_file_name_pattern
from ..compression import zip_text_file
from ..constants import DEFAULT_TIMEZONE
from ..database.client_files import ClientFileRepo
from ..database.file_schedule import FileScheduleRepo
from ..database.job_log import JobRepository
from ..glue import get_glue_logger
from ..model import FileScheduleProcessingTypeEnum, ClientFileFileTypeEnum, ClientFile, FileSchedule
from ..notifications import notify_outbound_file_delivery_to_cfx
from ..s3 import get_file_from_s3_trigger
from ..constants import TABLE_JOB_DETAIL, SCHEMA_DW

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# file path types - literals
TXT_FILES = "txtfiles"
ARC_FILES = "archive"
ERR_FILES = "error"

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")


def zip_handler(event, context) -> None:
    """
    Compresses the trigger file into a zip file and copies it to a specified location (bucket and path).
    It also archives both source file and zip file to an archive folder relative to trigger file.
    Environment variables used: DESTINATION_BUCKET_CFX, DESTINATION_FOLDER_CFX.
    :param event: lambda event
    :param context: lambda context
    :return: None
    """
    try:
        logger.info("Process started.. finding the file path of trigger event")
        s3_bucket_name, outgoing_file = get_file_from_s3_trigger(event)
        # outgoing_file = outbound/txtfiles/filename.txt
        logger.info(f"outgoing_file: %s", outgoing_file)

        # 'secret_name' is the only existing environment variable for lambda function
        secret_name = os.environ["SECRET_NAME"]

        # initialising the local time here becasue as soon as the inbound file arrives s3 event will be triggered
        current_utc_time = datetime.utcnow()
        current_local_time = convert_utc_to_local(current_utc_time, DEFAULT_TIMEZONE)

        outgoing_file_name = outgoing_file.split(os.sep)[-1]
        logger.info(f"outgoing_file_name: %s", outgoing_file_name)

        # by using 'outgoing_file_name' we can identify outbound file ownership as the file generated in txtfiles
        # folder will always have unique name. get ClientFile object to fetch client related info from db
        outgoing_file_name_pattern = get_file_name_pattern(outgoing_file_name)  # [ex: 'ACCDLYINT_TST_BCI']

        client_file: ClientFile = ClientFileRepo(secret_name=secret_name).get_client_file_from_name_pattern(outgoing_file_name_pattern)
        logger.info(f"client_file: %s", client_file)

        if client_file is not None and client_file.file_type == ClientFileFileTypeEnum.OUTBOUND:
            # restricting the departure of files that match inbound file name patterns
            logger.info(
                "Outgoing file name '%s' matches with prefix pattern '%s'. "
                " Hence, outgoing file will be moved to outbound/archive",
                outgoing_file_name,
                client_file.s3_output_file_name_prefix,
            )
            archive_folder = client_file.archive_folder  # outbound/archive
            # no client specific archive folder is maintained. Its all one common outbound/archive
            logger.info("Archive Folder is  %s ", archive_folder)

            ###################### first step: zip .txt file or not? ########################

            # whether the outgoing file is to be zipped or not will be checked here.
            # If TRUE, file will be zipped otherwise, just the .txt file as is will be moved to outbound/archive folder

            if client_file.zip_file:
                logger.info(
                    "'zip_file' alert is set to '%s'. "
                    " Hence, outgoing file '%s' will be zipped and moved to outbound/archive",
                    client_file.zip_file,
                    outgoing_file_name,
                )
                logger.info(
                    "_zipfile: zipping s3://%s/%s to s3://%s/%s",
                    s3_bucket_name,
                    outgoing_file,
                    s3_bucket_name,
                    archive_folder,
                )
                # send .zip file to archive folder. It will be awaiting for getting permission to enter into cfx.
                outgoing_file_full_path = zip_text_file(
                    s3_bucket_name, outgoing_file, archive_folder
                )
                # outbound/archive/filename.ZIP
                # 'outgoing_file_full_path' is passed as an argument to 'copy_to_cfx' function and is very crucial
                logger.info("zipped %s to %s", outgoing_file, outgoing_file_full_path)

            else:
                # send just the .txt file to archive folder.
                # It will be awaiting for getting permission to enter into cfx in fourth step.
                outgoing_file_full_path = os.path.join(archive_folder, outgoing_file_name)
                # outbound/archive/filename.TXT
                # 'outgoing_file_full_path' is passed as an argument to 'copy_to_cfx' function and is very crucial
                logger.info(
                    "'zip_file' alert is set to '%s'. Hence, zipping for '%s' will be skipped. "
                    "However it will be sent from '%s' to CFX as .txt format itself if its deliver_to_cfx alert is set to TRUE",
                    client_file.zip_file,
                    outgoing_file_name,
                    outgoing_file_full_path,
                )

            ###################### second step: archive .txt file ########################

            source_object = {"Bucket": s3_bucket_name, "Key": outgoing_file}
            source_file_archive_path = os.path.join(archive_folder, os.path.basename(outgoing_file))
            # copy .txt file to outbound/archive
            logger.info(
                "archiving source %r to s3://%s/%s",
                source_object,
                s3_bucket_name,
                source_file_archive_path,
            )
            s3_client.copy_object(
                Bucket=s3_bucket_name,
                Key=source_file_archive_path,
                CopySource=source_object,
            )
            logger.info(" ARCHIVE Operation SUCCESSFUL")

            ###################### third step: delete .txt file from 'outbound/txtfiles' folder ########################

            if os.path.basename(outgoing_file) != "":
                # Delete source file, since it's been archived
                logger.info("deleting origin file s3://%s/%s", s3_bucket_name, outgoing_file)
                s3_client.delete_object(Bucket=s3_bucket_name, Key=outgoing_file)
                logger.info(" DELETE Operation SUCCESSFUL")

            ###################### fourth step: Insert job_detail key in job_detail table ########################

            # Insert file export activity into the job_detail table by which if the file has been sent or not is known
            job_repo = JobRepository(
                secret_name=secret_name,
                tables_to_be_locked=[f"{SCHEMA_DW}.{TABLE_JOB_DETAIL}"],
            )
            # 'archive_detail_key' will let you know if the file is generated by AHub (if found corresponding job_key in job table)
            # or not (file placed manually in outbound/txtfiles folder)
            archive_detail_key, job_record_count = job_repo.insert_archive_and_export_job_detail(
                outgoing_file_name
            )

            if archive_detail_key:
                # we make an entry in job_detail table if the corresponding file name’s entry is found in the job table.
                logger.info("Corresponding job_detail_key is logged in job_detail table")
                # variables for sending alert upon successful outbound file generation and it's export to cfx
                file_origin = "generated"  # file got generated by AHub
                file_record_count = f"({job_record_count} records)"  # count derived from job table
            else:
                # corresponding job_key is not found i.e, file placed manually in outbound/txtfiles folder
                logger.info(
                    "job_detail_key is NOT logged in job_detail table as file is copied manually"
                )
                # variables for sending alert upon outbound file located externally and it's export to cfx
                file_origin = "found"  # file placed manually
                file_record_count = ""  # we dont have corresponding record count in job table. So defaulting to blank
            logger.info("file_record_count : %s", file_record_count)

            ###################### fivth step: do/don't copy to CFX and update entry in job_detail table ########################

            # 'deliver_files_to_client' is very crucial for business.
            # At times, business people request us to turn ON/OFF this variable as per their requirement.
            # this variable is set to TRUE or not will be checked below.
            # If TRUE, outbound file will be sent to CFX, otherwise will leave as is in archive folder

            dest_bucket = client_file.destination_bucket_cfx
            dest_folder = client_file.destination_folder_cfx

            if client_file.deliver_files_to_client:
                logger.info(
                    " DELIVER_FILES_TO_CLIENT variable is set to :%s ",
                    client_file.deliver_files_to_client,
                )
                cfx_delivery_path = cfx_copy_message = error_occurred = ""
                try:
                    logger.info(
                        " Attempting to send files to CFX and copying file from s3://%s/%s to s3://%s/%s",
                        s3_bucket_name,
                        archive_folder,
                        dest_bucket,
                        dest_folder,
                    )
                    # copy the file from outbound/archive to cfx bucket
                    cfx_copy_message, error_occurred = copy_to_cfx(
                        s3_bucket_name,
                        outgoing_file_full_path,
                        dest_bucket,
                        dest_folder,
                    )

                    cfx_delivery_path = (
                        "s3://"
                        + dest_bucket
                        + "/"
                        + os.path.join(dest_folder, os.path.basename(outgoing_file_full_path))
                    )

                    # corresponding job_key is found for outbound file landed in outbound/txtfiles
                    if archive_detail_key:
                        # update s3 path of archive on job detail table
                        job_repo.update_archive_and_export_job_detail(archive_detail_key, cfx_delivery_path)

                except Exception as e:
                    logger.exception(e)

            ####################  'deliver_files_to_client' alert is set to FALSE  #####################
            else:
                logger.info(
                    "'deliver_files_to_client' alert is set to FALSE. Hence, delivery to CFX for '%s' will be skipped.",
                    outgoing_file_name,
                )
                try:
                    # corresponding job_key is found for outbound file landed in outbound/txtfiles
                    if archive_detail_key:
                        # update s3 path of archive on job detail table
                        job_repo.update_archive_and_export_job_detail(archive_detail_key, outgoing_file_full_path)

                except Exception as e:
                    logger.exception(e)

                cfx_delivery_path = f"s3://{dest_bucket}/{dest_folder}"
                cfx_copy_message = (
                    f"This file will NOT be pushed to CFX because it's corresponding delivery flag is OFF (deliver_files_to_client = FALSE).\n"
                    f"When flag is turned on and this file is moved to 'outbound/scrfiles' folder, it will be delivered to below cfx path:"
                )
                # when delivery flag is false, file export to cfx will be on hold and there's no chance of copy failure.
                # Hence, defaulting 'error_occurred' to False.
                error_occurred = False

            # whether the file is Ahub generated or placed manually, cfx delivery will be acknowledged
            # if and only if outbound_successful_acknowledgement flag is true
            if client_file.outbound_successful_acknowledgement:
                logger.info(
                    "Alert is Active - Attempting to send outbound file generation and cfx delivery status alert"
                )
                file_schedule: FileSchedule = FileScheduleRepo(secret_name=secret_name).get_file_schedule_info(
                    client_file.client_file_id, FileScheduleProcessingTypeEnum.AHUB_TO_OUTBOUND)
                file_schedule.format_timestamps() if file_schedule is not None else None
                logger.info(" File Schedule Info : %s", file_schedule)

                # for CVS outbound (client_file_id =22), we dont have file_schedule_id in file_schedule table
                # so we get the adequate information from client_files table for sending processing alert
                if file_schedule is not None:
                    file_object = file_schedule
                else:
                    file_object = client_file
                try:
                    notify_outbound_file_delivery_to_cfx(client_file, file_object, outgoing_file_name,
                                                         file_origin, current_local_time, file_record_count,
                                                         cfx_delivery_path, cfx_copy_message, error_occurred)

                except Exception as e:
                    logging.exception(e)
                    logger.info("Error occurred: %s. Not able to send the notification for outbound file delivery", e)

            else:
                logger.info(
                    "Alert is set to 'False'. Hence, outbound file generation alert will not be sent"
                )

        else:
            logger.info(
                "Outgoing file '%s' name doesn't match with any of the prefix pattern. "
                "Hence, it will remain in the outbound/txtfiles folder. ",
                outgoing_file_name,
            )

    except:
        logging.exception("Error Occurred. File path could not be retrieved from the trigger event")
        return


def copy_to_cfx(
        source_bucket: str, outgoing_file_full_path: str, dest_bucket: str, dest_path: str,
) -> str:
    """Zips a file in a source bucket to a target file.
    Also archives both zip and source file.
    :param source_bucket: source bucket for the content to be zipped
    :param outgoing_file_full_path: outgoing file name path from where the file is to be exported to cfx
    :param dest_bucket: destination bucket for zip file to be placed
    :param dest_path: destination path where zip file to be placed
    :return : name of zip file"""

    zipfile_object = {"Bucket": source_bucket, "Key": outgoing_file_full_path}
    destination_cfx_file = os.path.join(dest_path, os.path.basename(outgoing_file_full_path))

    logger.info(
        " Attempting to Copy %r to s3://%s/%s",
        zipfile_object,
        dest_bucket,
        destination_cfx_file,
    )
    error_occurred = False
    try:
        s3_client.copy_object(
            ACL="bucket-owner-full-control",
            Bucket=dest_bucket,
            Key=destination_cfx_file,
            CopySource=zipfile_object,
        )
        logger.info(
            " Outgoing file '%s' has been successfully delivered to s3://%s/%s",
            outgoing_file_full_path,
            dest_bucket,
            dest_path,
        )
        copy_message = "Please note that the file has been pushed to below CFX path Successfully:"
        logger.info(" Copy file to CFX is SUCCESSFUL")
    except Exception as e:
        logger.exception(f"ERROR OCCURED : %s", e)
        error_occurred = True
        copy_message = (
            f"Error occurred while sending the file to CFX.{os.linesep}"
            f"Error: {e}{os.linesep * 2}"
            f"Please validate below CFX path and resend it.{os.linesep}"
        )
        logger.info(" Copy file to CFX is FAILED")
    return copy_message, error_occurred
