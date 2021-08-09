import io
import logging
import logging.config
import os
import zipfile
import boto3

from datetime import datetime

from ..common import convert_utc_to_local, get_file_name_pattern, run_glue_job
from ..database.client_files import ClientFileRepo
from ..constants import FAIL, DEFAULT_TIMEZONE, SUCCESS, UNKNOWN_FILES
from ..database.file_schedule import FileScheduleRepo
from ..database.job_log import JobRepository
from ..glue import get_glue_logger
from ..model import FileScheduleProcessingTypeEnum, ClientFileFileTypeEnum, ClientFile, FileSchedule
from ..notifications import notify_unknown_inbound_file_arrival, notify_inactive_inbound_file, \
    notify_inbound_file_extraction_error
from ..s3 import get_file_from_s3_trigger, move_file
from ..constants import TABLE_JOB_DETAIL, SCHEMA_DW

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")


def unzip_and_process_incoming_file_handler(event, context) -> None:
    """In a nutshell, this lambda function is a common watcher for the inbound source files of all the clients
    which process them by unzipping the zip files and move the extracted files to the corresponding staging folder based
    on client and archive the inbound file. In an even of unsuccesful processing, the file moves to error folder
    and sends an email notification to the receipents for the error encountered.
    :param event: lambda event
    :param context: lambda context
    :return: None
    """
    extracted_file = None
    try:
        logger.info("Process started. finding the file path of trigger event: %r", event)
        # incoming_file = inbound/srcfiles/filename.txt
        s3_bucket_name, incoming_file = get_file_from_s3_trigger(event)
        # incoming file is  such as  inbound/srcfiles/ACCDLYINT_TST_BCIINRX_200406190343430.zip
        logger.info("incoming_file : %s", incoming_file)

        # retrieve existing environment variables for lambda function
        secret_name = os.environ["SECRET_NAME"]
        environment = os.environ["ENVIRONMENT"]
        sns_name = os.environ["FILE_EXTRACTION_ERROR_NOTIFICATION_SNS_ARN"]

        # initialising the local time here becasue as soon as the inbound file arrives s3 event will be triggered
        current_utc_time = datetime.utcnow()
        current_local_time = convert_utc_to_local(current_utc_time, DEFAULT_TIMEZONE)

        # Pulls the name out of the incoming file for example the incoming file is : inbound/srcfiles/ACCDLYINT_TST_BCIINRX_200406190343430.zip

        # the incoming file name below would reteurn just the  ACCDLYINT_TST_BCIINRX_200406190343430.zip
        incoming_file_name = incoming_file.split(os.sep)[-1]
        incoming_file_path = os.path.dirname(incoming_file)  # inbound/srcfiles
        logger.info("incoming_file_name : %s", incoming_file_name)

        incoming_file_name_pattern = get_file_name_pattern(incoming_file_name)

        client_file: ClientFile = ClientFileRepo(secret_name=secret_name).get_client_file_from_name_pattern(
            incoming_file_name_pattern)
        logger.info("client_file : %s", client_file)

        if client_file is not None and client_file.file_type == ClientFileFileTypeEnum.INBOUND:
            # restricting the entry of files that match inbound file name patterns
            file_schedule: FileSchedule = FileScheduleRepo(secret_name=secret_name).get_file_schedule_info(
                client_file.client_file_id, FileScheduleProcessingTypeEnum.INBOUND_TO_AHUB)
            file_schedule.format_timestamps() if file_schedule is not None else None
            logger.info(" File Schedule Info : %s", file_schedule)
            # if 'is_active' is FALSE, do nothing. Otherwise it will move forward to check for zip file, send to cfx etc.,
            if client_file.is_active:
                logger.info(
                    "Incoming file name '%s' matches with file name pattern '%s' in client files table and 'is_active' alert is set to '%s'. "
                    "Hence, it will move forward to check for file extension and is moved to %s folder.",
                    incoming_file_name,
                    client_file.name_pattern,
                    client_file.is_active,
                    client_file.extract_folder,
                )

                # Now where the file needs to be extracted is determined as <s3 bucket>\<extract folder>\incoming_file_name
                # <extract folder>\incoming_file_name frees up the hard coding the client or folder name.
                customTuple = (
                    client_file.extract_folder,
                    incoming_file_name,
                )

                incoming_file_with_client = os.sep.join(customTuple)

                logger.info(
                    "From Try Block : s3_bucket_name = %s, incoming_file = %s, incoming_file_with_client = %s",
                    s3_bucket_name,
                    incoming_file,
                    incoming_file_with_client,
                )

                status = ""
                error_message = ""

                # inserting job_key and job_detail_key for 'unzip process' in job_detail table
                job_repo = JobRepository(
                    secret_name=secret_name,
                    tables_to_be_locked=[f"{SCHEMA_DW}.{TABLE_JOB_DETAIL}"],
                )
                job_detail_key = job_repo.insert_unzip_file_job_detail(incoming_file_name, status)

                try:
                    extracted_file = unzip_file(
                        s3_bucket_name,
                        incoming_file,
                        incoming_file_with_client,
                        client_file.extract_folder,
                        client_file.error_folder,
                        client_file.archive_folder,
                    )
                    logger.info("File Unzipped Successfully")
                    status = SUCCESS
                except Exception as e:
                    logger.info(
                        "File %s / %s was not unzipped successfully. It will not be processed further",
                        s3_bucket_name,
                        incoming_file_with_client,
                    )
                    logging.exception(e)
                    # Notification
                    logger.info("sending email notification for file extraction error")
                    incoming_file = f"{s3_bucket_name}/{incoming_file}"
                    res = notify_inbound_file_extraction_error(client_file, file_schedule, incoming_file,
                                                               current_local_time, str(e))
                    logger.info(res)
                    status = FAIL
                    error_message = f"{str(e)}."

                finally:
                    job_repo.update_unzip_file_job_detail(job_detail_key, extracted_file, status, error_message)
                    logger.info(f"update of {job_detail_key} for unzip file activity is completed")

                incoming_file_name_pattern = get_file_name_pattern(incoming_file_name)
                logger.info(f"incoming_file_name_pattern: %s", incoming_file_name_pattern)

                if extracted_file:  # if file is extracted successfully to txtfiles folder, then run process glue job
                    client_file: ClientFile = ClientFileRepo(secret_name=secret_name).get_client_file_from_name_pattern(
                        incoming_file_name_pattern)
                    logger.info(f"client_file : %s", client_file)

                    # logging to job and job_details table
                    glue_job_name = client_file.glue_job_name
                    logger.info("Calling Glue Job:: %s ", glue_job_name)

                    glue_job_arguments = {
                        # preparing arguments for glue job 'irxah_process_incoming_file_load_in_database'
                        # only string arguments are to be passed
                        "--SECRET_NAME": secret_name,
                        "--S3_FILE": extracted_file,
                        "--S3_FILE_BUCKET": s3_bucket_name,
                        "--UNZIP_JOB_DETAIL_KEY": str(job_detail_key),
                        # unique id of client file (retrieved from db)
                        "--CLIENT_FILE_ID": str(client_file.client_file_id),
                    }

                    run_glue_job(glue_job_name, glue_job_arguments)

            else:
                logger.info(
                    "Incoming file name '%s' matches with file name pattern '%s' in client files table. But 'is_active' flag is set to '%s'. "
                    "Hence, file processing will be on hold and it remains as is in '%s' until you push it manually after setting 'is_active' flag to TRUE. ",
                    incoming_file_name,
                    client_file.name_pattern,
                    client_file.is_active,
                    incoming_file_path
                )
                notify_inactive_inbound_file(client_file, file_schedule, incoming_file_name, incoming_file_path,
                                             current_local_time)

        # any file that doesnt qualify the above criteria will remain in 'inbound/srcfiles'.
        # So, this unprocessed file has to be moved as is to 'inbound/unknown' folder with help of below if condition.
        if client_file is None:
            # path_folders => ['inbound', 'srcfiles']
            path_folders = (os.path.dirname(incoming_file)).split(os.sep)
            src_folder = path_folders[1]  # srcfiles (or) history
            # sets archive_file_name_with_path to inbound/unknown/abc.txt
            archive_path = (os.sep.join(path_folders).replace(src_folder, UNKNOWN_FILES))
            archive_file_name_with_path = (
                    os.sep.join(path_folders).replace(src_folder, UNKNOWN_FILES)
                    + os.sep
                    + os.path.basename(incoming_file)
            )

            logger.info(
                "Incoming file '%s' name doesn't match with any of the expected Inbound file name pattern in db. "
                "Hence, file processing will be skipped and moved directly to 'inbound/unknown' folder.",
                incoming_file_name,
            )
            move_file(s3_bucket_name, incoming_file, s3_bucket_name, archive_file_name_with_path)
            notify_unknown_inbound_file_arrival(environment, sns_name, incoming_file_name, current_local_time,
                                                archive_path)

    except Exception as e:
        logging.exception(e)
        logging.exception("File path could not be retrieved from the S3 trigger event: %r", event)
        return


def unzip_file(
        bucket_name: str,
        incoming_file: str,
        incoming_file_with_client: str,
        extract_folder: str,
        error_folder: str,
        archive_folder: str,
) -> str:
    """
    This function does the following tasks

    1. This processes the incoming file from inbound path by unzipping and loads the content
    in the 'txtfiles' folder upon successful unzipping.
    Ex: For example, "incoming/bci/srcfiles/GOOD.ZIP" would be have its GOOD.TXT extracted to "incoming/bci/txtfiles/GOOD.TXT"

    2. Upon successful unzipping of the flle, It removes the source file after moving it from srcfiles to archive
       Ex: For example, "incoming/bci/srcfiles/GOOD.ZIP" would be have its GOOD.TXT extracted to "incoming/bci/archive/GOOD.TXT"

    3. In unsuccessful attempt of unzipping due to any reason , It moves the file to 'error' folder
    Ex: an invalid "incoming/cvs/srcfiles/BAD.ZIP" would be moved to "incoming/cvs/error/BAD.ZIP"

    4. If the souce file is .txt(.TXT) file , it skips the unzipping and moves it to txtfiles
    Ex: For example, "incoming/bci/srcfiles/good.txt"  to "incoming/bci/txtfiles/good.txt"

    :param bucket_name: s3 bucket name.
    :param incoming_file: inbound file path  like inbond/srcfiles
    :param incoming_file_with_client: inbound file path with client name like inbond/bci/srcfiles
    :return: The boolean to indicate unzip successful or not
    """
    unzipped_text_file = None

    # derivation of various file paths

    incoming_file_name = os.path.basename(incoming_file)

    inbound_src_file_path = os.path.dirname(incoming_file) + os.sep  # inbound/srcfiles/

    path_folders = (os.path.dirname(incoming_file_with_client)).split(os.sep)
    logger.info(
        "incoming_file=%s, incoming zip file name =%s, inbound_src_file_path=%s, path_folders=%s ",
        incoming_file,
        incoming_file_name,
        inbound_src_file_path,
        path_folders,
    )

    # src_folder = path_folders[2]  # srcfiles (or) history

    # Ensure that the path ends with os.sep ( the front-slash) at the end
    txt_file_path = os.path.join(extract_folder, "")
    archive_file_path = os.path.join(archive_folder, "")
    error_file_path = os.path.join(error_folder, "")

    logging.info(
        "incoming_file_name is : %s \n inbound_src_file_path is : %s"
        "\n txt_file_path is : %s \n archive_file_path is : %s \n error_file_path is : %s",
        incoming_file_name,
        inbound_src_file_path,
        txt_file_path,
        archive_file_path,
        error_file_path,
    )

    try:
        logger.info(
            "Incoming source file for processing  %s%s",
            inbound_src_file_path,
            incoming_file_name,
        )
        # handling .txt(.TXT) files
        if incoming_file_name.lower().endswith(".txt"):
            logger.info(
                " Copying from s3://%s/%s%s to s3://%s/%s%s",
                bucket_name,
                inbound_src_file_path,
                incoming_file_name,
                bucket_name,
                txt_file_path,
                incoming_file_name,
            )
            s3_resource.Object(bucket_name, txt_file_path + incoming_file_name).copy_from(
                CopySource={
                    "Bucket": bucket_name,
                    "Key": inbound_src_file_path + incoming_file_name,
                }
            )

            logger.info(" Copy SUCCESSFUL")
            extracted_file = txt_file_path + incoming_file_name
            # returning file should contain extract path name in it else the subsequent glue job will throw error
            return extracted_file

        else:
            put_object = None
            # throw error if not .zip(ZIP)
            if not incoming_file_name.lower().endswith(".zip"):
                raise AccumHubArchiveException(
                    "The inbound source file must be either .ZIP(.zip) or .TXT(.txt)"
                )

            # unzip the zip file
            obj = s3_client.get_object(
                Bucket=bucket_name, Key=inbound_src_file_path + incoming_file_name
            )

            with io.BytesIO(obj["Body"].read()) as tf:
                # rewind the file
                tf.seek(0)

                # Read the file as a zipfile and process the members
                with zipfile.ZipFile(tf, mode="r") as zipf:
                    zip_content_list = zipf.namelist()
                    # validate the txt file in the zip before extraction
                    txt_file_count = 0
                    zipped_text_file = None
                    for zipped_file in zip_content_list:
                        logger.info("zip content ..%s ", str(zipped_file))
                        if str(zipped_file).lower().endswith(".txt"):
                            txt_file_count += 1
                            zipped_text_file = zipped_file

                    if txt_file_count > 1:
                        raise AccumHubArchiveException(
                            "More than one text file present in archive. Expected only one"
                        )
                    if txt_file_count < 1:
                        raise AccumHubArchiveException("No text file present in archive")

                    # reading the zip content which is of .txt and writing to folder 'txtfiles'
                    if zipped_text_file is not None:
                        unzipped_text_file = txt_file_path + str(zipped_text_file)
                        logging.info(" fileName: %s", unzipped_text_file)
                        logging.info(
                            "%s is being extracted and its content is copying to %s ",
                            zipped_text_file,
                            unzipped_text_file,
                        )
                        put_object = s3_client.put_object(
                            Bucket=bucket_name,
                            Key=unzipped_text_file,
                            Body=zipf.read(zipped_text_file),
                        )
                        logging.info(
                            "The zip content  %s has been extracted successfully",
                            unzipped_text_file,
                        )

            # Move zip file after unzip to archive
            if put_object is not None:
                logging.info(
                    "The inbound source file %s%s is moving to archive folder",
                    inbound_src_file_path,
                    incoming_file_name,
                )
                s3_resource.Object(
                    bucket_name, archive_file_path + incoming_file_name
                ).copy_from(
                    CopySource={
                        "Bucket": bucket_name,
                        "Key": inbound_src_file_path + incoming_file_name,
                    }
                )
                logging.info(
                    "The inbound source file %s%s has been moved to archive folder successfully",
                    inbound_src_file_path,
                    incoming_file_name,
                )

    except AccumHubArchiveException as archive_error:
        unzipped_text_file = None
        logging.exception("AccumHubArchiveException.. %s ", archive_error)
        raise

    except Exception as unknown_error:
        unzipped_text_file = None
        logging.exception("Unknown error encountered unzipping file: %s", unknown_error)
        raise

    finally:
        if incoming_file_name.lower().endswith(".zip") and unzipped_text_file is None:
            # moving the file to error folder
            logging.info(
                "The inbound source file %s%s is moving  to error folder",
                inbound_src_file_path,
                incoming_file_name,
            )
            # copy to archive folder
            s3_resource.Object(
                bucket_name, error_file_path + incoming_file_name.upper()
            ).copy_from(
                CopySource={
                    "Bucket": bucket_name,
                    "Key": inbound_src_file_path + incoming_file_name,
                }
            )
            logging.info(
                "The inbound source file %s%s has been moved to error folder successfully",
                inbound_src_file_path,
                incoming_file_name,
            )

        # deleting the incoming zip upon moving to Archive or Error folder
        logging.info(
            "The inbound source file %s%s is being deleted",
            inbound_src_file_path,
            incoming_file_name,
        )
        _ = s3_client.delete_object(
            Bucket=bucket_name, Key=inbound_src_file_path + incoming_file_name
        )
        logging.info(
            "The inbound source file %s%s has been deleted successfully",
            inbound_src_file_path,
            incoming_file_name,
        )
    # returning the extracted (unzipped file name) along with extract path
    return unzipped_text_file


class AccumHubArchiveException(Exception):
    """ custom exception for zipping validations"""

    pass
