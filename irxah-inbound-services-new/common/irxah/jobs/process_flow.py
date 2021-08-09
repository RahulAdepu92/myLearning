import logging
import os
from datetime import datetime
from typing import Optional

from .accumhist import process_cvs_acc_history_file
from .custom_to_standard import generate_standard_row
from .load_in_db import load_incoming_file_into_database
from .sla_processor import check_and_notify_file_receipt_after_sla_end
from .split_incoming_file import split_file_handler
from .validate_file_columns import notify_data_error
from .validate_file_structure import validate_file, complete_unprocessed_file
from ..common import tz_now, get_local_midnight_time_from_utc_time, run_glue_job_after_creating_standard_file, \
    get_file_name_pattern
from ..constants import (
    SUCCESS,
    FILE_TIMESTAMP_FORMAT,
    FAIL, INCOMPLETE, UNPROCESSED_FILES)
from ..database import get_custom_to_standard_mappings_for_client
from ..database.client_files import ClientFileRepo
from ..database.file_schedule import FileScheduleRepo
from ..database.job_log import JobRepository
from ..file import get_timestamp
from ..glue import get_glue_logger
from ..mix import get_file_count, is_first_incoming_client_file_of_the_day, log_job_status_end, log_job_status_begin
from ..model import Job, FileScheduleProcessingTypeEnum, ClientFileIdEnum, FileSchedule, ClientFile
from ..notifications import notify_file_structure_error, notify_inbound_file, notify_multiple_file_arrival
from ..s3 import delete_s3_files, split_file, move_file, create_s3_file, is_it_duplicate_file

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)


def process_incoming_file_load_in_database(**kwargs) -> None:
    """This function processes the incoming any incoming client that include CVS (CVS Integrated and Non Integrated, Recon and Acchist),
     BCI(BCI commercial, BCI Error, BCI GP) , Aetna Melton, GPA Melton, THP Integrated and THP Error files depending upon the client_file_id
    obtained from triggered lambda.
    It will validate the file, split it, and load the file into database and sends the notification for any data errors.
    Jobs logs are tracked upon start and end of the function in the Redshift tables.
    """
    logger.info("Job initialisation started..")
    job_repo = JobRepository(**kwargs)
    client_file: ClientFile = ClientFileRepo(**kwargs).get_client_file_from_id(kwargs["CLIENT_FILE_ID"])
    job = job_repo.create_job(client_file, **kwargs)
    logger.info(log_job_status_begin(job))
    # updating the 'job_detail_key' for unzip activity happened previously and catch the exception if any..
    if kwargs["UNZIP_JOB_DETAIL_KEY"] is not None:
        try:
            job_repo.update_unzip_file_job(job.job_key, kwargs["UNZIP_JOB_DETAIL_KEY"])
        except:
            logger.info("Failed to update the job_detail_key for file unzip activity")

    if not client_file.validate_file_structure:
        complete_unprocessed_file(job, job_repo, client_file, "validate_file_structure")
        return None

    logger.info("validate_file_structure is TRUE. Hence, Validating the file..")
    job = validate_file(job, job_repo, client_file)

    if job is not None:
        # get file_schedule info as it is useful for sending SNS alerts below
        file_schedule: FileSchedule = FileScheduleRepo(secret_name=job.secret_name).get_file_schedule_info(
            job.client_file_id, FileScheduleProcessingTypeEnum.INBOUND_TO_AHUB)
        # converting the timestamps retrieved from FileSchedule object
        # prevent 'None type object error' which comes from file_schedule_ids that don't exist in file_schedule table (=23)
        file_schedule.format_timestamps() if file_schedule is not None else None

        # check if we can process multiple files? It is FALSE in PROD alone whereas TRUE in rest all of the environments.
        logger.info("process_multiple_file is: %s", client_file.process_multiple_file)
        if not client_file.process_multiple_file:
            # Split and start processing the file iff it is the first file of the day from particular client.
            # Else, skip the whole process and send an alert notifying about multiple file arrival(Ahub_463)
            # However it is not applicable to Inbound CVS as we get 12 files per day.
            if is_first_incoming_client_file_of_the_day(job):
                logger.info("Arrived file is the first file of the day. Hence, processing it.")
                split_file_and_load_in_database(job, job_repo, client_file, file_schedule)
                return None
            logger.info("Arrived file is NOT the first file of the day. Hence, it will not be processed.")
            job.job_status = INCOMPLETE  # meaning just the second s3 file arrived from client but not loaded into db
            job_repo.update_job(job, client_file)
            hold_multiple_file(job, file_schedule, client_file.processing_notification_sns, )
            return None
        # process multiple files is allowed in lower environments and no special treatment is required.
        split_file_and_load_in_database(job, job_repo, client_file, file_schedule)


def split_file_and_load_in_database(job: Job, job_repo: JobRepository, client_file: ClientFile,
                                    file_schedule: FileSchedule) -> None:
    """
    splits the validated incoming file and loads into the database.
        Args:
        job (Job): Job object
        file_schedule: FileSchedule object
    """
    logger.info("Split incoming file started..")
    # column split and detail record validation is done here.
    job = split_file_handler(job, job_repo, client_file)

    if job is not None:
        # if incoming file is empty (no detail records)
        if not job.continue_processing:
            logger.info(
                "There is nothing to load , split_file_handler found only header and trailer in a file "
            )
            job_repo.update_job(job, client_file)
            # move .txt file from inbound/txtfiles  to inbound/archive folder
            move_file(job.s3_bucket_name, job.incoming_file, job.s3_bucket_name, job.s3_archive_path)
            delete_s3_files(job.s3_bucket_name, f"{job.file_processing_location}/{str(job.job_key)}_")
            logger.info("inbound temp folder file clean up process is completed")
            complete_processed_file(job, client_file, file_schedule)
            logger.info(
                log_job_status_end(
                    job,
                    "There is nothing to load , split_file_handler found only header and trailer in a file",
                )
            )
            return None

        logger.info("Loading incoming Standard file started..")
        logger.info(
            "Incoming file is: %s and process name is: %s ",
            client_file.file_description,
            client_file.process_name,
        )

        # Accumhist files follow special treatment of processing. Hence, it's processing alone is handled separately.
        # TODO: Accumhist file is decommissioned long ago(in 2020) and now it is not in sync with latest modifications(2021)
        #  Fix the runtime errors and proceed if its requirement comes back again in future.
        if job.client_file_id == ClientFileIdEnum.INBOUND_CVS_ACCUMHISTORY:  # handling CVS Accum History file
            job = process_cvs_acc_history_file(job, job_repo, client_file)
        # if table name (job.load_table_name) in which incoming file has to be loaded exists
        else:
            job = load_incoming_file_into_database(job, job_repo, client_file)

        if job and job.job_status == SUCCESS:
            # sends successful inbound processing alert at this stage.
            complete_processed_file(job, client_file, file_schedule)
            logger.info(log_job_status_end(job))
        return None


def complete_processed_file(job: Job, client_file: ClientFile, file_schedule: FileSchedule) -> None:
    """Completes the rest of the Inbound Job
    Here inbound job_key is passed and process must be resulting in a Success.
    Args:
        job (Job): Job object
        file_schedule: FileSchedule object
    """
    # sends notification upon data errors are found in outbound file if is_data_error_alert_active flag is TRUE
    if client_file.is_data_error_alert_active:
        logger.info("Alert is Active - Attempting to check error")
        notify_data_error(job, client_file)

    # sends email notification upon receiving and processing the inbound file inside/ outside sla
    logger.info(
        "Now checking whether the SLA Failure Email need to be sent ( Client File ID =%d)  ",
        job.client_file_id,
    )
    if not check_and_notify_file_receipt_after_sla_end(job):  # inside or outside sla check for file will be done here.
        logger.info("Inbound file received inside the SLA")
        notify_inbound_file(file_schedule, job, client_file)


def hold_multiple_file(job: Job, file_schedule: FileSchedule, sns: str) -> None:
    """
    this function will update job related information even if the file is not a first file of the day.
    Note that the job status will be in INCOMPLETE because we never loaded the second file transactions as
    making the job SUCCESS will create false assumption.
    file will be moved to inbound/multiple folder and necessary action will be taken after notifying business and getting confimration on it.
    """
    # job.incoming_file = inbound/txtfiles/abc.txt
    file_destination_path = ("/".join([job.incoming_file.split("/")[0], UNPROCESSED_FILES,
                                       job.incoming_file.split("/")[-1]]))  # inbound/unprocessed/abc.txt
    move_file(job.s3_bucket_name, job.incoming_file, job.s3_bucket_name, file_destination_path)
    notify_multiple_file_arrival(job, file_schedule, sns)


# conversion of custom file from client (Gila river, client_file_id=16) to standard file

def custom_to_standard(**kwargs) -> None:
    """This function runs the whole custom to standard process.
    It gets the mapping table data for the given client id from custom_to_standard_mapping table
    Reads the custom file, generate standard file based on the mapping information
    """
    logger.info("Job initialization started....")
    job_repo = JobRepository(**kwargs)
    client_file: ClientFile = ClientFileRepo(**kwargs).get_client_file_from_id(kwargs["CLIENT_FILE_ID"])
    job = job_repo.create_job(client_file, **kwargs)
    logger.info(log_job_status_begin(job))
    # updating the 'job_detail_key' for unzip activity happened previously and catch the exception if any..
    if kwargs["UNZIP_JOB_DETAIL_KEY"] is not None:
        try:
            job_repo.update_unzip_file_job(job.job_key, kwargs["UNZIP_JOB_DETAIL_KEY"])
        except:
            logger.info("Failed to update the job_detail_key for file unzip activity")

    if not client_file.validate_file_structure:
        complete_unprocessed_file(job, job_repo, client_file, "validate_file_structure")
        return None

    # To Validate whether the file is duplicate
    if is_it_duplicate_file(
            job.s3_bucket_name,
            client_file.archive_folder,
            job.pre_processing_file_location.split("/")[-1],
    ):
        logger.info(
            " %s file found in the %s folder ",
            job.pre_processing_file_location,
            client_file.archive_folder,
        )
        em = "%s incoming file found in the %s folder, can not process it" % (
            job.pre_processing_file_location,
            client_file.archive_folder,
        )
        complete_custom_to_standard_job(
            job,
            job_repo,
            client_file,
            FAIL,
            file_record_count=0,
            error_message=em,
        )
        logger.critical(log_job_status_end(job))
        return None

    mapping_data = get_custom_to_standard_mappings_for_client(
        job.secret_name, client_file.client_file_id
    )
    if not mapping_data:
        complete_custom_to_standard_job(
            job,
            job_repo,
            client_file,
            FAIL,
            file_record_count=0,
            error_message=f"Mapping Information is not available for the client file id : {client_file.client_file_id}",
        )
        logger.critical(log_job_status_end(job))
        return None
    logger.info(
        "Retrieved %d Mapping Records for the cilent file id: %d ",
        len(mapping_data),
        client_file.client_file_id,
    )

    lines = split_file(job.s3_bucket_name, job.pre_processing_file_location)
    if not lines:
        complete_custom_to_standard_job(
            job,
            job_repo,
            client_file,
            FAIL,
            file_record_count=0,
            error_message=f"There are no lines to process from the file: {job.pre_processing_file_location}",
        )
        logger.critical(log_job_status_end(job))
        return None
    logger.info("Number of Input File Lines: %s", len(lines))

    file_record_count = len(lines) - 2  # Excluding header and trailer
    file_schedule: FileSchedule = FileScheduleRepo(secret_name=job.secret_name).get_file_schedule_info(
        job.client_file_id, FileScheduleProcessingTypeEnum.INBOUND_TO_AHUB)
    file_schedule.format_timestamps() if file_schedule is not None else None

    logger.info("process_multiple_file is: %s", client_file.process_multiple_file)
    if not client_file.process_multiple_file:
        if is_first_incoming_client_file_of_the_day(job):
            logger.info("Arrived file is the first file of the day. Hence, sending file arrival alert for it.")
            standard_file = create_standard_file(job, job_repo, client_file, file_schedule, file_record_count, lines,
                                                 mapping_data)
            if standard_file:
                # runs a glue job 'irxah_process_incoming_file_load_in_database'
                standard_file_name = standard_file.split(os.sep)[-1]
                standard_file_name_pattern = get_file_name_pattern(standard_file_name)
                client_file: ClientFile = ClientFileRepo(secret_name=job.secret_name).get_client_file_from_name_pattern(
                    standard_file_name_pattern)
                run_glue_job_after_creating_standard_file(client_file, job, standard_file)
            return None
        logger.info(
            "Arrived file is NOT the first file of the day. Hence, sending multiple file arrival alert for it")
        # meaning just the second s3 BCBSAZ custom file arrived from client but not generated any standard GRG file
        hold_multiple_file(job, file_schedule, client_file.processing_notification_sns, )
        complete_custom_to_standard_job(
            job, job_repo, client_file, INCOMPLETE, file_record_count=file_record_count,
        )
        return None
    standard_file = create_standard_file(job, job_repo, client_file, file_schedule, file_record_count, lines,
                                         mapping_data)
    if standard_file:
        standard_file_name = standard_file.split(os.sep)[-1]
        standard_file_name_pattern = get_file_name_pattern(standard_file_name)
        client_file: ClientFile = ClientFileRepo(secret_name=job.secret_name).get_client_file_from_name_pattern(
            standard_file_name_pattern)
        run_glue_job_after_creating_standard_file(client_file, job, standard_file)


def create_standard_file(job: Job, job_repo: JobRepository, client_file: ClientFile, file_schedule: FileSchedule,
                         file_record_count: int, lines: list, mapping_data: list) -> str:
    """
    Completes the inbound custom file arrival by sending the SNS alert after checking inside/outside SLA/multiple file arrival.
    """
    # sends email notification upon receiving and processing the missing inbound file outside sla
    standard_file_name_with_path = None
    try:
        # To get the unique batch number for every run if we receive multiple files on the same day
        start_timestamp = get_local_midnight_time_from_utc_time(client_file.file_timezone)
        current_timestamp = datetime.utcnow()

        file_count = get_file_count(
            job.secret_name, job.client_file_id, start_timestamp, current_timestamp
        )
        standard_lines = generate_standard_lines(lines, mapping_data, (file_count + 1))
        now_et = tz_now()
        standard_file_name_with_path = os.path.join(
            client_file.extract_folder,
            f"{client_file.s3_output_file_name_prefix}{now_et.strftime(FILE_TIMESTAMP_FORMAT)}.TXT",
        )
        create_s3_file(
            job.s3_bucket_name,
            standard_file_name_with_path,
            "".join(standard_lines),
        )
        logger.info(
            "custom_to_standard process completed, File has been generated. File Name: %s",
            standard_file_name_with_path,
        )

        complete_custom_to_standard_job(
            job,
            job_repo,
            client_file,
            SUCCESS,
            file_record_count=file_record_count,
            output_file_name=standard_file_name_with_path,
        )
        logger.info(log_job_status_end(job))

    except Exception as ex:
        logger.error("custom_to_standard process failed.")
        logger.exception(ex)
        complete_custom_to_standard_job(
            job, job_repo, client_file, FAIL, error_message=f"Failed to generate the standard File : {str(ex)}"
        )
        logger.critical(log_job_status_end(job))

    logger.info(
        "Now checking whether the SLA Failure Email  need to be sent ( Client File ID =%d)  ",
        job.client_file_id,
    )
    if not check_and_notify_file_receipt_after_sla_end(job):  # inside or outside sla check for file will be done here.
        logger.info("Inbound file received inside the SLA")
        notify_inbound_file(file_schedule, job, client_file)

    return standard_file_name_with_path


def generate_standard_lines(lines: list, mapping_data: list, batch_number: int) -> list:
    """Function to generate the standard lines from custom lines
    :param lines: Custom file lines
    :param mapping_data: maping infrmation
    :param batch_number: Holds the retrieved batch number value
    returns the standard lines
    """
    standard_lines = []
    try:
        standard_header_row = generate_standard_row(
            "Header", "", lines[0], mapping_data, "0", batch_number
        )
    except Exception as e:
        raise Exception("Error While generating standard Header Row :" + str(e))
    standard_lines.append(standard_header_row + os.linesep)
    for linenumber, line in enumerate(lines[1:-1], start=1):
        try:
            standard_lines.append(
                generate_standard_row(
                    "Detail", standard_header_row, line, mapping_data, str(linenumber), batch_number
                )
                + os.linesep
            )
        except Exception as e:
            raise Exception(
                f"Error While generating standard Detail Row. Line number {linenumber} : {str(e)}"
            )
    try:
        standard_footer_row = generate_standard_row(
            "Footer",
            standard_header_row,
            lines[len(lines) - 1],
            mapping_data,
            str(len(lines)),
            batch_number,
        )
    except Exception as e:
        raise Exception("Error While generating standard Footer Row :" + str(e))
    standard_lines.append(standard_footer_row + os.linesep)
    return standard_lines


def complete_custom_to_standard_job(
        job: Job,
        job_repo: JobRepository,
        client_file: ClientFile,
        job_status: str,
        file_record_count: Optional[int] = 0,
        output_file_name: Optional[str] = "",
        error_message: Optional[str] = "",
) -> None:
    """Function to update the Job table and move the files to either error /archive folder
    :param job: a Job object.
    :param client_file: a ClientFile object
    :param job_repo: a Job repository object
    :param job_status: status of the job,
    :param file_record_count: Total number of detail lines from inut file (total lines -2)
    :param output_file_name: Standard  file name generated
    :param error_message: If job_status  FAIL, then the corresponding error message
    :return: None
    """
    job.job_end_timestamp = get_timestamp()
    job.job_status = job_status
    if file_record_count:
        job.file_record_count = file_record_count
    if output_file_name:
        job.output_file_name = output_file_name
        job.post_processing_file_location = client_file.archive_folder
        move_file(
            job.s3_bucket_name,
            job.pre_processing_file_location,
            job.s3_bucket_name,
            os.path.join(
                client_file.archive_folder,
                os.path.basename(job.pre_processing_file_location),
            ),
        )
        logger.info("Moved the file to archive folder.")
    elif error_message:
        job.error_message = error_message
        job.post_processing_file_location = client_file.error_folder
        destination_file_name_with_path = os.path.join(
            client_file.error_folder,
            os.path.basename(job.pre_processing_file_location),
        )
        move_file(
            job.s3_bucket_name,
            job.pre_processing_file_location,
            job.s3_bucket_name,
            destination_file_name_with_path,
        )
        logger.info("Moved the file to error folder.")
        # notify the structure error
        notify_file_structure_error(
            client_file, destination_file_name_with_path, error_message
        )

    else:
        logger.info("File stays in txtfiles folder.")
        job.post_processing_file_location = client_file.extract_folder
    job.input_sender_id = client_file.input_sender_id
    job.input_receiver_id = client_file.input_receiver_id
    job_repo.update_job(job, client_file)
