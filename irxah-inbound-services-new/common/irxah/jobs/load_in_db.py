"""
Description : This Python script will copy s3 detail inbound split file to Redshift "stg_accum_dtl" table
  and unload it to header, detail and trailer output files followed by merging all of them.
  This code resides in a glue job "irx_accumhub_load_unload_merge_bci_to_cvs"
  and is called by lambda "irx_accumhub_SubmitGlueJobLambda_bci_to_cvs".
"""

import logging.config
from typing import List, Optional

from .cvs_manual_marking import ManualMarkingException
from ..constants import (
    TRANSMISSION_ID_COLUMN_POSITION,
    TRANSMISSION_ID_ERROR_CODE,
    TRANSMISSION_ID_ERROR_LEVEL,
    SCHEMA_DW,
    TABLE_CLIENT_FILES,
    TABLE_FILE_VALIDATION_RESULT, FAIL, SUCCESS)
from ..database.job_log import JobRepository
from ..file import get_timestamp
from ..glue import get_glue_logger
from ..mix import (
    get_client_files_import_columns,
    load_s3_file_into_table_column_specific,
    record_duplicate_transmission_identifier, log_job_status_end)
from ..model import Job, ClientFile
from ..notifications import account_id
from ..s3 import delete_s3_files, move_file

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)


def load_incoming_file_into_database(
        job: Job, job_repo: JobRepository, client_file: ClientFile, job_name: Optional[str] = "Load File in to Database"
) -> Job:
    """
    Loads inbound file in to database
    FYI : BCI Inbound Error file has a special kind of validation. Typically when we perform the level 1 and level 2
    validation, system generates a file ( containing level 1 and level 2 error)  and this file gets loaded in to
    validation_result table. However, for the BCI inbound error , we treat the detail level validations ( level 1 and level 2)
    just like structural validations , meaning that if any detail level validations fail then REJECT the whole file.
    Also, we are not capturing the detail level validations ( in a validation resul table ) for BCI Error file.
    Hence , for BCI Error file the detail level validation flag should be OFF

    Args:
        arguments (List[str]): Passed from the Lambda
        job (Job): object containing critical information about the job
        job_repo (JobRepository): object containing info for creating and updating the job properties
        client_file (ClientFile): object containing info of client file
        job_name (Optional[str], optional): [description]. Defaults to "Load  File in to Database".

    Returns:
        Job: [description]
    """

    # Earlier below variables were hard coded, now read from database
    em = ""
    s3_accumhub_bucket = job.s3_bucket_name
    iam_arn = client_file.get_redshift_glue_iam_role_arn(account_id)
    s3_inbound_detail_file = job.get_path(job.detail_file_name)
    error_occured = False

    # Create job detail log
    job_repo.create_job_detail(
        job,
        sequence_number=4,
        job_name=job_name,
        input_file_name=s3_inbound_detail_file,
    )
    logger.info(" Job Object Information is : %s", job)

    logger.info("getting column names to load s3 file into table")
    # get column names to where the data from incoming file has to be loaded
    columns = get_client_files_import_columns(SCHEMA_DW, TABLE_CLIENT_FILES, job, )
    if columns is None or columns == "":
        job.job_status = FAIL
        job.error_message = (
            "Unable to retrieve import columns for Accumulator Detail table"
        )
        job_repo.update_job_detail(job)
        return job

    # loading detail file records into accumulator table
    logger.info("attempting to load incoming file data into table")
    try:
        load_s3_file_into_table_column_specific(
            job.secret_name,
            SCHEMA_DW,
            client_file.load_table_name,
            columns,
            s3_accumhub_bucket,
            s3_inbound_detail_file,
            iam_arn,
        )
        logger.info("loading of incoming client file into accumulator table completed")

        # file should not be an error file to load the validation results into file_validation_result
        if not client_file.error_file and client_file.validate_file_detail_columns:
            validation_file = job.get_path(job.validation_file_name)
            logger.info(
                "validate_file_detail_columns is TRUE, loading 'file_validation_result' table started with file name : %s",
                validation_file,
            )
            # loading detail file into file_validation_result table
            load_s3_file_into_table_column_specific(
                job.secret_name,
                SCHEMA_DW,
                TABLE_FILE_VALIDATION_RESULT,
                "job_key,line_number,column_rules_id,validation_result,error_message",
                s3_accumhub_bucket,
                validation_file,
                iam_arn,
            )

            if client_file.column_rule_id_for_transmission_id_validation > 0:
                record_duplicate_transmission_identifier(
                    job.secret_name,
                    client_file.column_rule_id_for_transmission_id_validation,
                    TRANSMISSION_ID_COLUMN_POSITION,
                    TRANSMISSION_ID_ERROR_CODE,
                    TRANSMISSION_ID_ERROR_LEVEL,
                    job.job_key,
                )

            logger.info(" loading of file validations into file_validation_result completed")

    except Exception:
        error_occured = True
        em = "Failed to load the file into database"

    if error_occured:
        job.job_status = FAIL
        job.error_message = em
        job.post_processing_file_location = client_file.extract_folder  # logging the post_processing_file_location of a Failed file here
    else:
        job.job_status = SUCCESS
        job.post_processing_file_location = client_file.archive_folder  # logging the post_processing_file_location of a Success file here

    job.job_end_timestamp = get_timestamp()

    delete_s3_files(s3_accumhub_bucket, f"{job.file_processing_location}/{str(job.job_key)}_")
    logger.info("inbound temp folder file clean up process is completed")

    if job.job_status != SUCCESS:
        job_repo.update_job(job, client_file)
        job_repo.update_job_detail(job)
        logger.debug(job.error_message)
        logger.critical(log_job_status_end(job))
        raise ManualMarkingException(job.error_message)

    # move .txt file from inbound/txtfiles to archive folder if and only if loading process is finished successfully
    logger.info("archiving the .txt file after completion of loading process")
    move_file(job.s3_bucket_name, job.incoming_file, job.s3_bucket_name, job.s3_archive_path)

    logger.info("Job completed - now updating job related information")
    # updating job and job_detail table upon successful processing of incoming file.
    job_repo.update_job(job, client_file)
    job_repo.update_job_detail(job)

    return job
