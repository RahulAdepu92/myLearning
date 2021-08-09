import os
import logging
import functools
import boto3
from typing import Optional, Tuple

from ..file import get_timestamp
from ..mix import log_job_status_end, log_job_status_begin
from ..common import tz_now
from ..database.client_files import ClientFileRepo, ClientFile
from ..database.file_schedule import FileScheduleRepo
from ..database.job_log import JobRepository
from ..model import ClientFileFileTypeEnum, Job
from ..glue import get_glue_logger
from ..s3 import (
    merge_file_parts,
    get_file_record_count,
    delete_s3_files,
)
from ..constants import COMMON_TRAILER_RECORD_COUNT_POSITION

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)

# gives the account number as per environment like for SIT it is "795038802291"
account_id = boto3.client("sts").get_caller_identity()["Account"]

FILE_TIMESTAMP_FORMAT = "%y%m%d%H%M%S"


def client_file_job(export_func):
    """Decorator for a client file-based export job.
    Assumes the job receives a client_file_id or CLIENT_FILE_ID named arg.

    Usage:

    @client_file_job
    def export_outbound_file(client_file_id: int, **kwargs):
        pass
    """

    # Use functool.wraps to "wrap" the inner function so that to the world
    # the value returned by the decorator has the same __doc__ and params
    # and the wrapped function.
    # For more documentation see: https://docs.python.org/3/library/functools.html#wraps
    @functools.wraps(export_func)
    def wrapper(*args, **kwargs):
        """Exports an outbound file for the client_file_id."""
        logger.info("Calling %s with args=%r, kwargs=%r", export_func.__name__, args, kwargs)
        # tries to get the client file id several ways:
        # 1. As a positional parameter, assumed to be the first
        #
        client_file_id = 0

        # I don't know if we can guarantee this. Maybe kwargs are better
        # if len(args) > 0:
        #     client_file_id = args[0]

        # 2. client_file_id in kwargs
        # 3. CLIENT_FILE_ID in kwargs, likely coming from Glue job params or env vars
        client_file_id = kwargs.get("client_file_id", client_file_id)
        client_file_id = kwargs.get("CLIENT_FILE_ID", client_file_id)
        # since the python convention is snake_case, let's add client_file_id
        # into kwargs, in case it came from --CLIENT_FILE_ID
        kwargs["client_file_id"] = client_file_id

        if client_file_id == 0:
            logger.error(
                "Cannot find a client file id in arguments passed to %s: args=%r; %r",
                export_func.__name__,
                args,
                kwargs,
            )
            return

        logger.info("Job starting for %s: Export file %s", export_func.__name__, client_file_id)
        logger.debug("args=%r, kwargs=%r", args, kwargs)

        client_file: ClientFile = ClientFileRepo(**kwargs).get_client_file_from_id(client_file_id)
        if client_file is None:
            logger.error("Could not find a client file for id %s", client_file_id)
            return

        # TODO: this should be in the function - the wrapper shouldn't care about direction
        if client_file.file_type != ClientFileFileTypeEnum.OUTBOUND:
            logger.error(
                "Client file type is %s not %s",
                client_file.file_type,
                ClientFileFileTypeEnum.OUTBOUND,
            )
            return

        logger.info("Will handle %s file %s", client_file.file_type, client_file.client_file_id)

        job_repo = JobRepository(**kwargs)
        job = job_repo.create_job(client_file, **kwargs)
        logger.info(log_job_status_begin(job))

        kwargs["job"] = job
        kwargs["client_file"] = client_file

        logger.info(
            "Running %s with job=%s, client_file=%s",
            export_func.__name__,
            job.job_key,
            client_file.client_file_id,
        )

        ########################################################################
        # Execute Function
        ########################################################################
        try:
            # func return is supposed to be the file name
            (file, record_count) = export_func(*args, **kwargs)
            file_name = os.path.basename(file)
            logger.info(
                "Job for %s produced file %s with %d records.",
                export_func.__name__,
                file_name,
                record_count,
            )
            job.set_outbound_success(file_name, record_count)
        except Exception as ex:
            logger.exception(ex)
            logger.info("Client file: %r", client_file.__dict__)
            logger.info("Job: %r", job.__dict__)
            job.set_failure(f"CLient file {client_file_id} failed.")

        try:
            job.job_end_timestamp = get_timestamp()
            job_repo.update_job(job, client_file)
        except Exception as ex:
            logger.exception("Error while updating the job status.. %s", ex)
        finally:
            # set the is_running flag in file_schedule to false regardless of success or failure execution(AHUB-735)
            # this exception is caught only when the glue job enters into the code and fails in middle.
            logger.info("Attempting to update glue file schedule to next execution")
            FileScheduleRepo(secret_name=job.secret_name).update_file_schedule_to_next_execution_glue_curry(**kwargs)()

        if job.is_failing():
            return

        logger.info(log_job_status_end(job))

    return wrapper


@client_file_job
def export_client_file(
    client_file_id: int, job: Job = None, client_file: Optional[ClientFile] = None, **kwargs
) -> Tuple[str, int]:
    """
    Exports Redshift "stg_accum_dtl" table to header, detail
    and trailer output files followed by merging all of them and produce approved and rejected records files.
    :return: the outbound file name and the number of records exported to it
    """
    # Earlier below variables were hard coded, now read from database
    client_file_repo = ClientFileRepo(**kwargs)
    if client_file is None:
        client_file: ClientFile = client_file_repo.get_client_file_from_id(client_file_id)

    # TODO: job should not be None

    # Steps:
    # 1. Export header to header file
    # 2. Export details to detail file
    # 3. Export trailer to trailer file
    # 4. Merge the 3 files

    # for all files:
    s3_bucket_name = kwargs["S3_FILE_BUCKET"]

    file_fragments = client_file_repo.export_fragments(
        client_file=client_file, s3_out_bucket=s3_bucket_name, job_key=job.job_key
    )

    if len(file_fragments) == 0:
        logger.warning("No files produced for client_file_id=%s this run.", client_file_id)
        return "", 0

    (HEADER_FILE, DETAIL_FILE, TRAILER_FILE) = (0, 1, 2)
    logger.info(
        "Export file fragments: header=%s, detail=%s, trailer=%s.",
        file_fragments[HEADER_FILE],
        file_fragments[DETAIL_FILE],
        file_fragments[TRAILER_FILE],
    )

    record_count = get_file_record_count(
        s3_bucket_name,
        file_fragments[TRAILER_FILE],
        COMMON_TRAILER_RECORD_COUNT_POSITION,
        True,
    )
    logger.info("Detail file has total record count of : %s", record_count)

    now_et = tz_now()
    final_file = os.path.join(
        client_file.s3_merge_output_path,
        f"{client_file.s3_output_file_name_prefix}{now_et.strftime(FILE_TIMESTAMP_FORMAT)}.TXT",
    )

    # merges the 3 fragments into the final file (merge also deletes the files)
    merge_file_parts(
        s3_bucket=s3_bucket_name,
        outbound_path=final_file,
        detail=file_fragments[DETAIL_FILE],
        header=file_fragments[HEADER_FILE],
        trailer=file_fragments[TRAILER_FILE],
    )

    logger.info(
        "Merged header=%s, detail=%s, trailer=%s to %s",
        file_fragments[HEADER_FILE],
        file_fragments[DETAIL_FILE],
        file_fragments[TRAILER_FILE],
        final_file,
    )

    # TODO: delete file parts after merge
    if kwargs.get("DO_NOT_DELETE_TEMP_FILES", "no") == "yes":
        logger.warning("Leaving file parts around because I was asked to: %r", file_fragments)
    else:
        deleted_files = delete_s3_files(s3_bucket_name, *file_fragments)
        logger.info("Deleted merged parts: %r", deleted_files)

    return (final_file, record_count)
