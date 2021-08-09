import datetime
import logging
import re
from typing import List

import boto3
import pytz
# while running lambdas we encounter import error as awsglue is inbuilt module for Glue but not for lambda
try:
    from awsglue.utils import getResolvedOptions
except ImportError:
    pass

from ..common import convert_local_timestamp_to_utc_timestamp, run_glue_job
from ..constants import NA, DEFAULT_TIMEZONE
from ..database.client_files import ClientFileRepo
from ..mix import mark_unavailable_cvs_files_status_to_available
from ..glue import get_glue_logger
from ..model import ClientFileIdEnum, ClientFileFileTypeEnum, ClientFile

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)
glue = boto3.client("glue")


def manual_mark(arguments: List[str]):
    """
    This function will manually mark jobs in the job table which fulfills the following conditions: status = 'Success',
    file_status = 'Not Available', outbound_file_generation_complete = false, within the specified date range, matches
    the cvs file list, and finally matches the client file id. By marking, it will update the file_status to 'Available'
    where the daily file generation from CVS table glue job will be able generate a file from the matching jobs. Either
    a date range must be provided or the cvs file names list, not both. If a glue job is specified, it will run after
    updating the jobs.

    To ignore an argument, such as no date, use NA..

    client_file_ids:
    this glue variable is defaulted to all outbound cfds: 4, 13, 15, 17, 20, 21 for BCI, ATN, GPA, GR, THP respectively
    You can change these externally to take control on exporting the specific client files
    For example, client_file_ids = 13, 15 will send only Melton related transactions

    :param arguments: This is the system arguments, or environment variables manually set when executing glub job (manual).
    """
    args = getResolvedOptions(
        arguments,
        [
            "CLIENT_FILE_IDS",
            "SECRET_NAME",
            "START_TIME_IN_24_HOUR_FORMAT_LOCAL",
            "END_TIME_IN_24_HOUR_FORMAT_LOCAL",
            "LIST_OF_CVS_FILE_NAMES",
            "GLUE_JOB_TO_RUN",
        ],
    )
    logger.info("manual_mark : BEGIN")
    logger.info("manual_mark : ARGS %s", args)
    client_file_ids = args["CLIENT_FILE_IDS"]
    secret_name = args["SECRET_NAME"]
    start_time_local = args["START_TIME_IN_24_HOUR_FORMAT_LOCAL"]
    end_time_local = args["END_TIME_IN_24_HOUR_FORMAT_LOCAL"]
    list_of_cvs_file_names = args["LIST_OF_CVS_FILE_NAMES"]
    glue_job_to_run = args["GLUE_JOB_TO_RUN"]

    # get ClientFile object for client_file_id = 3
    # it is constant because, we supply inbound CVS files in file_list and try to make them AVAILABLE
    client_file: ClientFile = ClientFileRepo(secret_name=secret_name).get_client_file_from_id(ClientFileIdEnum.INBOUND_CVS_INTEGRATED.value) # Accessing enum member using value

    em = ""

    if glue_job_to_run == NA:
        glue_job_to_run = ""
    if list_of_cvs_file_names == NA:
        list_of_cvs_file_names = ""
    if start_time_local == NA:
        start_time_local = ""
    if end_time_local == NA:
        end_time_local = ""
    if client_file_ids == NA:
        client_file_ids = ""

    try:
        ### validate input file names or timestamp ###
        # No timestamp or filenames or client file ids provided
        if not start_time_local and not end_time_local and not list_of_cvs_file_names:
            em = "ERROR: Provide at least the date range or list of CVS file names"
            raise ManualMarkingException(em)
        # Only one timestamp provided
        if (start_time_local and not end_time_local) or (not start_time_local and end_time_local):
            em = "ERROR: Provide BOTH start time and end time arguments with either valid values, or NA"
            raise ManualMarkingException(em)
        # Both timestamp and filenames provided
        if start_time_local and end_time_local and list_of_cvs_file_names:
            em = "ERROR: Provide only date range or CVS file names, not both"
            raise ManualMarkingException(em)

        ### validate input glue job name ###
        if glue_job_to_run:
            try:
                glue.get_job(JobName=glue_job_to_run)
            except:
                em = f"ERROR: Glue job {glue_job_to_run} does not exist."
                raise ManualMarkingException(em)

        ### validate input client file ids ###
        if client_file_ids:  # if you provide list of client_file_ids
            # get list of outbound client file ids from db
            outbound_client_file_ids = ClientFileRepo(secret_name=secret_name).get_client_file_ids(ClientFileFileTypeEnum.OUTBOUND)  # returns list of tuples
            int_of_client_file_ids = [item for i in outbound_client_file_ids
                                      for item in i]  # convert list of tuples to list
            list_of_outbound_client_file_ids = [str(x) for x in int_of_client_file_ids]  # turns into ['4', '5', '13']
            logger.info(f"outbound client_file_ids are : %s", list_of_outbound_client_file_ids)

            # client_file_ids on GLUE variable will be of string type, ex: "4, 5, 13".
            # So converting them to list type ['4', '5', '13']
            input_client_file_ids = client_file_ids.split(",")

            for client_file_id in input_client_file_ids:
                client_file_id = client_file_id.strip()
                if client_file_id not in list_of_outbound_client_file_ids:
                    em = (
                        f"ERROR: One of the provided client_file_id = {client_file_id} is not of OUTBOUND file_type")
                    raise ManualMarkingException(em)

        try:
            ### validate input timestamp ###
            start_time_utc = None
            end_time_utc = None
            # Convert local times to utc for query
            if start_time_local and end_time_local:

                start_local_no_tz = datetime.datetime.strptime(
                    start_time_local, "%m/%d/%Y %H:%M:%S"
                )
                start_time_utc = convert_local_timestamp_to_utc_timestamp(
                    start_local_no_tz, DEFAULT_TIMEZONE
                )

                end_local_no_tz = datetime.datetime.strptime(end_time_local, "%m/%d/%Y %H:%M:%S")
                end_time_utc = convert_local_timestamp_to_utc_timestamp(
                    end_local_no_tz, DEFAULT_TIMEZONE
                )

                if not start_time_utc <= end_time_utc:
                    em = "ERROR: Start Time must be before End Time!"
                    raise ManualMarkingException(em)

                logger.info(
                    "Converted ENV Variable timestamps [start=%s, end=%s]. "
                    "Start time (UTC) = %s, End time (UTC) = %s",
                    start_time_local,
                    end_time_local,
                    start_time_utc,
                    end_time_utc,
                )

            ### validate input file names ###
            list_of_cvs_files = ""
            number_of_input_files = 0
            if list_of_cvs_file_names:  # if you provide list of file_names
                input_file_names = list_of_cvs_file_names.split(",")
                number_of_input_files = len(input_file_names)
                for cvs_file in input_file_names:
                    file_name = cvs_file.strip()

                    # Needs to end with .txt extension
                    if not file_name.lower().endswith(".txt"):
                        em = f"ERROR: CVS files must end with .txt! Found issue with '{file_name}'"
                        raise ManualMarkingException(em)

                    # Cannot contain special chars or spaces. Allowed characters alphanumeric, -, _, .
                    regex = re.compile("^[\w\-.]+$")
                    if regex.match(file_name.replace(".", "")) is None:
                        em = (
                            "ERROR: CVS files must not contain special characters or spaces, Allowed characters are "
                            f"(alphanumerics, -, _, .). Found issue with '{file_name}'"
                        )
                        raise ManualMarkingException(em)

                    # Must start with with correct prefix
                    # Utilizing FILE_TIMESTAMP_FORMAT constant. This constant should match client_files table Timestamp format..
                    prefix_end_index = 0
                    for c in file_name:
                        if c.isnumeric():
                            break
                        prefix_end_index += 1

                    client_filename_prefix = file_name[0:prefix_end_index]

                    if not client_file.name_pattern.startswith(client_filename_prefix):
                        em = (
                            f"ERROR: Given CVS File name '{file_name}' does not have the right prefix which is defined as "
                            f"'{client_filename_prefix}'"
                        )
                        raise ManualMarkingException(em)

                    list_of_cvs_files = f"{list_of_cvs_files}'{file_name}',"
                list_of_cvs_files = list_of_cvs_files.rstrip(",")

            # Mark the CVS files status to AVAILABLE, so as to make the outbound transactions eligible for export
            logger.info("Attempting to mark the CVS files %s status to AVAILABLE", list_of_cvs_files)
            em = mark_unavailable_cvs_files_status_to_available(
                secret_name,
                start_time_utc,
                end_time_utc,
                list_of_cvs_files,
                number_of_input_files,
            )

            if em:
                raise ManualMarkingException(em)

            logger.info(
                "Updated job records that matched provided ENV Variables. File_status for these jobs have been changed to 'Available'."
            )

            # running the export glue job
            if glue_job_to_run and client_file_ids:
                # Export glue job runs at the moment if and only if glue name <> NA and outbound client_file_ids <> NA.
                # Otherwise just files with UNAVAILABLE will be turned to AVAILABLE and will be picked up later
                # by nearby 30 mins schedule job (lambda).
                input_client_file_ids = client_file_ids.split(",")
                for client_file_id in input_client_file_ids:
                    client_file_id = client_file_id.strip()
                    logger.info(f"Executing glue job for client_file_id %s:", client_file_id)
                    glue_job_arguments = {
                        "--CLIENT_FILE_ID": str(client_file_id)
                    }
                    run_glue_job(glue_job_to_run, glue_job_arguments)
            else:
                logger.info("Either GLUE_JOB_TO_RUN or CLIENT_FILE_IDS were not provided. "
                            "Manual marking stops here and the automatic jobs will export the outbound files at their regular schedule")

        except ValueError:
            logger.exception(
                "ERROR: Environment variables for local time are in the incorrect format. "
                "Please make sure it is mm/dd/yyyy hh24:mi:ss such as 05/06/2020 23:30:00 which is 05/06/2020 11:30:00 PM."
                "User input is %s and %s", start_time_local, end_time_local
            )
            start_time_utc = ""
            end_time_utc = ""
            if not start_time_utc:
                em = (
                    "ERROR: START_TIME_IN_24_HOUR_FORMAT_LOCAL is in the "
                    "incorrect format. Please make sure it is mm/dd/yyyy hh24:mi:ss such as 05/06/2020 23:30:00 which is 05/06/2020 11:30:00 PM. "
                    f"User input is '{start_time_local}'"
                )
                raise ManualMarkingException(em)
            elif not end_time_utc:
                em = (
                    "ERROR: END_TIME_IN_24_HOUR_FORMAT_LOCAL is in the "
                    "incorrect format. Please make sure it is mm/dd/yyyy hh24:mi:ss such as 05/06/2020 23:30:00 which is 05/06/2020 11:30:00 PM "
                    f"User input is '{end_time_local}'"
                )
                raise ManualMarkingException(em)

        except pytz.UnknownTimeZoneError:
            logger.exception("Timezone is incorrect.")
            raise ManualMarkingException("ERROR: Timezone is incorrect for client")
    except ManualMarkingException as e:
        logger.exception("ManualMarkingException.. %s", e)
        raise ManualMarkingException(e)


class ManualMarkingException(Exception):
    """ custom exception for manual mark"""

    pass
