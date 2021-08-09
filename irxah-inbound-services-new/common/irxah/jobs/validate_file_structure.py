import logging
from typing import Optional

import boto3

from .validate_file_columns import ColumnRule, get_validations, get_pos
from ..constants import FAIL, SUCCESS, INCOMPLETE, COMMON_TRAILER_RECORD_COUNT_POSITION, UNPROCESSED_FILES
from ..database.job_log import JobRepository
from ..file import get_timestamp
from ..glue import get_glue_logger
from ..mix import log_job_status_end
from ..model import FileColumns, FileStructure, Job, ClientFile
from ..notifications import notify_file_structure_error, notify_validation_flag_off
from ..s3 import is_it_duplicate_file, move_file, get_positions, get_file_record_count

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)

# Constant Definition
IDENTIFIER_MISSING = 3  # Earlier it was 3 , so it has been kept like this !
ERROR_FILE_READING_POSITIONS = ["202:204", "270:271", "271:274"]
REQUIRED = "REQUIRED"
EQUALTO = "EQUALTO"

s3client = boto3.client("s3")
s3resource = boto3.resource("s3")


def validate_file(job: Job, job_repo: JobRepository, client_file: ClientFile,
                  job_name: Optional[str] = "Validate File Structure", ) -> Job:
    """Does file validation by checking if file is a duplicate. Basic header, trailer, and detail validation.
    The parameter Job is passed in to keep track of the existing details that will be used to populate the 
    record. Takes in the job as an argument to grab pre-existing values for job logging in Redshift.
    :param arguments: a list of variables that will be resolved to retrieve environment variables
    :param job: a Job object, from irxah/glue.py. Needs at least the following preconfigured: 
    *job_key
    :param job_name: User specified name for the job. 
    """

    archive_folder = client_file.archive_folder
    extract_folder = client_file.extract_folder
    error_folder = client_file.error_folder  # upon error file will be copied here.
    s3_bucket_name = job.s3_bucket_name
    structure_specification_file = client_file.structure_specification_file
    incoming_file = job.incoming_file
    expected_line_length = client_file.expected_line_length

    logger.info(
        " s3_bucket_name = %s, incoming_file = %s, structure_specification_file =%s, error_folder =%s,archive_folder=%s ",
        s3_bucket_name,
        incoming_file,
        structure_specification_file,
        error_folder,
        archive_folder,
    )
    file_name = incoming_file.split("/")[-1]
    error_file_path = error_folder + "/" + file_name

    job.file_type = "Inbound"

    logger.info("job Object is  %s", job)

    error_occured = False
    columns = None
    em = ""

    number_of_detail_rows = -1

    # Create job detail log
    job_repo.create_job_detail(
        job,
        sequence_number=2,
        job_name=job_name,
        input_file_name=file_name,
    )

    vo = None
    file_object = None
    validate_error_file_data_error_records_as_file_structure_error = None

    # We have a scenario where at the detail level error is considered as an absolute rejection.
    # So, either we could marry this scenario to BCI Error file or abort on detail_level_validation flag ,
    # when turned TRUE system performs the detail level validations (similar to what we do for a custom BCI code)
    # 'validate_error_file_data_error_records_as_file_structure_error' is a custom validation where detail
    # level validation is treated as structural validation (refer Ahub_181)
    if client_file.error_file:
        if client_file.validate_file_detail_columns:
            logger.info(
                "Inbound Error File is eligible for considering data errors (Level 1 and Level 2 validations) as "
                "structural validation ")
            columns = get_validations(job.secret_name, job.client_id, job.client_file_id)
            validate_error_file_data_error_records_as_file_structure_error = "YES"
            if columns is None:
                logger.info(" Seems like records missing, column validation can not be loaded")
                validate_error_file_data_error_records_as_file_structure_error = None
                em = " Missing a configuration to validate detail records"
                error_occured = True
        else:
            # send alert for unexpected configuration setting
            complete_unprocessed_file(job, job_repo, client_file, "validate_file_detail_columns")
            job_repo.update_job_detail(job, )
            return None

    # duplicate file if any will be checked here.
    if is_it_duplicate_file(s3_bucket_name, archive_folder, incoming_file.split("/")[-1]):
        logger.info(" %s file found in the %s folder ", incoming_file, archive_folder)
        em = "%s incoming file found in the %s folder, can not process it" % (
            incoming_file,
            archive_folder,
        )
        error_occured = True
    else:
        s3_file = boto3.client("s3")
        file_object = s3_file.get_object(Bucket=s3_bucket_name, Key=incoming_file)
        line_definition = get_positions(s3_bucket_name, structure_specification_file)
        vo = FileStructure(line_definition)
        # TO DO : Copy to error folder

    logger.info(" File to be processed : %s", incoming_file)

    batch_identifier_in_the_header_line = ""
    batch_identifier_in_the_trailer_line = ""
    sender_id_in_the_header_line = ""
    receiver_id_in_the_header_line = ""
    trailer_record_counter = 0
    detail_record_counter = 0
    file_record_count = 0
    trailer_found = False

    counter = 0

    # THIS IS DANGEROUSLY BIG CODE  ALTHOUGH IT WORKS PERFECTLY , BUT NEEDS TO BE REFACTORED

    # FOR LOOP BEGIN
    if not error_occured:
        for line in file_object["Body"].iter_lines():

            line = line.decode(encoding="utf-8", errors="ignore")
            # Following three values are used throughout in the conditional loop below , hence instead of calling at each place
            # it is written  here to make the code do little less run-time parsing.
            line_length = len(line)
            header_identifier_in_this_line = line[vo.header_pos_start: vo.header_pos_end]
            trailer_identifier_in_this_line = line[vo.trailer_pos_start: vo.trailer_pos_end]

            if counter % 50000 == 0:
                logger.info(" Processig Record  %d", counter)

            if line_length != expected_line_length:
                error_occured = True
                em = (
                    f"Line Number : {(counter + 1)} , Expecting line of a  {expected_line_length} character long. "
                    f"However, it is only {line_length} character long "
                )
                break

            if counter == 0:  # First Line

                result = validate_this_line(header_identifier_in_this_line, vo.header_identifier)
                if result == SUCCESS:
                    batch_identifier_in_the_header_line = line[
                                                          vo.header_batch_id_pos_start: vo.header_batch_id_pos_end
                                                          ]
                    sender_id_in_the_header_line = line[
                                                   vo.sender_id_pos_start: vo.sender_id_pos_end
                                                   ].strip()
                    receiver_id_in_the_header_line = line[
                                                     vo.receiver_id_pos_start: vo.receiver_id_pos_end
                                                     ].strip()

                    if sender_id_in_the_header_line != vo.sender_id_value:
                        em = (
                            f"Line Number : {(counter + 1)},  Sender ID {sender_id_in_the_header_line} does not match with "
                            f"system expected value of {vo.sender_id_value} "
                        )
                        error_occured = True
                        break

                    if receiver_id_in_the_header_line != vo.receiver_id_value:
                        em = (
                            f"Line Number : {(counter + 1)},  Receiver ID {receiver_id_in_the_header_line} does not match "
                            f"with system expected value of {vo.receiver_id_value}"
                        )
                        error_occured = True
                        break
                else:

                    em = f"Line Number : {(counter + 1)} , Header identifier is missing "
                    error_occured = True
                    break
            else:  # If not a first line then..
                result = validate_this_line(header_identifier_in_this_line, vo.header_identifier)

                # It means header style record found again ( in the detail level record)
                if result == SUCCESS:
                    error_occured = True

                    em = f"Line Number : {(counter + 1)} , Header found in the detail records "
                    break

                # else header style record is not found
                # is it a trailer record ?
                result = validate_this_line(trailer_identifier_in_this_line, vo.trailer_identifier)
                if result == SUCCESS:  # if is is qualifying  trailer record
                    if not trailer_found:  # and previously there was no such trailer found
                        trailer_found = True
                        if not line[
                               vo.trailer_record_count_pos_start: vo.trailer_record_count_pos_end
                               ].isdigit():
                            em = (
                                f"Line Number : {(counter + 1)} (Trailer), Trailer Record Count Value is :"
                                f"{line[vo.trailer_record_count_pos_start: vo.trailer_record_count_pos_end]} and it is not a valid number"
                            )
                            error_occured = True
                            break
                        trailer_record_counter = int(
                            line[
                            vo.trailer_record_count_pos_start: vo.trailer_record_count_pos_end
                            ]
                        )

                        batch_identifier_in_the_trailer_line = line[
                                                               vo.trailer_batch_id_pos_start: vo.trailer_batch_id_pos_end
                                                               ]

                    else:  # it means  previous records the trailer was found , and now it is found again
                        error_occured = True

                        em = f"Line Number : {(counter + 1)}  ( Trailer),  Another trailer found "
                        break
                else:  # Neither a header, nor a trailer - and a valid detail record
                    detail_record_counter = detail_record_counter + 1
                    # We do not expect any record after trailer, but we see the line occuring after trailer
                    if trailer_found:
                        em = f"Line Number : {(counter + 1)} , a detail record was found after a trailer"
                        error_occured = True
                        break

                    if validate_error_file_data_error_records_as_file_structure_error is not None:
                        em = validate_detail_line(columns, counter + 1, line)
                        if em is not None:
                            error_occured = True
                            break

                    result = SUCCESS  # success because neither header  nor trailer at the detail level and line length is 1700 and no error occured

                    # print("Detail Record Counter = ", detail_record_counter )
            counter = counter + 1

        # FOR LOOP END
    if trailer_found and (
            not error_occured
    ):  # When trailer was found and no other error reported
        # Record in trailer should match with detail record
        if trailer_record_counter != detail_record_counter:

            em = (
                f"Line Number :{counter}  ( Trailer),  Total {detail_record_counter} records found, "
                f"which does not match with Total : {trailer_record_counter}  specified in the trailer"
            )
            error_occured = True
        else:
            if batch_identifier_in_the_header_line != batch_identifier_in_the_trailer_line:
                # error_code = BATCH_ID_MISMATCH
                error_occured = True
                em = (
                    f"Line Number : {(counter + 1)}  ( Trailer),  Batch Identifier does not match,  "
                    f"Header Value : {batch_identifier_in_the_header_line} , Trailer Value : {batch_identifier_in_the_trailer_line}"
                )

            else:
                if trailer_record_counter == 2:  # after header immediately trailer was found
                    error_occured = False
                    number_of_detail_rows = 0

    if error_occured:
        number_of_detail_rows = -1
        job_status = FAIL
        logger.debug(em)
    else:
        if not trailer_found:  # RAn from top to bottom no error occured but trailer was not found
            error_occured = True

            em = " No trailer was found"
            job_status = FAIL
            logger.info("job_status = %s,  Error =  %s ", job_status, em)
            number_of_detail_rows = -1
        else:  # Ran from top to bottom , header was found , trailer was found  ( True) and no error was occured..
            em = ""
            job_status = SUCCESS
            # substracting 1 row for header and 1 row for trailer
            number_of_detail_rows = counter - 2

    if number_of_detail_rows > 0:
        file_record_count = number_of_detail_rows

    if error_occured:
        logger.info("Error has occured file will be moved to error folder")
        file_path = error_file_path
        move_file(s3_bucket_name, incoming_file, s3_bucket_name, file_path)
        logger.info("Update the Redshift table with error message")
        job_status = FAIL
        logger.info("job_status = %s,  Error =  %s ", job_status, em)
    else:
        # File Stays for further processing in txtfiles folder
        file_path = extract_folder  # ex: inbound/bci/txtfiles or inbound/cvs/txtfiles or inbound/txtfiles
        job_status = SUCCESS
        logger.info("Update the Redshift table with success message")

    job.job_end_timestamp = get_timestamp()
    job.file_record_count = file_record_count
    job.job_status = job_status
    job.error_message = em
    job.input_sender_id = sender_id_in_the_header_line
    job.input_receiver_id = receiver_id_in_the_header_line
    job.post_processing_file_location = file_path
    job.output_file_name = ""

    # Send alert notification
    if error_occured and client_file.is_data_error_alert_active:
        notify_file_structure_error(client_file, error_file_path, em)

    # Update job detail log
    job_repo.update_job_detail(job)

    logger.info("Number of Detail Rows = %d", number_of_detail_rows)
    logger.info("Job Key is : %d", job.job_key)

    if job.job_status != SUCCESS:
        job_repo.update_job(job, client_file)
        logger.debug("Failure at validate_file")
        logger.critical(log_job_status_end(job))
        return None

    return job


# This function returns success or identifier missing, it just performs the comparison and nothing else.


def validate_this_line(identifier_in_this_line, header_or_trailer_identifier):
    # means it is not a valid header or footer
    if identifier_in_this_line != header_or_trailer_identifier:
        return IDENTIFIER_MISSING
    # Success means this is either good header or trailer with a length of 1700 ( 1700 line length check is performed earlier)
    return SUCCESS


def validate_detail_line(columns, line_number: int, current_line: str) -> str:
    """Using the pre-defined reading poisitions, this function vaidates the incoming line and performs the 
    validation on a given columns , and returns error message if any of the column listed in the reading positions
    does not meet the criteria as defined in the columns

    Arguments:
        columns {[type]} -- [description]
        line_number {int} -- [description]
        current_line {str} -- [description]

    Returns:
        str -- [description]
    """
    em = None
    for item in ERROR_FILE_READING_POSITIONS:
        (start, end) = get_pos(item)
        current_value = current_line[start:end]
        filecolumns = columns[item]
        if filecolumns is not None:

            em = validate_this_column(current_value, filecolumns, line_number)
            if em is not None:
                return em
    return em


def validate_required(
        columnrule: ColumnRule, filecolumns: FileColumns, current_value: str, line_number: int
) -> str:
    """Validate whether the current_value is required or not, returns None if the non-empty value is found in the 
  current_value otherwise returns an error message.

  Arguments:
      columnrule {ColumnRule} -- validates the  column rule 
      filecolumns {FileColumns} -- for the given column ( represetnted as file column)
      current_value {str} -- for a given value

  Returns:
      str -- Returns None if no error otherewise fully formatted error description
  """
    if len(current_value.strip()) == 0:
        return (
            f"Line Number : {line_number} , Column : {filecolumns.column_name}  at position {filecolumns.column_report_position} can not be empty."
            f" Expected Value must be  {columnrule.equal_to}."
            f" Error Code : {columnrule.error_code}, Error Level : {columnrule.error_level}"
        )
    return None


def validate_equal_to(
        columnrule: ColumnRule, filecolumns: FileColumns, current_value: str, line_number: int
) -> str:
    """Validate whether the current_value matches a specification on a columnrule, returns None if the non-empty value is found in the 
  current_value otherwise returns an error message.

  Arguments:
      columnrule {ColumnRule} -- validates the  column rule 
      filecolumns {FileColumns} -- for the given column ( represetnted as file column)
      current_value {str} -- for a given value

  Returns:
      str -- Returns None if no error otherewise fully formatted error description
  """
    if len(columnrule.equal_to) > 0:
        current_value = current_value.strip()

        if current_value != columnrule.equal_to:
            return (
                f" Line Number : {line_number} , Column : {filecolumns.column_name}  at position {filecolumns.column_report_position}"
                f" has the value {current_value},"
                f" expected value must be {columnrule.equal_to}"
                f" Error Code : {columnrule.error_code}, Error Level : {columnrule.error_level}"
            )
    return None


def validate_this_column(v: str, filecolumns: FileColumns, line_number: int) -> str:
    """Validates the columns in a particular file column object.
  :param v  : current value being validated 
  :param fileColumns : A file column object containing the property of a column to be validated
  :param read_line : The entire row , the entir row is used to validate any other columns in conjuction with the current value 
  :param line_number: Line number within a file
  Returns error message if the column validation fails otherwise returns none

  """

    columnrules = filecolumns.column_rules

    em = ""

    for columnrule in columnrules:

        if columnrule.validation_type == REQUIRED:
            em = validate_required(columnrule, filecolumns, v, line_number)

        if columnrule.validation_type == EQUALTO:
            em = validate_equal_to(columnrule, filecolumns, v, line_number)

        if em is not None:
            return em
            # Earlier I was breaking the loop here..

    return None


def complete_unprocessed_file(job: Job, job_repo: JobRepository, client_file: ClientFile, flag: str) -> None:
    """
    completes the process of job loge entry, moving a file to archive and notifying the issue whenever the file is not
    supposed to load as per the validation flag status.
    """
    # alert about the flag and halt the file process
    logger.info("validate_file_structure is FALSE. As this is an unexpected configuration, sending an alert")
    notify_validation_flag_off(client_file, job, flag)
    # as validate file is OFF we get record count and end_timestamp from here
    file_destination_path = ("/".join([job.incoming_file.split("/")[0], UNPROCESSED_FILES,
                                       job.incoming_file.split("/")[-1]]))  # inbound/unprocessed/abc.txt
    file_record_count = get_file_record_count(job.s3_bucket_name, job.incoming_file,
                                              COMMON_TRAILER_RECORD_COUNT_POSITION, )
    job.file_record_count = file_record_count
    job.job_end_timestamp = get_timestamp()
    # move to archive folder
    move_file(job.s3_bucket_name, job.incoming_file, job.s3_bucket_name, file_destination_path)
    # update job
    job.post_processing_file_location = file_destination_path
    job.job_status = INCOMPLETE  # because we haven't loaded the file into db
    job_repo.update_job(job, client_file)
