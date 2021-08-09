import csv
import io
import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError, ParamValidationError

from .cvs_manual_marking import ManualMarkingException
from .validate_file_columns import validate_incoming_file_column, get_validations
from .validate_file_structure import complete_unprocessed_file
from ..constants import COLUMN_DELIMITER, FAIL, SUCCESS
from ..database.job_log import JobRepository
from ..file import get_timestamp
from ..glue import get_glue_logger
from ..mix import get_client_file_ids_require_data_validaion
from ..mix import log_job_status_end
from ..model import Job, TextFiles, FileReadEnum, FileSplitEnums, ClientFile

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)


def split_file_handler(job: Job, job_repo: JobRepository, client_file: ClientFile,
                       job_name: Optional[str] = "Split File Handler") -> Job:
    """In a nutshell, this lambda function watches for the particular folder,
    as soon as file is copied it parses the file
    uses the pre-defined the range specification file to identify the begin and end positions;
    upon parsing completion the file is copied in to the respective destination folder,
    and it is deleted from the source folder
    the entire code uses Lambda Environment variables.
    The parameter Job is passed in to keep track of the existing details that will be used 
    to populate the record. Takes in the job as an argument to grab 
    pre-existing values for job logging in Redshift.
    :param arguments: a list of variables that will be resolved to retrieve environment variables
    :param job: The Job Object, from irxah/glue.py. Needs at least the following preconfigured: 
    *parent_key
    :param job_name: User specified name for the job. 
    """

    job.file_processing_location = client_file.file_processing_location

    logger.info(" Job Key is %d, job object is %s", job.job_key, job)

    # duplicate file if any will be checked here.
    archive_folder = client_file.archive_folder
    s3_bucket_name = job.s3_bucket_name
    position_specification_file = client_file.position_specification_file

    em = ""
    error_occured = False

    validation_file_name = None

    # Create job detail log
    job_repo.create_job_detail(
        job,
        sequence_number=3,
        job_name=job_name,
        input_file_name=job.incoming_file_name,
    )

    # This instance is to be optimized for error
    try:
        s3_client = boto3.client("s3")
        if not error_occured:
            # This code is divided in to the three section
            # Section 1 : Read the specification file ( the position file) and create three objects ( header, detail, trailer) along with its attributes
            # Section 2 : Reads the source file ( the file that needs to be parsed), parses according to the specification ( see section 1)
            # and write files to the folder
            # Section 3 : Archive the source file ( the file just parsed through section 2)
            columns = None

            # halt the process if any integrated file doesn't require row level validations
            if client_file.client_file_id in get_client_file_ids_require_data_validaion(job.secret_name):
                if not client_file.validate_file_detail_columns:
                    complete_unprocessed_file(job, job_repo, client_file, "validate_file_detail_columns")
                    job_repo.update_job_detail(job,)
                    return None

                # does incoming file needs detail record level validation? i.e, capture level1 and level2 errors?
                logger.info("Validate File detail columns is TRUE, now Executing get_validations")
                columns = get_validations(job.secret_name, job.client_id, job.client_file_id)
                validation_file_name = job.get_path(job.validation_file_name)
                logger.info("Validation File is :  %s", validation_file_name)

            else:
                logger.info(
                    "'file_column' table doesn't have validation rules to validate it's file detail columns. "
                    "Hence level1 and level2 errors will NOT be captured. "
                )

            # *******************SECTION 1 ( Read the Specification )******************************
            # Reads the range specification
            # Range specification
            # Each line contains the start position and end positions as comma seperated arguments for example
            #  BEGIN1:END1, BEGIN2:END2, BEGIN3:END3..so on

            position_file = s3_client.get_object(
                Bucket=s3_bucket_name, Key=position_specification_file
            )
            content = position_file["Body"].read().decode(encoding="utf-8", errors="ignore")
            all_lines = content.splitlines()

            logger.info(
                "position_specification_file =  %s ,",
                position_specification_file,
            )
            # print(all_lines[TRAILER])
            # 3 File Objects are created and each contains its attribute

            # Loads the position specification for each line

            header_file = TextFiles(
                all_lines[FileSplitEnums.HEADER],
                FileSplitEnums.HEADER,
                s3_bucket_name,
                job.file_processing_location,
                job.header_file_name,
            )
            detail_file = TextFiles(
                all_lines[FileSplitEnums.DETAIL],
                FileSplitEnums.DETAIL,
                s3_bucket_name,
                job.file_processing_location,
                job.detail_file_name,
            )
            trailer_file = TextFiles(
                all_lines[FileSplitEnums.TRAILER],
                FileSplitEnums.TRAILER,
                s3_bucket_name,
                job.file_processing_location,
                job.trailer_file_name,
            )

            # ************** End SECTION 1 *********************************************************************

            # Read the file and write to the respective destination

            incoming_text = s3_client.get_object(Bucket=s3_bucket_name, Key=job.incoming_file)
            incoming_content = (
                incoming_text["Body"].read().decode(encoding="utf-8", errors="ignore")
            )

            # Content is split in to multiple lines, line 0 is the header line, last line is the footer line ( which is all lines minus one)
            all_incoming_lines = incoming_content.splitlines()

            logger.info(
                "job.incoming_file =  %s , Total Lines are : %d",
                job.incoming_file,
                len(all_incoming_lines),
            )

            # *******************SECTION 2 ( Writes only if the overall content has more than 2 lines)

            if len(all_incoming_lines) > 2:

                # print(all_incoming_lines[3])
                # Writes the header file , the file object ( the second argument) contains the file path , file destination etc..
                write_file(all_incoming_lines[FileSplitEnums.HEADER], header_file)

                # Writes the detail file, please note that the entire content is passed here in the first argument
                write_file(
                    incoming_content,
                    detail_file,
                    job_key=job.job_key,
                    columns=columns,
                    validation_file_name=validation_file_name,
                )

                # Writes the footer file, please note that the only footer content which is the last line ( the trailer)
                # in a big file is passed in the first argument

                write_file(all_incoming_lines[len(all_incoming_lines) - 1], trailer_file)

                # -****************END SECTION 2 *****************************************************

            else:
                job.continue_processing = (
                    False  # Indicates do not continue any more as there is nothing to process.
                )
                logger.info(
                    " job.incoming_file =  %s , Total Lines are ONLY %d, no more further processing required",
                    job.incoming_file,
                    len(all_incoming_lines),
                )

            # **************END SECTION 3
    except s3_client.exceptions.NoSuchKey as e:
        error_occured = True
        em = e
        logger.exception(" No Such Key error: %s", e)
    except ParamValidationError as e:
        error_occured = True
        em = e
        logger.exception("Param Validation error: %s", e)
    except ClientError as e:
        error_occured = True
        em = e
        logger.exception("Unexpected error: %s", e)

    if error_occured:
        job.job_status = FAIL
        job.error_message = em
    else:
        job.job_status = SUCCESS
        job.post_processing_file_location = archive_folder  # logging the post_processing_file_location of a Success file here

    job.job_end_timestamp = get_timestamp()

    # Update job detail log
    job_repo.update_job_detail(job)

    if job.job_status != SUCCESS:
        job_repo.update_job(job, client_file)
        logger.debug("Failure at split file")
        logger.critical(log_job_status_end(job))
        raise ManualMarkingException(job.error_message)

    return job


def write_file(
    the_content: list,
    file_type_object: TextFiles,
    job_key: Optional[int] = None,
    columns: Optional[dict] = None,
    validation_file_name: Optional[str] = None,
) -> None:
    """ This method takes the content ( what needs to be parsed   in the_content) and where it needs to be converted and using which type of position
    definition ( specified in file_type_object)  and write to a file
    :param job: the_content, usually a string object whichi needs to be parsed
    :file_type_object : TextFiles contains the attributes of the string object
    :columns : A dictionary object that contains the information about the columns that need to be validated.
    See valiate_file_columns.get_validations for more information.
    :job_id : the job id which is used for login
     

    Args:
        the_content ([list]): what needs to be parsed   in the_content
        file_type_object (TextFiles): Text File specification
        job_key (Optional[int], optional): Job Key Defaults to None.
        columns (Optional[dict], optional): Column Definition. Defaults to None.
        validation_file_name (Optional[str], optional): [ Defaults to None.
    """

    validate_file_columns = False
    validation_io = None
    validation_writer = None
    s3_client = boto3.client("s3")

    content_line = the_content.splitlines()
    # print(content_line)
    file_name = f"{file_type_object.file_path}/{file_type_object.file_name}"
    reading_positions = file_type_object.read_positions

    start_line = 0
    end_line = 1
    first_submission_value = ""

    csv_io = io.StringIO()
    file_writer = csv.writer(csv_io, delimiter=COLUMN_DELIMITER)
    if job_key is not None:
        logger.info(
            "write_file(the_content, file_type_object: TextFiles, job_key=None, columns=None) Job Key %d",
            job_key,
        )

    if columns is not None:
        logger.info(" Columns is Not None")
        validate_file_columns = True
        validation_io = io.StringIO()
        validation_writer = csv.writer(
            validation_io, quoting=csv.QUOTE_NONE, delimiter="~", escapechar="", quotechar=""
        )

    # If total lines of the content in a file is greater than 2 then for the detail start reading from line 1 to the last line minus one

    if len(content_line) > 2:
        start_line = 1
        end_line = len(content_line) - 1

    for line in range(start_line, end_line):
        # print(line)
        read_line = content_line[line]
        write_line = []
        continue_on_this_line = True
        accum_count = 0
        for position in reading_positions:
            read_start = position[FileReadEnum.READ_START]
            read_end = position[FileReadEnum.READ_END]
            write_line.append(read_line[read_start:read_end])

            if (file_type_object.file_type == FileSplitEnums.DETAIL) and (validate_file_columns):
                my_column = f"{str(read_start)}:{str(read_end)}"

                try:
                    filecolumns = columns[my_column]
                    # print(" Reading Value : ", my_column)
                except KeyError:
                    filecolumns = None

                if (filecolumns is not None) and (continue_on_this_line):
                    read_value = read_line[read_start:read_end]
                    validation_result = []
                    # print("Position =", my_column , " Column Name =" , fc.column_name ," Column ID =" , fc.column_id , "  Value = " , read_value)
                    (
                        validation_result,
                        continue_on_this_line,
                        accum_count,
                        first_submission_value,
                    ) = validate_incoming_file_column(
                        read_value,
                        filecolumns,
                        read_line,
                        line,
                        job_key,
                        accum_count,
                        first_submission_value,
                    )
                    # validation_result = validate_column(
                    #     read_value, filecolumns, read_line, line + 1, job_key
                    # )

                    for item in validation_result:
                        validation_writer.writerow([item])

        if file_type_object.file_type == FileSplitEnums.DETAIL:
            write_line.append(line + 1)
            write_line.append(job_key)

        file_writer.writerow(write_line)

    logger.info("Creating file: %s", file_name)
    s3_client.put_object(
        Body=csv_io.getvalue(),
        ContentType="text/plain",
        Bucket=file_type_object.bucket_name,
        Key=file_name,
    )
    csv_io.close()

    if validate_file_columns:
        logger.info("write_file : Validation File is  %s", validation_file_name)

        s3_client.put_object(
            Body=validation_io.getvalue(),
            ContentType="text/plain",
            Bucket=file_type_object.bucket_name,
            Key=validation_file_name,
        )
        validation_io.close()
    logger.info(" write_file : file_type_object is : %s", file_type_object.file_type)
    del file_writer

# This is an attempt to refactor a code, commented out after realizing that custom code validation ( which also passes the vera code scan)
# will not work !!
# def validate_file_column_value(
#     read_line: str, line: int, read_start: int, read_end: int, job_key: int, columns
# ) -> str:
#     my_column = f"{str(read_start)}:{str(read_end)}"
#     filecolumns = None
#     try:
#         filecolumns = columns[my_column]
#     except KeyError:
#         filecolumns = None
#         return None
#     if filecolumns is not None:
#         read_value = read_line[read_start:read_end]
#         # print("Position =", my_column , " Column Name =" , fc.column_name ," Column ID =" , fc.column_id , "  Value = " , read_value)
#         validation_result = validate_column(read_value, filecolumns, read_line, line + 1, job_key)
#         return validation_result
