import logging.config
import os
from datetime import datetime
from functools import reduce
from types import TracebackType
from typing import List, Optional, Tuple

import boto3
import dateutil
import pytz
from croniter import croniter

from .constants import DEFAULT_TIMEZONE
from .glue import get_glue_logger
from .model import CronExpressionError, Job, ClientFile

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)


def convert_utc_to_local(
    utc_timestamp: datetime, timezone: str, format_string: Optional[str] = "%m/%d/%Y %I:%M:%S %p %Z"
) -> str:
    """Converts the utc_timestamp to a given timezone , using the default format_string
    Args:
        utc_timestamp (datetime): UTC Timesstamp
        timezone (str): Timezone string such as 'America/New_York'
        format_string (Optional[str], optional): [description]. Defaults to "%m/%d/%Y %I:%M:%S %p %Z".

    Returns:
        str: Local time in the format_string format
    """
    utc_timestamp = utc_timestamp.replace(tzinfo=pytz.UTC)
    tz = pytz.timezone(timezone)
    return utc_timestamp.astimezone(tz).strftime(format_string)


def convert_local_to_utc(
    local_timestring: str,
    local_timezone: str,
    format_string: Optional[str] = "%m/%d/%Y %I:%M:%S %p",
) -> str:
    """Converts the local_timestamp to a given time zone, using the defualt format string

    Args:
        local_timestring (str): Local timestamp
        local_timezone (str): Local timestamp's timeone
        format_string (Optional[str], optional): [description]. Defaults to "%m/%d/%Y %I:%M:%S %p".

    Returns:
        str: UTC time in the format_string format
    """
    local_timestamp = datetime.strptime(local_timestring, format_string)
    tz_local = pytz.timezone(local_timezone)
    local_timestamp = tz_local.localize(local_timestamp)
    utc_timestamp = local_timestamp.astimezone(pytz.UTC)
    return utc_timestamp.strftime(format_string)


def convert_local_timestamp_to_utc_timestamp(
    local_timestamp: datetime, local_timezone: str
) -> datetime:
    """Converts local timestamp to UTC timestamp

    Args:
        local_timestamp (datetime): local_timestamp
        local_timezone (str): local_timestamp's timezone such as 'America/New_York'

    Returns:
        datetime: Returns a date time object, please this will be in UTC without any timezone information.
    """
    tz_local = pytz.timezone(local_timezone)
    local_timestamp = tz_local.localize(local_timestamp)

    utc_timestamp = local_timestamp.astimezone(pytz.UTC)
    return datetime(
        utc_timestamp.year,
        utc_timestamp.month,
        utc_timestamp.day,
        utc_timestamp.hour,
        utc_timestamp.minute,
        utc_timestamp.second,
        utc_timestamp.microsecond,
    )


def convert_utc_timestamp_to_local_timestamp(
    utc_timestamp: datetime, local_timezone: str
) -> datetime:
    """Converts UTC timestamp to local timestamp

    Args:
        utc_timestamp (datetime): UTC Timestamp
        local_timezone (str): Local timezon where you want it to converit it to for example 'America/New_York'

    Returns:
        datetime: Returns a local timestamp
    """
    utc_timestamp = utc_timestamp.replace(tzinfo=pytz.UTC)
    tz = pytz.timezone(local_timezone)
    local_timestamp = utc_timestamp.astimezone(tz)
    return datetime(
        local_timestamp.year,
        local_timestamp.month,
        local_timestamp.day,
        local_timestamp.hour,
        local_timestamp.minute,
        local_timestamp.second,
        local_timestamp.microsecond,
    )


def get_local_midnight_time_from_utc_time(timezone: str) -> datetime:
    """Obtains the local midnight time in the UTC format for the given timezone
    This is useful in the outbound SLA procesing where we look for whether the file was sent out today between m
    midnight of the local time to curernt time.
    We know the current time in UTC but we don't know what would be the UTC time for the local time.
    For example : It is 09:30 AM UTC ( 05:30 AM Eastern), but we are intersted in getting the time of
    midnight eastern, this functio gives that midnight eatern time in UTC for example 04:00 AM UTC is the mid night easten time.

    Args:
        timezone (str): Timezon's whose equivalent mid night UTC needs to be obtained.

    Returns:
        datetime: [a date time
    """
    utc_time = datetime.utcnow()
    local_midnight_timestamp = datetime(utc_time.year, utc_time.month, utc_time.day, 0, 0, 0, 0)
    return convert_local_timestamp_to_utc_timestamp(local_midnight_timestamp, timezone)


def format_utc_time(hour: int, minute: int, second: int) -> datetime:
    """Returns a formatted UTC time using hour,minute and second

    Args:
        hour (int): [description]
        minute (int): [description]
        second (int): [description]

    Returns:
        datetime: [description]
    """
    utc_time = datetime.utcnow()
    return datetime(utc_time.year, utc_time.month, utc_time.day, hour, minute, second, 0)


def format_string_time_to_timestamp(
    local_timestring: str,
    local_timezone: Optional[str] = "UTC",
    format_string: Optional[str] = "%Y-%m-%d %H:%M:%S",
) -> datetime:
    """Typically used to translate redshift time that is typically in the format of 2020-03-26 14:36:35, in to corresponding timestamp with UTC

    Args:
        local_timestring (str): Time string , if you are passing the format such as 2020-03-26 14:36:35 , and wanted to get the time as timestamp value
                                then do not pass the local_timezone and format_string , the default value satisfies it.
                                However, if you need to translate the local_timestring to be returned in to desired local_timezone then pass the
                                local_timezon and format_string( if not satisfied with the default format of %Y-%m-%d %H:%M:%S )
        local_timezone (Optional[str], optional): [description]. Defaults to "UTC".
        format_string (Optional[str], optional): [description]. Defaults to "%Y-%m-%d %H:%M:%S".

    Returns:
        datetime: [description]
    """
    local_timestamp = datetime.strptime(local_timestring, format_string)
    tz_local = pytz.timezone(local_timezone)
    local_timestamp = tz_local.localize(local_timestamp)
    utc_timestamp = local_timestamp.astimezone(pytz.UTC)  # replace method
    return utc_timestamp


def get_pos(item: str) -> Tuple[int, int]:
    """Splits incoming string by delimiter :
    For example if you pass "10:12" , it will return integer 10 and integer 12
    Is there any short hand routine available rather than this method ??


    Arguments:
        item {str} -- String containing integer and seperated by :


    Returns:
        Tuple[int,int] -- Returns the integer
    """
    positions = item.split(":")
    return int(positions[0]), int(positions[1])


def tz_now(timezone_name: str = DEFAULT_TIMEZONE) -> datetime:
    """Returns datetime.now in a specific timezone (default ET aka America/New_York)."""
    target_tz = dateutil.tz.gettz(timezone_name)
    return datetime.now(tz=target_tz)


def get_next_iteration(
    cron_expression: str,
    timezone: Optional[str] = DEFAULT_TIMEZONE,
    base: Optional[datetime] = datetime.utcnow(),
) -> datetime:
    """Given a cron_expression generates the next schedule time.
    cron_expresion should be provided in the following format

    Every Sunday at 07:30 AM UTC is  utc : 30 7 * * SUN
    Every dayt at 10:30 AM local is  local : 30 10 * * *
    Every 2 Hour at the 30th minute is utc: 30 0/2 * * *

    Those who follows the day light savings time specify the cron expression starting with local
    Those who never follows the day light savings time specify the cronss expression starting with utc.


    Args:
        cron_expression (str):  Cron Expression it should either begin with utc or local followed by : and a valid cron expression
        timezone (Optional[str], optional): Timezone , required only if local is used. Defaults to "America/New_York".
        base (Optional[datetime], optional):  Defaults to datetime.utcnow().

    Returns:
        datetime: A datetime value in UTC. Please note that system will automatically translate the local 10:30 AM in to the corresponding UTC time.
        And , this translation is DST sensitive.
    """
    logger.info(
        " Cron Expression is : %s, Time Zone : %s, Base Time : %s", cron_expression, timezone, base
    )

    cron_list = cron_expression.split(":")
    # By default only one iteration is required
    iterations_required = 1

    # Cron list must be seperated by ":""
    if len(cron_list) not in [2, 3]:
        raise CronExpressionError(
            cron_expression, "Provided Expression is too big or too small", 1001
        )

    # First three character of the expression must be either UTC or LOCAL ( case insensitive)
    if cron_list[0].strip().upper() not in [
        "UTC",
        "LOCAL",
    ]:
        raise CronExpressionError(
            cron_expression, "Expression does not begin with utc or local", 1002
        )

    # Validate the number of iteration required
    if len(cron_list) == 3:

        iteration_str = cron_list[2].strip()

        # Empty number of iteration is not permitted
        if len(iteration_str) == 0:
            raise CronExpressionError(
                cron_expression, "Number of iteration required field is  empty", 1003
            )
        # Junk value is not permitted in number of iteration
        if not iteration_str.isdigit():
            raise CronExpressionError(
                cron_expression, "Number of iteration required field is not a number ", 1004
            )

        iterations_required = int(iteration_str)

        if iterations_required <= 0:
            raise CronExpressionError(
                cron_expression, " Number of iteration field can not be negative or zero", 1005
            )

    cron_expr = cron_list[1].strip()

    # Expression must be a valid expression
    if not croniter.is_valid(cron_expr):
        raise CronExpressionError(cron_expression, "Invalid Cron Expression", 1006)

    # Will enter here only if required values are valid
    if cron_list[0].strip().upper() == "UTC":
        iteration = croniter(cron_expr, base)
        return _get_iteration(iteration, iterations_required)

    iteration = croniter(cron_expr, convert_utc_timestamp_to_local_timestamp(base, timezone))
    # The time obtained below will also be local
    next_iteration_time_in_local = _get_iteration(iteration, iterations_required)
    # Convert the time obtained in local to UTC
    return convert_local_timestamp_to_utc_timestamp(next_iteration_time_in_local, timezone)


def _get_iteration(iteration: croniter, number_of_iterations) -> datetime:
    """[summary]

    Args:
        iteration (croniter): [description]
        number_of_iterations ([type]): [description]

    Returns:
        datetime: [description]
    """
    for num in range(0, number_of_iterations):

        if num == number_of_iterations - 1:
            # print("Value is returned")
            return iteration.get_next(datetime)
        else:
            # print("Value is advanced")
            iteration.get_next(datetime)
    return None


def run_glue_job(glue_job_name: str, glue_job_arguments: dict) -> None:
    """Runs a glue job with glue job arguments

    Args:
        glue_job_name (str): [description]
        glue_job_arguments (dict): [description]
    """
    glue_job = boto3.client("glue")
    logger.info("Calling Glue Job:: %s ", glue_job_name)
    response = glue_job.start_job_run(
        JobName=glue_job_name,
        Arguments=glue_job_arguments,
    )
    if "JobRunId" in response:
        job_id = response["JobRunId"]
        logger.info("%s job run with id %s", glue_job_name, response["JobRunId"])
        _ = glue_job.get_job_run(JobName=glue_job_name, RunId=job_id)


def run_glue_job_after_creating_standard_file(client_file: ClientFile, job: Job, generated_file: str) -> None:
    """
    Runs a glue job with glue job arguments
    """

    glue_job_arguments = {
        # preparing arguments for glue job 'irxah_process_incoming_file_load_in_database'
        "--SECRET_NAME": job.secret_name,  # for connecting db (retrieved as lambda environment variable)
        "--S3_FILE": generated_file,  # file_name with path (retrieved from s3 trigger event)
        "--S3_FILE_BUCKET": job.s3_bucket_name,  # s3_bucket_name (retrieved from s3 trigger event)
        "--CLIENT_FILE_ID": str(client_file.client_file_id),  # unique id of client file (retrieved from db)
        # we are passing a dummy job key as generation of standard file (GILA) from custom file (BCBAZ) will not have a unique unzip key.
        # by this, we are avoiding 'irxah_process_incoming_file' glue job failure
        "--UNZIP_JOB_DETAIL_KEY": "0",
    }
    run_glue_job(client_file.glue_job_name, glue_job_arguments)


def get_traceback(tb: TracebackType) -> str:
    """[summary]

    Args:
        tb (TracebackType): Traceback Type OBject

    Returns:
        str:  Formatted Traceback Information
    """
    return f"filename {tb.tb_frame.f_code} {os.linesep} Fucntion Name: {tb.tb_frame.f_code.co_name}{os.linesep}Line Number: {tb.tb_lineno}"


def format_string(input_str: str, length: int, format_char: str, align: str) -> str:
    """Function to format the input string to the specified length with given format character and alignment
    :param input_str: The string value to be formatted
    :param length: specifies the required length
    :param format_char: specifies the char to be filled to format the string
    :param align: specifies the alignment, "<" to fill the character to the right ">" to fill the characters to the left
    :return:the formatted string

    """
    return "{message:{fill}{align}{width}}".format(
        message=input_str.strip(),
        fill=format_char,
        align=align,
        width=length,
    )


def trim_and_combine_values(values: List[str]) -> str:
    """Function to trim and combine the values in the given list
    :param values: List of values
    :return: string of combined values from the list
    """
    return reduce((lambda x, y: x.strip() + y.strip()), values, "")


def get_current_date_in_format(date_format: str, timezone: str) -> str:
    """Function to get the current date in the give time zone with the given format
    :param date_format: format of the date string
    :param timezone: time zone like"MST", "EST"
    :return: string current date in the give time zone with the given format
    """
    date = tz_now(timezone)
    return date.strftime(date_format)


def convert_date_format(date: str, from_date_format: str, to_date_format: str) -> str:
    """Function to convert the given date string from <from_date_format>to the required format <to_date_format>
    :param date: input date string
    :param from_date_format: input date format, example '%Y/%M/%d'
    :param to_date_format: required date format, example '%M%d%Y'
    :return: string the given date in the required date format
    """
    return datetime.strptime(date, from_date_format).strftime(to_date_format)


def get_earlier_date(date1: str, date2: str, date_format: str) -> str:
    """Function to get the earlier (min) date from the given 2 date values
    :param date1: first date
    :param date2: second date
    :param date_format: the format of the input date
    :return: the maximum date from date1 and date2
    """
    if datetime.strptime(date1, date_format) >= datetime.strptime(date2, date_format):
        return date2
    return date1


def get_file_name_pattern(file_name: str) -> str:
    """
    Function to get full string before 3rd '_' character in outgoing file and checks if any name_pattern exists in db. For ex 'ACCHIST_TST_BCIINRX'
    :param file_name: name of the processing file
    """
    processing_file_name_pattern = file_name[: file_name.replace("_", "X", 2).find("_")]
    logger.info("processing file_name_pattern is : %s", processing_file_name_pattern)

    return processing_file_name_pattern
