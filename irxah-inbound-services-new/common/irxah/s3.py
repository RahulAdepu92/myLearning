import csv
import locale
import logging
import os
from typing import Callable, Iterable, List, Tuple, Optional
from urllib.parse import unquote_plus

import boto3

from .common import get_file_name_pattern
from .constants import COLUMN_DELIMITER, BLANK
from .glue import get_glue_logger

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)

try:
    locale.setlocale(locale.LC_ALL, "en_US")
except locale.Error:
    # logging.exception is the right thing to do, but it creates red herrings in the logs
    # switching to debug instead
    logger.debug("Cannot set locale %s en_US", locale.LC_ALL)

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")


def get_file_from_s3_trigger(event) -> Tuple[str, str]:
    """
    Attempts to detect if a Lambda event contained an S3 file trigger
    and if so returns the bucket and the file path
    :param event: a Lambda event
    :return a tuple where the first item is the bucket and the second is the file path,
      or (None, None) if a file was not detected in the event
    """
    if "Records" not in event and "s3" not in event["Records"][0]:
        return (None, None)

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    file_path = unquote_plus(event["Records"][0]["s3"]["object"]["key"])
    # print (f"s3.event: bucket={bucket}, file={file_path}")
    return (bucket, file_path)


def get_first_file_path(bucket_name: str, bucket_path: str) -> str:
    """This method returns the name of the file that has been just copied in to the bucket path folder.
    This is a path on which we would have a lambda S3 trigger."""
    # TO DO : Figure out a way if the multiple files are copied at once, this would execute the lambda function twice !!
    bucket = s3_resource.Bucket(bucket_name)
    for s3_objects in bucket.objects.filter(Prefix=bucket_path):
        if s3_objects.get()["ContentType"] != "application/x-directory":
            return s3_objects.key
        # else - it's a directory
        logger.info("Directory is: %s", s3_objects.key)
    return ""


def move_file(
    source_bucket_name,
    source_file_name_with_path,
    destination_bucket_name,
    destination_file_name_with_path,
):
    copy_file(source_bucket_name, source_file_name_with_path, destination_bucket_name, destination_file_name_with_path,)
    s3_resource.Object(source_bucket_name, source_file_name_with_path).delete()


def copy_file(
    source_bucket_name,
    source_file_name_with_path,
    destination_bucket_name,
    destination_file_name_with_path,
):
    logger.info(
        " Copying from bucket name %s , path : %s, to destination bucket : %s , path : %s",
        source_bucket_name,
        source_file_name_with_path,
        destination_bucket_name,
        destination_file_name_with_path,
    )
    s3_resource.Object(destination_bucket_name, destination_file_name_with_path).copy_from(
        CopySource={"Bucket": source_bucket_name, "Key": source_file_name_with_path}
    )
    logger.info("Now Deleting the file: %s", source_file_name_with_path)


def read_file(s3_bucket: str, s3_path: str, split_string:[Optional] = None, text_preproc: Callable[[str], str] = None,) -> str:
    """
    Reads an S3 file and optionally processes the content before returning it
    :param text_preproc: function that takes a str argument and returns the transformed string
    """
    logger.debug("read_file: (s3_bucket=%s, s3_path=%s)", s3_bucket, s3_path)
    s3_obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_path)
    content = s3_obj["Body"].read().decode(encoding="utf-8", errors="ignore")
    if text_preproc is not None:
        content = text_preproc(content)
    if split_string is not None:
        content = content.split(split_string)
    return content


def split_file(
    s3_bucket: str, s3_path: str, keepends: bool = False, text_preproc: Callable[[str], str] = None
):
    """
    Reads an S3 file and splits it into lines.
    :param keepends: whether to trim the ending \n or to keep them (default trim)
    """
    content = read_file(s3_bucket, s3_path, text_preproc=text_preproc)
    return content.splitlines(keepends)


def delete_files_in_bucket_paths(
    s3_bucket: str, *paths: Iterable[str], file_extension: str = ".txt"
) -> List[str]:
    """
    Function to remove the files with a given extension from multiple paths.
    :param s3_bucket: bucket where to look for the paths
    :param *paths: multiple paths where to look for files
    :param file_extension: extension of files to delete (default .txt)
    """
    removed_files: List[str] = []
    file_extension = file_extension.upper()
    s3bucket = s3_resource.Bucket(s3_bucket)
    for folder_name in paths:
        for key in s3bucket.objects.filter(Prefix=folder_name, Delimiter="/"):
            if key.key.upper().endswith(file_extension.upper()):
                s3_client.delete_object(Bucket=s3_bucket, Key=key.key)
                removed_files.append(key)
    return removed_files


# Here the csv_file_name is the file containing the comma seperated column heading and its position for example :
# HEADER_POS_START,HEADER_POS_END,TRAILER_POS_START,TRAILER_POS_END,HEADER_IDENTIFIER,TRAILER_IDENTIFIER
# 200,202,200,202,HD,TR,1700,1700,275,282,202,209
# This is an attempt to avoid hard coding the numbers.


def get_positions(bucket_name, csv_file_name):
    my_line = None
    # s3 = boto3.client('s3')
    position_file = s3_client.get_object(Bucket=bucket_name, Key=csv_file_name)
    lines = position_file["Body"].read().decode(encoding="utf-8", errors="ignore").splitlines(True)
    for row in csv.DictReader(lines):
        my_line = row
    return my_line


def is_it_duplicate_file(bucket_name: str, folder_name: str, the_file: str) -> bool:
    """For the given bucket_name, it finds the first occurence of the_file
     in that bucket_name and returns  True if the_file is found.
    :param folder_name: is actually a path within a bucket;
        if you specify the wrong folder name or bucket name,
        the system will return the False
    """
    s3_client = boto3.client("s3")
    logger.info(
        "s3: is_it_duplicate_file(bucket_name: %s, folder_name: %s, the_file: %s)",
        bucket_name,
        folder_name,
        the_file,
    )
    try:
        resp = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        for obj in resp["Contents"]:
            if obj["Key"].split("/")[-1] == the_file:
                return True
        return False
    except:
        return False


def delete_s3_files(bucket_name: str, *file_path_prefixes: str) -> List[str]:
    """Deletes files in a bucket_name that match any of the file_path_prefixes.
    :param bucket_name: bucket where to look for the paths
    :param file_path_prefixes: paths to look for
    Example :
    - deletes3objects("irx-file-validation","inbound/bci/temp/256_") -> deletes the file which starts with inbound/bci/temp/256_
    - deletes3objects("irx-file-validation","inbound/bci/temp/256_", "inbound/bci/temp/257_")
      -> deletes the files which starts with inbound/bci/temp/256_ or inbound/bci/temp/257_"""
    logger.info("Deleting files from bucket %r: %r", bucket_name, file_path_prefixes)
    s3_resource = boto3.resource("s3")
    removed_files: List[str] = []
    bucket = s3_resource.Bucket(bucket_name)
    for s3_object in bucket.objects.all():
        file = str(s3_object.key)
        if any(file.startswith(file_path_prefix) for file_path_prefix in file_path_prefixes):
            removed_files.append(s3_object.key)
            s3_object.delete()
    return removed_files


def merge_file_parts(
    s3_bucket: str, detail: str, header: str, trailer: str, outbound_path: str
) -> None:
    """Merge header , detail and trailer in to one file as specified in the outbound_path.
    :param s3_bucket: bucket where the file is located such as irx-dev-data
    :param detail: the fully qualified detail file path within s3_bucket such as outbound/bci/temp/detail.txt
    :param header: the fully qualified header file path within s3_bucket such as outbound/bci/temp/header.txt
    :param trailer: the fully qualified trailer file path within s3_bucket such as outbound/bci/temp/trailer.txt
    :param outbound_path: the fully qualified output file path  where the header,detail and fooer will be merged in to hone outbound/bci/temp/trailer.txt
    NOTE : There is already a merge_file_parts function, and it follows the similar algorithm.
    This version takes care of the detail file. If detail file is empty ( zero byte file), then there is no need to include the detail record.
    """

    header_line = read_file(
        s3_bucket, header, text_preproc=lambda line: line.replace(COLUMN_DELIMITER, BLANK)
    )
    detail_lines = read_file(s3_bucket, detail)
    trailer_line = read_file(s3_bucket, trailer)

    """with open(trailer_line, 'r') as file:
        line = file.readlines()
        detail_record_count = line[210:220]"""

    # Detail line may be emtpy means it empty, the code below checks for that..

    number_of_line_in_detail_lines = len(detail_lines.splitlines())

    if number_of_line_in_detail_lines > 0:

        # ensure each of the lines ends up with a new line
        # so that when we join the lines we get both separation and avoid double \n
        if not header_line.endswith(os.linesep):
            header_line += os.linesep
        if not detail_lines.endswith(os.linesep):
            detail_lines += os.linesep
        if not trailer_line.endswith(os.linesep):
            trailer_line += os.linesep
        result = "".join([header_line, detail_lines, trailer_line])
    else:

        if not header_line.endswith(os.linesep):
            header_line += os.linesep
        if not trailer_line.endswith(os.linesep):
            trailer_line += os.linesep
        result = "".join([header_line, trailer_line])

    outbound_path = f"{os.path.dirname(outbound_path)}/{os.path.basename(outbound_path).upper()}"
    s3_client.put_object(Body=result, Bucket=s3_bucket, Key=outbound_path)

    logger.info(
        "Combined %s + %s + %s (%s bytes) from %s into %s",
        header,
        detail,
        trailer,
        locale.format("%d", len(result), grouping=True),
        s3_bucket,
        outbound_path,
    )


def get_argument_value(arguments: List[str], argument_parameter: str) -> str:
    """Time to time we run in to the situation where we need to read a value of the argument which is typically
    not the part of the common things. Such as file needs a detail level validation at the file structure level
    Such FLAG will be passed from a particular Lambda only. So it needs to be read special way using get_argument_value
    Also, this is an equivalent version of glue.utils.getResolvedOptions.

    Arguments:
        arguments {List[str]} --  List of  arguments passed from Lambda function
        argument_parameter {str} -- argument parameter whose value needs to be found.

    Returns:
        str -- None if the argument is not found otherwise returns the corresponding value of the argument.
    """

    argument_value = None

    logger.info("s3: get_argument_value:  argument_parameter is :  %s ", argument_parameter)
    try:
        argument_index = arguments.index(f"--{argument_parameter}")
        argument_value = arguments[argument_index + 1]
    except (ValueError, IndexError):
        logger.debug(" Error Occured")

    logger.info(" Returning  Value : %s ", argument_value)
    return argument_value


def get_file_record_count(s3_bucket: str, file: str, string_position: str, is_trailer_file: Optional[bool] = False) -> int:
    """Returns the record_count of the outbound file by reading trailer file and extracting string at defined positions
    :param s3_bucket: s3 bucket
    :param file: the file to be read
    :param string_position: staring and ending position of string to be read.(In Ahub, we use it for column 'record_count' in trailer line of outbound file)
    :param is_full_file: Optional boolean value. If the full file (all lines) to be read, it will be True.
    """
    if is_trailer_file:
        trailer_line = read_file(s3_bucket, file)  # feed will be a file that contains a trailer line alone in it
        record_count = 0
        try:
            trailer_record_count_string = trailer_line[
                                          int(string_position.split(":")[0]): int(string_position.split(":")[1])
                                          ].lstrip("0")

            # if the field is all zeroes (we didn't export anything)
            # lstrip("0") will return empty. Skip raising an error unnecessary
            if trailer_record_count_string == "":
                return 0

            record_count = int(trailer_record_count_string)
        except ValueError:
            logger.exception()
            logger.error(
                "Error occurred in %s / %s. Empty record (pos %s) in the trailer for line: %s",
                s3_bucket,
                file,
                string_position,
                trailer_line,
            )
        return record_count

    else:
        # getting file record count by reading the whole file. This is helpful when validate_file_structure flag is OFF.
        # Otherwise there itself we would fetch the count.
        all_lines = read_file(s3_bucket, file,)
        number_of_total_lines = len(all_lines.splitlines())
        file_record_count = (number_of_total_lines - 2)  # excluding header and trailer from the total count
        if file_record_count == "":
            return 0

        record_count = int(file_record_count)

    return record_count


def create_s3_file(s3_bucket: str, s3_key: str, data: str) -> None:
    """Function to create the s3 file with the given data
    param s3_bucket : Holds the name of s3 bucket
    param s3_key : Holds the name of s3 file name with prefix
    param data : file content to be saved as s3 file in the format of string
    returns None
    """
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=data)


def is_inbound_file_in_txtfiles(s3_bucket: str, file_path: str, client_file_name: str) -> str:
    """
    checks if any file gets stuck in txtfiles folder while loading into db. This happens unusually when glue fails abruptly
    with some AWS internal issue. By this, we can avoid sending false inbound SLA alerts where we claim
    that the file has not receieved to AHub just by checking the entry in Job table. With addition of current check,
    we can verify if the file is present in txtfiles folder.
    param s3_bucket : Holds the name of s3 bucket
    param file_path : file path where search has to happen
    param client_file_name : file name of the client from client_file table (recieved from CLientFile object)
    returns boolean value
    """
    # don't use Delimiter="/" parameter because file_path we pass will be in format "inbound/bci/txtfiles" with no
    # ending "/" character. if you give "/", then the object search will happen in "inbound/bci"(string before "/" char)
    resp = s3_client.list_objects(Bucket=s3_bucket, Prefix=file_path,)
    if "Contents" in resp:
        for obj in resp["Contents"]:
            inbound_file_name = obj["Key"].split("/")[-1]
            if get_file_name_pattern(inbound_file_name) == get_file_name_pattern(client_file_name):
                logger.info(
                    f"file {inbound_file_name} Found in txtfiles folder. "
                    f"Hence, it is not considered as SLA violation and the file needs to be reprocessed"
                )
                yield inbound_file_name