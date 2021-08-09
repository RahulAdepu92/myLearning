import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_timestamp() -> str:
    """This function gets the timestamp in UTC.
    :returns: A string of the timestamp.
    """
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def change_directory(path: str, directory_to_change_to: str) -> str:
    """This function changes the path and returns the  new path.

     For example : change_directory ("s3://irx-file-validation/inbound/bci/temp/dtl/ACCDLYINT_", "bci")
                   returns s3://irx-file-validation/inbound/bci/

    :param path: The fully qualified path such 3://irx-file-validation/inbound/bci/temp/dtl/ACCDLYINT_
    :directory_to_change_to : from the path above which path you want as a final path for you.
    NOTE : The function will provide the wrong result if the path is something like below 
    change_directory ("s3://irx-file-validation/inbound/bci/temp/bci/ACCDLYINT_", "bci") and your intention is to get  s3://irx-file-validation/inbound/bci/temp/bci/ as a result.
    work around is that you give "temp/bci" as a second parameter
    """

    if (path is not None) and (directory_to_change_to is not None):
        if len(path.strip()) > 0:
            path = path.strip()
        else:
            return None

        if len(directory_to_change_to.strip()) > 0:
            directory_to_change_to = directory_to_change_to.strip()
        else:
            return None
        dir_length = len(directory_to_change_to)
        end_pos = path.find(directory_to_change_to)
        if end_pos == -1:
            return None
        end_pos = end_pos + dir_length + 1
        return path[0:end_pos]
    else:
        return None
