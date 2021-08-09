import boto3, os, sys, logging
from typing import Dict, Tuple, Optional

INVALID_ENV_KEYS = {
    "exact": ["PATH", "PYTHONPATH", "LD_LIBRARY_PATH"],
    "startswith": ["LAMBDA_", "AWS_", "_"],
}


def run_job(job_name: str, arguments: Dict[str, str]) -> Tuple[str, Dict]:
    """Runs a job with the specified name (string) and arguments (dictionary of string: string).
    Returns a tuple of (job id, job run status),
    where the status is a complex objects whose definition is at:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_job_run
    """
    glue = boto3.client("glue")
    response = glue.start_job_run(JobName=job_name, Arguments=arguments,)

    job_id = response["JobRunId"]
    status = glue.get_job_run(JobName=job_name, RunId=job_id)
    return (job_id, status)


def is_valid_env_key(key: str) -> bool:
    """Verifies an environment key is does not start or is one of the reserved keys."""
    if any(key.upper() == x for x in INVALID_ENV_KEYS["exact"]):
        return False
    if any(key.upper().startswith(x) for x in INVALID_ENV_KEYS["startswith"]):
        return False
    return True


def make_arguments_from_environment(**kwargs) -> Dict[str, str]:
    """Constructs glue job arguments from environment variables. Excludes reserved variables.
    Optionally, adds the specified key-value pairs.
    For example make_arguments_from_environment(SOURCE_FILE_NAME="foo.txt"), will add
    the variable "SOURCE_FILE_NAME" to the list of job arguments with the value "foo.txt")
    """
    job_args = {}
    for key in os.environ:
        if is_valid_env_key(key):
            job_args[f"--{key}"] = os.environ[key]

    for supp_key in kwargs:
        job_args[f"--{supp_key}"] = kwargs[supp_key]

    return job_args


__GLUE_LOGGERS = {}
MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s.%(funcName)s  : %(message)s"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_glue_logger(name: Optional[str] = None, logging_level: Optional = None):
    """
    Gets a logger that works with Glue output.
    :param: name: if not specified, it uses the __name__ or __file__ variable
    :logging_level: preferred logging level; default logging.INFO
    :return: a logger just like logging.getLogger(name), but properly configured for Glue
    """
    if name is None:
        name = f"{__name__}" if __name__ != "__main__" else os.path.basename(__file__)

    if name in __GLUE_LOGGERS:
        return __GLUE_LOGGERS[name]

    logging_level = logging_level or logging.INFO

    # glue likes print and logging doesn't quite work
    # so we create a logger that writes to stdout
    logger = logging.getLogger(name)
    logger.setLevel(logging_level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(MSG_FORMAT))
    logger.addHandler(handler)
    __GLUE_LOGGERS[name] = logger
    return logger

