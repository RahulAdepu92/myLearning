import sys
import logging 
import time
from irxah.jobs import process_flow
# while running lambdas we encounter import error as awsglue is inbuilt module for Glue but not for lambda
try:
    from awsglue.utils import getResolvedOptions
except ImportError:
    pass
from irxah.jobs.export import export_client_file
from irxah.glue import get_glue_logger

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)
logger.info("Warm Pool Job Started .. Now wait for 30 seconds")
time.sleep(30)
logger.info("Warm Pool Job Ended")
