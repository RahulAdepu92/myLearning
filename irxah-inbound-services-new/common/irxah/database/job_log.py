import logging
import time
from datetime import datetime
from typing import List

from .dbi import AhubDb
from ..constants import (
    TABLE_JOB,
    TABLE_JOB_DETAIL,
    SCHEMA_DW,
    RUNNING,
    FAIL,
    SUCCESS,
)
from ..file import get_timestamp
from ..glue import get_glue_logger
from ..mix import notify_process_duration, insert_id, log_job_status_end
from ..model import ClientFile, Job

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)


class JobRepository(AhubDb):
    def __init__(self, job_schema=SCHEMA_DW, tables_to_be_locked: List[str] = None, **kwargs):
        if tables_to_be_locked:
            super().__init__(
                tables_to_lock=tables_to_be_locked,
                **kwargs,
            )
        else:
            super().__init__(
                tables_to_lock=[f"{SCHEMA_DW}.{TABLE_JOB}", f"{SCHEMA_DW}.{TABLE_JOB_DETAIL}"],
                **kwargs,
            )
        self.schema = job_schema

    # logging for JOB table

    def create_job(self, client_file: ClientFile, **kwargs) -> Job:
        """Creates a new job to process a given file.
        :param client_file: int or ClientFile: the client file this job processes
        :param file:
        :return: a job object. Note: it could be in error if the database operation didn't work.
                 Check for job_status == RUNNING.
        """
        job = Job()

        job.secret_name = kwargs["SECRET_NAME"]
        job.s3_bucket_name = kwargs["S3_FILE_BUCKET"]
        # empty incoming file name is meant for processing outbound file
        job.incoming_file = kwargs.get("S3_FILE", "")  # inbound/txtfiles/abc.txt
        job.incoming_file_name = job.incoming_file.split("/")[-1]  # abc.txt
        job.job_status = RUNNING  # initialising with running status
        job.job_start_timestamp = job.job_initial_start_timestamp = get_timestamp()
        job.job_name = f"{client_file.file_description}"
        job.file_type = client_file.file_type
        job.client_id = client_file.client_id
        job.client_file_id = client_file.client_file_id

        if job.incoming_file:  # means file_type= INBOUND
            job.pre_processing_file_location = job.incoming_file  # [inbound/txtfiles/abc.txt]
            job.input_sender_id = client_file.input_sender_id
            job.input_receiver_id = client_file.input_receiver_id
            job.s3_archive_path = client_file.archive_folder + "/" + job.incoming_file_name
        else:  # means file_type= OUTBOUND
            job.pre_processing_file_location = client_file.s3_merge_output_path  # [outbound/txtfiles]

        job.job_key = self._insert_job(job)
        logger.info("Generated Job key: %s for name %s", job.job_key, job.job_name)

        # setting customized temp file names
        job.set_file_name(job.job_key)

        if job.job_key == 0:
            logger.warning(
                "Database Error trying to insert record into job table for job %s", job.job_name
            )
            job.job_status = FAIL
            job.error_message = "Database Error occured"

        if job.job_status != RUNNING:
            self.update_job(job, client_file)
            logger.debug("Failure at initialize job_logger")
            logger.critical(log_job_status_end(job))
            return None

        return job

    def _insert_job(self, job: Job) -> int:
        """Inserts a job and returns the job_key"""
        insert_sql = f"""insert into {self.schema}.job
            (name, start_timestamp, inbound_file_name,
            pre_processing_file_location,
            file_type, status, client_id,
            client_file_id,
            error_message, file_status)
            values (:job_name, :job_start, :job_file_name,
            :job_pre_file,
            :job_file_type, :job_status, :job_client_id,
            :job_client_file_id,
            :job_error, :job_file_status)"""

        params = {
            "job_name": job.job_name,
            "job_start": job.job_start_timestamp,
            "job_file_name": job.incoming_file_name,
            "job_pre_file": job.pre_processing_file_location,
            "job_file_type": job.file_type,
            "job_status": job.job_status,
            "job_client_id": job.client_id,
            "job_client_file_id": job.client_file_id,
            "job_error": job.error_message,
            "job_file_status": job.file_status,
        }

        self.execute(query=insert_sql, **params)
        id_row = self.get_one(
            f"select job_key from {self.schema}.job where start_timestamp=:ts",
            ts=job.job_start_timestamp,
        )
        return id_row["job_key"]

    def update_job(self, job: Job, client_file: ClientFile):
        sql = f"""UPDATE {self.schema}.job SET
            end_timestamp=:end_timestamp, status=:status, error_message=:error_message, 
            file_record_count=:file_record_count, input_sender_id=:input_sender_id, 
            input_receiver_id=:input_receiver_id, post_processing_file_location= :post_processing_file_location, 
            updated_by=:updated_by, updated_timestamp=:updated_timestamp, outbound_file_name=:outbound_file_name, 
            outbound_file_generation_complete=:outbound_file_generation_complete 
            where job_key=:job_key"""
        args = {
            "end_timestamp": datetime.utcnow(),
            "status": job.job_status,
            "error_message": job.error_message,
            "file_record_count": job.file_record_count,
            "input_sender_id": job.input_sender_id,
            "input_receiver_id": job.input_receiver_id,
            "post_processing_file_location": job.post_processing_file_location,
            "updated_by": job.updated_by,
            "updated_timestamp": datetime.utcnow(),
            "outbound_file_name": job.outbound_file_name,
            "outbound_file_generation_complete": job.outbound_file_generation_complete,
            "job_key": job.job_key,
        }
        self.execute(sql, **args)

        notify_process_duration(
            job, client_file
        )

    # logging for JOB_DETAIL table

    def create_job_detail(self, job: Job, **kwargs):
        job.job_name = kwargs["job_name"]
        job.input_file_name = kwargs.get("input_file_name", "")
        job.output_file_name = kwargs.get("output_file_name", "")
        job.job_status = RUNNING
        job.job_start_timestamp = get_timestamp()

        job_detail_key = self._insert_job_detail(
            job=job, sequence_number=kwargs["sequence_number"]
        )

        if job_detail_key == 0:
            logger.info(
                "%s: Database Error occured when trying to insert record into ahub_dw.job_detail table",
                job.job_name,
            )
            job.job_status = FAIL
            job.error_message = "Database error occured"
            self.update_job_detail(job)
            logger.debug("Failure at initialize job_logger")
            logger.critical(log_job_status_end(job))
            return None

        logger.info("Generated ID is %s", job_detail_key)
        job.job_detail_key = job_detail_key

        return job

    def _insert_job_detail(self, job: Job, sequence_number: int) -> int:
        """Inserts a job and returns the job_key"""
        insert_sql = (
            f"insert into {self.schema}.{TABLE_JOB_DETAIL} (job_key,sequence_number,name,start_timestamp,input_file_name,output_file_name,status) "
            f"values (:job_key,:sequence_number,:name,:start_timestamp,:input_file_name,:output_file_name,:status)"
        )
        insert_sql_args = {
            "job_key": job.job_key,
            "sequence_number": sequence_number,
            "name": job.job_name,
            "start_timestamp": job.job_start_timestamp,
            "input_file_name": job.input_file_name,
            "output_file_name": job.output_file_name,
            "status": job.job_status,
        }
        select_sql = f"select job_detail_key as key from {self.schema}.{TABLE_JOB_DETAIL} where start_timestamp=:job_start_timestamp"
        select_sql_args = {"job_start_timestamp": job.job_start_timestamp, }

        logger.info(" Executing Insert: %s", insert_sql)

        # Returns job detail key

        job_detail_key = insert_id(job.secret_name, insert_sql, insert_sql_args, select_sql, select_sql_args)
        logger.info(
            " Generated Job Detail Key, %s, for Job Name: %s", job_detail_key, job.job_name,
        )

        return job_detail_key

    def update_job_detail(self, job: Job):
        job.updated_by = "AHUB ETL"
        job.updated_timestamp = get_timestamp()

        update_sql = (
            f"update {self.schema}.{TABLE_JOB_DETAIL} "
            f" set end_timestamp=:end_timestamp, status=:status, error_message=:error_message, "
            f" updated_by=:updated_by, updated_timestamp=:updated_timestamp "
            f" where job_detail_key=:job_detail_key"
        )
        update_sql_args = {
            "end_timestamp": job.job_end_timestamp,
            "status": job.job_status,
            "error_message": job.error_message,
            "updated_by": job.updated_by,
            "updated_timestamp": job.updated_timestamp,
            "job_detail_key": job.job_detail_key,
        }
        logger.info(
            "Executing Update: %s with arguments %s", update_sql, update_sql_args,
        )
        # rows_affected = execute_update(conn, update_sql)
        rows_affected = AhubDb(secret_name=job.secret_name,
                               tables_to_lock=[f"{self.schema}.{TABLE_JOB_DETAIL}"]).execute(update_sql,
                                                                                             **update_sql_args)

        logger.info(
            "Update :: %s for Job Detail Key, %d :: Status = %s :: Rows Affected %s",
            job.job_name,
            job.job_detail_key,
            job.job_status,
            rows_affected,
        )

    # logging for unzipping inbound file activity in 'job_detail' table (sequence_number = 1)

    def insert_unzip_file_job_detail(self, incoming_file: str, status: str, ) -> int:
        """
            inserts a dummy job_key ('0') since we want the job_key to be passed and unique job_detail_key for each
            incoming file that goes through unzipping process and returns job_detail_key value after successful insertion
        """
        insert_sql = f"""insert into {self.schema}.job_detail
                    (job_key, sequence_number,name,start_timestamp,input_file_name,status)
                    values (:job_key, :sequence_number, :job_name, :job_start, :job_input_file, :job_status)"""

        job_start_timestamp = get_timestamp()
        params = {
            # file unzip and push to txtfiles folder is considered the first sequence for INBOUND process
            "job_key": 0,
            "sequence_number": 1,
            "job_name": "Unzip Inbound File",
            "job_start": job_start_timestamp,
            "job_input_file": incoming_file,
            "job_status": status,
        }

        self.execute(query=insert_sql, **params)

        select_sql = f"""select job_detail_key as key from {self.schema}.{TABLE_JOB_DETAIL} 
                        where start_timestamp=:job_start_timestamp"""
        row = self.get_one(
            select_sql,
            job_start_timestamp=job_start_timestamp,
        )
        return row["key"]

    def update_unzip_file_job_detail(self, job_detail_key: int, output_file_name: str, status: str, error_message: str):
        """updates the job_detail_key entry
        :param job_detail_key: unique job id of archival process of the outbound file
        :param output_file_name: outbound file name
        :param status: running status of unzip file activity
        :param error_message: failure message if any
        """
        sql = f"""UPDATE {self.schema}.job_detail SET
           output_file_name = :job_output_file,
           end_timestamp = :job_end,
           updated_by = :updated_by,
           updated_timestamp = :updated_timestamp,
           status = :job_status,
           error_message = :error_message
           WHERE job_detail_key = :job_detail_key"""
        args = {
            "job_output_file": output_file_name,
            "job_end": datetime.utcnow(),
            "updated_by": "AHUB ETL",
            "updated_timestamp": datetime.utcnow(),
            "job_status": status,
            "error_message": error_message,
            "job_detail_key": job_detail_key,
        }
        self.execute(sql, **args)

        logger.info(f"job_detail_key: {job_detail_key} in job_detail table is updated")

    def update_unzip_file_job(self, job_key: int, job_detail_key: int, ):
        """

        """
        sql = f"""UPDATE {self.schema}.job_detail SET
                   job_key = :job_key
                   WHERE job_detail_key = :job_detail_key"""
        args = {
            "job_key": job_key,
            "job_detail_key": job_detail_key,
        }
        self.execute(sql, **args)

        logger.info(f"job_detail_key: {job_detail_key} in job_detail table is updated")

    # logging for outbound file archival activity in 'job_detail' table (sequence_number = 1)

    def insert_archive_and_export_job_detail(self, file_name: str) -> (int, int):
        """Inserts a job_detail and returns the job_detail_key
        :param file_name: outbound file name whose Archival entry should be made
        """
        job_id_row = None
        job_record_count = 0
        attempt = 1

        while attempt <= 3:
            try:
                row = self.get_one(
                    f""" select job_key, file_record_count from {self.schema}.job 
                        where outbound_file_name like :out_file_name """,
                    out_file_name="%" + file_name + "%",
                )
                job_id_row = row["job_key"]
                job_record_count = row["file_record_count"]
            except TypeError as e:
                job_id_row = None
            # by the time this function is executing, the job_key with output file name may or maynot be updated in job table.
            # so trying to execute in the loop till it returns not None value
            if job_id_row is None:
                attempt += 1
                time.sleep(30)
                print(f"searching for job_key in attempt: {attempt}")
            else:
                break

        if job_id_row is None:
            logger.info(
                f" '{file_name}' is copied into 'outbound/txtfiles' folder. It's corresponding job_key entry is not found in Job table. "
                f"Hence, job_detail log process will be skipped."
            )
            return None, None

        job = Job()
        job.job_key = job_id_row
        job.job_start_timestamp = get_timestamp()
        job.job_name = "Archive Outbound File"
        job.output_file_name = "output_file_location"
        job.job_status = RUNNING

        insert_sql = f"""insert into {self.schema}.job_detail
            (job_key,sequence_number,name,start_timestamp,input_file_name,output_file_name,status)
            values (:job_key, :sequence_number, :job_name,
            :job_start, :job_input_file,
            :job_output_file, :job_status)"""

        params = {
            "job_key": job.job_key,
            "sequence_number": 1,  # file archive & export to cfx is the only activity we log for OUTBOUND process
            "job_name": job.job_name,
            "job_start": job.job_start_timestamp,
            "job_input_file": file_name,
            "job_output_file": job.output_file_name,  # It will be updated during update job_detail
            "job_status": job.job_status,  # It will be updated during update job_detail
        }

        self.execute(query=insert_sql, **params)

        job_detail_id_row = self.get_one(
            f"select job_detail_key from {self.schema}.job_detail where start_timestamp=:ts",
            ts=job.job_start_timestamp,
        )
        return job_detail_id_row["job_detail_key"], job_record_count

    def update_archive_and_export_job_detail(self, job_detail_key: int, output_file_name: str):
        """updates the job_detail_key entry
        :param job_detail_key: unique job id of archival process of the outbound file
        :param output_file_name: outbound file name
        """
        sql = f"""UPDATE {self.schema}.job_detail SET
           output_file_name = :job_outbound,
           end_timestamp = :job_end,
           updated_by = :updated_by,
           updated_timestamp = :updated_timestamp,
           status = :job_status 
           WHERE job_detail_key = :job_detail_key"""
        args = {
            "job_outbound": output_file_name,
            "job_end": datetime.utcnow(),
            "updated_by": "AHUB ETL",
            "updated_timestamp": datetime.utcnow(),
            "job_status": SUCCESS,
            "job_detail_key": job_detail_key,
        }
        self.execute(sql, **args)

        logger.info("job_detail_key in job_detail table is updated")
