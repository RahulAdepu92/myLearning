import logging
from datetime import datetime, timedelta
from typing import Callable, List, Optional

from .dbi import AhubDb
from ..common import (
    convert_utc_to_local,
    get_next_iteration,
    get_traceback,
)
from ..constants import SECRET_NAME_KEY, SCHEMA_DW, FILE_SCHEDULE
from ..glue import get_glue_logger
from ..model import FileSchedule, FileScheduleProcessingTypeEnum, CronExpressionError
from ..notifications import send_email_notification_when_failure_executing_sla

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)


class FileScheduleRepo(AhubDb):
    _select_query = f"""SELECT f.client_file_id, c.client_id, c.name, c.abbreviated_name as client_abbreviated_name, f.file_schedule_id, 
            f.frequency_type, f.frequency_count, f.total_files_per_day, f.grace_period, f.notification_sns, 
            f.notification_timezone, f.file_category, f.file_description, f.is_active,f.is_running, f.aws_resource_name,
            f.last_poll_time, f.next_poll_time, f.updated_by,  f.updated_timestamp, upper(f.processing_type) processing_type, 
            f.environment_level, f.cron_expression, f.cron_description
            FROM AHUB_DW.file_schedule f 
            LEFT OUTER JOIN ahub_dw.client_files cf ON f.client_file_id = cf.client_file_id 
            LEFT OUTER JOIN ahub_dw.client c ON cf.client_id = c.client_id 
            """

    def __init__(self, **kwargs):
        # assigning 'tables_to_lock' is mandatory. Else execute method is failing with 'None Type object' Error
        super().__init__(tables_to_lock=[f"{SCHEMA_DW}.{FILE_SCHEDULE}"], **kwargs)

    def get_file_schedule_info(self, client_file_id: int,
                               processing_type: Optional[
                                   FileScheduleProcessingTypeEnum] = FileScheduleProcessingTypeEnum.GLUE) -> FileSchedule:
        """Returns a file schedule specific information.

            Args:
                conn ([type]): Redshift connection
                client_file_id (int): Client File ID
                processing_type (FileScheduleProcessingTypeEnum):  Procesing Type

            Returns:
                FileSchedule: Non-empty file schedule object
        """
        # Pass a client file ID and returns the client file information in a FileSchedule Object
        sql = f"""{FileScheduleRepo._select_query} where f.client_file_id = :id and f.processing_type = :type 
                   order by f.next_poll_time limit 1 """
        # records limited to 1 as Gila inbound has two different schedules and we need to pick the latest one only (bug_fix: AHub_771)

        row = self.get_one(sql, id=client_file_id, type=processing_type)
        if row is None:
            return None

        fs = FileSchedule.from_dict(**row)
        # returns sql output--> [aws_resource_name=None; client_abbreviated_name=BCI; client_file_id=1; client_id=1;]
        return fs

    def get_file_schedules_requiring_sla_check(self) -> List[FileSchedule]:
        """Gets all the file schedule requiring SLA check.
            A simple check is performed and that is is the current time ( UTC) is greater than the next poll time.
            For example, current time is 11/15/2020 05:00 AM UTC then this function would return all those
            file schedule's information which had a poll time in past. Poll time in past means it expires.
            If expired then SLA check needs to be made.

        Args:
            conn ([type]): Redshift Connection Object

        Returns:
            List[FileSchedule]: Collection of file schedules where next poll time <= current utc time
        """
        # Pass a client ID and returns the client files information in a collection
        sql = f"""{FileScheduleRepo._select_query} where f.is_active=True and f.is_running=False and 
                   f.next_poll_time <= :time order by f.file_schedule_id """

        rows = self.iquery(sql, time=datetime.utcnow())
        if rows is None or len(rows) == 0:
            return None

        return [FileSchedule.get_file_schedule_from_record(row) for row in rows]

    def get_number_of_schedules(self, client_file_id) -> int:
        """This function retrieves number of file schedules available for particular client file id.
        :param conn: The redshift connection.
        :returns: count (no.of schedules) in int.
        """
        # Pass a client file ID and returns the client file information in a Client_File Object
        sql = f""" select count from (select processing_type,client_file_id,count(*) count from ahub_dw.file_schedule where
               processing_type = 'INBOUND->AHUB' and client_file_id = :id group by processing_type, client_file_id )"""

        row = self.get_one(sql, id=client_file_id)
        if row is None:
            return 0

        return row["count"]

    def get_file_schedule_info_by_file_schedule_id(self, file_schedule_id: int, ) -> FileSchedule:
        """Returns a file schedule specific information.
        Args:
            file_schedule_id (int): File Schedule ID
        Returns:
            FileSchedule: Non-empty file schedule object
        """
        # Pass a File Schedule ID and returns the client file information in a Client_File Object
        sql = f"{FileScheduleRepo._select_query} where  f.file_schedule_id = :id "
        row = self.get_one(sql, id=file_schedule_id, )

        if row is None:
            return None

        fs = FileSchedule.from_dict(**row)
        return fs

    def get_schedules_running_status(self) -> List[int]:
        """gets list of glue schedule ids whose is_running status is TRUE. Usually after successful execution of each glue
        corresponding id status will turn to false at the end. But any initial failures (due to AWS internal issue) make
        their status leave TRUE as is and makes the next execution stop (because only FALSE running_status will be processed).
        Args:
        Returns:
            [type]: List[int]
        """
        sql = (
            f"select FILE_SCHEDULE_ID from ahub_dw.FILE_SCHEDULE where IS_RUNNING = True "
        )
        rows = self.iquery(sql, )
        # >> ([10], [20], [30], [40],...)
        if rows is None:
            return None

        logger.info(" Above Query output is non empty")

        result = [row[0] for row in rows]  # list comprehension
        # >> [10, 20, 30, 40,...]
        return result

    def update_is_running_to_false(self, file_schedule_id: int) -> None:
        """For a given schedule ID ( aka file schedule ID) , it sets is_running to FALSE.
        The reason we set it is that if any job fails and couldn't proceed for subsequent execution, it will help in making
        eligible for the next execution in upcoming schedule.
        Args:
            file_schedule_id (int): File Schedule ID
        Returns:
            [type]: None
        """
        sql = (
            f"update ahub_dw.FILE_SCHEDULE SET IS_RUNNING = False WHERE FILE_SCHEDULE_ID = :id"
        )

        rows_updated = self.execute(sql, id=file_schedule_id)
        logger.info("UPDATE is_running = False with file_schedule_id=%s: %s rows", file_schedule_id, rows_updated)

    def update_file_schedule_to_running(self, file_schedule_id: int) -> None:
        """For a given schedule ID ( aka file schedule ID) , it sets to running.
        The reason we set it to running is that if one job is already running then
        the other job should not be running with a same file schedule ID

        Args:
            connection ([type]): Redshift Connection
            file_schedule_id (int): File Schedule ID

        Returns:
            [type]: None
        """
        sql = (
            f"update ahub_dw.FILE_SCHEDULE  SET IS_RUNNING = True, updated_timestamp = GETDATE(), "
            f"updated_by ='AHUB ETL (update_file_schedule_to_running)' WHERE FILE_SCHEDULE_ID = :id"
        )

        rows_updated = self.execute(sql, id=file_schedule_id)
        logger.info("UPDATE is_running = True with file_schedule_id=%s: %s rows", file_schedule_id, rows_updated)

    def update_file_schedule_to_next_execution_glue_curry(self, **kwargs) -> Callable:
        """Creates a curried instance of update_file_schedule_to_next_execution_glue
        based on whether appropriate parameters are passed.
        There parameters are typically passed to scheduled glue functions.
        These method exists so that glue code doesn't have to specifically unpack
        variables it does not need to function.

        :param SECRET_NAME: the name of the secret file
        :param CLIENT_FILE_ID: the id of the client file whose execution is being updated
        :param UPDATE_FILE_SCHEDULE_STATUS: whether to update the schedule
        :param FILE_SCHEDULE_ID: Optional the specific schedule id that triggered the job

        Example usage:
            args = getResolvedOptions(
                sys.argv,
                [
                    "CLIENT_FILE_ID",
                    "SECRET_NAME",
                    "S3_FILE_BUCKET",
                    "FILE_SCHEDULE_ID",
                    "UPDATE_FILE_SCHEDULE_STATUS",
                ],
            )
            update_file_schedule_to_next_execution_glue_curry(**args)()
        """
        update_status = kwargs.get("UPDATE_FILE_SCHEDULE_STATUS", "FALSE")

        if update_status.upper() != "TRUE":
            logger.debug(
                "UPDATE_FILE_SCHEDULE_STATUS is not TRUE (%s) so not updating file schedule",
                update_status,
            )
            return lambda: None

        secret_name = kwargs.get(SECRET_NAME_KEY, "")
        client_file_id = kwargs.get("CLIENT_FILE_ID", 0)
        return lambda: FileScheduleRepo.update_file_schedule_to_next_execution_glue(self, secret_name, client_file_id)

    def update_file_schedule_to_next_execution_glue(
            self,
            secret_name: str,
            client_file_id: int,
            processing_type: Optional[FileScheduleProcessingTypeEnum] = FileScheduleProcessingTypeEnum.GLUE,
    ) -> None:
        """[summary]

        Args:
            self (str): AWS Secret Name ( when used in a Glue Job , the underlying code will establish PG connection)
            client_file_id (int): Client File ID
            processing_type (Optional[FileScheduleProcessingTypeEnum], optional): Defaults to FileScheduleProcessingTypeEnum.GLUE.
        """
        # Since it is passed from Glue ( could be a pg8000 connection - but the code below deals with pg connection)
        logger.info("Updating file schedule id for client file: %s", client_file_id)

        file_schedule: FileSchedule = FileScheduleRepo(secret_name=secret_name).get_file_schedule_info(
            client_file_id)
        file_schedule.format_timestamps() if file_schedule is not None else None
        if not file_schedule:
            logger.warning("Cannot retrieve file schedule for client_file_id=%s", client_file_id)
            return

        FileScheduleRepo(secret_name=secret_name).update_file_schedule_to_next_execution(
            secret_name,
            file_schedule.file_schedule_id,
            file_schedule.cron_expression,
            file_schedule.notification_timezone,
        )

    def update_file_schedule_to_next_execution(
            self, secret_name: str, file_schedule_id: int, cron_expression: str, notification_timezone: str
    ) -> None:
        """Using the cron expression decides the next execution time ( also known as next poll time).
            If cron_expressioin is specified in local then notification_timezone is also used.
        Args:
            connection ([type]): Redshift Connection
            file_schedule_id (int): File Schedule ID ( used in the update)
            cron_expression (str): Cron Expression , it must begin with either utc or local
                                    For example -> local : 30 10 * * * means Everyday 10:30 in the notification_timezone
                                                    utc  : 30 15 * * * means Everdya 15:30 in the UTC
            notification_timezone (str): pytz compliant timezone string such as 'America/New_York'

        Returns:
            None
        """
        sql = (
            f"update ahub_dw.FILE_SCHEDULE SET IS_RUNNING = False, last_poll_time=next_poll_time, "
            f" next_poll_time= :time, "
            f" updated_timestamp = GETDATE(), updated_by ='AHUB ETL (update_file_schedule_to_next_execution)' "
            f" WHERE FILE_SCHEDULE_ID = :id"
        )
        try:
            next_schedule_time = get_next_iteration(cron_expression, notification_timezone)
            if next_schedule_time is None:
                logger.info(
                    "Unable to assign the Next Schedule Time for file schedule id %d , process is aborting with is_running True",
                    file_schedule_id,
                )
                return
            logger.info(
                " Next Scheduled Poll Time for cron_expression : %s  is : %s",
                cron_expression,
                next_schedule_time,
            )
            update_sql_args = (next_schedule_time, file_schedule_id)

            logger.info(
                "Executing Update: %s with arguments %s",
                sql,
                update_sql_args,
            )
            rows_updated = self.execute(sql, time=next_schedule_time, id=file_schedule_id)
            logger.info("UPDATE with file_schedule_id=%s: %s rows", file_schedule_id, rows_updated)

        except CronExpressionError as e:
            logger.exception(" Error occurred while processing SLA, Error is : %s", e)
            logger.exception(get_traceback(e.__traceback__))
            file_schedule: FileSchedule = FileScheduleRepo(
                secret_name=secret_name).get_file_schedule_info_by_file_schedule_id(file_schedule_id)
            file_schedule.format_timestamps() if file_schedule is not None else None
            # Send alert
            send_email_notification_when_failure_executing_sla(file_schedule, e)


