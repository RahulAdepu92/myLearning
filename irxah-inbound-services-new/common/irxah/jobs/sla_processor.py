""" Uses :  Provides necessary function to satisfy all Service Level Agreements such as...
            * Inbound  file must be received by certain time 
            * Outubound file must be generated by certain time
            * A glue job must be executed asynchronously by certain time
"""
from ..database.client_files import ClientFileRepo
from ..database.dbi import AhubDb
from ..glue import get_glue_logger
from ..model import FileSchedule, Job, FileScheduleProcessingTypeEnum, ClientFile
from ..database.file_schedule import FileScheduleRepo
from ..constants import SCHEMA_DW, TABLE_JOB

import logging.config

from datetime import datetime, timedelta
from typing import Tuple, List
from ..common import (
    format_string_time_to_timestamp,
    convert_utc_to_local,
    run_glue_job,
)

from ..notifications import (
    send_email_notification_sla_inbound,
    send_email_notification_cvs,
    send_email_notification_sla_outbound,
    send_email_notification_when_file_received_outside_sla,
    notify_file_loading_error)
from ..s3 import is_inbound_file_in_txtfiles, copy_file

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)


def process_sla(secret_name: str, s3_bucket_name: str) -> None:
    """Invokes by the Lambda function and this is a starting point of processing SLA.

    Args:
        secret_name (str):  Non-empty secret name is used in establishing connection.
                            secret_name specified here is also used in invoking a glue job.
    """

    file_schedule_repo = FileScheduleRepo(secret_name=secret_name)
    # usually any glue failure at the initial stage (due to AWS network issue) will make is_running flag TRUE
    # and leave as is and stop exporting the transactions in next schedule. so, here we attempt to get list of such
    # file_schedule_ids whose is_running remained TRUE in previous run and convert them to FALSE in next schedule.
    # So every 30 mins sla schedule will search for such schedules and do this routine activity.
    file_schedule_ids = file_schedule_repo.get_schedules_running_status()
    if file_schedule_ids:
        for id in file_schedule_ids:
            file_schedule_repo.update_is_running_to_false(id)

    file_schedules: List[FileSchedule] = file_schedule_repo.get_file_schedules_requiring_sla_check()

    if file_schedules is None:
        logger.info("No Active File Schedules Found")
        return

    logger.info(" Total : %d Active File Schedules Found", len(file_schedules))
    for file_schedule in file_schedules:
        # formatting the default object timestamps
        file_schedule.format_timestamps() if file_schedule is not None else None
        process_schedule(secret_name, file_schedule, s3_bucket_name)


def process_schedule(secret_name: str, file_schedule: FileSchedule, s3_bucket_name: str) -> None:
    """Process a given file schedule.
        Based on a characteristics of a file , either a check is made about SLA
        or a glue job is triggered.
        Please note that glue job gets triggered asynchronously , which means that
        after invoking a job the control returns back.


    Args:
        conn ([type]): Redshift Connection
        secret_name (str): Secret Name ( AWS Secret Manager) Name
        file_schedule (FileSchedule): FileSchedule object containing the informatio about the schedule
        s3_bucket_name: retrieved one from environmental variable at Lambda
    """

    logger.info(
        "File Schedule ID : %d, Client File ID : %d, File Processing Type : %s , File Category : %s , File Description : %s ",
        file_schedule.file_schedule_id,
        file_schedule.client_file_id,
        file_schedule.processing_type,
        file_schedule.file_category,
        file_schedule.file_description,
    )
    # If for a given client file ID , there is an active maintenance widnow is going on then do not execute the SLA
    if is_active_maintenance(secret_name, file_schedule.client_file_id):
        logger.info(
            " Active Window Maintenance Found for client_file_id=%d. Hence, No Alert Need to be sent"
        )
        return

    logger.info(
        " No Active Window Maintenance found for file schedule id = %d and  client file id=%d",
        file_schedule.file_schedule_id,
        file_schedule.client_file_id,
    )

    # Glue Job ( Processing Type = 5)
    if file_schedule.processing_type == FileScheduleProcessingTypeEnum.GLUE:
        _run_glue_job_asynchronously(secret_name, file_schedule)
        return

    # SLA Type of Job

    _run_sla_requirement(secret_name, file_schedule, s3_bucket_name)

    # SLA  Requirement ( Processing Type = 5)


def _run_glue_job_asynchronously(secret_name: str, file_schedule: FileSchedule) -> None:
    """Runs a Glue Job

    Args:
        conn ([type]): Redshift Connection
        file_schedule (FileSchedule): File Schedule
    """
    file_schedule_repo = FileScheduleRepo(secret_name=secret_name)

    logger.info(" A Glue Job will be run..")
    glue_job_arguments = {
        "--SECRET_NAME": secret_name,
        "--FILE_SCHEDULE_ID": str(file_schedule.file_schedule_id),
        "--CLIENT_FILE_ID": str(file_schedule.client_file_id),
        "--PROCESSING_TYPE": file_schedule.processing_type,
        "--UPDATE_FILE_SCHEDULE_STATUS": "TRUE",
    }

    # After initiating a glue job , updates it to running.
    run_glue_job(file_schedule.aws_resource_name, glue_job_arguments)  # runs a glue job
    file_schedule_repo.update_file_schedule_to_running(
        file_schedule.file_schedule_id)  # next updates is_running to TRUE


def _run_sla_requirement(secret_name: str, file_schedule: FileSchedule, s3_bucket_name: str) -> None:
    """Code block that runs SLA related requirement

    Args:
        conn ([type]): Redshift
        file_schedule (FileSchedule): Non-empty File Schedule
    """
    file_schedule_repo = FileScheduleRepo(secret_name=secret_name)
    logger.info(" Begin Validating SLA Requirement ")
    # Update it Running
    file_schedule_repo.update_file_schedule_to_running(file_schedule.file_schedule_id)

    # Validate SLA , and if not satisfied  send notification
    validate_sla_and_send_notification(secret_name, file_schedule, s3_bucket_name)

    # Determine the next schedule time and update Running to False
    file_schedule_repo.update_file_schedule_to_next_execution(
        secret_name,
        file_schedule.file_schedule_id,
        file_schedule.cron_expression,
        file_schedule.notification_timezone,
    )
    logger.info(" End Validating SLA Requirement ")


def validate_sla_and_send_notification(secret_name, file_schedule: FileSchedule, s3_bucket_name: str) -> None:
    """Validate the SLA agreement for each file schedule and sends the notification email

    Args:
        secret_name ([type]): secret_name for Redshit Connection
        file_schedule (FileSchedule): NOn-empty FileSchedule
    """
    client_file: ClientFile = ClientFileRepo(secret_name=secret_name).get_client_file_from_id(
        file_schedule.client_file_id)
    # Inbound to AccumHub
    if file_schedule.processing_type == FileScheduleProcessingTypeEnum.INBOUND_TO_AHUB:
        logger.info("checking if any entry is present in job table during SLA window..")
        if not validate_inbound_sla(secret_name, file_schedule):
            inbound_file = client_file.get_expected_file_name_from_pattern()
            logger.info("checking if the file got stuck in txtfiles folder..")
            # is_inbound_file_in_txtfiles is a generator function and returns a generator object.
            # Donot store its output in a variable as it is one time use but can convert it into list using list object
            if len(list(is_inbound_file_in_txtfiles(s3_bucket_name, client_file.extract_folder,
                                                    client_file.name_pattern))) == 0:
                # there are no files stuck in txtfiles folder, so send usual sla email alert
                logger.info("Sending email for not receiving inbound file")
                send_email_notification_sla_inbound(file_schedule, inbound_file)
                return None
            for file in list(is_inbound_file_in_txtfiles(s3_bucket_name, client_file.extract_folder,
                                                         client_file.name_pattern)):
                inbound_file_path = "/".join([client_file.extract_folder, file])
                copy_file(s3_bucket_name, inbound_file_path, s3_bucket_name, inbound_file_path, )
                logger.info("Sending email for receiving the inbound file but failed to process it")
                notify_file_loading_error(file_schedule, file, file_schedule.notification_sns)

    # From AccumHub going to CVS (every 30 min run activity)
    if file_schedule.processing_type == FileScheduleProcessingTypeEnum.AHUB_TO_CVS:
        if not validate_cvs_outbound_sla(secret_name, file_schedule):
            logger.info("Sending email for not sending  the required outbound file to CVS ")
            outbound_file = client_file.get_expected_file_name_from_pattern()
            send_email_notification_sla_outbound(file_schedule, outbound_file)

    # From AHUB going to Respective Client

    if file_schedule.processing_type == FileScheduleProcessingTypeEnum.AHUB_TO_OUTBOUND:
        if not validate_outbound_sla(secret_name, file_schedule):
            logger.info(
                "Sending email for not sending  the required outbound file to respective Client"
            )
            outbound_file = client_file.get_expected_file_name_from_pattern()
            send_email_notification_sla_outbound(file_schedule, outbound_file)

    # CVS Custom Logic ( Checking for did we receive 12 files )
    if file_schedule.processing_type == FileScheduleProcessingTypeEnum.CVS_12_FILES_REQUIREMENT:
        validate_cvs_file_count(secret_name, file_schedule)


def validate_inbound_sla(secret_name: str, file_schedule: FileSchedule, ) -> bool:
    """Returns true if a successful file receipt is found between start time and end time
    Args:
        secret_name ([type]): secret_name for Redshift Connection
        file_schedule (FileSchedule):   File Schedule consisting of non empty start and end time
                                        and a client file ID

    Returns:
        bool: Returns   True if the inbound file is found.
                        False if no inbound file is found
    """

    logger.info(
        " Validating inbound SLA with start time (UTC) : %s , end time (UTC)  : %s for client_file_id = %d",
        file_schedule.start_timestamp,
        file_schedule.end_timestamp,
        file_schedule.client_file_id,
    )

    # WHERE A.START_TIMESTAMP BETWEEN to_char(getdate(),'YYYY-MM-DD HH24:00:00')::timestamp and
    #     dateadd(min,B.GRACE_PERIOD,to_char(getdate(),'YYYY-MM-DD HH24:00:00')::timestamp)

    sql = """SELECT count(*) FROM AHUB_DW.JOB A
        INNER JOIN AHUB_DW.CLIENT_FILES B 
        ON A.CLIENT_FILE_ID=B.CLIENT_FILE_ID 
        WHERE A.START_TIMESTAMP BETWEEN :start_timestamp and :end_timestamp
        AND A.STATUS IN ('Success')
        and a.client_file_id= :client_file_id
        AND NOT EXISTS ( SELECT 1 FROM AHUB_DW.FILE_MAINTENANCE_WINDOW F
        WHERE GETDATE() BETWEEN F.START_TIMESTAMP AND F.END_TIMESTAMP AND F.CLIENT_FILE_ID = :client_file_id)"""

    params = {
        "start_timestamp": file_schedule.start_timestamp,
        "end_timestamp": file_schedule.end_timestamp,
        "client_file_id": file_schedule.client_file_id
    }

    row = AhubDb(secret_name=secret_name).get_one(sql, **params)

    if row["count"] == 0:
        logger.info(
            "query: File Not  Found - Inbound SLA Not Satisfied ; File Not Received Timely, SLA Alert Notification will be sent"
        )
        return False

    logger.info("query: File Found - SLA Satisfied ; SLA Alert Notificatton  will NOT be sent")
    return True


def validate_outbound_sla(secret_name: str, file_schedule: FileSchedule) -> bool:
    """Checks whether the particular outbound file was generated in a given time frame or not.
    Args:
        secret_name ([type]): secret_name for Redshift Connection
        file_schedule (FileSchedule): File Schedule Object
    Returns:
        bool: True if the file is found , False if the file is not found ( not generated)
    """
    logger.info(
        " Validating outbound SLA with start time (UTC) : %s , end time (UTC)  : %s for client_file_id =%d",
        file_schedule.start_timestamp,
        file_schedule.end_timestamp,
        file_schedule.client_file_id,
    )

    # Given a Start Time ( End time minus grace period) and End Time
    # whether the system has generated an outbound file for a given client file ID .

    sql = f"""SELECT count(*) FROM AHUB_DW.JOB A
        INNER JOIN AHUB_DW.CLIENT_FILES B 
        ON A.CLIENT_FILE_ID=B.CLIENT_FILE_ID 
        WHERE A.END_TIMESTAMP BETWEEN :start_timestamp AND :end_timestamp
        AND A.STATUS='Success'
        and a.client_file_id= :client_file_id
        AND NOT EXISTS ( SELECT 1 FROM AHUB_DW.FILE_MAINTENANCE_WINDOW F
         WHERE GETDATE() BETWEEN F.START_TIMESTAMP AND F.END_TIMESTAMP AND F.CLIENT_FILE_ID= :client_file_id)"""

    params = {
        "start_timestamp": file_schedule.start_timestamp,
        "end_timestamp": file_schedule.end_timestamp,
        "client_file_id": file_schedule.client_file_id
    }

    row = AhubDb(secret_name=secret_name).get_one(sql, **params)

    if row["count"] == 0:
        logger.info(
            "Outbound File Not  Found -Outbound SLA Not Satisfied ; SLA Alert Notification will be sent"
        )
        return False

    logger.info(
        "Outbound File Found - Outbound SLA Satisfied ; SLA Alert Notificaiton  will NOT be sent"
    )
    return True


def validate_cvs_outbound_sla(secret_name: str, file_schedule: FileSchedule) -> bool:
    """Checks whether the CVS outbound file was generated within 30 min of inbound file arrival.

    For example, if a BCI file arrives at 2.15AM EST, it will be sent to CVS at 2.30AM EST (nearest 30 min schedule)
    The SLA check which runs at 2.30AM EST will not worry about these transactions because out_job_key will not be assigned by the time this function is executed.
    So the SLA check made at 3 AM EST will check whether the transactions arrived in between 2 and 2.30AM EST are sent to CVS
    or not. It will be done by checking if an out_job_key is assigned for corresponding job_key are not. If not assigned,
    which implies the transactions are not exported, an SLA alert will be send.
        Args:
            secret_name ([type]): secret_name for Redshift Connection
            file_schedule (FileSchedule): File Schedule Object

        Returns:
            bool: True if the file is found , False if the file is not found ( not generated)
        """
    logger.info(
        " Validating outbound CVS SLA with start time (UTC) : %s , end time (UTC)  : %s for client_file_id =%d",
        file_schedule.start_timestamp,
        file_schedule.end_timestamp,
        file_schedule.client_file_id,
    )
    # Condition 1: considering job_keys of DQ records alone because DR records will not be sent to CVS.
    # Instead they are directly reported and sent to client in DR file.
    # Condition 2: exclude job_keys who has error level1 records since they wont be sent to CVS.

    sql = f"""select count(*) from ahub_dw.accumulator_detail a 
              where a.out_job_key is null 
              and a.transmission_file_type ='DQ'
              and a.job_key in
              ( select job_key from ahub_dw.job j INNER JOIN ahub_dw.client_files cf
                ON j.client_file_id = cf.client_file_id
                where
                j.status = 'Success' and cf.file_type = 'INBOUND'
                and cf.client_id not in (2)
                and j.end_timestamp between :start_timestamp AND :less_end_timestamp
                AND NOT EXISTS ( SELECT 1 FROM AHUB_DW.FILE_MAINTENANCE_WINDOW F
                WHERE GETDATE() BETWEEN F.START_TIMESTAMP AND F.END_TIMESTAMP AND F.CLIENT_FILE_ID = :id)
              ) 
              and a.job_key not in 
              ( SELECT job_key FROM 
                (
                  SELECT *,
                  ROW_NUMBER() OVER (PARTITION BY job_key,line_number ORDER BY error_level ASC,file_column_id ASC) rnk
                  FROM 
                    ( SELECT val.validation_result, val.job_key, val.line_number, cr.file_column_id, cr.error_level,
                      cr.error_code, cr.column_rules_id FROM ahub_dw.column_rules cr
                      JOIN ahub_dw.file_validation_result AS val ON cr.column_rules_id = val.column_rules_id
                      WHERE val.validation_result = 'N'
                    )
                ) 
                WHERE rnk = 1 AND error_level = 1
              )"""

    params = {
        "start_timestamp": file_schedule.start_timestamp,
        "less_end_timestamp": file_schedule.less_end_timestamp,
        "id": file_schedule.client_file_id
    }

    row = AhubDb(secret_name=secret_name).get_one(sql, **params)

    if row["count"] > 0:  # there are transactions with out_job_key as NULL(meaning not delivered yet) in given schedule
        logger.info(
            "Undelivered CVS transactions found -Outbound SLA Not Satisfied ; SLA Alert Notification will be sent"
        )
        return False

    logger.info(
        "All CVS transactions are delivered - Outbound SLA Satisfied ; SLA Alert Notification will NOT be sent"
    )
    return True


def get_file_count(
        secret_name: str,
        client_file_id: int,
        start_timestamp: datetime,
        end_timestamp: datetime,
) -> int:
    """This function retursn the number of files between start_timestam and end_timestamp
    :param secret_name: secret_name for redshift conn
    :param client_file_id:  Client File id
    :param start_timestamp:  From time stamp in UTC
    :param end_timestamp: TEnd time stamp in UTC
    :return: Number of files
    """

    sql = """SELECT count(*) FROM AHUB_DW.JOB A WHERE A.CLIENT_FILE_ID=:client_file_id
                        AND A.OUTBOUND_FILE_GENERATION_COMPLETE=FALSE
                        AND A.FILE_STATUS='Not Available'
                        AND A.STATUS='Success'
                        AND A.START_TIMESTAMP BETWEEN  :start_timestamp AND :end_timestamp """

    logger.info(
        "get_file_count:: Start Time Stamp : %s, End Time Stamp : %s ",
        start_timestamp,
        end_timestamp,
    )

    params = {
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "client_file_id": client_file_id
    }

    row = AhubDb(secret_name=secret_name).get_one(sql, **params)

    return row["count"]


def is_active_maintenance(secret_name: str, client_file_id: int) -> bool:
    """This function retrieves whether for the given file is there any active mainteance window going on ?
    :param secret_name: secret_name for redshift connection.
    :param client_file_id: The client file id  which denoes the file type
    :returns: True/False , True indicates active maintenance window, and False indicates no active mainteance window
    """
    # Pass a client file ID and returns the client file information in a Client_File Object

    logger.info(
        " Trying to find whether the client_file_id = %d is in maintenance window ", client_file_id
    )
    sql = """SELECT COUNT(*) FROM AHUB_DW.FILE_MAINTENANCE_WINDOW A
            WHERE GETDATE() BETWEEN A.START_TIMESTAMP AND A.END_TIMESTAMP AND A.CLIENT_FILE_ID = :id """

    row = AhubDb(secret_name=secret_name).get_one(sql, id=client_file_id)
    # returns { 'count': 0 }
    if row["count"] == 0:
        logger.info(" No Active Maintenance Window Found")
        return False

    logger.info(" Active Maintenance Window is Found")
    return True


def validate_cvs_file_count(secret_name: str, file_schedule: FileSchedule) -> None:
    """For a given file schedule whether AHUB has received the required number of files ( Total Files per day)
        If Yes , then update the file file_status in the client file table for the respective client_file_id to Available

    Args:
        secret_name ([type]): secret_name for Redshift Connection
        file_schedule (FileSchedule): Non-empty file schedule
    """

    # End time will be something like 11/17/2020 06:00 UTC
    # one second need to be susbtracted to make it 11/17/2020 05:59
    file_schedule.end_timestamp = file_schedule.end_timestamp + timedelta(seconds=-1)

    logger.info(
        " Validating whether 12 files received between start time (UTC) : %s and  end time (UTC)  : %s  for client_file_id = %d",
        file_schedule.start_timestamp,
        file_schedule.end_timestamp,
        file_schedule.client_file_id,
    )

    # Get the numbmer of files received between start_timestamp and end_timestamp
    count = get_file_count(
        secret_name,
        file_schedule.client_file_id,
        file_schedule.start_timestamp,
        file_schedule.end_timestamp,
    )

    if count <= 0:
        logger.info(
            " Count is ZERO ( this would happen when CVS does not send any files in last 24 hours)"
        )
        return

        # If number of files received and number of files are expected not matching means we need to send an email.
    if count != file_schedule.total_files_per_day:
        logger.info(
            "Total Files Found : %d, Total Files Per Day ( as per dateabase) %d, SLA NOT Satisfied : Email alert will be sent ",
            count,
            file_schedule.total_files_per_day,
        )
        # Local start time and end time obtained and then used in the corresponding email
        file_schedule.current_sla_start_time = convert_utc_to_local(
            file_schedule.start_timestamp, file_schedule.notification_timezone
        )
        file_schedule.current_sla_end_time = convert_utc_to_local(
            file_schedule.end_timestamp, file_schedule.notification_timezone
        )
        send_email_notification_cvs(file_schedule, count)

        # If we find 12 (file_schedule.total_files_per_day ), then it is marked as available
    if count == file_schedule.total_files_per_day:
        logger.info(
            "Total Files Found : %d, Total Files Per Day ( as per database) %d, SLA  Satisfied : Email alert will NOT  be sent ",
            count,
            file_schedule.total_files_per_day,
        )
        # Mark CVS files available between start and end time
        update_job_file_status(
            secret_name,
            file_schedule.client_file_id,
            file_schedule.start_timestamp,
            file_schedule.end_timestamp,
        )


def update_job_file_status(
        secret_name: str, client_file_id: int, start_timestamp: datetime, end_timestamp: datetime
) -> None:
    """Updates the job file status to available for a given client file ID and
        start time (UTC) and end time (UTC)

    Args:
        secret_name ([type]): secret_name for Redshift Connection
        client_file_id (int): Client File ID
        start_timestamp (datetime): Start Time Stamp ( in UTC)
        end_timestamp (datetime): End Time Stamp ( in UTC)
    """

    update_sql = f"""UPDATE AHUB_DW.JOB  SET FILE_STATUS='Available', UPDATED_TIMESTAMP=GETDATE() 
        WHERE CLIENT_FILE_ID= :client_file_id AND OUTBOUND_FILE_GENERATION_COMPLETE = FALSE  
        AND STATUS='Success' 
        AND FILE_STATUS='Not Available' 
        AND START_TIMESTAMP BETWEEN :start_timestamp AND :end_timestamp """

    params = {
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "client_file_id": client_file_id
    }

    logger.info(
        "Executing Update: %s  with arguments %s",
        update_sql,
        params,
    )

    # rows_affected = execute_update(conn, update_sql)
    rows_affected = AhubDb(secret_name=secret_name, tables_to_lock=[f"{SCHEMA_DW}.{TABLE_JOB}"]).execute(update_sql,
                                                                                                         **params)
    logger.info("Execute : Completed : Rows Affected %s", rows_affected)


def validate_file_receipt_after_sla_end(
        secret_name: str, job_key: int, client_file_id: int
) -> Tuple[FileSchedule, bool]:
    """Used by Glue Job. It tries to find out that given a processing type of inbound ( 1) whether the file is received
      successfully after the last poll time and the next poll time.
      For a given job key and  inbound client file ID ( which basically helps determine the other file attrributes).
      In case of two different schedules for one client_file_id (currently we have such case for Gila river) it searches
      for the poll times (either next or past) that matches with current run date(UTC) and picks that alone.
      This way we can pick one eligible schedule for that particular day and avoid misleading or duplicate alerts.

    Args:
        secret_name (str): Secret name for Redshift Connection
        job_key (int):  Inbound Job Key
        client_file_id (int): Used to find out the information about the inbound client file schedule

    Returns:
        Tuple[FileSchedule, bool]: [description]
    """
    file_schedule = None
    file_schedule_repo = FileScheduleRepo(secret_name=secret_name)

    # Query below works with only pg module ( used by Glue job only).

    outside_sla_check_sql = """ SELECT cast(DATEADD( minutes, -1*b.grace_period, B.LAST_POLL_TIME) as varchar) start_time, 
        cast(B.LAST_POLL_TIME as varchar) end_time, cast(a.end_timestamp as varchar) receipt_time,  
        cast(DATEADD( minutes, -1*b.grace_period, B.NEXT_POLL_TIME) as varchar) next_time 
        FROM AHUB_DW.JOB A
        INNER JOIN AHUB_DW.FILE_SCHEDULE B 
        ON A.CLIENT_FILE_ID=B.CLIENT_FILE_ID  
        WHERE  a.end_timestamp  BETWEEN B.LAST_POLL_TIME
        AND  DATEADD( minutes, -1*b.grace_period, B.NEXT_POLL_TIME)
        AND A.STATUS IN ('Success')
        AND a.job_key= :job_key
        AND  B.PROCESSING_TYPE='INBOUND->AHUB'  """

    sql_for_single_schedule = f"""{outside_sla_check_sql}"""
    # As Gila has two different schedules pick the schedule (to check SLA) whose last_poll_time/current_poll_time
    # is equal to current run day. This way we can avoid false/duplicate SLA alerts for having multiple schedules.
    sql_for_multiple_schedules = f"""{outside_sla_check_sql} 
                    AND(substring(B.LAST_POLL_TIME, 1, 10) = :date or substring(B.NEXT_POLL_TIME, 1, 10) = :date) """

    number_of_schedules = file_schedule_repo.get_number_of_schedules(client_file_id)
    logger.info(f"number_of_schedules for client_file_id %s: %s", client_file_id, number_of_schedules)

    # if client_file_id has either zero or one unique schedule
    # to guard against client_file_ids that doesnt have file_Schedules (ex: Standard Gila RIver (=23))
    current_utc_date = datetime.utcnow().strftime("%Y-%m-%d")  # to fetch current run date
    if number_of_schedules <= 1:
        sql = sql_for_single_schedule
    # client_file_id has more than one schedule (ex: Custom Gila RIver (=16))
    else:
        sql = sql_for_multiple_schedules

    rows = AhubDb(secret_name=secret_name).iquery(sql, job_key=job_key, date=current_utc_date)

    if rows is None:  # there is NO job entry (file received) which is outside sla.
        logger.info(
            " File received on-time. Hence, alert [Success after SLA End time (Failure)] will not be sent"
        )
        return file_schedule, False

    # there is a job entry (file received) which is outside sla. Hence, outside sla processing alert is sent.
    for row in rows:
        start_time, end_time, receipt_time, next_time = row
        logger.info(
            "File not received on-time - SLA Failure Criteria Satisfied. Hence, alert [Success after SLA End time (Failure)] will be sent"
        )
        # Get the File Schedule only if the record is found
        logger.info(
            " Start Time of SLA = %s, End Time of SLA = %s, File Receipt Time = %s. File is considered in SLA violation till %s ",
            start_time,
            end_time,
            receipt_time,
            next_time,
        )
        file_schedule: FileSchedule = file_schedule_repo.get_file_schedule_info(
            client_file_id, FileScheduleProcessingTypeEnum.INBOUND_TO_AHUB)
        file_schedule.current_sla_start_time = convert_utc_to_local(
            format_string_time_to_timestamp(start_time),
            file_schedule.notification_timezone,
            "%m/%d/%Y %I:%M %p",
        )
        logger.info("current_sla_start_time: %s", file_schedule.current_sla_start_time)
        file_schedule.current_sla_end_time = convert_utc_to_local(
            format_string_time_to_timestamp(end_time), file_schedule.notification_timezone
        )
        logger.info("current_sla_end_time: %s", file_schedule.current_sla_end_time)
        # pg module issue, rows returns always non-None value hence return file_schedule, True can not be outside for loop.
        return file_schedule, True

    return None, False


def check_and_notify_file_receipt_after_sla_end(job: Job) -> bool:
    """For a given job ( which contains the job key and client file ID and non-emtpy file name that was received),
       this function attempts  to find that whether the file was received after the SLA end time.
       For example:
       If file's SLA is that it should be received daily between 12:00 AM to 10:30 AM.
       If file received time is 11/29/2020 01:00 PM which means the file is received between
       11/29/2020 10:30 AM and 11/30/2020 12:00 AM hence it will be considered as received afer SLA.
    Args:
        conn ([type]): Connection
        job (Job): Job consisting of non-empty job key and client file ID and file name.

    Returns:
        bool: returns True if the file receipt is found.

    """
    logger.info(" Entered in to check_and_notify_file_receipt_after_sla_end ")
    file_schedule, result = validate_file_receipt_after_sla_end(job.secret_name, job.job_key, job.client_file_id)
    if result:
        job_end_timestamp = datetime.strptime(job.job_end_timestamp, "%Y-%m-%d %H:%M:%S.%f")
        send_email_notification_when_file_received_outside_sla(
            file_schedule,
            job.incoming_file_name,
            convert_utc_to_local(job_end_timestamp, file_schedule.notification_timezone),
            job.file_record_count,
        )
    return result
