import logging.config
import os
from datetime import datetime
from typing import Optional

from .constants import TABLE_FILE_VALIDATION_RESULT, TABLE_ACCUM_DETAIL_DW, SCHEMA_DW, TABLE_JOB, SUCCESS
from .database.dbi import AhubDb
from .glue import get_glue_logger
from .model import Job, ClientFile
from .notifications import send_email_notification_for_process_duration

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)


def notify_process_duration(
        job: Job,
        client_file: ClientFile,
):
    """
    This function determines if a job has reached the threshold before sending an email alert notifying users
    that the process exceeded the set amount.
    :param job: Non-empty Job object
    """

    # Formating timestamp into Datetime object and grabbing the difference as elapsed time
    start_time = datetime.strptime(job.job_start_timestamp, "%Y-%m-%d %H:%M:%S.%f")
    end_time = datetime.strptime(job.job_end_timestamp, "%Y-%m-%d %H:%M:%S.%f")
    elapsed_time = (end_time - start_time).total_seconds()

    # Send notification when client_file_id exists in db
    if job.client_file_id > 0:
        logger.info(
            "Elapsed time of process: %d, Theshold value is %d",
            elapsed_time,
            client_file.process_duration_threshold,
        )
        if elapsed_time >= client_file.process_duration_threshold:
            logger.info(
                "Job exceeded process duration threshold of %d seconds",
                client_file.process_duration_threshold,
            )
            send_email_notification_for_process_duration(client_file, job.job_key, elapsed_time)


def get_file_count(
        secret_name: str,
        client_file_id: int,
        start_timestamp: datetime,
        end_timestamp: datetime,
) -> int:
    """This function returs the number of successful jobs between start_timestamp and end_timestamp forthe given client file id
    :param client_file_id:  Client File id
    :param start_timestamp:  From time stamp
    :param end_timestamp: End time stamp
    :return: Number of files
    """

    sql = f"""SELECT count(*) FROM AHUB_DW.JOB A WHERE A.CLIENT_FILE_ID=:id
                        AND A.STATUS='Success'
                        AND A.START_TIMESTAMP BETWEEN  :start_timestamp AND :end_timestamp """

    logger.info(
        "get_file_count:: Start Time Stamp : %s, End Time Stamp : %s ",
        start_timestamp,
        end_timestamp,
    )

    params = {
        "id": client_file_id,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
    }
    logger.info("query: (%s) with arguments (%s)", sql, params)
    row = AhubDb(secret_name=secret_name).get_one(sql, **params)

    if row["count"] == 0:
        return 0

    return row["count"]


def is_first_incoming_client_file_of_the_day(job) -> bool:
    """This function returns whether the arrived file is the first file or not from the client on the run day.
    :param job:  The Job Object
    :return: boolean value
    """
    # The motto here is to get the count of files (job_key) arrived on current day in form of boolean value.
    # If first file of the day its is TRUE else FALSE
    # pass current UTC date and client_file_id as parameter
    # Note: job_keys whose status with either 'success' (in previous run)/'running' (current run) are considered to get the count

    current_utc_date = datetime.utcnow().strftime("%Y-%m-%d")  # to fetch current run date

    sql = f"""  select count(*) from
                (select job_key,file_arrival_date,client_file_id,
                row_number() over (partition by file_arrival_date,client_file_id order by start_timestamp) rnum
                from (
                select j.job_key, substring(j.start_timestamp, 1, 10) as file_arrival_date, j.client_file_id, 
                j.start_timestamp 
                from AHUB_DW.JOB j
                where j.file_type = 'INBOUND' 
                and j.status not in ('Fail')
                and j.client_file_id not in (0,3) 
                and substring(j.start_timestamp, 1, 10) = :current_utc_date
                and j.client_file_id = :id)A 
                )B
                where job_key= :job_key and
                B.rnum > (select top 1 total_files_per_day from ahub_dw.file_schedule 
                where client_file_id = :id and processing_type= 'INBOUND->AHUB') """

    params = {
        "current_utc_date": current_utc_date,
        "job_key": job.job_key,
        "id": job.client_file_id,
    }
    logger.info("is_first_incoming_client_file_of_the_day:: query: (%s) with arguments (%s)", sql, params)

    row = AhubDb(secret_name=job.secret_name).get_one(sql, **params)

    if row["count"] == 0:
        # if count of files whose rnum > 1 is zero, the arrived file (referred by job_key) is the first file of the day
        return True

    # if count of files whose rnum > 1 is NOT zero, the arrived file is not the first file of the day
    # its count will be more than the allowed count (total_files_per_day) for that client
    return False


def get_error_records_count(secret_name: str, job_key: int) -> int:
    """
    This function gets the total number of error records in the incoming file that got recorded in 'ahub_dw.file_validation_result' table:
    :param secret_name: secret_name for redshift connection
    :param job_key: job object which should be populated from model.py
    """
    # this query returns None when count(job_key) is zero. So beware when writing row["count"]
    # row["count"]!=0 throws None type object Nonsubscriptable error.
    error_records_query = f"""select count from (
                select job_key,count(job_key)count from( 
                    select job_key from( 
                        select job_key,line_number,row_number() over (partition by job_key,line_number) rnum 
                        FROM  {SCHEMA_DW}.{TABLE_FILE_VALIDATION_RESULT} 
                        where job_key =:job_key and validation_result='N' )x 
                    where rnum =1 )y 
                group by job_key )z"""

    row = AhubDb(secret_name=secret_name).get_one(error_records_query, job_key=job_key)

    if row is None or row["count"] == 0:
        return 0

    logger.info("error_records_count_query: Result is not None")
    return row["count"]


def get_detail_records_count(secret_name: str, job_key: int) -> int:
    """
    This function gets the total number of detail records in the incoming file that got recorded in 'ahub_dw.accumulator_detail' table:
    :param secret_name: secret_name for connection establishment to redshift table
    :param job_key: job object which should be populated from model.py
    """
    detail_records_query = (
        f"select count(1) from  {SCHEMA_DW}.{TABLE_ACCUM_DETAIL_DW} where job_key=:job_key"
    )
    logger.info(
        "( data_error_threshold <1 ) Executing the Query : %s ",
        detail_records_query,
    )
    row = AhubDb(secret_name=secret_name).get_one(detail_records_query, job_key=job_key)

    return row["count"]


def mark_unavailable_cvs_files_status_to_available(
        secret_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        list_of_files: Optional[str] = "",
        num_of_files: Optional[int] = 0,
) -> str:
    """
    This function marks the unavailable file status to available as well as update the updated_timestamp column in the job table.
    :param secret_name: secret_name for connection to redshift.
    :param start_time: The bounded datetime.
    :param end_time: The bounded datetime.
    :param list_of_files: Comma separated file names, no surrounding quotes.
    :param num_of_files: Number of file names which are distinguished by comma separated
    """
    em = ""

    # Search if all files are valid
    if list_of_files:
        select_query = (
            f"select count(distinct inbound_file_name) from {SCHEMA_DW}.{TABLE_JOB} where file_status='Not Available' "
            f" and client_file_id= 3 and "  # we mark Available for CVS Inbound alone
            f" outbound_file_generation_complete=false and status='Success' and inbound_file_name in ({list_of_files})"
        )

        logger.info(
            " :: Provided list of cvs files :: query: %s",
            select_query,
        )

        row = AhubDb(secret_name=secret_name).get_one(select_query, )
        print("count: %s", row["count"])  # remove this

        if row["count"] != num_of_files or row["count"] == 0:
            em = (
                f"ERROR: One of the provided files in the list '{list_of_files}' with file_status='Not Available' cannot be found."
                f" Update the given input file(s) status to NOT AVAILABLE and rerun."
            )
            return em

    # Update Not Available to Available
    params = ""
    if not list_of_files:
        update_query = (
            f"update {SCHEMA_DW}.{TABLE_JOB} set file_status='Available', updated_timestamp=GETDATE() where file_status='Not Available' "
            f" and client_file_id= 3"  # we mark Available for CVS Inbound alone
            f" and outbound_file_generation_complete=false and status='Success'"
            f" and start_timestamp between :start_time and :end_time"
        )
        params = {
            "start_time": start_time,
            "end_time": end_time,
        }
        rows_affected = AhubDb(secret_name=secret_name, tables_to_lock=[f"{SCHEMA_DW}.{TABLE_JOB}"]).execute(
            update_query, **params)
        em = "ERROR: No records were updated in the table with the provided timeframe"
    elif not start_time and not end_time:
        update_query = (
            f"update {SCHEMA_DW}.{TABLE_JOB} set file_status='Available', updated_timestamp=GETDATE() where file_status='Not Available' "
            f" and client_file_id= 3"  # we mark Available for CVS Inbound alone
            f" and outbound_file_generation_complete=false and status='Success'"
            f" and inbound_file_name in ({list_of_files})"
        )
        rows_affected = AhubDb(secret_name=secret_name, tables_to_lock=[f"{SCHEMA_DW}.{TABLE_JOB}"]).execute(
            update_query)  # params not required
        em = "ERROR: No records were updated in the table with the list of file names"
    else:
        logger.info(" Not enough arguments were given!")
        em = "ERROR: Not enough arguments were given for the query"
        return em

    logger.info(
        " query: %s, args: %s",
        update_query,
        params,
    )

    logger.info(
        "Rows Affected %s",
        rows_affected,
    )

    if rows_affected is not None and int(rows_affected) > 0:
        em = ""

    return em


def load_s3_file_into_table(
        secret_name: str, db_schema: str, redshift_table: str, s3_in_bucket: str, s3_in_file: str, iam_role
) -> None:
    """
    Loads file from S3 bucket and into specified redshift tables
    :param conn: an opened conn (see create_redshift_connection)
    :param db_schema: The schema for the database
    :param redshift_table: The table for client files. This table should have the import_columns information.
    :param s3_in_bucket: s3 bucket name of incoming file
    :param s3_in_file: s3 file name with path of incoming file
    :param iam_role: IAM role which allows file copy operation from s3 to redshift database
    """
    copy_query = f"COPY {db_schema}.{redshift_table} FROM 's3://{s3_in_bucket}/{s3_in_file}' iam_role '{iam_role}' delimiter '|';"
    logger.info(" Query is : %s", copy_query)

    AhubDb(secret_name=secret_name, tables_to_lock=[f"{db_schema}.{redshift_table}"]).execute(copy_query, )

    logging.info(
        "Loaded %s into %s", os.path.join(s3_in_bucket, s3_in_file), f"{db_schema}.{redshift_table}"
    )


def load_s3_file_into_table_column_specific(
        secret_name: str,
        db_schema: str,
        redshift_table: str,
        columns: str,
        s3_in_bucket: str,
        s3_in_file: str,
        iam_role,
) -> None:
    """Loads file from S3 bucket and into specified redshift tables, column specific
    :param connection: The connection to Redshift
    :param db_schema: The schema for the database
    :param redshift_table: The table for client files. This table should have the import_columns information.
    :param columns: specific columns into which the data is copied from s3 file
    :param s3_in_bucket: s3 bucket name of incoming file
    :param s3_in_file: s3 file name with path of incoming file
    :param iam_role: IAM role which allows file copy operation from s3 to redshift database

    Constructs the SQL dialect as follows :

            copy ahub_dw.FILE_VALIDATION_RESULT (job_key,line_number,column_number,validation_result,error_message)
        FROM 's3://irx-accumhub-dv-data2/log/data.txt'
        iam_role 'arn:aws:iam::474156701944:role/IRX-IDW-Redshift'
        region 'us-east-1'
        delimiter '|';"""
    copy_query = f"COPY {db_schema}.{redshift_table} ({columns}) FROM 's3://{s3_in_bucket}/{s3_in_file}' iam_role '{iam_role}' delimiter '|';"
    logger.info(" Query is : %s", copy_query)

    AhubDb(secret_name=secret_name, tables_to_lock=[f"{db_schema}.{redshift_table}"]).execute(copy_query, )

    logging.info(
        "Loaded %s into %s", os.path.join(s3_in_bucket, s3_in_file), f"{db_schema}.{redshift_table}"
    )


def insert_id(
        secret_name: str,
        insert_sql: str,
        insert_sql_args,
        select_sql: Optional[str] = None,
        select_sql_args=None,
):
    """
    Executes an insert statement and an optional select statement if the insert was successful.
    :param redshift_connection: an opened conn (see create_redshift_connection)
    :param insert_sql: the insert sql statement on a table
    :param insert_sql_args: arguments passed to the insert sql statement
    :param select_sql: optional parameter. runs the select sql statement
    :param select_sql_args: takes the select sql statement arguments
    :returns: the first column if select_sql was present, or the number of inserted records otherwise"""
    logger.info(
        " insert_sql = %s",
        insert_sql,
    )

    rows_affected = AhubDb(secret_name=secret_name, tables_to_lock=[f"{SCHEMA_DW}.{TABLE_JOB}"]).execute(
        insert_sql, **insert_sql_args)

    logger.info(
        " rows_affected = %s",
        rows_affected,
    )
    if int(rows_affected) == 1 and select_sql is not None:
        logger.info(" select_sql = %s", insert_sql)
        row = AhubDb(secret_name=secret_name).get_one(select_sql, **select_sql_args)

        if row["key"] is not None:
            logger.info(
                " job_detail_key = %s",
                row["key"],
            )
            return row["key"]

        return rows_affected
    return rows_affected


def get_client_files_import_columns(
        db_schema: str,
        redshift_table: str,
        job: Job,
) -> str:
    """This function allows you to read the client_files table and retrieve the column names for a specific job"
    :param connection: The connection to Redshift
    :param db_schema: The schema for the database
    :param redshift_table: The table for client files. This table should have the import_columns information.
    :param job: The job object to search for import columns. It should contain the client_file_id, client_id, sender_id, and receiver_id.
    :param secret_name: Optional param. Specify the secret_name for the Redshift connection.
    """
    select_sql = f"select import_columns from {db_schema}.{redshift_table} where client_file_id=:client_file_id"

    return_value = ""

    select_sql_args = {
        "client_file_id": job.client_file_id,
    }
    logger.info(
        " sql = %s :: args = %s",
        select_sql,
        select_sql_args,
    )
    row = AhubDb(secret_name=job.secret_name).get_one(select_sql, **select_sql_args)

    if row is not None:
        logger.info(
            " record = %s",
            row["import_columns"],
        )
        return_value = row["import_columns"]

    return return_value


def record_duplicate_transmission_identifier(
        secret_name: str,
        column_rule_id: int,
        column_position: str,
        error_code: int,
        error_level: int,
        job_key: int,
) -> None:
    """This method takes the job key, identifies duplicate in the permanent table and reports it in to the validation result
    :param connection: The connection to Redshift
    :param column_rule_id: The column rule id , the rule which is failing
    :param column_position: position value of column TRANSMISSION_ID
    :param error_code : Error code to report for duplicate transmission
    :param error_level : Error Level to report for duplicate transmission
    :param job_key: job_key
    """
    insert_sql = (
        f"INSERT INTO {SCHEMA_DW}.{TABLE_FILE_VALIDATION_RESULT} (job_key,line_number,column_rules_id, validation_result,error_message) "
        f"SELECT {job_key}, CR.line_number, {column_rule_id}, 'N', 'Column : Transmission ID  at position {column_position} is duplicate. "
        f" Duplicate Value is ' || CR.TRANSMISSION_IDENTIFIER || ' Error Code : {error_code}, Error Level : {error_level} ' "
        f"FROM "
        f" (SELECT a.line_number,"
        f"  ROW_NUMBER() OVER(PARTITION BY a.TRANSMISSION_IDENTIFIER ORDER BY A.LINE_NUMBER )"
        f"AS ROW_NUM , count(a.TRANSMISSION_IDENTIFIER) over ( partition by a.TRANSMISSION_IDENTIFIER ) AS V_COUNT ,  a.TRANSMISSION_IDENTIFIER "
        f"from {SCHEMA_DW}.{TABLE_ACCUM_DETAIL_DW} a WHERE a.job_key=:job_key ) CR "
        f"WHERE CR.V_COUNT > 1"
        f"AND CR.ROW_NUM > 1 "
        f"ORDER BY   CR.V_COUNT DESC,CR.ROW_NUM ASC"
    )

    logger.info(
        "sql = %s :: args = %s",
        insert_sql,
        job_key,
    )
    rows_affected = AhubDb(secret_name=secret_name,
                           tables_to_lock=[f"{SCHEMA_DW}.{TABLE_FILE_VALIDATION_RESULT}"]).execute(
        insert_sql, job_key=job_key)

    logger.info("record_duplicate_transmission_identifier:: Job key, %d, updated with %d rows", job_key, rows_affected)


def get_client_file_ids_require_data_validaion(secret_name: str) -> list:
    """
        This function gets the list of client file ids given the file_type
        :param conn: The connection to redshift.
        :param file_type: The file type (INBOUND/OUTBOUND).
    """
    sql = f"select distinct client_file_id from ahub_dw.file_columns"
    db = AhubDb(secret_name=secret_name)
    rows = db.iquery(sql, )

    if rows is None:
        return None

    return [item for i in rows for item in i]


def cleanup_table(
        secret_name: str,
        db_schema: str,
        table_name: str,
        conditional_column: Optional[str] = None,
        conditional_value: Optional[str] = None,
):
    """
    Cleans up the specified table.
    :param conn: an opened conn (see create_redshift_connection)
    :param db_schema: name of database scheme
    :param table_name: name of table within db_schema
    :param conditional_column: Optional parameter. Takes the name of specific column on which delete operation is to be performed
    :param conditional_value: Optional parameter. Specifies if any special condition is required before performing deletion operation
    """
    # TODO: beware of Little Bobby Tables injection: https://xkcd.com/327/
    if conditional_column is not None and conditional_value is not None:
        delete_query = f"DELETE FROM {db_schema}.{table_name} WHERE {conditional_column}=:conditional_value"
        logger.info("Delete Query is : %s", delete_query)

        rows_affected = AhubDb(secret_name=secret_name,
                               tables_to_lock=[f"{db_schema}.{table_name}"]).execute(
            delete_query, conditional_value=conditional_value)

        logger.info("cleanup_table:: Job key, %d, deleted %d rows", rows_affected)
    else:
        truncate = "truncate table {}.{} ".format(db_schema, table_name)
        logger.info("Truncate Query is : %s", truncate)

        rows_affected = AhubDb(secret_name=secret_name,
                               tables_to_lock=[f"{db_schema}.{table_name}"]).execute(
            truncate, conditional_value=conditional_value)

        logger.info("cleanup_table:: Job key, %d, truncated %d rows", rows_affected)


def log_job_status_begin(job: Job) -> str:
    """
    This function has no business impact but will make the life of a developer very easy. Earlier when you look at the job log
    in CloudWatch, it was very difficult to understand which is the start and end step after each run. But now log will clearly show the
    details like job_key, process_name, status and output_file_name etc..
    """
    log_result = f"{'*' * 100} "
    return f"{log_result} {os.linesep}  Process : {job.job_name} [ Job Key : {job.job_key}] :  Started  : Incoming File Name is : {get_name(job.pre_processing_file_location)} {os.linesep}{log_result}"


def log_job_status_end(job: Job, custom_message: Optional = None) -> str:
    """
    This function has no business impact but will make the life of a developer very easy. Earlier when you look at the job log
    in CloudWatch, it was very difficult to understand which is the start and end step after each run. But now log will clearly show the
    details like job_key, process_name, status and output_file_name etc..
    """
    log_result = f"{'*' * 100} "
    if custom_message is None:
        if job.job_status != SUCCESS:
            return f"{log_result}{os.linesep} Process : {job.job_name} [ Job Key : {job.job_key}]:  Result :{job.job_status}  (No Outbound File Generated) Error Message (if any) : {job.error_message}{os.linesep} {log_result}"

        return f"{log_result}{os.linesep}Process : {job.job_name} [ Job Key : {job.job_key}]:  Result :  {job.job_status} Output File : {get_name(job.output_file_name)}  {os.linesep}{log_result}"

    return f"{log_result}{os.linesep} Process : {job.job_name} [ Job Key : {job.job_key}]: Result : {custom_message} {os.linesep}{log_result}"


def get_name(file_object) -> str:
    if isinstance(file_object, list):
        return file_object
    if not file_object or file_object.isspace():
        return "EMPTY (Not Required)"
    return file_object
