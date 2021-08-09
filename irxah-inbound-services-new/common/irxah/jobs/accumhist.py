"""
Description : This Python script will compare the old PBM accumulation history file
  with crosswalk file and replace the old PBM carrier,
  account and group with IRX carrier, account and group respectively
"""
import csv
import datetime
import io
import logging.config
import os
from typing import List, Optional

import boto3
import dateutil.tz

from ..constants import (
    VIEW_ACCUM_HISTORY_DETAIL_STAGING,
    TABLE_ACCUM_HISTORY_DETAIL_STAGING,
    TABLE_ACCUM_HISTORY_CROSSWALK_STAGING,
    COLUMN_DELIMITER,
    BLANK,
    SCHEMA_STAGING,
)
from ..database.dbi import AhubDb
from ..database.job_log import JobRepository
from ..glue import get_glue_logger
from ..mix import load_s3_file_into_table, cleanup_table
from ..model import Job, ClientFile
from ..notifications import notify_file_loading_error, account_id
from ..s3 import merge_file_parts, delete_s3_files, read_file

logger = get_glue_logger(logging_level=logging.DEBUG)
positive_over_punch_characters = ["{", "A", "B", "C", "D", "E", "F", "G", "H", "I"]
negative_over_punch_characters = ["}", "J", "K", "L", "M", "N", "O", "P", "Q", "R"]
DIVISION_FACTOR = 9999999


def format_number(the_amount: int, is_amount_positive: bool) -> str:
    """Decorates the the_amount with overpunch character based on negative or positive value.
    For example positive 45993 will become 004599C ( here C represents number 3)
    negative 56789 will become 005678R ( here R reprsents number 9)
    C and R are overpunch characters.

    Arguments:
        the_amount {int} -- This is the amount, an integer value.
        is_amount_positive {bool} -- Whether amount is negative or posivie.

    Returns:
        str -- formatted number which is filled with zeros ( prefix) and overpunch character in the end
    """

    # str_amount=f"{the_amount:7.2f}"
    str_amount = str(the_amount)
    formatted_last_character = ""
    str_amount = str_amount.strip().replace(".", "").zfill(7)
    first_six_characters = str_amount[0:6]
    last_character = str_amount[-1]
    if is_amount_positive:
        formatted_last_character = positive_over_punch_characters[int(last_character)]
    else:
        formatted_last_character = negative_over_punch_characters[int(last_character)]
    return f"{first_six_characters}{formatted_last_character}"


def split_amount(amount_to_be_splitted: int, is_positive: bool) -> List[str]:
    """Divides the integer in to multiple amounts , the multiple amount is decided by
    DIVISION FACTOR ( at present 9999999), consider the division factor as a 7 byte value.
    If the integer is bigger than 9999999 such as 20000000 ( 8 Byte) then it should be divided
    as 7 Byte integer ( string) follows :
    9999999 ( 1st Value) = 999999I ( 7 Byte String)
    9999999 (2nd Value) =  999999I ( 7 Byte String)
    0000002 (3rd Value) =  000000C ( 7 Byte String)
    ---------------------------------------------
    Total of above three value ( 7 Byte)  will be 20000000 ( 8 Byte)

    The sum of the above three elements ( 7 Byte numbers) should be equal to 20000000 ( 8 Byte number)

    This function takes integer greater than division factor and returns the list of
    values equipped with overpunch character in the end as shown in the example above.



    Arguments:
        amount_to_be_splitted {int} -- Integer Amount to be splitted, it must not be less than 7 byte character
        is_positive {bool} -- Whether the integer amount is negative or positive. If amount is negative than
                            the overpunch character ( which is the last character) in 7 byte string will be different.

    Returns:
        List[str] -- List of formatted number
    """
    list_of_formatted_amount: List[str] = []

    print("Dividing amount : ", amount_to_be_splitted)
    formatted_constant = ""

    if amount_to_be_splitted <= DIVISION_FACTOR:
        return list_of_formatted_amount
    if is_positive:
        formatted_constant = format_number(DIVISION_FACTOR, True)
    else:
        formatted_constant = format_number(DIVISION_FACTOR, False)

    while amount_to_be_splitted > DIVISION_FACTOR:
        list_of_formatted_amount.append(formatted_constant)
        amount_to_be_splitted = amount_to_be_splitted - DIVISION_FACTOR

    list_of_formatted_amount.append(format_number(amount_to_be_splitted, is_positive))

    return list_of_formatted_amount


def split_records_and_write_in_to_file(
    secret_name: str, s3_accumhub_bucket: str, s3_merge_detail_file: str
) -> int:
    """Executes the query (usig conn) which returns the qualifying records that needs to be
    splitted in to multiple records, and then each amount is splitted in to 7 byte amount.
    Hence if a record contains 1000000 then it is divided in to two records
    and written in to s3:\\s3_accumhub_bucket\s3_merge_detail_file

    Returns the number of records generated, please note that this is not the SQL record count
    It is the total number of records ( lines) after splitting various amounts in to multiple rows.

    Arguments:
        conn {[type]} -- Redshift Connection
        s3_accumhub_bucket {str} -- Bucket Name
        s3_merge_detail_file {str} -- Fully Qualified File Path

    Returns:
        int -- Number of records
    """
    split_query = f"""SELECT rpad(dtl.recd_typ,1)|| rpad(dtl.member_id,18) || rpad(stg.irx_carrier_nbr,9) ||  rpad(stg.irx_account_nbr,15) || 
        rpad(stg.irx_group_id ,15) || rpad(dtl.adj_typ,1) ||  '@@' ||   rpad(dtl.accum_cd,10) || 
        rpad(dtl.adj_dt,7) ||  rpad(dtl.adj_cd,10) ||   rpad(dtl.RESERVED_SP,207) as line_one,   dtl.int_amount 
        FROM {SCHEMA_STAGING}.{VIEW_ACCUM_HISTORY_DETAIL_STAGING} dtl 
        LEFT JOIN (SELECT carrier_nbr,  account_nbr, cvs_group_id, eff_dt, term_dt, cvs_plan, irx_carrier_nbr, irx_account_nbr, 
        irx_group_id, row_num FROM 
        (SELECT carrier_nbr, account_nbr, cvs_group_id, eff_dt, term_dt, cvs_plan, irx_carrier_nbr, irx_account_nbr, irx_group_id, 
        ROW_NUMBER() OVER (PARTITION BY carrier_nbr,account_nbr,cvs_group_id ORDER BY term_dt DESC) row_num 
        FROM {SCHEMA_STAGING}.{TABLE_ACCUM_HISTORY_CROSSWALK_STAGING}) hist  WHERE hist.row_num = 1) stg 
        ON (dtl.carrier_nbr = stg.carrier_nbr AND dtl.account_nbr = stg.account_nbr  AND dtl.group_id = stg.cvs_group_id) 
        where dtl.int_amount not  between -9999999 and 9999999 """
    # In above query @@ is a place holder which will be replaced run time.
    logger.info("split_records_and_write_in_to_file : Executing  Split SQL = %s", split_query)
    rows = AhubDb(secret_name=secret_name).iquery(split_query)

    validation_io = io.StringIO()
    record_count = 0
    validation_writer = csv.writer(
        validation_io, quoting=csv.QUOTE_NONE, delimiter="~", escapechar="", quotechar=""
    )
    for row in rows:
        current_line = row[0]  # Column Position
        list_of_formatted_amount: List[str] = []
        current_amount = int(row[1])

        if current_amount < 0:
            current_amount = current_amount * -1
            logger.info(
                "Current Amount = (Negative)%d, it will be split in to %d lines",
                current_amount,
                int(current_amount / DIVISION_FACTOR) + 1,
            )
            list_of_formatted_amount = split_amount(current_amount, False)
        else:
            logger.info(
                " Current_Amount =(Positive) %d, it will be split in to %d lines",
                current_amount,
                int(current_amount / DIVISION_FACTOR) + 1,
            )
            list_of_formatted_amount = split_amount(current_amount, True)

        for item in list_of_formatted_amount:
            formatted_line = current_line.replace("@@", item)
            record_count = record_count + 1
            validation_writer.writerow([formatted_line])

    # Only if there are records which meets the criteria of NOT between -99999.99  and 99999.99
    if record_count > 0:
        logger.info(" Record Count : %d ", record_count)
        s3_client = boto3.client("s3")
        logger.info(
            " Reading the content from bucket %s , file : %s",
            s3_accumhub_bucket,
            s3_merge_detail_file,
        )
        detail_lines = read_file(s3_accumhub_bucket, s3_merge_detail_file)
        logger.info(" Content of Detail Line is : %s", detail_lines)
        if len(detail_lines) > 0:
            detail_lines = detail_lines + validation_io.getvalue()
        else:
            detail_lines = validation_io.getvalue()

        s3_client.put_object(
            Body=detail_lines,
            ContentType="text/plain",
            Bucket=s3_accumhub_bucket,
            Key=s3_merge_detail_file,
        )
    logger.info("split_records_and_write_in_to_file : Number of Records = %d", record_count)
    return record_count


def process_cvs_acc_history_file(
    job: Job, job_repo: JobRepository, client_file: ClientFile,  job_name: Optional[str] = "Load CVS Accum History and Generate File",
) -> Job:
    """Processes the incoming CVS Accum History file based on the arguments passed.

    Arguments:
        arguments {List[str]} -- List of arguments passed from Lambda
        job {Job} -- Job object carrying important information about the file being processed.

    Keyword Arguments:
        job_name {Optional[str]} -- Reader friendly name of the process being executed by a job

    Returns:
        Job -- Modified Job object which also contains any other important information ( including pass/fail) of a job
    """
    # Earlier below variables were hard coded, now read from database
    s3_accumhub_bucket = job.s3_bucket_name
    iam_arn = client_file.get_redshift_glue_iam_role_arn(account_id)

    eastern = dateutil.tz.gettz("US/Eastern")
    timestamp_file = datetime.datetime.now(tz=eastern).strftime("%y%m%d%H%M%S")
    s3_merge_output_path = client_file.s3_merge_output_path
    s3_merge_file_name = client_file.s3_output_file_name_prefix + f"{timestamp_file}.TXT"
    cross_walk_file = client_file.reserved_variable

    s3_inbound_detail_file = job.get_path(job.detail_file_name)
    s3_merge_header_file = job.get_path(job.header_file_name)
    s3_outbound_detail_file = job.get_path(job.outbound_detail_file_name)
    s3_outbound_trailer_file = job.get_path(job.outbound_trailer_file_name)
    s3_merge_detail_file = job.get_path(job.merge_detail_file_name)
    s3_merge_trailer_file = job.get_path(job.merge_trailer_file_name)
    s3_temp_file_path_job_key = f"{job.file_processing_location}/{str(job.job_key)}_"  ##   args["S3_OUTBOUND_TEMP_FILE_PATH"]

    error_occured = False

    job.post_processing_file_location = s3_merge_output_path

    # Create job detail log
    job_repo.create_job_detail(
        job,
        sequence_number=2,
        job_name=job_name,
        input_file_name=job.output_file_name,
        output_file_name=s3_merge_file_name,
    )

    logger.info("accumhist.py process_cvs_acc_history_file : : Job Object Information is : %s", job)

    logger.info("cvs accumhist.py process_cvs_acc_history_file : : Started ")

    unload_detail_query = f"""UNLOAD('
        select dtl.recd_typ,dtl.member_id,stg.irx_carrier_nbr as carrier_nbr,stg.irx_account_nbr as account, 
        stg.irx_group_id as group_id,dtl.adj_typ,dtl.adj_amount_revised,dtl.accum_cd,dtl.adj_dt,dtl.adj_cd, dtl.RESERVED_SP 
        from {SCHEMA_STAGING}.{VIEW_ACCUM_HISTORY_DETAIL_STAGING} dtl 
        left join 
        (select carrier_nbr, account_nbr, cvs_group_id,eff_dt,term_dt,cvs_plan,irx_carrier_nbr,irx_account_nbr,
        irx_group_id,row_num 
        from (SELECT carrier_nbr,account_nbr, cvs_group_id,eff_dt,term_dt,cvs_plan,irx_carrier_nbr,irx_account_nbr, 
        irx_group_id,ROW_NUMBER() OVER (PARTITION BY carrier_nbr,account_nbr,cvs_group_id ORDER BY term_dt desc) 
        row_num from {SCHEMA_STAGING}.{TABLE_ACCUM_HISTORY_CROSSWALK_STAGING}) hist where hist.row_num=1)stg on (dtl.carrier_nbr=stg.carrier_nbr AND 
        dtl.account_nbr=stg.account_nbr AND dtl.group_id=stg.cvs_group_id) WHERE dtl.int_amount  between -9999999 and 9999999 
        ') 
        TO 's3://{s3_accumhub_bucket}/{s3_outbound_detail_file}' iam_role '{iam_arn}' 
        FIXEDWIDTH 
        '0:1,1:18,2:9,3:15,4:15,5:1,6:7,7:10,8:7,9:10,10:207'
        ALLOWOVERWRITE 
        parallel off;"""

    #  FYI : The code comments below are intentionally kept , as new unload_trailer_query is written.
    #  unload_trailer_query = (
    #     f"UNLOAD('select ''9'' as recd_typ,''Caremark'' as member_id,(select lpad(count(*),10,''0'') from (select "
    #     f"dtl.recd_typ,dtl.member_id,stg.irx_carrier_nbr as carrier,stg.irx_account_nbr as account,stg.irx_group_id as "
    #     f"group,dtl.adj_typ,dtl.adj_amt,dtl.accum_cd,dtl.adj_dt,dtl.adj_cd,dtl.plan_cd,dtl.care_facility,"
    #     f"dtl.member_state_cd,dtl.patient_first_nm,dtl.patient_last_nm,dtl.patient_DOB,dtl.Patient_relationship_cd ,"
    #     f"dtl.RESERVED_SP from {TABLE_ACCUM_HISTORY_DETAIL_STAGING} dtl "
    #     f"left join(select carrier_nbr, account_nbr, cvs_group_id,eff_dt,term_dt,cvs_plan,irx_carrier_nbr,"
    #     f"irx_account_nbr,irx_group_id,row_num "
    #     f"from(SELECT carrier_nbr,account_nbr, cvs_group_id,eff_dt,term_dt,cvs_plan,irx_carrier_nbr,irx_account_nbr,"
    #     f"irx_group_id,ROW_NUMBER() OVER (PARTITION BY carrier_nbr,account_nbr,cvs_group_id ORDER BY term_dt desc) "
    #     f"row_num FROM {TABLE_ACCUM_HISTORY_CROSSWALK_STAGING})hist where hist.row_num=1)stg on dtl.carrier_nbr=stg.carrier_nbr AND "
    #     f"dtl.account_nbr=stg.account_nbr AND dtl.group_id=stg.cvs_group_id)) as carrier_nbr,'' '' as account_nbr "
    #     f"from(select * from (select dtl.recd_typ,dtl.member_id,stg.irx_carrier_nbr as carrier_nbr,stg.irx_account_nbr "
    #     f"as account,stg.irx_group_id as group_id,dtl.adj_typ,dtl.adj_amt,dtl.accum_cd,dtl.adj_dt,dtl.adj_cd,"
    #     f"dtl.plan_cd,dtl.care_facility,dtl.member_state_cd,dtl.patient_first_nm,dtl.patient_last_nm,dtl.patient_DOB,"
    #     f"dtl.Patient_relationship_cd ,dtl.RESERVED_SP "
    #     f"from {TABLE_ACCUM_HISTORY_DETAIL_STAGING} dtl left join(select carrier_nbr, account_nbr, cvs_group_id,eff_dt,term_dt,cvs_plan,"
    #     f"irx_carrier_nbr,irx_account_nbr,irx_group_id,row_num "
    #     f"from (SELECT carrier_nbr,account_nbr, cvs_group_id,eff_dt,term_dt,cvs_plan,irx_carrier_nbr,irx_account_nbr,"
    #     f"irx_group_id,ROW_NUMBER() OVER (PARTITION BY carrier_nbr,account_nbr,cvs_group_id ORDER BY term_dt desc) "
    #     f"row_num FROM {TABLE_ACCUM_HISTORY_CROSSWALK_STAGING})hist where hist.row_num=1)stg on dtl.carrier_nbr=stg.carrier_nbr AND "
    #     f"dtl.account_nbr=stg.account_nbr AND dtl.group_id=stg.cvs_group_id)limit 1)') "
    #     f"TO 's3://{s3_accumhub_bucket}/{s3_outbound_trailer_file}' iam_role '{iam_arn}' "
    #     f"FIXEDWIDTH '0:1,1:9,2:10,3:280' "
    #     f"ALLOWOVERWRITE "
    #     f"parallel off; "
    # )
    try:

        logger.info("cleanup of crosswalk table Started")
        cleanup_table(job.secret_name, SCHEMA_STAGING, TABLE_ACCUM_HISTORY_CROSSWALK_STAGING)
        logger.info("cleanup of crosswalk table  Completed")
        logger.info("cleanup of accums history table Started")
        cleanup_table(job.secret_name, SCHEMA_STAGING, TABLE_ACCUM_HISTORY_DETAIL_STAGING)
        logger.info("cleanup of accums history table is completed")
        logger.info("Loading data in to %s", TABLE_ACCUM_HISTORY_CROSSWALK_STAGING)
        load_s3_file_into_table(
            job.secret_name,
            SCHEMA_STAGING,
            TABLE_ACCUM_HISTORY_CROSSWALK_STAGING,
            s3_accumhub_bucket,
            cross_walk_file,
            iam_arn,
        )
        logger.info("Loading data in to %s - Completed", TABLE_ACCUM_HISTORY_CROSSWALK_STAGING)
        logger.info("Loading data in to %s", TABLE_ACCUM_HISTORY_DETAIL_STAGING)
        load_s3_file_into_table(
            job.secret_name,
            SCHEMA_STAGING,
            TABLE_ACCUM_HISTORY_DETAIL_STAGING,
            s3_accumhub_bucket,
            s3_inbound_detail_file,
            iam_arn,
        )
        logger.info("Loading data in to %s - Completed ", TABLE_ACCUM_HISTORY_DETAIL_STAGING)
        logger.info("Executing unload_detail_query : %s", unload_detail_query)
        AhubDb(secret_name=job.secret_name,tables_to_lock=f"{SCHEMA_STAGING}.{TABLE_ACCUM_HISTORY_DETAIL_STAGING}").execute(unload_detail_query)

        ## Split Record Logic is below

        record_count = split_records_and_write_in_to_file(
            job.secret_name, s3_accumhub_bucket, s3_merge_detail_file
        )

        logger.info("unloading completed for HISTORY,")

        # New version of unload_trailer_query

        header_line = read_file(
            s3_accumhub_bucket,
            s3_merge_header_file,
            text_preproc=lambda line: line.replace(COLUMN_DELIMITER, BLANK),
        )

        logger.info(" Header content is %s", header_line)
        carrier_id = header_line[1:10]
        logger.info(" carrier_id  = Header Content's 2nd to 10th character is : %s", carrier_id)

        unload_trailer_query = f""" UNLOAD ( '
             SELECT  ''9'' ,''{carrier_id}'' ,lpad(count(*)+ {record_count},9,''0''), '''' FROM {SCHEMA_STAGING}.{VIEW_ACCUM_HISTORY_DETAIL_STAGING} dtl 
             LEFT JOIN (SELECT carrier_nbr, account_nbr, cvs_group_id,row_num  
             FROM (SELECT carrier_nbr,account_nbr,cvs_group_id, term_dt, 
             ROW_NUMBER() OVER (PARTITION BY carrier_nbr,account_nbr,cvs_group_id ORDER BY term_dt DESC) row_num  
             FROM {SCHEMA_STAGING}.{TABLE_ACCUM_HISTORY_CROSSWALK_STAGING}) hist WHERE hist.row_num = 1) stg  
             ON (dtl.carrier_nbr = stg.carrier_nbr AND dtl.account_nbr = stg.account_nbr AND dtl.group_id = stg.cvs_group_id) 
             WHERE dtl.int_amount BETWEEN -9999999 AND 9999999  
             ' )
             TO 's3://{s3_accumhub_bucket}/{s3_outbound_trailer_file}' iam_role '{iam_arn}'  
            FIXEDWIDTH '0:1,1:9,2:9,3:281'  
            ALLOWOVERWRITE  
            parallel off; """
        logger.info("Executing unload_trailer_query : %s", unload_trailer_query)

        AhubDb(secret_name=job.secret_name,tables_to_lock=f"{SCHEMA_STAGING}.{TABLE_ACCUM_HISTORY_CROSSWALK_STAGING}").execute(unload_trailer_query)
        logger.info("unloading completed for TLR!")
        logger.info(
            " Merging files : s3_merge_header_file= %s, s3_merge_detail_file = %s, s3_merge_trailer_file = %s, outbound path = %s ",
            s3_merge_header_file,
            s3_merge_detail_file,
            s3_merge_trailer_file,
            os.path.join(s3_merge_output_path, s3_merge_file_name),
        )

        merge_file_parts(
            s3_accumhub_bucket,
            s3_merge_detail_file,
            s3_merge_header_file,
            s3_merge_trailer_file,
            os.path.join(s3_merge_output_path, s3_merge_file_name),
        )
        logger.info("Merging completed!!")
        delete_s3_files(s3_accumhub_bucket, s3_temp_file_path_job_key)
        # delete_files_in_bucket_paths(s3_accumhub_bucket, s3_outbound_temp_file_path)
        logger.info(
            "bci process_bci_file delete_files_in_bucket_paths :clean up process is completed"
        )

    except:
        logger.exception("ERROR OCCURED")
        error_occured = True
        # Send alert notification
        if error_occured and client_file.is_data_error_alert_active:
            notify_file_loading_error(
                client_file, job.error_message, client_file.processing_notification_sns
            )

    job_repo.update_job_detail(job)
    logger.info("outbound temp folder file clean up process is completed")
    return job
