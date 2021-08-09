import logging
import textwrap

# Time Delta is used by Custom function
from datetime import timedelta  # Note: Do not remove this line
from datetime import datetime
from typing import List, Tuple

from dateutil.parser import parse

from ..constants import (
    SCHEMA_DW,
    LIST_OF_ACCUM_QUALIFIERS,
    TABLE_FILE_COLUMNS,
    TABLE_COLUMN_RULES,
    TABLE_COLUMN_ERROR_CODES,
    TABLE_CLIENT_FILES,
)
from ..database.dbi import AhubDb
from ..glue import get_glue_logger
from ..model import FileColumns, ColumnRule, ClientFile, Job
from ..notifications import notify_file_data_errors
from ..mix import get_error_records_count, get_detail_records_count

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)

current_line = ""
current_value = ""
REQUIRED = "REQUIRED"
CUSTOM = "CUSTOM"
EQUALTO = "EQUALTO"
RANGE = "RANGE"
NUMBER = "NUMBER"
DATE = "DATE"
TIME = "TIME"
BLANK = "BLANK"
REQUIRED_AND_RANGE = "REQUIRED_AND_RANGE"
BLANK_OR_RANGE = "BLANK_OR_RANGE"
MUST_NOT_EQUAL_TO = "MUST_NOT_EQUAL_TO"


## TO DO : Consider converting above constants to Enum


def get_validations(secret_name: str, client_id: int, client_file_id: int) -> dict:
    """
    Return columns a dictionary object consisting of FileColumns objects and it can be accessed through dictionary["column start:column end"].

    :param conn: Redshift Connection
    :param client_id : the client for which , we need to load the FileColumns
    :param client_file_id : the client file id

    A client has a file and file contains  columns ( stored as FileColumns) , and each column has its own position within a single line
    Each column has one or more rules.
    The object that is returned here is a collection of FileColumns, each FileColumn has a collection of rules ( the ColumnRules).
    """

    select_sql = f""" SELECT B.CLIENT_ID, A.CLIENT_FILE_ID,A.FILE_COLUMN_ID, A.FILE_COLUMN_NAME, A.COLUMN_POSITION,C.COLUMN_RULES_ID,
                C.VALIDATION_TYPE,C.PRIORITY, C.EQUAL_TO, C.PYTHON_FORMULA,
                C.LIST_OF_VALUES, C.ERROR_CODE, D.ERROR_MESSAGE, A.IS_ACCUMULATOR,C.ERROR_LEVEL
                FROM  {SCHEMA_DW}.{TABLE_FILE_COLUMNS} A INNER JOIN
                        {SCHEMA_DW}.{TABLE_COLUMN_RULES} C ON
                A.FILE_COLUMN_ID=C.FILE_COLUMN_ID
                        INNER JOIN {SCHEMA_DW}.{TABLE_CLIENT_FILES} B ON
                                A.CLIENT_FILE_ID=B.CLIENT_FILE_ID
                        INNER JOIN {SCHEMA_DW}.{TABLE_COLUMN_ERROR_CODES} D ON
                            C.ERROR_CODE = D.ERROR_CODE
                    WHERE B.CLIENT_ID=:client_id AND A.CLIENT_FILE_ID=:client_file_id 
                AND C.IS_ACTIVE='Y' AND C.VALIDATION_TYPE NOT IN ('EXTERNAL')
                ORDER BY A.FILE_COLUMN_ID, C.PRIORITY  """
    logger.info(
        "Executing SQL = %s,  client_id = %d , client_file_id = %d ",
        select_sql,
        client_id,
        client_file_id,
    )

    # print(query)
    select_sql_args = {
        "client_id": client_id,
        "client_file_id": client_file_id,
    }
    rows = AhubDb(secret_name=secret_name).iquery(select_sql, **select_sql_args)
    record_count = 0
    columns = {}  # Dictionary
    previous_position = 0
    current_position = 0
    filecolumn = None  # Dictionary is indexed by [startPos:endPos] and its value is fileColumn
    columnrule = None  # Each fileColumn will have a columnrules ( it could be one or more)
    for row in rows:
        current_position = row[4]  # Column Position
        # print(" Row Number : ", record_count , " Position : " , current_position)
        if (record_count == 0) or (current_position != previous_position):

            if record_count >= 1:
                columns[filecolumn.column_position] = filecolumn
            # print("Assigned the File object for Position : ", fc.file_position, " Number of Validations : " , len ( fc.column_validations))
            # print(" Created File Object at Row Number : " , record_count)
            filecolumn = FileColumns()
            filecolumn.client_id = int(row[0])
            filecolumn.client_file_id = int(row[1])
            filecolumn.file_column_id = row[2]
            filecolumn.column_name = row[3]
            filecolumn.column_position = row[4]
            filecolumn.column_accumulator = row[13]

            (start_pos, end_pos) = get_pos(filecolumn.column_position)
            filecolumn.column_report_position = f"{start_pos+1}:{end_pos}"
            # fc.column_type=row[4]
            previous_position = row[4]
        if current_position == previous_position:
            columnrule = ColumnRule()
            columnrule.column_rules_id = row[5]
            columnrule.validation_type = row[6]
            columnrule.priority = int(row[7])
            columnrule.equal_to = row[8]
            columnrule.python_formula = row[9]
            columnrule.list_of_values = row[10]
            if (not (columnrule.list_of_values is None)) and (len(columnrule.list_of_values) > 0):
                columnrule.set_range(columnrule.list_of_values)

            columnrule.error_code = row[11]
            columnrule.error_message = row[12]
            columnrule.error_level = row[14]
            filecolumn.column_rules.append(columnrule)
        previous_position = row[4]
        record_count = record_count + 1
    if record_count > 0:
        logger.info("Number of Differnt Attributes to Validate = %d ", record_count)
        columns[filecolumn.column_position] = filecolumn
    else:
        logger.info("No Records Found to Validate")
        columns = None

    logger.info(" END get_validations")

    return columns


def log_result(
    result: [], job_key: int, line_number: int, column_rules_id: int, error_message: str
) -> bool:
    """Records the result in the result array. If length of error message is greater than zero than it records as a validation failed N ( Fail)
    otherwise Y (Pass) in the result array
    :param job_key  : job_key
    :param line_number : Line number in a file
    :param column_rules_id : Column Rules Key
    :param error_message: Error Message
    :return bool : If there is a failure ( meaning error occured known through error message) then there is no point in continue and function will result
    false meaning that break the loop , if there is a success ( meaning validaiton passed) then keep doing the validation on the same columns ( if any)

    """
    if error_message is not None and len(error_message) > 0:
        result.append(f"{job_key}|{line_number}|{column_rules_id}|N|{error_message}")
        #print(f"{job_key}|{line_number}|{column_rules_id}|N|{error_message} return false")
        return False

    result.append(f"{job_key}|{line_number}|{column_rules_id}|Y| ")
    #print(f"{job_key}|{line_number}|{column_rules_id}|Y|{error_message} return true")
    return True


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


def validate_accum_count_on_this_line(
    current_value: str, filecolumns: FileColumns, line: str, line_number: int, job_key: int
) -> Tuple[List[str], bool, int]:
    """Validates the Accumulator Balance Count by looking at each of the Accum Balance Count position


    Arguments:
        current_value {str} -- Current Value of a particular column ( here it will be what is hold in the Accumulator
                               Balance Count position )
        filecolumns {FileColumns} -- A file column object containing the characteristics of the
                                     Accumulator Balance Count's column
        line {str} -- The value of the entire line
        line_number {int} -- curent line number
        job_key {int} -- job key

    Returns:
        Tuple[List[str],bool,int] -- List[str] contains the result , 2nd argument returns whether the validation failed
                                     with Accumulator Balance Count, third  argument returns the accumulator count if the
                                     validations are successful, this accumulator count ( which is a Accumulator Balance
                                  Count is used in follow up processes)
    """
    returnresult = []
    column_validation_failed = False
    total_accum_count = 0
    accumulator_balance_count = 0
    # First we validate the common characteristics of the column Accumulator Balance Count
    (returnresult, column_validation_failed, _) = validate_column(
        current_value, filecolumns, line, line_number, job_key
    )

    # After validating the common characteristics of the the column Accumulator Balance Count, if all is good then only proceed further

    if not column_validation_failed:
        # If column validation failed = True , then no point in running more business processes with the Accumulator Balance Count

        column_rule = filecolumns.column_rules[
            0
        ]  # the first rule is used to drive any error message
        for item in LIST_OF_ACCUM_QUALIFIERS:
            start_pos, end_pos = get_pos(item)

            """
            It is possible that we Accumulator Count is 3, and when we look for the Accuulator Balance Qualifer list
            we don't find the consecutive list of Accumulator , for example
            Accumulator Count  is 3 means Accumulator Balance Qualifer 1, Accumulator Balance Qualifer 2, Accumulator 
            Balance Qualifer 3 to be non-empty
            If Accumulator Count  is 3 and then ccumulator Balance Qualifer 1, Accumulator Balance Qualifer 2 and 
            then Accumulator Balance Qualifer 3 empty and Accumulator Balance Qualifer 4
            non-empty means not a valid count present.
            """
            if len(current_line[start_pos:end_pos].strip()) > 0:
                try:
                    # We check that each non-empty Accumulator contains a valid integers
                    accum_value_at_this_pos = int(current_line[start_pos:end_pos])
                    if accum_value_at_this_pos in (2, 4, 5, 6, 7, 8, 14, 15):
                        total_accum_count = total_accum_count + 1
                    else:
                        break

                except ValueError:  # If not for example , accumulator qualifer containing some junk values
                    total_accum_count = 0
                    break
            else:  # As soon as first empty Accumulator Balance Qualifer is encountered, no longer need to validate any further
                break
        accumulator_balance_count = int(current_value)
        # We have already done the Required and Number validations on the Accumulator Balance Count, so direct int translation will work.
        # If what is specified in the Accumulator Balance Count and number of Accumulator Balance Qualifer does not match
        if accumulator_balance_count != total_accum_count:
            returnresult.clear()  # Clear the result of prior validation ( required and number), we will now directly say that Accuulator Balance failed.

            error_message = (
                f"Column : {filecolumns.column_name}  at position {filecolumns.column_report_position} is {current_value}."
                f" This value does not match the total number of non-empty and valid Accumulator Balance Qualifiers in Accumulator Section ( from 1 up to 12)."
                f" Error Code : {column_rule.error_code}, Error Level : {column_rule.error_level}"
            )
            column_validation_failed = True
            log_result(
                returnresult, job_key, line_number, column_rule.column_rules_id, error_message
            )
        # else:
        #     log_result(returnresult, job_key, line_number, column_rule.column_rules_id, None)
    # print (" accumulator_balance_count =", accumulator_balance_count, " column_validation_failed =",column_validation_failed )
    return (returnresult, column_validation_failed, accumulator_balance_count)


def validate_accum_column_category_type(
    current_value: str, filecolumns: FileColumns, line: str, line_number: int, job_key: int
) -> Tuple[List[str], bool]:
    """Validates Position # 55, Accumulator Specific Category Type , it has a special validation which is as follows :
    For a given list of accumulator balance qualifier, if any of the Accumulator Balance Qualifer contains the 08
    ( Condition Specific Maximum Amount) then  Accumulator Specific Category Type can not be empty and it must belong to
    predefined list of certain values.
    If none of the Accumulator Balance Qualifer contains 08 then Accumulator Specific Category Type must be empty ( blank spaces)


    Arguments:
        current_value {str} -- Current Value of a particular column ( here it will be what is hold in the Position # 55,
                               Accumulator Specific Category Type position )
        filecolumns {FileColumns} -- A file column object containing the characteristics of the   Accumulator Specific
                                     Category Type's column
        line {str} -- The value of the entire line
        line_number {int} -- curent line number
        job_key {int} -- job key

    Returns:
        Tuple[List[str],bool] -- List[str] contains the result , 2nd argument returns whether the validation failed with
                                  Accumulator Specific Category Type, third

    """

    expect_blank = True
    for item in LIST_OF_ACCUM_QUALIFIERS:
        start_pos, end_pos = get_pos(item)
        if line[start_pos:end_pos] == "08":
            expect_blank = False
            break
    columnrules = filecolumns.column_rules
    column_validation_failed = False
    em = None
    # print(" Expecting Blank =" , expect_blank)
    returnresult = []

    for columnrule in columnrules:

        if not expect_blank:
            if (
                columnrule.validation_type == REQUIRED_AND_RANGE
            ):  ## We expect value to be speecified
                em = validate_required_and_range(columnrule, filecolumns, current_value)
        else:
            em = validate_blank(columnrule, filecolumns, current_value)

        if em is not None:
            column_validation_failed = True
            # print (" Error was : ", em)

        log_result(returnresult, job_key, line_number, columnrule.column_rules_id, em)
    return (returnresult, column_validation_failed)


def validate_accum_column(
    current_value: str,
    filecolumns: FileColumns,
    line_number: int,
    job_key: int,
    total_accum_count: int,
) -> Tuple[List[str], bool]:
    """Accumulator Related column is designated in the respective column with IS_ACCUMULATOR = Y property, and accumulator column
         requires special validations.
         Total Accum Count is used to validate that given the list of accumulators how many we need to validate.
         For example Accumulator Balance Count is 3 , then first 3 accumulators will be fully validated, and for the rest of the
         accumulators ( 4 to 12) , system will validate  Accumulator Network Indicator (blank) ,Accumulator Applied Amount (zero),
         Action Code (blank).
       Please note that we validate accumulator colums ONLY if the Accumulator Balance Count is truly valid And, that is known through
       validate_accum_count_on_this_line. The way columns are ordered ( not enfored by developer), the Accumulator Balance Count appears
       first and then those Accumulator Specific Columns .



    Arguments:
        current_value {str} -- Current Value of a particular column ( here it will be what is hold in the Position # 55, Accumulator
                              Specific Category Type position )
        filecolumns {FileColumns} -- A file column object containing the characteristics of the   Accumulator  Column
        line {str} -- The value of the entire line
        line_number {int} -- curent line number
        job_key {int} -- job key

    Returns:
        Tuple[List[str],bool] -- Resultants list ( containing the result) and if there was any failure reported in the result ( bool)
    """
    column_name = filecolumns.column_name
    ## All Accumulator Specific column names are named as Accumulator Balance Qualifier 1, Accumulator Balance Qualifier 2.. so on so just pulling the
    ## integer out of the last two character gives us , the current accum count
    current_accum_number = int(column_name[len(column_name) - 2: len(column_name)])
    columnrules = filecolumns.column_rules
    column_validation_failed = False
    em = None
    returnresult = []

    if current_accum_number <= total_accum_count:
        ## Apply the strict set of validations because the current accum needs to be fully validated..
        for columnrule in columnrules:

            if columnrule.validation_type == REQUIRED:  ## We expect value to be speecified
                em = validate_required(columnrule, filecolumns, current_value)

            ## We expect value to be speecified
            if columnrule.validation_type == REQUIRED_AND_RANGE:
                em = validate_required_and_range(columnrule, filecolumns, current_value)

            if columnrule.validation_type == RANGE:  ##
                em = validate_range(columnrule, filecolumns, current_value)

            if columnrule.validation_type == NUMBER:  ## We expect value to be speecified
                em = validate_numeric(columnrule, filecolumns, current_value)

            if columnrule.validation_type == EQUALTO:  ## We expect value to be speecified
                em = validate_equal_to(columnrule, filecolumns, current_value)

            if (
                columnrule.validation_type == MUST_NOT_EQUAL_TO
            ):  ## If a value is certain value it is considered as an error
                em = validate_must_not_equal_to(columnrule, filecolumns, current_value)

            log_result(returnresult, job_key, line_number, columnrule.column_rules_id, em)
            if em is not None:
                column_validation_failed = True
                break

    else:
        ## For other accumulator we expect it to be blank
        for columnrule in columnrules:
            if columnrule.validation_type in [
                REQUIRED,
                REQUIRED_AND_RANGE,
            ]:  ## We expect value to be blank
                em = validate_blank(columnrule, filecolumns, current_value)
                if em is not None:  ## Expecting blank and value found
                    if column_name.find("Applied Amount") != -1:
                        em = validate_all_zeros(columnrule, filecolumns, current_value)
                log_result(returnresult, job_key, line_number, columnrule.column_rules_id, em)
                break
    if em is not None:
        column_validation_failed = True
    return (returnresult, column_validation_failed)


def validate_required(columnrule: ColumnRule, filecolumns: FileColumns, current_value: str) -> str:
    """Validate whether the current_value is required or not, returns None if the non-empty value is found in the
    current_value otherwise returns an error message.

    Arguments:
        columnrule {ColumnRule} -- validates the  column rule
        filecolumns {FileColumns} -- for the given column ( represetnted as file column)
        current_value {str} -- for a given value

    Returns:
        str -- Returns None if no error otherewise fully formatted error description
    """
    if len(current_value.strip()) == 0:
        return (
            f"Column : {filecolumns.column_name}  at position {filecolumns.column_report_position} can not be empty."
            f" Error Code : {columnrule.error_code} , Error Level : {columnrule.error_level}"
        )
    return None


def validate_equal_to(columnrule: ColumnRule, filecolumns: FileColumns, current_value: str) -> str:
    """Validate whether the current_value matches a specification on a columnrule, returns None if the non-empty value is found in the
    current_value otherwise returns an error message.

    Arguments:
        columnrule {ColumnRule} -- validates the  column rule
        filecolumns {FileColumns} -- for the given column ( represetnted as file column)
        current_value {str} -- for a given value

    Returns:
        str -- Returns None if no error otherewise fully formatted error description
    """
    if len(columnrule.equal_to) > 0:
        current_value = current_value.strip()

        if current_value != columnrule.equal_to:
            return (
                f"Column : {filecolumns.column_name}  at position {filecolumns.column_report_position}"
                f" has the value {current_value},"
                f" expected value is {columnrule.equal_to}"
                f" Error Code : {columnrule.error_code}, Error Level : {columnrule.error_level}"
            )
    return None


def validate_must_not_equal_to(
    columnrule: ColumnRule, filecolumns: FileColumns, current_value: str
) -> str:
    """Returns error message ( to be logged) if  the current_value matches  with a value 
    set in the columnrule.equal_to.
    Returns None if the current value does not match with a value set in the column rule's equal to

    Goal of this routine is to validate that the passed value MUST NOT equal to certain value.
    To put thils validation to use , make sure to set the property in the ahub_dw.column_rules.equal_to
    

    Current Uses : To validate the inbound custom Gila File Accmulator Amount 
    And Patient ID. If value equals certain value , the level 1 error will be raised.

    Arguments:
        columnrule {ColumnRule} -- validates the  column rule
        filecolumns {FileColumns} -- for the given column ( represented as file column)
        current_value {str} -- for a given value

    Returns:
        str -- Returns None if no error ( the value equals to sometehing else) 
               Returns a  fully formatted error description ( if value equals to a certain value)
    """

    if len(columnrule.equal_to) > 0:
        current_value = current_value.strip()

        if current_value == columnrule.equal_to:
            return (
                f"Column : {filecolumns.column_name}  at position {filecolumns.column_report_position}"
                f" has the value {current_value},"
                f" System does not expect this value {columnrule.equal_to} here."
                f" Error Code : {columnrule.error_code}, Error Level : {columnrule.error_level}"
            )
    return None


def validate_range(columnrule: ColumnRule, filecolumns: FileColumns, current_value: str) -> str:
    """Validate whether the current_value matches the range specification of a columnrule, returns None if the non-empty current_value belongs to a range
    otherwise returns an error message.

    Arguments:
        columnrule {ColumnRule} -- validates the  column rule
        filecolumns {FileColumns} -- for the given column ( represetnted as file column)
        current_value {str} -- for a given value

    Returns:
        str -- Returns None if no error otherewise fully formatted error description
    """
    # Blank is not okay here..
    if current_value.strip() not in columnrule.range:
        return (
            f"Column : {filecolumns.column_name}  at position {filecolumns.column_report_position}"
            f" has the value {current_value},"
            f" and it does not belong to the list {columnrule.range}."
            f" Error Code : {columnrule.error_code}, Error Level : {columnrule.error_level}"
        )
    return None


def validate_blank_or_range(
    columnrule: ColumnRule, filecolumns: FileColumns, current_value: str
) -> str:
    """Validate whether the current_value matches the range specification of a columnrule or a blank returns None if the non-empty current_value belongs to a range
      or blank otherwise returns an error message.

    Arguments:
        columnrule {ColumnRule} -- validates the  column rule
        filecolumns {FileColumns} -- for the given column ( represetnted as file column)
        current_value {str} -- for a given value

    Returns:
        str -- Returns None if no error otherewise fully formatted error description
    """
    ## Blank is Okay here..
    if len(current_value.strip()) == 0:
        return None

    return validate_range(columnrule, filecolumns, current_value)


def validate_numeric(columnrule: ColumnRule, filecolumns: FileColumns, current_value: str) -> str:
    """Validate whether the current_value is numeric or not, returns None if the current_value is numeric  otherwise returns an error message.
      Number with decimal point, positive (+) sign, negative (-) sign, $ sign all considered as invalid numbers.

    Arguments:
        columnrule {ColumnRule} -- validates the  column rule
        filecolumns {FileColumns} -- for the given column ( represetnted as file column)
        current_value {str} -- for a given value

    Returns:
        str -- Returns None if no error otherewise fully formatted error description
    """
    if not current_value.isdigit():
        return (
            f"Column : {filecolumns.column_name}  at position {filecolumns.column_report_position}"
            f" has the value {current_value} and it is not a number."
            f" Field can only contain digits and no other characters."
            f" Error Code : {columnrule.error_code}, Error Level : {columnrule.error_level}"
        )
    return None


def validate_blank(columnrule: ColumnRule, filecolumns: FileColumns, current_value: str) -> str:
    """Validate whether the current_value is blank or not, returns None if the current_value is blank  otherwise returns an error message.

    Arguments:
        columnrule {ColumnRule} -- validates the  column rule
        filecolumns {FileColumns} -- for the given column ( represetnted as file column)
        current_value {str} -- for a given value

    Returns:
        str -- Returns None if successful otherewise fully formatted error description
    """
    if len(current_value.strip()) > 0:
        return (
            f"Column : {filecolumns.column_name}  at position {filecolumns.column_report_position}"
            f" has the value {current_value}, but blank/empty space(s) is expected here."
            f" Error Code : {columnrule.error_code}, Error Level : {columnrule.error_level}"
        )
    return None


def validate_all_zeros(columnrule: ColumnRule, filecolumns: FileColumns, current_value: str) -> str:
    """Validate whether the current_value contains all zero, returns None if the current_value meets the specified ZEROs  otherwise returns an error message.

    Arguments:
        columnrule {ColumnRule} -- validates the  column rule
        filecolumns {FileColumns} -- for the given column ( represetnted as file column)
        current_value {str} -- for a given value

    Returns:
        str -- Returns None if successful otherewise fully formatted error description
    """
    if not ((current_value.isdigit()) and (int(current_value) == 0)):
        return (
            f"Column : {filecolumns.column_name}  at position {filecolumns.column_report_position}"
            f" has the value {current_value}  Value filled with 0 (zero) is expected here."
            f" Error Code : {columnrule.error_code}, Error Level : {columnrule.error_level}"
        )
    return None


def validate_date(columnrule: ColumnRule, filecolumns: FileColumns, current_value: str) -> str:
    """Validate whether the current_value is a date, returns None if the current_value is a valid date otherwise returns an error message.

    Arguments:
        columnrule {ColumnRule} -- validates the  column rule
        filecolumns {FileColumns} -- for the given column ( represetnted as file column)
        current_value {str} -- for a given value

    Returns:
        str -- Returns None if successful otherewise fully formatted error description
    """
    if not is_date(current_value, True):
        return (
            f"Column : {filecolumns.column_name}  at position {filecolumns.column_report_position} has the value"
            f" {current_value}, it is not a valid date format."
            f" Error Code : {columnrule.error_code}, Error Level : {columnrule.error_level}"
        )
    return None


def validate_time(columnrule: ColumnRule, filecolumns: FileColumns, current_value: str) -> str:
    """Validate whether the current_value is a time, returns None if the current_value is a valid time otherwise returns an error message.

    Arguments:
        columnrule {ColumnRule} -- validates the  column rule
        filecolumns {FileColumns} -- for the given column ( represetnted as file column)
        current_value {str} -- for a given value

    Returns:
        str -- Returns None if successful otherewise fully formatted error description
    """
    if not is_time(current_value):
        return (
            f"Column : {filecolumns.column_name}  at position {filecolumns.column_report_position}"
            f" has the value {current_value}, it is not a valid time format."
            f" Error Code : {columnrule.error_code}, Error Level : {columnrule.error_level}"
        )
    return None


def validate_required_and_range(
    columnrule: ColumnRule, filecolumns: FileColumns, current_value: str
) -> str:
    """Validate whether the current_value is a required field and belongs to a list of a range , returns None if successful otherwise returns an error message.

    Arguments:
        columnrule {ColumnRule} -- validates the  column rule
        filecolumns {FileColumns} -- for the given column ( represetnted as file column)
        current_value {str} -- for a given value

    Returns:
        str -- Returns None if successful otherewise fully formatted error description
    """
    em = validate_required(columnrule, filecolumns, current_value)
    if em is None:  # Required validation is sucesssfull
        em = validate_range(columnrule, filecolumns, current_value)
    return em


def validate_column(
    v: str, filecolumns: FileColumns, read_line: str, line_number: int, job_key: int
) -> Tuple[List[str], bool]:
    """Validates the columns in a particular file column object.
    :param v  : current value being validated
    :param fileColumns : A file column object containing the property of a column to be validated
    :param read_line : The entire row , the entir row is used to validate any other columns in conjuction with the current value
    :param line_number: Line number within a file
    Returns a job_key, line_number, column_rules_id, error message in a pipe delimited format string array.

    """

    # Globals are declared here to suppor the exec functionality.
    # There are only two functions which uses the exec feature.
    global current_line
    global current_value

    # print(" The Line is : " , current_line)
    columnrules = filecolumns.column_rules

    # print(" len(validations) =" , len(validations))
    em = ""
    column_rules_id = -1
    returnresult = []

    em = ""
    column_validation_failed = False
    skip_remaining_columns = False
    for columnrule in columnrules:
        column_rules_id = columnrule.column_rules_id

        # print("Entered  Current Value " , v , " Validation is : " ,columnrule.validation_type,
        # " File Column Name : " ,filecolumns.column_name ,
        #  " Column Rules Key : ", column_rules_id , ":  Position : " ,filecolumns.column_position,
        # " Error Messag : ", em)
        # print ("Validating for : " , cv.validation_type, ", value is ", v , " READLINE[202:204]",
        # read_line[202:204], " len(v.strip())= " , len(v.strip()))
        if columnrule.validation_type == REQUIRED:
            em = validate_required(columnrule, filecolumns, v)

        if columnrule.validation_type == EQUALTO:
            em = validate_equal_to(columnrule, filecolumns, v)

        if columnrule.validation_type == RANGE:
            em = validate_range(columnrule, filecolumns, v)

        if columnrule.validation_type == NUMBER:
            em = validate_numeric(columnrule, filecolumns, v)

        if columnrule.validation_type == BLANK:
            em = validate_blank(columnrule, filecolumns, v)

        if columnrule.validation_type == DATE:
            em = validate_date(columnrule, filecolumns, v)

        if columnrule.validation_type == TIME:
            em = validate_time(columnrule, filecolumns, v)

        if columnrule.validation_type == BLANK_OR_RANGE:
            em = validate_blank_or_range(columnrule, filecolumns, v)

        if columnrule.validation_type == MUST_NOT_EQUAL_TO:
            em = validate_must_not_equal_to(columnrule, filecolumns, v)

        if columnrule.validation_type == CUSTOM:
            # print("Entered in to custom module : Current Value " , v , " File Column Name :" ,filecolumns.column_name ,
            # " Position : " ,filecolumns.column_position)
            error_occured = False
            skip = False
            current_line = read_line
            current_value = v
            return_dict = {}
            # print("Current Value = " , current_value)
            exec(columnrule.python_formula, globals(), return_dict)
            error_occured = return_dict["error_occured"]
            if "skip" in return_dict.keys():
                skip = return_dict["skip"]

            # Skip is usually going to be false unless transmission file type is DR, with response status as A or R (AHUB-342)
            # Skip the remaining validations on the line
            if skip:
                skip_remaining_columns = True
            if error_occured:
                em = (
                    f"Column : {filecolumns.column_name}  at position {filecolumns.column_report_position}"
                    f" has the value {v}, and it does not pass the require validations."
                    f" Error Code : {columnrule.error_code} , Error Level : {columnrule.error_level}"
                )

        log_result(returnresult, job_key, line_number, column_rules_id, em)

        if em is not None:
            column_validation_failed = True
            # Earlier I was breaking the loop here..

    return (returnresult, column_validation_failed, skip_remaining_columns)


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False


def is_time(the_time):
    """
    Return whether the string can be interpreted as a time in a format of HHMMSSDD.
    Here the last part DD is decimal seconds and is expressed as hundredths (00-99)
    :param the_time: str, the_time in a format of HHMMSSDD
    """
    try:
        formatted_time = ":".join(textwrap.wrap(the_time, 2))  # to turn HHMMSSDD into HH:MM:SS:DD
        _ = datetime.strptime(formatted_time, "%H:%M:%S:%f")
        return True
    except ValueError:
        return False


def validate_submission_number_on_this_line(
    current_value: str,
    filecolumns: FileColumns,
    line: str,
    line_number: int,
    job_key: int,
    first_submission_value: str,
) -> Tuple[List[str], bool, str]:
    """Validates the Accumulator Balance Count by looking at each of the Accum Balance Count position


    Arguments:
        current_value {str} -- Current Value of a particular column
                              ( here it will be what is hold in the Accumulator Balance Count position )
        filecolumns {FileColumns} -- A file column object containing the characteristics of the
                                    Accumulator Balance Count's column
        line {str} -- The value of the entire line
        line_number {int} -- curent line number
        job_key {int} -- job key

    Returns:
        Tuple[List[str],bool,int] -- List[str] contains the result ,
                                  -- bool : 2nd argument returns whether the validation failed with Accumulator Balance Count
                                  -- int : returns the accumulator count if the validations are successful,
                                          this accumulator count ( which is a Accumulator Balance Count is used in follow up processes)
    """

    returnresult = []
    column_validation_failed = False

    error_message = None
    # First we validate the common characteristics of the column Accumulator Balance Count
    (returnresult, column_validation_failed, _) = validate_column(
        current_value, filecolumns, line, line_number, job_key
    )

    if line_number == 2:
        first_submission_value = current_value

    # After validating the common characteristics of the column Accumulator Balance Count, if all is good then 
    # only proceed further 
    if (line_number > 2) and (
        not column_validation_failed
    ):  # If column validation failed = True , then no point in running more business processes with the Accumulator 
        # Balance Count 
        column_rule = filecolumns.column_rules[1]
        # the first rule is used to drive any error message

        if current_value != first_submission_value:
            returnresult.clear()  # Clear the result of prior validation ( required and number), we will now directly say that Accuulator Balance failed.
            error_message = (
                f"Column : {filecolumns.column_name}  at position {filecolumns.column_report_position}"
                f" is {current_value} and it does not  match the first submission value {first_submission_value}."
                f" Error Code : {column_rule.error_code}, Error Level : {column_rule.error_level}"
            )
            column_validation_failed = True
            log_result(
                returnresult, job_key, line_number, column_rule.column_rules_id, error_message
            )

    # print (" accumulator_balance_count =", accumulator_balance_count, " column_validation_failed =",column_validation_failed )
    return (returnresult, column_validation_failed, first_submission_value)


def validate_incoming_file_column(
    read_value: str,
    filecolumns: FileColumns,
    read_line: str,
    line: int,
    job_key: int,
    accum_count: int,
    first_submission_value: str,
) -> Tuple[List[str], bool, int, str]:
    """[summary]

    Arguments:
        read_value {str} -- Current Value at this column
        filecolumns {FileColumns} -- Characteristics of this column
        read_line {str} -- Current content of the entire line
        line {int} -- Line Number
        job_key {int} -- Job Key
        accum_count {int} -- Accum Count

    Returns:
        Tuple[List[str], bool, int]
         -- List[str] returns the pipe delimited result ( to be stored in a file)
            bool returns whether process should be continued on this line ( because if invalid accum
                 are found then we do not continue processing)
            int returns the accum count ( Please note that after colum #Accumulator Balance Count,
                all other columns are Accumulator Specific)
            str return the string for the subm

    """
    returnresult = []
    continue_on_this_line = True
    column_validation_failed = False
    skip_remaining_columns = False
    if filecolumns.column_name.find("Accumulator Balance Count") != -1:
        (returnresult, column_validation_failed, accum_count) = validate_accum_count_on_this_line(
            read_value, filecolumns, read_line, line + 1, job_key
        )
        if column_validation_failed:
            # print ("THIS COLUMN VALIDATION FAILED AT POSITION :" , filecolumns.column_position , " No More validation that needs to be performed")
            continue_on_this_line = False
            # Above flag setting it to fales indicates that no more accumulator processing is required
    else:
        if filecolumns.column_name.find("Accumulator Specific Category Type") != -1:
            (returnresult, column_validation_failed) = validate_accum_column_category_type(
                read_value, filecolumns, read_line, line + 1, job_key
            )
        else:
            if filecolumns.column_name.find("Submission Number") != -1:
                (
                    returnresult,
                    column_validation_failed,
                    first_submission_value,
                ) = validate_submission_number_on_this_line(
                    read_value, filecolumns, line, line + 1, job_key, first_submission_value
                )
            else:
                ## column_accumulator == "N" means regular column
                if filecolumns.column_accumulator == "N":
                    (
                        returnresult,
                        column_validation_failed,
                        skip_remaining_columns,
                    ) = validate_column(read_value, filecolumns, read_line, line + 1, job_key)
                    if skip_remaining_columns:
                        continue_on_this_line = False
                else:
                    ##  Accum column
                    (returnresult, column_validation_failed) = validate_accum_column(
                        read_value, filecolumns, line + 1, job_key, accum_count
                    )
    return (returnresult, continue_on_this_line, accum_count, first_submission_value)


def notify_data_error(job: Job, client_file: ClientFile) -> None:
    """
    This function forms the subject and body of the email to be sent as a notification when data errors are encountered
    in the range of data_error_threshold value obtained from redshift table client_files
    conditions:
    :param conn: connection establishment to redshift table via pg module
    :param file_name: job object which should be populated from model.py
    :param job_key: job object which should be populated from model.py
    """
    record_count_of_error_records = get_error_records_count(job.secret_name, job.job_key)

    if record_count_of_error_records != 0:  # there are no error records in incoming file
        # converting to float type because 'data_error_threshold' (retrieved from db) is of float type
        # This will avoid error during variable comparison and conversion
        error_records_count = float(record_count_of_error_records)
        logger.info("count of error records is %d", error_records_count)

        data_error_threshold = float(client_file.data_error_threshold)
        logger.info("Alert Threshold  is %f", data_error_threshold)

        if error_records_count > 0:
            if data_error_threshold < 1:  # It means Alert Threshold is specified in percentages
                record_count_of_detail_records = get_detail_records_count(job.secret_name, job.job_key)
                # converting to float type because 'data_error_threshold' is of float type
                detail_records_count = float(record_count_of_detail_records)

                threshold_error_record_count = (
                    detail_records_count * data_error_threshold
                )  # formula to get error threshold value
                logger.info(
                    "error_records_count= %d, detail_records_count = %f , threshold_error_record_count = %f ",
                    error_records_count,
                    detail_records_count,
                    threshold_error_record_count,
                )
                if error_records_count >= threshold_error_record_count:
                    logger.info(
                        "error_records_count >= threshold_error_record_count - Email will be sent"
                    )
                    notify_file_data_errors(
                        client_file, int(error_records_count), job.incoming_file_name
                    )
                else:
                    logger.info("no error records found, hence nothing to notify")
            else:  # It means alert threshold is specified in number
                if error_records_count >= data_error_threshold:
                    logger.info(
                        "( error_records_count >= data_error_threshold) - Email will be sent"
                    )
                    notify_file_data_errors(
                        client_file, int(error_records_count), job.incoming_file_name
                    )
                else:
                    logger.info(
                        "notify_data_error :No error records found, hence nothing to notify"
                    )
        else:
            logger.info("notify_data_error :No error records found, hence nothing to notify")

    else:
        logger.info("notify_data_error :No error records found, hence nothing to notify")
