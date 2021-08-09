"""
model.py is the module centralizing the all the model classes in the project.
The invoker of any model class has to import it from model.py

example:
from model import TextFiles

"""
import json
import enum
import logging
import os
from datetime import datetime, timedelta
from typing import List, NamedTuple, Optional

import pytz

from .constants import SUCCESS, FAIL
from .glue import get_glue_logger

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)


@enum.unique
class ClientFileIdEnum(enum.IntEnum):
    """
    Access enum members using value.
    For example: to get cleint_file_id =3, write ClientFileIdEnum.INBOUND_CVS_INTEGRATED.value
    Whereas for comparison (==), ClientFileIdEnum.INBOUND_CVS_INTEGRATED is enough
    """
    # ahub_dw.client_files table is the source of info for below values i.e, whatever the value we assign for each
    # client file in db, the same is written over here.
    INBOUND_BCI_INTEGRATED = 1
    INBOUND_CVS_INTEGRATED = 3
    OUTBOUND_BCI_INTEGRATED = 4
    OUTBOUND_BCI_ERROR = 5
    INBOUND_CVS_RECON_INTEGRATED = 6
    INBOUND_BCI_ERROR = 7
    INBOUND_CVS_ACCUMHISTORY = 8
    INBOUND_BCI_GP = 9
    INBOUND_CVS_RECON_NON_INTEGRATED = 10
    INBOUND_CVS_NON_INTEGRATED = 11


class FileSplitEnums(enum.IntEnum):
    HEADER = 0
    DETAIL = 1
    TRAILER = 2


class FileSplitEnum(str, enum.Enum):
    HEADER = "HDR"
    DETAIL = "DTL"
    TRAILER = "TLR"


class FileReadEnum(enum.IntEnum):
    READ_START = 0
    READ_END = 1


class ClientFileFileTypeEnum(str, enum.Enum):
    """Defines the type of files that a client file is"""

    OUTBOUND = "OUTBOUND"
    INBOUND = "INBOUND"


class TextFiles:
    """TextFiles class has the following attributes
    read positions : the two dimension array of the positions
    file type : the type of the file ( HEADER, DETAIL, TRAILER)
    the file path ( usually set through runtime environments)
    file name  ( name of the file)"""

    def __init__(
            self,
            read_positions: List[List[int]],
            file_type: FileSplitEnums,
            bucket_name: str,
            file_path: str,
            file_name: str,
    ):
        self.read_positions = TextFiles.return_positions(read_positions)
        self.file_type = file_type
        self.file_path = file_path
        self.file_name = file_name
        self.bucket_name = bucket_name

    @staticmethod
    def return_positions(line) -> List[List[int]]:
        """This method returns the two dimension array of the positions
        The line contains the start position and end positions as comma seperated arguments for example
        BEGIN1:END1, BEGIN2:END2, BEGIN3:END3...so on
        This method converts such line in to two dimensional array for example
        Array = { [BEGIN1,END1],[BEGIN2, END2].
        In a code it is accessed as Array[0][0] = BEGIN1, Array[0][1]= END1, Array[1][0]= BEGIN2, Array[1][1]=END2 and so on."""
        matrix_of_read_positions = [[]]
        range_string = line.split(",")
        logging.info("Total Elements in this lines are %s", str(len(range_string)))
        w, h = 2, len(range_string)
        matrix_of_read_positions = [[0 for x in range(w)] for y in range(h)]
        count = 0
        for element in range_string:
            range_elements = element.split(":")
            matrix_of_read_positions[count][FileReadEnum.READ_START] = int(
                range_elements[FileReadEnum.READ_START]
            )
            matrix_of_read_positions[count][FileReadEnum.READ_END] = int(
                range_elements[FileReadEnum.READ_END]
            )
            count = count + 1
        # print(" Array Content")
        # print_array(matrix_of_read_positions)
        return matrix_of_read_positions

    def __repr__(self):
        """ to represent the class and its properties in string format"""
        return to_string(self)


class ClientFile:
    ROUTING_FORMAT = "%m%d%Y%H%M%S"

    class ExportSettings(NamedTuple):
        client_file_id: int
        header_query: str
        detail_query: str
        trailer_query: str
        update_query: str
        precondition: str
        create_empty_file: bool = True

    def __init__(self):
        self.client_file_id = 0
        self.client_id = 0
        self.name = ""
        self.client_abbreviated_name = ""
        self.name_pattern = ""
        self.file_type = ""
        self.data_error_notification_sns = ""
        self.data_error_threshold = 0
        self.is_data_error_alert_active = False
        self.processing_notification_sns = ""
        self.file_timezone = ""
        self.process_duration_threshold = 0
        self.file_description = ""
        self.outbound_file_generation_notification_sns = ""
        self.outbound_successful_acknowledgement = False
        self.environment_level = ""
        self.archive_folder = ""
        self.error_folder = ""
        self.structure_specification_file = ""
        self.expected_line_length = ""
        self.iam_arn = ""
        self.s3_merge_output_path = ""
        self.outbound_file_type_column = ""
        self.outbound_transmission_type_column = ""
        self.input_sender_id = ""
        self.output_sender_id = ""
        self.input_receiver_id = ""
        self.output_receiver_id = ""
        self.s3_output_file_name_prefix = ""
        self.file_processing_location = ""
        self.position_specification_file = ""
        self.validate_file_structure = False
        self.validate_file_detail_columns = False
        self.reserved_variable = ""
        self.process_name = ""
        self.process_description = ""
        self.file_category = ""
        self.file_extraction_error_notification_sns = ""
        self.is_active = False
        self.zip_file = False
        self.deliver_files_to_client = False
        self.destination_bucket_cfx = ""
        self.destination_folder_cfx = ""
        self.glue_job_name = ""
        self.redshift_glue_iam_role_name = ""
        self.extract_folder = ""
        self.column_rule_id_for_transmission_id_validation = 0
        self.error_file = False
        self.inbound_successful_acknowledgement = False
        self.inbound_file_arrival_notification_sns = ""
        self.process_multiple_file = False
        self.load_table_name = ""
        # TO DO  ( post 1/1 ??): Member hinting for all Classes  in model.py
        # This field gets initialized by ClietnFileRepo.get_export_settings
        # and is used internally. Care is needed when accessing this field,
        # hence it's "protected" nature.
        self._export_settings: ClientFile.ExportSettings = None

    def __repr__(self):
        """ to represent the class and its properties in string format"""
        return to_string(self)

    def __getitem__(self, key):
        # Allows to use the class as a dictionary
        return getattr(self, key)

    def get_redshift_glue_iam_role_arn(self, account_id: str):
        """
        Gets the Redshift-Glue IAM Role ARN by combining the account_id,
        retrievable with `account_id = boto3.client("sts").get_caller_identity()["Account"]`,
        and the defined role name
        """
        return f"arn:aws:iam::{account_id}:role/{self.redshift_glue_iam_role_name}"

    def build_process_routing_id(self, time: datetime, time_format: str = "%m%d%Y%H%M%S") -> str:
        timestamp_process_routing = time.strftime(time_format)
        return f"{self.output_sender_id}{timestamp_process_routing}"

    def get_expected_file_name_from_pattern(self) -> str:
        """Formats at file name pattern into a fully resolved name.
        For example, a name_pattern like ACCDLYINT_PRD_CVSINRX_MMDDYY.HHMMSS
        gets resolved into ACCDLYINT_PRD_CVSINRX_120120.141516 on 12/1/2020 at 2:15:16 pm

        :return: str: File name with month day and year specified.
        """
        file_suffix_full = {
            "YYMMDDHHMMSS": "%y%m%d",
            "MMDDYY.HHMMSS": "%m%d%y",
            "MMDDYYHHMMSS": "%m%d%y",
        }
        file_suffix_partial = {
            "YYMMDDHHMMSS": "HHMMSS",
            "MMDDYY.HHMMSS": ".HHMMSS",
            "MMDDYYHHMMSS": "HHMMSS",
        }
        utc_now = datetime.utcnow()
        # Splits the incoming pattern and pulls out the last part based on underscore
        # If pattern is : ACCDLYINT_PRD_CVSINRX_MMDDYY.HHMMSS, then MMDDYY.HHMMSS
        substring = self.name_pattern.split("_")[-1]
        # Identifies teh string based on the dictionary
        # If MMDDYY.HHMMSS needs to be translated in to python equilvalent of %m%d%y
        time_prefix = utc_now.strftime(file_suffix_full[substring])

        prefix = self.name_pattern[0: self.name_pattern.find(substring)]
        # prefix would contain from the begin 0 to the last underscore
        # in case of ACCDLYINT_PRD_CVSINRX_MMDDYY.HHMMSS , it would return the ACCDLYINT_PRD_CVSINRX_
        return f"{prefix}{time_prefix}{file_suffix_partial[substring]}"

    @classmethod
    def from_dict(cls, **kwargs):
        """Updates the class properties from a dictionary,
        such as one retrieved from a database call.
        Properties have to match in name and type."""
        c = cls()
        for key in kwargs:
            if key in c.__dict__:
                # ensure they are compatible types, but allow incoming None to override
                if kwargs[key] is None or isinstance(kwargs[key], type(c.__dict__[key])):
                    c.__dict__[key] = kwargs[key]
                else:
                    c.__dict__[key] = kwargs[key]
                    logger.warning(
                        "Assigning key=%s to ClientFile field, however it's of type %s and ClientFile expects %s",
                        key,
                        type(kwargs[key]),
                        type(c.__dict__[key]),
                    )
            else:
                logger.debug("Key %s is not a field of ClientFile", key)
        return c


class FileStructure:
    def __init__(self, lines):
        self.header_pos_start: int = int(lines["HEADER_POS_START"])
        self.header_pos_end: int = int(lines["HEADER_POS_END"])
        self.trailer_pos_start: int = int(lines["TRAILER_POS_START"])
        self.trailer_pos_end: int = int(lines["TRAILER_POS_END"])
        self.header_identifier = lines["HEADER_IDENTIFIER"]
        self.trailer_identifier = lines["TRAILER_IDENTIFIER"]
        self.header_line_length: int = int(lines["HEADER_LINE_LENGTH"])
        self.trailer_line_length: int = int(lines["TRAILER_LINE_LENGTH"])
        self.header_batch_id_pos_start: int = int(lines["HEADER_BATCH_ID_POS_START"])
        self.header_batch_id_pos_end: int = int(lines["HEADER_BATCH_ID_POS_END"])
        self.trailer_batch_id_pos_start: int = int(lines["TRAILER_BATCH_ID_POS_START"])
        self.trailer_batch_id_pos_end: int = int(lines["TRAILER_BATCH_ID_POS_END"])
        self.trailer_record_count_pos_start: int = int(lines["TRAILER_RECORD_COUNT_POS_START"])
        self.trailer_record_count_pos_end: int = int(lines["TRAILER_RECORD_COUNT_POS_END"])
        self.sender_id_pos_start: int = int(lines["SENDER_ID_POS_START"])
        self.sender_id_pos_end: int = int(lines["SENDER_ID_POS_END"])
        self.receiver_id_pos_start: int = int(lines["RECEIVER_ID_POS_START"])
        self.receiver_id_pos_end: int = int(lines["RECEIVER_ID_POS_END"])
        self.sender_id_value = lines["SENDER_ID_VALUE"]
        self.receiver_id_value = lines["RECEIVER_ID_VALUE"]

    def __repr__(self):
        """ to represent the class and its properties in string format"""
        return to_string(self)


class Job:
    def __init__(self):
        self.job_key = 0
        self.job_detail_key = 0
        self.job_name = ""
        self.inbound_file_name = ""
        self.job_start_timestamp = ""
        self.job_initial_start_timestamp = ""
        self.job_end_timestamp = ""

        # TODO: since this seems to always be set to pre_processing_file_location.split(/)[-1]
        #       why not make it a get_file_name method?
        self.file_name = ""
        self.input_file_name = ""
        self.output_file_name = ""
        self.file_type = ""
        self.file_record_count = 0
        self.pre_processing_file_location = ""
        self.job_status = ""
        self.error_message = ""
        self.input_sender_id = ""
        self.input_receiver_id = ""
        self.post_processing_file_location = ""
        self.source_code = ""
        self.created_by = ""
        self.created_timestamp = ""
        self.updated_by = ""
        self.updated_timestamp = ""
        self.header_file_name = ""
        self.detail_file_name = ""
        self.trailer_file_name = ""
        self.validation_file_name = ""
        self.file_processing_location = ""
        self.file_name_with_path = ""
        self.parent_key = 0
        self.client_id = 0
        self.client_file_id = 0
        self.validate_file_structure = False
        self.validate_file_detail_columns = False
        # Usually we continue processing but in certain cases we do note and this flag will be marked with value of False
        self.continue_processing = True
        # Consider Refactoring it in to the type of client you get then only initialize and use this,
        # so when I do string (__repr__), it just gives me what I need to be aware
        # of rather than having a bunch of property with empty content.
        self.outbound_header_file_name = ""
        self.outbound_detail_file_name = ""
        self.outbound_trailer_file_name = ""
        self.merge_header_file_name = ""
        self.merge_detail_file_name = ""
        self.merge_trailer_file_name = ""
        self.outbound_file_generation_complete = False
        self.file_status = "Not Available"
        self.outbound_file_name = ""
        self.client_file_info = None
        self.secret_name = ""
        self.s3_bucket_name = ""
        self.incoming_file_name = ""
        self.incoming_file = ""
        self.s3_archive_path = ""
        self.record_count = 0
        self.process_description = ""
        self.process_multiple_file = None
        self.load_table_name = ""

    def set_job_id(self, job_id: int):
        """Sets the job_id field."""
        self.job_id = job_id

    def set_file_name(self, job_key: int):
        self.header_file_name = f"{str(job_key)}_INBOUND_HDR.txt"
        self.detail_file_name = f"{str(job_key)}_INBOUND_DTL.txt"
        self.trailer_file_name = f"{str(job_key)}_INBOUND_TLR.txt"
        self.validation_file_name = f"{str(job_key)}_INBOUND_VALIDATION.txt"
        # Following are passed in the arguments for unload and hence the resultants files are generated with 000 at the end
        # We coulld potentially pass the INBOUND_<DTL>.txt also but then the file generated ( unloaded with be )INBOUND_<DTL>.txt000
        # This would potentially confuse the debugging.

        # First we say that , unload the file with this pattern, but Redshift will put automatically 000 at the end !
        self.outbound_header_file_name = f"{str(job_key)}_OUTBOUND_HDR.txt"
        self.outbound_detail_file_name = f"{str(job_key)}_OUTBOUND_DTL.txt"
        self.outbound_trailer_file_name = f"{str(job_key)}_OUTBOUND_TLR.txt"
        # And now we know that  resultant file follows these patterns
        # For increase readability ( if you re debugging) multiple properties are used
        # The argumentes below are used at the time of merging.
        self.merge_header_file_name = f"{str(job_key)}_OUTBOUND_HDR.txt000"
        self.merge_detail_file_name = f"{str(job_key)}_OUTBOUND_DTL.txt000"
        self.merge_trailer_file_name = f"{str(job_key)}_OUTBOUND_TLR.txt000"

    def get_path(self, file_name: str) -> str:
        """This function returns the fully qualified path for a given file_name"""
        return f"{self.file_processing_location}/{file_name}"

    def set_outbound_success(self, outbound_file_name: str, file_record_count: int):
        """Marks the job as an outbound success."""
        self.job_status = SUCCESS
        self.outbound_file_name = outbound_file_name
        self.file_record_count = file_record_count
        self.error_message = None

    def set_failure(self, error_message: str):
        """Marks the job as an failure."""
        self.job_status = FAIL
        self.outbound_file_name = None
        self.record_count = 0
        self.error_message = error_message

    def is_failing(self):
        return self.job_status == FAIL

    def __repr__(self):
        """ to represent the class and its properties in string format"""
        return to_string(self)


class FileColumns:
    def __init__(self):
        self.client_id = 0
        self.clinet_file_key = 0
        self.file_column_id = ""
        self.column_name = ""
        self.column_position = ""
        self.column_type = ""
        self.column_accumulator = ""
        self.column_report_position = ""
        self.column_rules = []

    def __repr__(self):
        """ to represent the class and its properties in string format"""
        return to_string(self)


class ColumnRule:
    def __init__(self):
        self.column_rules_id = ""
        self.validation_type = ""
        self.priority = 0
        self.equal_to = ""
        self.python_formula = ""
        self.list_of_values = ""
        self.error_code = ""
        self.error_message = ""
        self.error_level = 0
        self.range = []

    def set_range(self, range_value):
        if len(range_value) > 0:
            self.range = list(range_value.split(","))

    def __repr__(self):
        """ to represent the class and its properties in string format"""
        return to_string(self)


class FileSchedule:
    def __init__(self):
        self.client_file_id = 0
        self.client_id = 0
        self.name = ""
        self.client_abbreviated_name = ""
        self.file_schedule_id = 0
        self.frequency_type = ""
        self.frequency_count = 0
        self.total_files_per_day = 0
        self.grace_period = 0
        self.notification_sns = ""
        self.notification_timezone = ""
        self.file_category = ""
        self.file_description = ""
        self.is_active = False
        self.is_running = False
        self.aws_resource_name = ""
        self.last_poll_time = ""
        self.next_poll_time = ""
        self.less_end_timestamp = ""
        self.updated_by = ""
        self.updated_timestamp = ""
        self.processing_type = 0
        self.current_sla_start_time = ""
        self.current_sla_end_time = ""
        self.end_timestamp = datetime.utcnow()
        self.start_timestamp = datetime.utcnow()
        self.environment_level = ""
        self.cron_expression = ""
        self.cron_description = ""
        # self.format_timestamps()  # TODO: it throws error. Somehow achieve calling format_timestamps here

    def format_timestamps(self):
        """
        formats the UTC timestamps to Local ET timestamps.Call this method as soon as you declare the file schedule object
        """
        self.end_timestamp = self.next_poll_time
        self.start_timestamp = self.end_timestamp + timedelta(
            minutes=self.grace_period * -1
        )
        # while checking CVS outbound sla we consider 30 mins less to actual next_timestamp.
        self.less_end_timestamp = self.end_timestamp + timedelta(minutes=-30)
        self.current_sla_start_time = convert_utc_to_local(
            self.start_timestamp, self.notification_timezone, "%m/%d/%Y %I:%M %p"
        )
        # In the conversion to a string the timezone information is also included via %Z
        self.current_sla_end_time = convert_utc_to_local(
            self.end_timestamp, self.notification_timezone, "%m/%d/%Y %I:%M %p %Z"
        )

    def __repr__(self):
        """ to represent the class and its properties in string format"""
        return to_string(self)

    @classmethod
    def from_dict(cls, **kwargs):
        """Updates the class properties from a dictionary,
        such as one retrieved from a database call.
        Properties have to match in name and type."""

        # c stores the entire class attributes
        # [ex: aws_resource_name=; client_abbreviated_name=; client_file_id=0; client_id=0; cron_description=; ]
        c = cls()
        for key in kwargs:
            if key in c.__dict__:
                # ensure they are compatible types, but allow incoming None to override
                if kwargs[key] is None or isinstance(kwargs[key], type(c.__dict__[key])):
                    c.__dict__[key] = kwargs[key]
                else:
                    c.__dict__[key] = kwargs[key]
                    logger.warning(
                        "Assigning key=%s to FileSchedule field, however it's of type %s and FileSchedule expects %s",
                        key,
                        type(kwargs[key]),
                        type(c.__dict__[key]),
                    )
            else:
                logger.debug("Key %s is not a field of ClientFile", key)
        return c

    @staticmethod
    def get_file_schedule_from_record(row):
        """This function helps set the information from the iquery into a file_schedule object.
        iquery returns the output in tuple(list). So the attempt is made to convert this tuple into a usable object.
        :param row: The row/record of the result set from the pg query.
        """
        (
            client_file_id,
            client_id,
            client_name,
            client_abbreviated_name,
            file_schedule_id,
            frequency_type,
            frequency_count,
            total_files_per_day,
            grace_period,
            notification_sns,
            notification_timezone,
            file_category,
            file_description,
            is_active,
            is_running,
            aws_resource_name,
            last_poll_time,
            next_poll_time,
            updated_by,
            updated_timestamp,
            processing_type,
            environment_level,
            cron_expression,
            cron_description,
        ) = row

        # unpack sql result output (a list) to variables.
        # make sure that the order of columns specified in '_select_query' above should be in sync while unpacking the indexes

        file_schedule = FileSchedule()
        file_schedule.client_file_id = (
                client_file_id or file_schedule.client_file_id
        )  # random assigned value to each inbound or outbound file
        file_schedule.client_id = (
                client_id or file_schedule.client_id
        )  # random assigned value to each client
        file_schedule.client_name = client_name or file_schedule.client_name  # full name of client
        file_schedule.client_abbreviated_name = (
                client_abbreviated_name or file_schedule.client_abbreviated_name
        )
        file_schedule.file_schedule_id = file_schedule_id or file_schedule.file_schedule_id
        file_schedule.frequency_type = frequency_type or file_schedule.frequency_type
        file_schedule.frequency_count = frequency_count or file_schedule.frequency_count
        file_schedule.total_files_per_day = total_files_per_day or file_schedule.total_files_per_day
        file_schedule.grace_period = grace_period or file_schedule.grace_period
        file_schedule.notification_sns = notification_sns or file_schedule.notification_sns
        file_schedule.notification_timezone = (
                notification_timezone or file_schedule.notification_timezone
        )
        file_schedule.file_category = file_category or file_schedule.file_category

        file_schedule.file_description = file_description or file_schedule.file_description
        file_schedule.is_active = is_active or file_schedule.is_active
        file_schedule.is_running = is_running or file_schedule.is_running
        file_schedule.aws_resource_name = aws_resource_name or file_schedule.aws_resource_name
        file_schedule.last_poll_time = last_poll_time or file_schedule.last_poll_time
        file_schedule.next_poll_time = next_poll_time or file_schedule.next_poll_time
        file_schedule.end_timestamp = file_schedule.next_poll_time
        file_schedule.start_timestamp = file_schedule.end_timestamp + timedelta(
            minutes=file_schedule.grace_period * -1
        )
        # while checking CVS outbound sla we consider 30 mins less to actual next_timestamp.
        file_schedule.less_end_timestamp = file_schedule.end_timestamp + timedelta(minutes=-30)
        file_schedule.current_sla_start_time = convert_utc_to_local(
            file_schedule.start_timestamp, file_schedule.notification_timezone, "%m/%d/%Y %I:%M %p"
        )
        # In the conversion to a string the timezone information is also included via %Z
        file_schedule.current_sla_end_time = convert_utc_to_local(
            file_schedule.end_timestamp, file_schedule.notification_timezone, "%m/%d/%Y %I:%M %p %Z"
        )
        file_schedule.updated_by = updated_by or file_schedule.updated_by
        file_schedule.updated_timestamp = updated_timestamp or file_schedule.updated_timestamp
        file_schedule.processing_type = processing_type or file_schedule.processing_type
        file_schedule.environment_level = environment_level or file_schedule.environment_level
        file_schedule.cron_expression = cron_expression or file_schedule.cron_expression
        file_schedule.cron_description = cron_description or file_schedule.cron_description
        return file_schedule


def to_string(self):
    """It returns String representation of the model (class) properties in
    ascending order
    """
    props = list(self.__dict__.keys())
    props.sort()
    return "; ".join((f"{p}={self.__dict__[p]}" for p in props))


class FileScheduleProcessingTypeEnum(str, enum.Enum):
    INBOUND_TO_AHUB = "INBOUND->AHUB"
    AHUB_TO_CVS = "AHUB->CVS"
    AHUB_TO_OUTBOUND = "AHUB->OUTBOUND"
    CVS_12_FILES_REQUIREMENT = "12 FILES FROM CVS"
    GLUE = "GLUE"
    LAMBDA = "LAMBDA"


class CronExpressionError(Exception):
    """Exception raised for errors in the Cron Expression

    Attributes:
        CronExpression -- Expresion That Needs to be parsed
        mesage  -- small explanation of the error
        error_code -- the error code ( basically we would have an error code directory on Confluence which tells more about it)
    """

    def __init__(self, cron_expression: str, message: str, error_code: int):
        self.cron_expression = cron_expression
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)

    def __str__(self):
        return f" Cron Expression : {self.cron_expression} -> {self.message} [AHUB-{self.error_code}]{os.linesep}Refer to AHUB Error Code Manual for more information."


class CustomToStandardMapping:
    """Custom To Standard Mapping configuration for the client which sends custom fiel to AHUB

    Attributes:
        mapping_id -- mapping_id is sequence number for custom_to_standard_mapping table.
        client_file_id  -- The value of client_file_id from client-files table. Defines for which client file id this mapping belongs to.
        row_type -- Row type defines whether mapping is for 'Header' or 'Detail' or 'Footer' row
        src_col_position -- Defines the value of index (start:end) position in the custom file where the data to be read
        dest_col_length -- Specifies the total length of the field in the standard file
        src_col_name -- Defines the fields name of the standard file
        dest_col_position -- Defines the value of index (start:end) position in the standard file
        conversion_type -- Specifies how the data to be applied in standard file. It holds any one of the below value:
            COPY_AS_IS - Copy the value from the custom file (postion defined src_col_position) as it is and fill blanks at the end to achieve the required length of data (length column defines expected length)
            REPLACE - Read and replace the data from value column in the standard file. This also fill blanks at the end to achieve the required length of data
            CUSTOM - Reads the value from the custom file (postion defined src_col_position) if t is not null, and it execute the python method defined in conversion_formula_id column, Fills the return values into the standard file
            FILL_WITH_THIS_VALUE - Fills the data by the character defined in value column (like '0' or ' ') in the standard file for the specified length
        dest_col_value -- Specifies the values to be replaced with or to be filled with in the standard file
        conversion_formula_id -- Specifies the python methods name which applies custom conversion like transformation of values, generating the current date with given time zone, etc..
        description -- describe the story what the row does
    """

    def __init__(self):
        self.mapping_id: int = 0
        self.client_file_id: int = 0
        self.row_type: str = ""
        self.src_col_position: str = ""
        self.dest_col_length: int = 0
        self.src_col_name: str = ""
        self.dest_col_position: str = ""
        self.conversion_type: str = ""
        self.dest_col_value: str = ""
        self.conversion_formula_id: str = ""
        self.description: str = ""

    def __repr__(self):
        """ to represent the class and its properties in string format"""
        return to_string(self)


class GlueFailureEvent:
    """
    unpacks the dict response received from the glue failure event
    """

    def __init__(self, data):
        self.time = None
        self.detail = None
        self.id = None
        self.__dict__ = json.loads(data)


# TODO: though this function exists in common.py, we cant import this in model.py becasue of circular dependent
#  imports issue--> https://stackoverflow.com/questions/9252543/importerror-cannot-import-name-x . Try to resolve this
#  issue and get this method cleaned up from here.
def convert_utc_to_local(
        utc_timestamp: datetime, timezone: str, format_string: Optional[str] = "%m/%d/%Y %I:%M:%S %p %Z"
) -> str:
    """Converts the utc_timestamp to a given timezone , using the default format_string
    Args:
        utc_timestamp (datetime): UTC Timesstamp
        timezone (str): Timezone string such as 'America/New_York'
        format_string (Optional[str], optional): [description]. Defaults to "%m/%d/%Y %I:%M:%S %p %Z".

    Returns:
        str: Local time in the format_string format
    """
    utc_timestamp = utc_timestamp.replace(tzinfo=pytz.UTC)
    tz = pytz.timezone(timezone)
    return utc_timestamp.astimezone(tz).strftime(format_string)
