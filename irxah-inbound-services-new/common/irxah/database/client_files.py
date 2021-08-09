import logging
from datetime import datetime
from typing import List, Tuple, Union

from .dbi import AhubDb
from ..common import tz_now
from ..constants import SCHEMA_DW, TABLE_CLIENT_FILES, TABLE_CLIENT, EXPORT_SETTING
from ..glue import get_glue_logger
from ..model import ClientFile, FileSplitEnum
from ..notifications import account_id
from ..s3 import delete_s3_files

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)

current_utc_time = datetime.utcnow()


class ClientFileRepo(AhubDb):
    _select_query = f"""
        select cf.client_file_id, cf.client_id, c.name, c.abbreviated_name as client_abbreviated_name, cf.name_pattern, 
        cf.file_type, cf.data_error_notification_sns, coalesce(cf.data_error_threshold,0.00) data_error_threshold, 
        cf.is_data_error_alert_active, cf.processing_notification_sns, cf.file_timezone, cf.process_duration_threshold, 
        cf.file_description, cf.outbound_file_generation_notification_sns, cf.outbound_successful_acknowledgement, 
        cf.environment_level, cf.archive_folder, cf.error_folder, cf.structure_specification_file, 
        coalesce(cf.expected_line_length,0) expected_line_length, cf.redshift_glue_iam_role_name, cf.s3_merge_output_path, 
        cf.outbound_file_type_column, cf.outbound_transmission_type_column, cf.input_sender_id, cf.output_sender_id,
        cf.input_receiver_id, cf.output_receiver_id, cf.s3_output_file_name_prefix, cf.file_processing_location, 
        cf.position_specification_file, cf.validate_file_structure, cf.validate_file_detail_columns, 
        cf.process_name, cf.process_description, cf.reserved_variable, cf.file_category, cf.is_active,  
        cf.glue_job_name, cf.file_extraction_error_notification_sns, cf.deliver_files_to_client, 
        cf.destination_bucket_cfx, cf.destination_folder_cfx, cf.zip_file, cf.extract_folder, 
        coalesce(cf.column_rule_id_for_transmission_id_validation,0) column_rule_id_for_transmission_id_validation, 
        cf.error_file, cf.inbound_successful_acknowledgement, cf.inbound_file_arrival_notification_sns,
        cf.process_multiple_file, cf.load_table_name
        from {SCHEMA_DW}.{TABLE_CLIENT_FILES} cf inner join {SCHEMA_DW}.{TABLE_CLIENT} c 
        on cf.client_id = c.client_id """

    def __init__(self, **kwargs):
        # used for locking the table while exporting the file where column 'update_query' has to be updated..
        super().__init__(tables_to_lock=[f"{SCHEMA_DW}.{EXPORT_SETTING}"], **kwargs)

    def get_client_file_from_name_pattern(self, file_name_pattern: str) -> ClientFile:
        """Retrieves a ClientFile based on its name_pattern
           :param file_name_pattern: incoming s3 file prefix ( before 3rd '_' character)  which is retrieved from trigger event"""
        sql = f"{ClientFileRepo._select_query} where cf.name_pattern like :name_pattern "
        # include 3rd underscore in search criteria by adding backslash character ('\\') that will escape metacharacters in the file name pattern.
        # else, ACCDLYERR_TST_BCIINRX and ACCDLYERR_TST_BCI will be considered same
        row = self.get_one(sql, name_pattern="%" + file_name_pattern + "\\_%")
        if row is None:
            return None

        cf = ClientFile.from_dict(**row)
        return cf

    def get_client_file_from_id(self, client_file_id: int) -> ClientFile:
        """Retrieves a ClientFile based on its id
        :param client_file_id: the id of the client file to get retrieved."""
        sql = f"{ClientFileRepo._select_query} where cf.client_file_id = :id "
        row = self.get_one(sql, id=client_file_id)
        if row is None:
            return None

        cf = ClientFile.from_dict(**row)
        return cf

    def get_client_file_ids(self, file_type: str):
        """
        This function gets the list of client file ids given the file_type
        :param conn: The connection to redshift.
        :param file_type: The file type (INBOUND/OUTBOUND).
        """
        sql = f"select client_file_id from {SCHEMA_DW}.{TABLE_CLIENT_FILES} where file_type= :filetype "
        rows = self.iquery(sql, filetype=file_type)
        if rows is None:
            return None

        return rows

    def get_export_settings(
        self, client_file: Union[int, str, ClientFile], for_date: datetime = None
    ) -> ClientFile.ExportSettings:
        """Gets the export settings for a client file id.
        If for_date is None, it assumes current date.
        :param client_file_it: int or string - the client file id
        :param for_date: effective date time for settings. A client file
        may have different settings for different date ranges.
        If None, assumes "today"
        :return: a tuple of header query, detail query, trailer query, update_query

        Note: the queries will need to be run through string.format
        and have the following parameters provided:
        * routing_id - a routing identifier for the file so that all the records have a
        consistent identifier;
        * s3_out_bucket - the bucket where we export the file
        * s3_file_path - the path to the file where we export the data
        * iam_role - the role used to export the file
        """
        sql = (
            "select header_query, detail_query, trailer_query, update_query, precondition, create_empty_file "
            "from ahub_dw.export_setting "
            "where client_file_id=:id and effective_from <= :date and :date <= effective_to"
        )

        for_date = datetime.now().date() if for_date is None else for_date  # gets only the current date not timestamp
        # date above fetched will be in UTC and dates in the db (effective_to/from) will also be in UTC by default.
        # So getting results by comparing the dates which are in same timezones from above sql using where condition
        if isinstance(client_file, ClientFile):
            client_file_id = client_file.client_file_id
        else:
            client_file_id = client_file

        logger.info("attempting to fetch unload queries from export_setting table for client_file_id = '%s'",
                    client_file_id)
        row = self.get_one(sql, id=client_file_id, date=for_date)

        export_settings = None
        if row is not None:
            export_settings = ClientFile.ExportSettings(
                client_file_id=client_file_id,
                header_query=row["header_query"],
                detail_query=row["detail_query"],
                trailer_query=row["trailer_query"],
                update_query=row["update_query"],
                precondition=row["precondition"],
                create_empty_file=bool(row["create_empty_file"]),
            )

        if isinstance(client_file, ClientFile):
            client_file._export_settings = export_settings

        return export_settings

    def export_fragments(
        self, client_file: ClientFile, s3_out_bucket: str, job_key: str
    ) -> List[str]:
        """Exports the client file to a specified bucket into 3 files:
        a header file that identifies the parties (sender and receiver),
        a detail file containing all the records, and
        a trailer file that typically contains a checksum-like count.

        :param client_file: the client file to be exported
        :param s3_out_bucket: the bucket where to export the files
        :param job_key: the job that exports the records (used to mark
        the records as exported upon method completion)
        :return: a list of files, in order header, detail, trailer,
        that contains the full path of the exported files within the
        provided S3 bucket.

        If there no records to export and create_empty_file export setting was True,
        the function returns an empty array indicative of no files having been created."""
        export_settings: ClientFile.ExportSettings = self.get_export_settings(client_file)

        if not self.can_export(export_settings)[0]:
            logger.info(
                "Client_file_id %s doesn't %s to export file at this time.",
                client_file.client_file_id,
                self.can_export(export_settings)[1]
            )
            return []

        export_files_and_queries: List[Tuple[str, str]] = ClientFileRepo._build_exports(
            client_file=client_file,
            export_settings=export_settings,
            s3_out_bucket=s3_out_bucket,
            temp_file_prefix=job_key,
        )

        for (export_file, export_query) in export_files_and_queries:
            try:
                logger.info("FILE: %s export using query: %s", export_file, export_query)
                self.execute(export_query)
            except Exception as ex:
                logger.exception()
                raise FileExportException(
                    client_file.client_file_id, export_query, export_file
                ) from ex

        # HACK: even with PARALLEL OFF, Redshift will unload to a {f}000 file.
        # we probably should look up the actual files using the {f} prefix,
        # because Redshift splits them up across multiple files ({f}000, {f}001, etc)
        # at the 6.2 GB boundary
        # https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html#unload-parallel
        actual_files = [f"{f}000" for (f, _) in export_files_and_queries]

        logger.debug("UPDATE with job_key=%s: %s", job_key, export_settings.update_query)
        # executing update query
        rows_updated = self.execute(export_settings.update_query, job_key=job_key)
        logger.info("UPDATE with job_key=%s: %s rows", job_key, rows_updated)

        if rows_updated == 0 and not export_settings.create_empty_file:
            # delete all the fragments since we don't export an empty file
            logger.info(
                "Zero records exported and create_empty_file is False. Deleting fragments: %r",
                actual_files,
            )
            delete_s3_files(s3_out_bucket, *actual_files)
            return []

        return actual_files

    def can_export(self, export_settings: ClientFile.ExportSettings) -> Tuple[bool, str]:
        """Determines whether the file meets the precondition set in export_settings.
        :param export_settings: a client file export settings instance.

        The requirement laid by AHUB-624 is that we don't create an empty file
        if we received no INBOUND files.
        For AHUB-to-Client export this precondition ensures we don't export
        empty files if we received no files from CVS; conversely, we don't
        create empty files to CVS if we received no client files for a given
        period.

        The purpose of this method is not to pass judgement or capture that requirement
        but merely to check if a pre-condition is met before exporting a file.
        As of the 2020-12-18 release, this precondition is captured in the form of
        query, but in the future we could very well expand this to more
        complex python code.
        """
        if export_settings is None:
            # corresponding client file is not supposed to export as export_settings returned None
            return False, "fall in active date range"
        if not export_settings.precondition:
            logger.info(
                "No export precondition declared for client file id %s. Export assumed ok.",
                export_settings.client_file_id,
            )
            return True, ""

        # TODO: assumes a URI scheme.
        # If the precondition starts with "python:", then execute a python function,
        # otherwise assume a SQL string.
        # For a SQL string, the precondition should:
        #  1) return 1 row (no rows assumes the precondition failed)
        #  2) the first column should be a truthy value that indicates whether
        #     the export can be performed.
        rows = self.get_one(export_settings.precondition)
        # returns FALSE if precondition is not met (no rows returned) i.e, a CVS job_key with 'Available' status is
        # not present in job_key table which is greater than max out_job_key of particular client.
        # In a nut shell, since the export to particular client has happened, no CVS file has arrived to AHub.
        if rows is None or rows["count"] == 0:
            logger.info(
                "Precondition for client file %s returned no rows thus implying it should not run. Query: %s",
                export_settings.client_file_id,
                export_settings.precondition,
            )
            return False, "meet the precondition"

        return True, ""

    def _build_exports(
        client_file: ClientFile,
        export_settings: ClientFile.ExportSettings,
        s3_out_bucket: str,
        temp_file_prefix: Union[str, int],
    ) -> List[Tuple[str, str]]:
        """
        Builds the partial files and the queries to execute to export data
        into those files.
        :param client_file: the client file for which to build the exports
        :param export_settings: the export settings for the client file
        :param s3_out_bucket: where to export the files
        :param temp_file_prefix: a string or an int to prefix the exported
        parts (typicall the job key).
        :return: a list of (file_path, query_to_execute) in the right order
        """
        # determine now in a predefined time-frame (Eastern time) so that we have a consistent timestamps throughout
        about_damn_time = tz_now()

        # Builds identifier fields that are used in the export
        # For example, we use a combination of client file sender id + date time, eg. 00489INGENIORX112722020171819
        # 1. Exporting header
        routing_number = client_file.build_process_routing_id(about_damn_time)

        order = [
            (export_settings.header_query, FileSplitEnum.HEADER),
            (export_settings.detail_query, FileSplitEnum.DETAIL),
            (export_settings.trailer_query, FileSplitEnum.TRAILER),
        ]

        return [
            ClientFileRepo.__build_query(
                query=q,
                query_kind=qkind,
                client_file=client_file,
                routing_id=routing_number,
                s3_out_bucket=s3_out_bucket,
                file_prefix=temp_file_prefix,
            )
            for (q, qkind) in order
        ]

    def __build_query(
        query: str,
        query_kind: FileSplitEnum,
        client_file: ClientFile,
        routing_id: str,
        s3_out_bucket: str,
        file_prefix: str,
    ) -> Tuple[str, str]:
        """Builds the queries to export the files.
        The parametess of this method are all format strings
        found within the query.
        :return: a tuple where the first element is the file path and
          the second is the query, formatted according to the parameters."""
        s3_outbound_temp_path = client_file.file_processing_location
        s3_outbound_temp_file_prefix = f"{file_prefix}_{client_file.s3_output_file_name_prefix}"
        s3_file_path = ClientFileRepo.__make_temp_merge_file_path(
            query_kind, s3_outbound_temp_path, s3_outbound_temp_file_prefix
        )
        iam_role = client_file.get_redshift_glue_iam_role_arn(account_id)

        # These are known strings that show up in the queries a "{name}"
        # Please do not unnecessarily expand this list
        format_args = {
            "routing_id": routing_id,
            "s3_out_bucket": s3_out_bucket,
            "s3_file_path": s3_file_path,
            "iam_role": iam_role,
        }
        try:
            formatted_query = query.format(**format_args)
        except KeyError as e:
            logger.error(
                "Missing key %s when formatting %s using dictionary: %r", e.args, query, format_args
            )
            raise ExportConfigurationException(
                client_file_id=client_file.client_file_id,
                item_in_error=query_kind,
                error_description=f"Cannot find key {e.args}  for string.format",
            )

        return (s3_file_path, formatted_query)

    def __make_temp_merge_file_path(file_type: FileSplitEnum, *path_fragments) -> str:
        """Creates a temporary merge file out of path fragments.
        :param file_type: FileSplitEnum - one of the header, detail, trailer components.
        :param *path_fragments: a list of at least 1 path fragment to turn into a merge file.
        :return: A string representing a full file path

        Examples:
        make_temp_merge_file(FileSplitEnum.HEADER, "foo", "bar") # => "foo/bar_HDR.txt"
        make_temp_merge_file(FileSplitEnum.HEADER, "foo/bar/baz") # => "foo/bar/baz_HDR.txt"
        """
        # the last fragment is the file. take it, append HDR.txt to it
        path = "/".join(path_fragments)
        return f"{path}_{file_type}.txt"


class ExportConfigurationException(Exception):
    """Raised when a configuration exception ocurred.
    The error exposes:
    * client_file_id - the id of the client file having the configuration error
    """

    def __init__(self, client_file_id: Union[str, int], item_in_error: str, error_description: str):
        self.client_file_id = client_file_id
        self.item_in_error = item_in_error
        self.error_description = error_description
        self.message = f"Incorrectly configured {item_in_error} for client file {client_file_id}: {error_description}"
        super().__init__(self.message)


class FileExportException(Exception):
    """Raised when an unload query fails.
    Provides info about the query and the file_path."""

    def __init__(self, client_file_id: Union[str, int], query: str, file_path: str):
        self.client_file_id = client_file_id
        self.query = query
        self.file = file_path
        self.message = f"Failed to export a fragment of client file {client_file_id} to {file_path} using {query}."
        super().__init__(self.message)
