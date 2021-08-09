import logging
from typing import List

from .dbi import AhubDb
from ..constants import SCHEMA_DW, TABLE_CUSTOM_TO_STANDARD_MAPPING
from ..model import CustomToStandardMapping
from ..glue import get_glue_logger

logger = get_glue_logger(name=f"{__name__}", logging_level=logging.DEBUG)

common_sql = f"""SELECT mapping_id, client_file_id, row_type, src_col_position, dest_col_length, src_column_name, 
                dest_col_position, conversion_type, dest_col_value, conversion_formula_id
                from {SCHEMA_DW}.{TABLE_CUSTOM_TO_STANDARD_MAPPING} """


def get_custom_to_standard_mappings_for_client(
        secret_name: str, client_file_id: int
) -> List[CustomToStandardMapping]:
    """This function retrieves the record from the custom_to_standard_mapping table and creates a collection of CustomToStandardMapping objects to be used.
    :param secret_name: the secret name for Redshift connection
    :param client_file_id: The client File id that is used to get the records from the table.
    :returns: A collection of CustomToStandardMapping objects containing all the information from the query.
    """
    # Pass a client File ID and returns the CustomToStandardMapping information in a collection
    sql = f"{common_sql} where client_file_id = :id order by mapping_id"

    db = AhubDb(secret_name=secret_name)
    rows = db.iquery(sql, id=client_file_id)

    custom_to_standard_mappings = []
    if rows is None:
        logger.info("Above Query Returns None")
        return None

    logger.info(" Above Query output is non empty")
    for row in rows:
        custom_to_standard_mappings.append(get_custom_to_standard_mapping_from_record(row))

    return custom_to_standard_mappings


def get_custom_to_standard_mapping_from_record(row) -> CustomToStandardMapping:
    """This function helps set the information from the pg query into a CustomToStandardMapping object.
    :param row: The row/record of the result set from the pg query.
    :returns CustomToStandardMapping object
    """
    custom_to_standard_mapping = CustomToStandardMapping()

    # unpack sql result output (a list) to variables
    # make sure that the order of columns specified in 'common_sql' above should be in sync while unpacking the indexes
    (
        mapping_id,
        client_file_id,
        row_type,
        src_col_position,
        dest_col_length,
        src_col_name,
        dest_col_position,
        conversion_type,
        dest_col_value,
        conversion_formula_id,
    ) = row

    custom_to_standard_mapping.mapping_id = mapping_id or custom_to_standard_mapping.mapping_id
    custom_to_standard_mapping.client_file_id = (
        client_file_id or custom_to_standard_mapping.client_file_id
    )

    custom_to_standard_mapping.row_type = row_type or custom_to_standard_mapping.row_type
    custom_to_standard_mapping.src_col_position = (
        src_col_position or custom_to_standard_mapping.src_col_position
    )
    custom_to_standard_mapping.dest_col_length = (
        dest_col_length or custom_to_standard_mapping.dest_col_length
    )
    custom_to_standard_mapping.src_col_name = (
        src_col_name or custom_to_standard_mapping.src_col_name
    )

    custom_to_standard_mapping.dest_col_position = (
        dest_col_position or custom_to_standard_mapping.dest_col_position
    )

    custom_to_standard_mapping.conversion_type = (
        conversion_type or custom_to_standard_mapping.conversion_type
    )
    custom_to_standard_mapping.dest_col_value = (
        dest_col_value or custom_to_standard_mapping.dest_col_value
    )
    custom_to_standard_mapping.conversion_formula_id = (
        conversion_formula_id or custom_to_standard_mapping.conversion_formula_id
    )
    custom_to_standard_mapping.description = custom_to_standard_mapping.description
    return custom_to_standard_mapping

