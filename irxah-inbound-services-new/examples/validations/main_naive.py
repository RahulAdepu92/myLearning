import logging
import sys
import os
from typing import List, Optional, Callable, Tuple, Iterable

# don't do this
from validations import *


def process_bci(args: List[str]):
    file_name, lines = get_lines(args[1])

    # structure validation
    if log_error(file_name, validate_header_is_first_line(lines), line=lines[0]):
        return
    if log_error(file_name, validate_trailer_is_last_line(lines), line=lines[-1]):
        return

    for line in lines[1:-1]:
        if log_error(file_name, validate_line_is_detail(line), line=line):
            return

    # data validation - wrapper for all methods
    def validate_line(line: str):
        log_error(
            file_name,
            validate_field_is_present("transmission_file_type", line[202:204]),
            field_name="#3 203-204",
        )
        log_error(
            file_name,
            validate_field_is_present("transmission_id", line[357:407]),
            field_name="#19 358-407",
        )
        log_error(
            file_name,
            validate_field_is_range("benefit_type", line[407:408], ["", "9"]),
            field_name="#20 408-408",
        )
        log_error(
            file_name,
            validate_field_is_present("date_of_birth", line[677:685]),
            field_name="#41 678-685",
        )
        log_error(
            file_name,
            validate_field_is_date("date_of_birth", line[677:685]),
            field_name="#41 678-685",
        )

    # data validation
    for line in lines[1:-1]:
        validate_line(line)


################################################################################

process_bci(sys.argv)

# PROS:
# - easy to understand, even for junior pythonistas
# - minimal Glue setup
# CONS:
# - requires code deploy
# - duplicate configuration
