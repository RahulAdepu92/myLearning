import sys
from typing import List, Optional, Callable, Tuple, Iterable

# don't do this
from validations import *


def process_bci(
    args: List[str],
    file_validations: Iterable[Callable[[List[str]], str]],
    line_validations: Iterable[FieldValidation],
):
    """Configured externally with validations.
    :param args: - list of arguments. First is the file name.
    :param file_validations: list of methods that take in an array of lines and return an error message.
        That would be a list of methods like `def validate(lines: List[str]) -> str:`
    :param line_validations: list of field validations to be applied to each line
    """

    file_name, lines = get_lines(args[1])

    for file_validation in file_validations:
        if log_error(file_name, file_validation(lines)):
            return

    for line_no, line in enumerate(lines[1:-1]):
        for field_validation in line_validations:
            errors = field_validation.run_validations(line)
            log_error(
                file_name,
                *errors,
                line=line_no,
                field_name=f"{field_validation.name} {field_validation.position}",
            )


################################################################################


process_bci(
    sys.argv,
    file_validations=[
        validate_header_is_first_line,
        validate_trailer_is_last_line,
        # validate_lines_are_detail,
        lambda lines: next((validate_line_is_detail(line) for line in lines[1:-1]), None),
    ],
    line_validations=[
        FieldValidation(
            name="transmission_file_type",
            position=(202, 204),
            validations=[validate_field_is_present,],
        ),
        FieldValidation(
            name="transmission_id",
            position=(357, 407),
            validations=[validate_field_is_present,]
        ),
        FieldValidation(
            name="benefit_type",
            position=(407, 408),
            # validations=[lambda field, value: validate_field_is_in_range(field, value, ["", "9"]),],
            validations=[build_field_range_validation(["", "9"]),],
        ),
        FieldValidation(
            name="date_of_birth",
            position=(677, 685),
            validations=[validate_field_is_present, validate_field_is_date],
        ),
    ],
)


# PROS:
# - minimal common code
# - configuration stored in Glue job code
# CONS:
# - harder to understand => more changes of mistakes
# - requires intermediate Python knowledge
