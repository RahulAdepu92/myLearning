import logging
import os
from datetime import datetime as dt
from typing import List, Tuple, NamedTuple, Callable, Optional, Iterable


def validate_header_is_first_line(lines: List[str]) -> str:
    if len(lines) > 0 and lines[0][200:202] == "HD":
        return None
    return "Expected first line to be a header (HD) line"


def validate_trailer_is_last_line(lines: List[str]) -> str:
    if len(lines) > 0 and lines[-1][200:202] == "TR":
        return None
    return "Expected last line to be a trailer (trailer) line"


def validate_line_is_detail(line: str, line_number: Optional[int] = None) -> str:
    if len(line) < 202:
        return f"Line {line_number or ''} is of insufficient length to determine type.(found {len(line)})"
    if line[200:202] == "DT":
        return None
    return f"Expected line {line_number or ''} to be a detail (DT) line, but found {line[200:202]} instead"


def validate_lines_are_detail(lines: List[str]) -> str:
    for ix, line in enumerate(lines[1:-1]):
        error = validate_line_is_detail(line, ix)
        if error is not None:
            return error
    return None


def validate_field_is_present(field_designation: str, value: str) -> str:
    if value.strip() != "":
        return None
    return f"Field {field_designation} is required but does not have a value"


def validate_field_is_date(field_designation: str, value: str, date_format: str = "%Y%m%d") -> str:
    try:
        dt.strptime(value, date_format)
        return None
    except ValueError:
        return f"Field {field_designation} does not contain a valid date ({value})"


def validate_field_is_range(field_designation: str, value: str, range_of_values: List[str]) -> str:
    val = (value or "").strip()
    if any((x.lower() == val.lower() for x in range_of_values)):
        return None
    return f"Expected {field_designation}'s value, {value}, to be in range: {range_of_values}"


def build_field_range_validation(range_of_values: List[str]) -> Callable[[str, str], str]:
    return lambda f, v: validate_field_is_range(f, v, range_of_values)


class FieldValidation(NamedTuple):
    name: str
    position: Tuple[int, int]
    validations: List[Callable[[str, str], str]]

    def run_validations(self, line: str) -> Iterable[str]:
        field_value = line[self.position[0] : self.position[1]]
        # for v in self.validations: print(f"{self.name}: {field_value}")
        return [v(self.name, field_value) for v in self.validations]
        # val_result = []
        # for v in self.validations:
        #    val_result = v(self.name, field_value)
        #    val_result.append(val_result)
        # return val_results


def get_lines(file: str) -> Tuple[str, List[str]]:
    file_name = os.path.basename(file)
    lines: List[str] = []

    with open(file, "r") as f:
        lines = f.readlines()

    return (file_name, lines)


def log_error(
    file: str, *errors: str, line: Optional[str] = None, field_name: Optional[str] = None
) -> bool:
    for error in errors:
        if error is not None:
            logging.error("File %s: %s. Field=%s, Line=%s", file, error, field_name, line)
    return all((error is not None for error in errors))
