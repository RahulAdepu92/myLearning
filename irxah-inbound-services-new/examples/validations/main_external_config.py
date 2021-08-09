import logging
import sys
import validations as v
from typing import Dict, List, Iterable, Callable, NamedTuple, Tuple


def _read_config(file: str):
    import json

    with open(file, "r") as f:
        return json.load(f)


def _build_file_validations(names: List[str]) -> Iterable[Callable[[List[str]], str]]:
    for name in names:
        try:
            yield getattr(v, name)
            # file_validation_func = getattr(v, name)
        except AttributeError:
            logging.error("Unknown file validation method: %s", name)


def _build_field_validations(configs) -> Iterable[v.FieldValidation]:
    for field_name in configs:
        config = configs[field_name]
        position: Tuple[int, int] = config["position"]

        validations: List[Callable[[str, str], str]] = []
        if "required" in config and config["required"] == True:
            validations.append(v.validate_field_is_present)
        if "range" in config:
            validations.append(v.build_field_range_validation(config["range"]))
        if "format" in config:
            field_format: str = config["format"]
            if field_format.startswith("$"):
                try:
                    validations.append(getattr(v, f"validate_field_is_{field_format[1:]}"))
                except AttributeError:
                    logging.error(
                        "Unknown field validation method: validate_filed_is_%s for %s",
                        field_format[1:],
                        field_name,
                    )
            # else: other types or fall back on regex
        if field_name and position and validations:
            yield v.FieldValidation(name=field_name, position=position, validations=validations)


def process_bci(args: List[str]):
    file_name, lines = v.get_lines(args[1])
    config = _read_config(args[2])

    file_validations = _build_file_validations(config["file"]["BCI"])

    for file_validation in file_validations:
        if v.log_error(file_name, file_validation(lines)):
            return

    detail_validations = list(_build_field_validations(config["detail"]["BCI"]))
    for line_no, line in enumerate(lines[1:-1]):
        for field_validation in detail_validations:
            errors = field_validation.run_validations(line)
            v.log_error(
                file_name,
                *errors,
                line=line_no,
                field_name=f"{field_validation.name} {field_validation.position}",
            )


################################################################################

process_bci(sys.argv)


# PROS:
# - small, flexible code
# - minimal Glue job
# - ability to update config file without code deployment
# CONS:
# - requires understanding of validation config structure
# - lack of in-code documentation
