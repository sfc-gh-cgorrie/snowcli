import re
from click import ClickException
from typing import Callable, Optional, List, Tuple
from snowflake.connector.cursor import DictCursor
from snowcli.cli.project.util import DB_SCHEMA_AND_NAME, unquote_identifier

DB_SCHEMA_NAME_ARGS = f"{DB_SCHEMA_AND_NAME}([(].*[)])?"
STAGE_IMPORT_REGEX = f"@({DB_SCHEMA_AND_NAME})/"


class NotAQualifiedNameError(ClickException):
    def __init__(self, identifier: str):
        super().__init__(f"Not an appropriately-qualified name: {identifier}")


def find_row(cursor: DictCursor, predicate: Callable[[dict], bool]) -> Optional[dict]:
    """Returns the first row that matches the predicate, or None."""
    return next(
        (row for row in cursor.fetchall() if predicate(row)),
        None,
    )


def extract_stages(imports: str) -> List[str]:
    """
    Parses a list in the format returned by describe procedure's "imports" value.
    These lists look like [@db1.abc.stage1/xyz, @db2.abc.stage2/xyz].
    We'll return the stages that are referenced, e.g. ["db1.abc.stage1", "db2.abc.stage2"].
    """
    groups = re.findall(STAGE_IMPORT_REGEX, imports)
    return [group[0] for group in groups]


def split_fqn_id(id: str) -> Tuple[str, str, str]:
    """
    Splits a fully-qualified identifier into its consituent parts.
    Returns (database, schema, name); quoting carries over from the input.
    Name can have arguments in it, e.g. for callable objects.
    """
    if match := re.fullmatch(DB_SCHEMA_NAME_ARGS, id):
        args = match.group(4)
        name = match.group(3) if args is None else f"{match.group(3)}{args}"
        return (match.group(1), match.group(2), name)
    raise NotAQualifiedNameError(id)


def fqn_matches(a: str, b: str) -> bool:
    """
    Returns True iff a and b refer to the same fully-qualified name
    after Snowflake quoting rules have been taken into consideration.
    """
    (a1, a2, a3) = split_fqn_id(a)
    (b1, b2, b3) = split_fqn_id(b)
    return (
        unquote_identifier(a1) == unquote_identifier(b1)
        and unquote_identifier(a2) == unquote_identifier(b2)
        and unquote_identifier(a3) == unquote_identifier(b3)
    )
