import re
from click import ClickException
from typing import Callable, Optional, List, Tuple
from snowflake.connector.cursor import DictCursor
from snowcli.cli.project.util import DB_SCHEMA_AND_NAME

STAGE_IMPORT_REGEX = f"@({DB_SCHEMA_AND_NAME})/"


class NotAFullyQualifiedNameError(ClickException):
    def __init__(self, identifier: str):
        super().__init__(f"Not a fully-qualified name: {identifier}")


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
    """
    if match := re.fullmatch(DB_SCHEMA_AND_NAME, id):
        return (match.group(1), match.group(2), match.group(3))
    raise NotAFullyQualifiedNameError(id)
