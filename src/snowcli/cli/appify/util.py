import re
from typing import Callable, Optional, List
from snowflake.connector.cursor import DictCursor
from snowcli.cli.project.util import DB_SCHEMA_AND_NAME

STAGE_IMPORT_REGEX = f"@({DB_SCHEMA_AND_NAME})/"


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
