from typing import Callable, Optional
from snowflake.connector.cursor import DictCursor


def find_row(cursor: DictCursor, predicate: Callable[[dict], bool]) -> Optional[dict]:
    """Returns the first row that matches the predicate, or None."""
    return next(
        (row for row in cursor.fetchall() if predicate(row)),
        None,
    )
