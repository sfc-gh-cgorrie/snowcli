from __future__ import annotations

import logging
from functools import cached_property
from pathlib import Path
from typing import Callable, List, Literal, Optional

from click.exceptions import ClickException
from snowflake.connector.cursor import DictCursor
from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.object.stage.manager import StageManager
from snowcli.cli.project.util import to_identifier, to_string_literal

log = logging.getLogger(__name__)

REFERENCES_BY_NAME_JSON = "references_by_name.json"
REFERENCES_OBJECT_TYPES = ["function", "table", "view"]

BLACKLISTED_SCHEMAS = ["INFORMATION_SCHEMA"]


class MetadataDumper(SqlExecutionMixin):
    """
    Dumps a Snowflake database as folders and files in a local filesystem.
    Schemas become directories, and other objects become sql files with their DDL.
    Dependencies between objects that use the reference framework are stored in a JSON file.
    Stages are dumped in entirety and become directories.
    """

    database: str
    target_path: Path
    schemas: List[dict]

    def __init__(self, database: str, target_path: Path):
        super().__init__()
        self.database = database
        self.target_path = target_path

    def _schema_id(self, schema: str) -> str:
        return f"{to_identifier(self.database)}.{to_identifier(schema)}"

    def _object_id(self, schema: str, object: str) -> str:
        return f"{self._schema_id(schema)}.{to_identifier(object)}"

    def _object_literal(self, schema: str, object: str) -> str:
        return to_string_literal(f"{self.database}.{schema}.{object}")

    def execute(self) -> None:
        """
        Connects to the target database and dumps metadata into the target path.
        """
        schemas = self._execute_query(
            f"show schemas in database {to_identifier(self.database)}",
            cursor_class=DictCursor,
        )
        for schema in schemas.fetchall():
            name = schema["name"]
            if name not in BLACKLISTED_SCHEMAS:
                self.process_schema(name)

    def process_schema(self, schema: str) -> None:
        """
        Dumps all metadata from a given schema.
        """
        schema_path = self.target_path / schema
        schema_path.mkdir(parents=True, exist_ok=True)

        objects = self._execute_query(
            f"show objects in schema {self._schema_id(schema)}",
            cursor_class=DictCursor,
        )
        for object in objects.fetchall():
            literal = self._object_literal(schema, object["name"])
            kind = object["kind"]
            ddl_cursor = self._execute_query(f"select get_ddl('{kind}', {literal})")
            ddl = ddl_cursor.fetchone()[0]
            filename = f"{object['name']}.sql"
            with open(schema_path / filename, "w") as f:
                f.write(ddl)
