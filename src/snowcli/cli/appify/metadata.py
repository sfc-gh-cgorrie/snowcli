from __future__ import annotations

import re
import logging
from functools import cached_property
from pathlib import Path
from typing import Callable, List, Literal, Optional

from click.exceptions import ClickException
from snowflake.connector.cursor import DictCursor
from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.object.stage.manager import StageManager
from snowcli.cli.project.util import to_identifier, to_string_literal
from snowcli.cli.appify.util import find_row

log = logging.getLogger(__name__)

REFERENCES_BY_NAME_JSON = "references_by_name.json"
REFERENCES_DOMAINS = ["function", "table", "view"]

DOMAIN_TO_SHOW_COMMAND_NOUN = {
    "function": "user functions",
    "table": "tables",
    "view": "views",
    "procedure": "user procedures",
    "stage": "stages",
    "streamlit": "streamlits",
}

WHITELISTED_DOMAINS = [
    "table",
    "view",
    "function",
    "procedure",
    "stage",
    "streamlit",
]

BLACKLISTED_SCHEMAS = ["INFORMATION_SCHEMA"]

ARGUMENTS_REGEX = "(.+) RETURN.+"


class ObjectNotFoundError(ClickException):
    def __init__(self, identifier: str):
        super().__init__(f"Object did not exist or no rights: {identifier}")


class UnexpectedArgumentsFormatError(ClickException):
    def __init__(self, arguments: str):
        super().__init__(f"Unexpected arguments literal: {arguments}")


def name_from_object_row(object: dict) -> str:
    if "arguments" not in object:
        return object["name"]

    if match := re.fullmatch(ARGUMENTS_REGEX, object["arguments"]):
        return match.group(1)

    raise UnexpectedArgumentsFormatError(object["arguments"])


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

    def _is_callable_callers_rights(self, domain: str, identifier: str) -> str:
        cursor = self._execute_query(
            f"describe {domain} {identifier}", cursor_class=DictCursor
        )
        execute_as = find_row(cursor, lambda r: r["property"] == "execute as")
        if not execute_as:
            raise ObjectNotFoundError(identifier)
        return execute_as["value"] == "CALLER"

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

        for domain in WHITELISTED_DOMAINS:
            objects = self._execute_query(
                f"show {DOMAIN_TO_SHOW_COMMAND_NOUN[domain]} in schema {self._schema_id(schema)}",
                cursor_class=DictCursor,
            )
            for object in objects.fetchall():
                object_name = name_from_object_row(object)
                # FIXME: need to refactor to split name + arguments so we can quote only the name
                object_identifier = f"{self._schema_id(schema)}.{object_name}"

                # callers' rights procedures cannot become part of a Native Application
                if domain == "procedure" and self._is_callable_callers_rights(
                    domain, object_identifier
                ):
                    log.info(f"Skipping callers' rights procedure {object_identifier}")
                    pass

                literal = self._object_literal(schema, object_name)
                ddl_cursor = self._execute_query(
                    f"select get_ddl('{domain}', {literal})"
                )
                ddl = ddl_cursor.fetchone()[0]
                filename = f"{object['name']}.sql"
                with open(schema_path / filename, "w") as f:
                    f.write(ddl)
