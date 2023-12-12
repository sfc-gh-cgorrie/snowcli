from __future__ import annotations

import re
import logging
from functools import cached_property
import graphlib
from pathlib import Path
from typing import Callable, Tuple, List, Union
import json
import pprint

from click.exceptions import ClickException
from snowflake.connector.cursor import DictCursor
from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.object.stage.manager import StageManager
from snowcli.cli.project.util import (
    to_identifier,
    to_string_literal,
)
from snowcli.cli.appify.util import find_row, extract_stages, split_fqn_id

log = logging.getLogger(__name__)

REFERENCES_BY_NAME_JSON = "references_by_name.json"
REFERENCES_DOMAINS = ["function", "table", "view"]
REFERENCES_FILE_NAME = "references.json"

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

    stage_manager: StageManager
    database: str
    project_path: Path
    schemas: List[dict]
    referenced_stage_ids: List[str]
    references: dict
    ordering_graph: dict

    def __init__(self, database: str, project_path: Path):
        super().__init__()
        self.stage_manager = StageManager()
        # FIXME: deal with quoted identifiers
        self.database = database.upper()
        self.project_path = project_path
        self.schemas = []
        self.referenced_stage_ids = []
        self.references = {}
        self.ordering_graph = {}

    @cached_property
    def metadata_path(self) -> Path:
        return self.project_path / "metadata"

    @cached_property
    def stages_path(self) -> Path:
        return self.project_path / "stages"

    def get_stage_path(self, stage_id: str) -> Path:
        (db, schema, stage_name) = split_fqn_id(stage_id)
        return self.stages_path / db / schema / stage_name

    def _schema_id(self, schema: str) -> str:
        return f"{to_identifier(self.database)}.{to_identifier(schema)}"

    def _object_id(self, schema: str, object: str) -> str:
        return f"{self._schema_id(schema)}.{to_identifier(object)}"

    def _object_literal(self, schema: str, object: str) -> str:
        return to_string_literal(f"{self.database}.{schema}.{object}")

    def get_object_fully_qualified_name(self, schema: str, object_name: str) -> str:
        return f"{self.database}.{schema}.{object_name}"

    def _is_procedure_callers_rights(self, identifier: str) -> str:
        cursor = self._execute_query(
            f"describe procedure {identifier}", cursor_class=DictCursor
        )
        execute_as = find_row(cursor, lambda r: r["property"] == "execute as")
        if not execute_as:
            raise ObjectNotFoundError(identifier)
        return execute_as["value"] == "CALLER"

    def _get_callable_stage_ids(self, domain: str, identifier: str) -> List[str]:
        """
        Returns the stage IDs that are imported by this callable (procedure / function),
        or the empty list if the callable is not backed by code in a stage.
        """
        cursor = self._execute_query(
            f"describe {domain} {identifier}", cursor_class=DictCursor
        )
        imports = find_row(cursor, lambda r: r["property"] == "imports")
        return [] if not imports else extract_stages(imports["value"])

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
                self.schemas.append(name)

        # whenever code lives in a stage, we need to copy the entirety
        # of their contents into our metadata dump so we can re-create
        # functions, procedures, and streamlits appropriately.
        for stage_id in self.referenced_stage_ids:
            self.dump_stage(stage_id)
        
        self.dump_references(self.metadata_path)
        ordered_objects = self.get_ordering()

    def process_schema(self, schema: str) -> None:
        """
        Dumps all metadata from a given schema.
        """
        schema_path = self.metadata_path / schema
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
                if domain == "procedure" and self._is_procedure_callers_rights(
                    object_identifier
                ):
                    log.info(f"Skipping callers' rights procedure {object_identifier}")
                    continue

                # collect all the referenced stages
                if domain in ["procedure", "function"]:
                    for stage_id in self._get_callable_stage_ids(
                        domain, object_identifier
                    ):
                        if stage_id not in self.referenced_stage_ids:
                            self.referenced_stage_ids.append(stage_id)

                literal = self._object_literal(schema, object_name)
                ddl_cursor = self._execute_query(
                    f"select get_ddl('{domain}', {literal})"
                )
                ddl = ddl_cursor.fetchone()[0]
                filename = f"{object['name']}.sql"
                with open(schema_path / filename, "w") as f:
                    f.write(ddl)
                self.update_references(schema_path, schema, object_name, domain)

    def dump_references(self, path: str): 
        # dump references
        with open(path / REFERENCES_FILE_NAME, "w") as ref_file:
            json.dump(self.references, ref_file)

    def dump_stage(self, stage_id: str) -> None:
        """
        Downloads the entire contents of a stage, recursively.
        """
        stage_folder = self.get_stage_path(stage_id)
        stage_folder.mkdir(parents=True)
        self.stage_manager.get(stage_id, stage_folder)

    def update_references(
        self, schema_path: str, schema: str, object_name: str, domain: str
    ) -> None:
        log.info(f"grabbing references for object {schema}.{object_name} with domain {domain}")
        literal = self._object_literal(schema, object_name)
        references_cursor = self._execute_query(
            f"select system$GET_REFERENCES_BY_NAME_AS_OF_TIME({literal}, '{domain}')"
        )
        references_list = json.loads(references_cursor.fetchone()[0])
        cleaned_up_ref_list = []
        clean_ref_names = []
        for reference in references_list: 
            name = reference[0]
            domain = reference[1]
            if domain.upper() in ["FUNCTION"]:     
                cleaned_up_name = re.sub(r'^(.*\))(.*)$', r'\1', name) + "\""
                cleaned_up_ref_list.append([cleaned_up_name, domain])
                clean_ref_names.append(cleaned_up_name)
            else:
                cleaned_up_ref_list.append(reference)
                clean_ref_names.append(name)
        fqn = self.get_object_fully_qualified_name(schema, object_name)
        self.references[self.get_object_fully_qualified_name(schema, object_name)] = { 
            "references": cleaned_up_ref_list,
            "kind": domain,
            "object_name": object_name,
            "schema": schema,
            "database": self.database,
        }
        self.ordering_graph[fqn] = set(clean_ref_names)

    def get_ordering(self) -> List[str]:
        log.info(f"ordering graph {self.ordering_graph}")
        ts = graphlib.TopologicalSorter(self.ordering_graph)
        ordered_objects  = list(ts.static_order())
        return ordered_objects
        




        
