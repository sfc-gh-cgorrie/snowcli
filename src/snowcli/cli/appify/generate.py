from typing import Generator, List, Tuple

import re
import json
from textwrap import dedent
from contextlib import contextmanager
from pathlib import Path
from strictyaml import YAML, load
from click import ClickException

from snowcli.cli.appify.util import split_fqn_id

from snowcli.cli.project.util import to_identifier
from snowcli.cli.project.schemas.project_definition import project_schema

APP_PUBLIC = "app_public"
CALLABLE_KINDS = ["function", "procedure"]
GRANT_BY_KIND = {
    "function": "usage on function",
    "procedure": "usage on procedure",
    "table": "select on table",  # FIXME: what if they want editing? what if they want nothing?
    "view": "select on view",
    "streamlit": "usage on streamlit",
}

# FIXME: current streamlit get_ddl misses the final single quote
STREAMLIT_NAME = re.compile(r"^\s*create or replace streamlit (.+)$", re.MULTILINE)
STREAMLIT_ROOT_LOCATION = re.compile(r"^\s*root_location='(.+)$", re.MULTILINE)
STREAMLIT_MAIN_FILE = re.compile(r"^\s*main_file='(.+)'$", re.MULTILINE)


class MalformedStreamlitError(ClickException):
    def __init__(self, property: str, path: Path):
        super().__init__(f"Streamlit DDL is non-conforming for {property} at {path}")


@contextmanager
def modifications(path: Path) -> Generator[YAML, None, None]:
    """
    Read, then write back modifications made to a project definition file.
    """
    with open(path, "r") as f:
        yml = load(f.read(), schema=project_schema)

    yield yml

    with open(path, "w") as f:
        f.write(yml.as_yaml())


def get_ordering(catalog: dict) -> List[Tuple[str, str]]:
    """
    Return a list of (schema, object name) tuples that represent a
    depth-first search of the DAG that represents their dependencies.
    Object names must include arguments for callable types.
    """
    return []


def load_catalog(catalog_json: Path) -> dict:
    """
    Returns the metadata catalog for the database, containing reference
    and kind information of the objects we dumped metadata for.
    """
    with open(catalog_json, "r") as f:
        return json.load(f)


def rewrite_stage_imports(
    catalog: dict, stage_ids: List[str], metadata_path: Path
) -> None:
    """
    Rewrite the "imports" part of callable / streamlit DDL statements as they now need to
    reference paths inside our application stage. We re-write the streamlit DDL fully, as
    there are missing features in NA (e.g. query_warehouse) and bugs in its get_ddl impl.
    """

    def _rewrite_imports(s: str) -> str:
        # FIXME: likely quoting is wrong here.
        for stage_id in stage_ids:
            (stage_db, stage_schema, stage_name) = split_fqn_id(stage_id)
            needle = f"@{stage_id}/"
            replacement = f"/stages/{stage_db}/{stage_schema}/{stage_name}/"
            s = s.replace(needle, replacement)
        return s

    for id, object in catalog.items():
        if object["kind"] in CALLABLE_KINDS:
            (_db, schema, object_name) = split_fqn_id(id)
            sql_path = metadata_path / schema / f"{object_name}.sql"
            ddl_statement = _rewrite_imports(sql_path.read_text())
            sql_path.write_text(ddl_statement)

        elif object["kind"] == "streamlit":
            (_db, schema, object_name) = split_fqn_id(id)
            sql_path = metadata_path / schema / f"{object_name}.sql"
            ddl_statement = sql_path.read_text()

            if match := STREAMLIT_NAME.match(ddl_statement):
                name = match.group(1)
            else:
                raise MalformedStreamlitError("name", sql_path)

            if match := STREAMLIT_MAIN_FILE.match(ddl_statement):
                main_file = match.group(1)
            else:
                raise MalformedStreamlitError("main_file", sql_path)

            if match := STREAMLIT_ROOT_LOCATION.match(ddl_statement):
                root_location = match.group(1)
            else:
                raise MalformedStreamlitError("root_location", sql_path)

            from_clause = _rewrite_imports(root_location)
            sql_path.write_text(
                dedent(
                    f"""
                        create or replace streamlit {name}
                        FROM '{from_clause}'
                        MAIN_FILE='{main_file};
                    """
                )
            )


def generate_setup_statements(
    catalog: dict,
) -> Generator[str, None, None]:
    """
    Generator that yields all the statements necessary to build the setup script.
    """
    yield f"create application role if not exists {APP_PUBLIC};"

    all_object_ids = list(catalog.keys())
    schemas = list(set([split_fqn_id(x)[0] for x in all_object_ids]))

    for schema in schemas:
        yield f"create or alter versioned schema {to_identifier(schema)};"
        yield f"grant usage on schema {to_identifier(schema)} to application role {APP_PUBLIC};"

    for fqn in get_ordering(catalog):
        (_db, schema, object_name) = split_fqn_id(fqn)
        kind = catalog[fqn]["kind"]
        yield f"use schema {to_identifier(schema)};"
        # XXX: is this correct quoting?
        yield f"execute immediate from './metadata/{schema}/{object_name}.sql';"
        if kind in GRANT_BY_KIND:
            # FIXME: need to refactor to split name + arguments so we can quote only the name
            yield f"""
                grant {GRANT_BY_KIND[kind]} {to_identifier(schema)}.{object_name} to application role {APP_PUBLIC};
            """.strip()
