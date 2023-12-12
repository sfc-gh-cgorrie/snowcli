from typing import Generator, List, Tuple

import json
from contextlib import contextmanager
from pathlib import Path
from strictyaml import YAML, load

from snowcli.cli.appify.util import split_schema_and_object_id

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


def get_kind(catalog: dict, schema: str, object_name: str) -> str:
    """
    Determine the kind of an object based on the metadata catalog.
    """
    pass


def load_catalog(catalog_json: Path) -> dict:
    """
    Returns the metadata catalog for the database, containing reference
    and kind information of the objects we dumped metadata for.
    """
    with open(catalog_json, "r") as f:
        return json.load(f)


def generate_setup_statements(
    catalog: dict,
) -> Generator[str, None, None]:
    """
    Generator that yields all the statements necessary to build the setup script.
    """
    yield f"create application role if not exists {APP_PUBLIC}"

    all_object_ids = list(catalog.keys())
    schemas = list(set([split_schema_and_object_id(x)[0] for x in all_object_ids]))

    for schema in schemas:
        yield f"create or alter versioned schema {to_identifier(schema)}"
        yield f"grant usage on schema {to_identifier(schema)} to application role {APP_PUBLIC}"

    for schema, object_name in get_ordering(catalog):
        kind = get_kind(catalog, schema, object_name)
        yield f"use schema {to_identifier(schema)}"
        # XXX: is this correct quoting?
        yield f"execute immediate from './metadata/{schema}/{object_name}.sql'"
        if kind in GRANT_BY_KIND:
            # FIXME: need to refactor to split name + arguments so we can quote only the name
            yield f"grant {GRANT_BY_KIND[kind]} {to_identifier(schema)}.{object_name}"
