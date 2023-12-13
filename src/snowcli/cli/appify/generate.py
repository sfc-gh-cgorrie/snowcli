from typing import Generator, List, Tuple

import re
from textwrap import dedent
from contextlib import contextmanager
from pathlib import Path
from strictyaml import YAML, load
from click import ClickException

from snowcli.cli.appify.util import split_fqn_id

from snowcli.cli.project.util import to_identifier, IDENTIFIER
from snowcli.cli.project.schemas.project_definition import project_schema

APP_PUBLIC = "app_public"
CALLABLE_LOWER_KINDS = ["function", "procedure"]
GRANT_BY_LOWER_KIND = {
    "function": "usage on function",
    "procedure": "usage on procedure",
    "table": "select on table",  # FIXME: what if they want editing? what if they want nothing?
    "view": "select on view",
    "streamlit": "usage on streamlit",
}

STREAMLIT_NAME = re.compile(r"^\s*create or replace streamlit (.+)$", re.MULTILINE)
STREAMLIT_ROOT_LOCATION = re.compile(r"^\s*root_location='(.+)$", re.MULTILINE)
STREAMLIT_MAIN_FILE = re.compile(r"^\s*main_file='(.+)'$", re.MULTILINE)


class MalformedDDLError(ClickException):
    def __init__(self, property: str, path: Path):
        super().__init__(f"DDL ({property}) is non-conforming at {path}")


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


def rewrite_stage_imports(
    catalog: dict, stage_ids: List[str], metadata_path: Path
) -> None:
    """
    Rewrite the "imports" part of callable / streamlit DDL statements as they now need to
    reference paths inside our application stage. We re-write the streamlit DDL fully, as
    there are missing features in NA (e.g. query_warehouse) and bugs in its get_ddl impl.
    """

    def _rewrite_imports(stmt: str, suffix: str = "") -> str:
        # FIXME: likely quoting is wrong here.
        for stage_id in stage_ids:
            (stage_db, stage_schema, stage_name) = split_fqn_id(stage_id)
            needle = f"@{stage_id}{suffix}"
            replacement = f"/stages/{stage_db}/{stage_schema}/{stage_name}{suffix}"
            stmt = stmt.replace(needle, replacement)
        return stmt

    for id, object in catalog.items():
        (_db, schema, object_name) = split_fqn_id(id)
        sql_path = metadata_path / schema / f"{object_name}.sql"
        ddl_statement = sql_path.read_text()
        kind = object["kind"]

        if kind == "streamlit":
            # streamlits need to be entirely rewritten in addition
            # to their stage imports, as get_ddl misses the final single quote
            if match := STREAMLIT_NAME.search(ddl_statement):
                name = match.group(1)
            else:
                raise MalformedDDLError("streamlit.name", sql_path)

            if match := STREAMLIT_ROOT_LOCATION.search(ddl_statement):
                root_location = match.group(1)
            else:
                raise MalformedDDLError("streamlit.root_location", sql_path)

            if match := STREAMLIT_MAIN_FILE.search(ddl_statement):
                main_file = match.group(1)
            else:
                raise MalformedDDLError("streamlit.main_file", sql_path)

            from_clause = _rewrite_imports(root_location)
            ddl_statement = dedent(
                f"""
                    create or replace streamlit {schema}.{name}
                    FROM '{from_clause}'
                    MAIN_FILE='{main_file}';
                """
            )

        else:
            # we need the bare object name; our callable names may have parameters
            # but they don't have their names, so we'll need to preserve those in
            # the DDL that we dumped earlier.
            bare_object_name = (
                object_name
                if kind.lower() not in CALLABLE_LOWER_KINDS
                else object_name[: object_name.index("(")]
            )

            # other object types need to be schema-qualified separately
            # as we'll keep the rest of their definition.
            expr = re.compile(
                f"^(create or replace [a-zA-Z_]+ {IDENTIFIER})\s*[(\n]", re.IGNORECASE
            )
            if match := expr.match(ddl_statement):
                finalpos = len(match.group(1))
                # FIXME: likely quoting is wrong here.
                ddl_statement = (
                    f"create or replace {kind} {schema}.{bare_object_name}"
                    + ddl_statement[finalpos:]
                )
            else:
                raise MalformedDDLError(f"{kind}.name", sql_path)

            if kind.lower() in CALLABLE_LOWER_KINDS:
                # callables need their stage imports rewritten
                ddl_statement = _rewrite_imports(ddl_statement, "/")

        sql_path.write_text(ddl_statement)


def generate_setup_statements(
    catalog: dict,
    ordering: List[str],
) -> Generator[str, None, None]:
    """
    Generator that yields all the statements necessary to build the setup script.
    """
    yield f"create application role if not exists {APP_PUBLIC};"

    all_object_ids = list(catalog.keys())
    schemas = list(set([split_fqn_id(x)[1] for x in all_object_ids]))

    for schema in schemas:
        yield f"create or alter versioned schema {to_identifier(schema)};"
        yield f"grant usage on schema {to_identifier(schema)} to application role {APP_PUBLIC};"

    for fqn in ordering:
        (_db, schema, object_name) = split_fqn_id(fqn)
        kind = catalog[fqn]["kind"]
        # XXX: is this correct quoting?
        yield f"execute immediate from './metadata/{schema}/{object_name}.sql';"
        if kind.lower() in GRANT_BY_LOWER_KIND:
            # FIXME: need to refactor to split name + arguments so we can quote only the name
            yield f"""
                grant {GRANT_BY_LOWER_KIND[kind.lower()]}
                    {to_identifier(schema)}.{object_name} to application role {APP_PUBLIC};
            """.strip()
