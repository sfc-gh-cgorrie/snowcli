from typing import Generator, List, Tuple

import re
import logging
from textwrap import dedent
from contextlib import contextmanager
from pathlib import Path
from strictyaml import YAML, load, MapCombined, Str, Any
from click import ClickException

from snowcli.cli.appify.util import split_fqn_id, fqn_matches

from snowcli.cli.project.util import (
    append_to_identifier,
    to_identifier,
    IDENTIFIER,
    DB_SCHEMA_AND_NAME,
)

log = logging.getLogger(__name__)

APP_PUBLIC = "app_public"

CALLABLE_LOWER_KINDS = ["function", "procedure"]
VIEW_LOWER_KIND = "table"  # surprisingly
GRANT_BY_LOWER_KIND = {
    "function": "usage on function",
    "procedure": "usage on procedure",
    "table": "select on table",  # FIXME: what if they want editing? what if they want nothing?
    "view": "select on view",
    "streamlit": "usage on streamlit",
}

STREAMLIT_NAME = re.compile(r"^\s*create or replace streamlit (.+)$", re.MULTILINE)
STREAMLIT_ROOT_LOCATION = re.compile(r"^\s*root_location='(.+)[\s;]*$", re.MULTILINE)
STREAMLIT_MAIN_FILE = re.compile(r"^\s*main_file='(.+)'[\s;]*$", re.MULTILINE)

REF_PACKAGE_SCHEMA_NAME = "appify_pkg_schema"
JINJA_PACKAGE_NAME = "{{ package_name }}"


class MalformedDDLError(ClickException):
    def __init__(self, property: str, path: Path):
        super().__init__(f"DDL ({property}) is non-conforming at {path}")


class UnsupportedExternalReferenceError(ClickException):
    def __init__(self, kind: str, id: str):
        super().__init__(f"Unsupported external reference: {kind} {id}")


@contextmanager
def modifications(
    path: Path, schema=MapCombined({}, Str(), Any())
) -> Generator[YAML, None, None]:
    """
    Read, then write back modifications made to a project definition file.
    """
    with open(path, "r") as f:
        yml = load(f.read(), schema=schema)

    yield yml

    with open(path, "w") as f:
        f.write(yml.as_yaml())


def get_external_referenced_tables(object: dict) -> List[str]:
    """
    Returns the list of external FQNs that this object references if the given object is a view
    that references one or more tables / views that live outside of the appified database.
    """
    database: str = object["database"]
    referenced_tables = [
        ref[0] for ref in object["references"] if ref[1].lower() == "table"
    ]

    # skip views that only reference other tables / views in the appified database
    return [fqn for fqn in referenced_tables if not fqn.startswith(database)]


def get_reference_usage_view_id(fqn: str) -> Tuple[str, str]:
    """
    Given that the supplied FQN refers to a table / view that lives outside
    of the appified database (i.e. was referenced by a "reference usage view"),
    returns the (schema, object_name) of the view we set up for it in our
    package schema.

    These end up looking something like appify_pkg_schema.db_schema_table and
    honour Snowflake's identifier quoting rules.
    """
    (db, schema, object_name) = split_fqn_id(fqn)

    name = append_to_identifier(db, "_")
    name = append_to_identifier(name, schema)
    name = append_to_identifier(name, "_")
    name = append_to_identifier(name, object_name)

    return (REF_PACKAGE_SCHEMA_NAME, name)


def rewrite_ddl(catalog: dict, stage_ids: List[str], metadata_path: Path) -> None:
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
        kind: str = object["kind"]

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

            # qualify the identifier with the schema it lives within in the app
            expr = re.compile(
                f"^(create or replace ([a-zA-Z_]+) {IDENTIFIER})\s*[(\n]", re.IGNORECASE
            )
            if match := expr.match(ddl_statement):
                finalpos = len(match.group(1))
                actual_kind = match.group(2)  # XXX: otherwise views become tables
                # FIXME: likely quoting is wrong here.
                ddl_statement = (
                    f"create or replace {actual_kind} {schema}.{bare_object_name}"
                    + ddl_statement[finalpos:]
                )
            else:
                raise MalformedDDLError(f"{kind}.name", sql_path)

            # views that reference external tables need their ddl
            # rewritten to reference the package schema view, not the
            # original external table.
            if kind.lower() == VIEW_LOWER_KIND:
                external_tables = get_external_referenced_tables(object)
                if external_tables:

                    def replace_with_view(match: re.Match[str]) -> str:
                        found_fqn = match.group(0)
                        for external_table_fqn in external_tables:
                            if fqn_matches(found_fqn, external_table_fqn):
                                (
                                    package_schema,
                                    package_view_name,
                                ) = get_reference_usage_view_id(external_table_fqn)
                                return f"{package_schema}.{package_view_name}"

                        return found_fqn  # do not change this identifer

                    ddl_statement = re.sub(
                        DB_SCHEMA_AND_NAME, replace_with_view, ddl_statement
                    )

            # callables need their stage imports rewritten
            if kind.lower() in CALLABLE_LOWER_KINDS:
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

        if fqn not in catalog:
            log.debug(f"Setup script: skipping {fqn} (not in catalog)")
            continue

        kind = catalog[fqn]["kind"]
        # XXX: is this correct quoting?
        yield f"execute immediate from './metadata/{schema}/{object_name}.sql';"
        if kind.lower() in GRANT_BY_LOWER_KIND:
            # FIXME: need to refactor to split name + arguments so we can quote only the name
            yield f"""
                grant {GRANT_BY_LOWER_KIND[kind.lower()]}
                    {to_identifier(schema)}.{object_name} to application role {APP_PUBLIC};
            """.strip()


def discover_external_tables(catalog: dict) -> List[str]:
    """
    Returns a list of external tables that are referenced by
    views inside of the database we are "appifying".
    """
    seen_external_tables: List[str] = []
    for id, object in catalog.items():
        external_tables = get_external_referenced_tables(object)
        if not external_tables:
            continue

        # reference_usage only works for views
        kind: str = object["kind"]
        if kind.lower() != VIEW_LOWER_KIND:
            raise UnsupportedExternalReferenceError(kind, id)

        # this is a view that references external tables / views
        # each one will need an entry created in the package script
        for external_fqn in external_tables:
            if not any(
                [fqn_matches(external_fqn, fqn) for fqn in seen_external_tables]
            ):
                seen_external_tables.append(external_fqn)

    return seen_external_tables


def generate_package_statements(
    seen_external_tables: List[str],
) -> Generator[str, None, None]:
    yield f"create schema if not exists {JINJA_PACKAGE_NAME}.{REF_PACKAGE_SCHEMA_NAME};"
    yield f"grant usage on schema {JINJA_PACKAGE_NAME}.{REF_PACKAGE_SCHEMA_NAME} to share in application package {JINJA_PACKAGE_NAME};"

    # grant reference_usage to all unique databases our external tables live in
    seen_external_dbs = set([split_fqn_id(fqn)[0] for fqn in seen_external_tables])
    for db in seen_external_dbs:
        yield f"grant reference_usage on database {db} to share in application package {JINJA_PACKAGE_NAME};"

    # generate a view with SELECT privileges for each external table referenced by a view
    for external_fqn in seen_external_tables:
        (schema, view_name) = get_reference_usage_view_id(external_fqn)
        package_view_fqn = f"{JINJA_PACKAGE_NAME}.{schema}.{view_name}"

        # FIXME: only select available for reference_usage views right now
        yield f"create view if not exists {package_view_fqn} as select * from {external_fqn};"
        yield f"grant select on view {package_view_fqn} to share in application package {JINJA_PACKAGE_NAME};"
