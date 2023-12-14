import logging
from typing import Optional

import json
import typer
from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.nativeapp.init import nativeapp_init
from snowcli.output.decorators import with_output
from snowcli.output.types import CommandResult, MessageResult

from snowcli.cli.project.schemas.project_definition import project_schema

from snowcli.cli.appify.metadata import MetadataDumper
from snowcli.cli.appify.generate import (
    modifications,
    rewrite_ddl,
    discover_external_tables,
    generate_setup_statements,
    generate_package_statements,
)

from strictyaml import as_document

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="appify",
    help="Generate a Native Application project from an existing database",
)

log = logging.getLogger(__name__)


@app.command()
@with_output
@global_options_with_connection
def appify(
    db: str = typer.Argument(
        ...,
        help="The database to extract metadata from and turn into an app.",
    ),
    name: str = typer.Option(
        None,
        help=f"""The name of the native application project to include in snowflake.yml. When not specified, it is
        generated from the name of the database. Names are assumed to be unquoted identifiers whenever possible, but
        can be forced to be quoted by including the surrounding quote characters in the provided value.""",
    ),
    **options,
) -> CommandResult:
    """
    Initializes a Native Apps project from a database.
    """
    project = nativeapp_init(path=db, name=name)

    dumper = MetadataDumper(db, project.path)
    dumper.execute()

    catalog = json.loads(dumper.catalog_path.read_text())
    ordering = json.loads(dumper.ordering_path.read_text())
    rewrite_ddl(catalog, dumper.referenced_stage_ids, dumper.metadata_path)

    # generate the setup script
    setup_statements = list(generate_setup_statements(catalog, ordering))
    with open(project.path / "app" / "setup_script.sql", "w") as setup_sql:
        setup_sql.write("\n".join(setup_statements))
        setup_sql.write("\n")

    # generate the package script, if required
    seen_external_tables = discover_external_tables(catalog)
    if seen_external_tables:
        package_statements = list(generate_package_statements(seen_external_tables))
        with open(project.path / "package.sql", "w") as package_sql:
            package_sql.write("\n".join(package_statements))
            package_sql.write("\n")

    # modify the project definition
    with modifications(
        project.path / "snowflake.yml", schema=project_schema
    ) as snowflake_yml:
        # include referenced stages + metadata in our app stage
        artifacts = snowflake_yml["native_app"]["artifacts"].data
        artifacts.append(
            dict(
                src=str(dumper.metadata_path.relative_to(project.path)),
                dest="./metadata",
            )
        )
        if dumper.referenced_stage_ids:
            artifacts.append(
                dict(
                    src=str(dumper.stages_path.relative_to(project.path)),
                    dest="./stages",
                )
            )
        snowflake_yml["native_app"]["artifacts"] = as_document(artifacts)

        # add the package script, if we created one
        if seen_external_tables:
            # XXX: changing the template could cause us to lose other "package:" keys
            snowflake_yml["native_app"]["package"] = {"scripts": ["package.sql"]}

    # if we found any streamlits, just choose the first
    if dumper.streamlits:
        streamlit = dumper.streamlits[0]
        with modifications(project.path / "app" / "manifest.yml") as manifest_yml:
            manifest_yml["artifacts"]["default_streamlit"] = streamlit

    return MessageResult(f"Created Native Application project from {db}.")
