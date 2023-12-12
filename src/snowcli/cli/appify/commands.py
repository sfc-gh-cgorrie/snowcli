import logging
from typing import Optional

import typer
from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.nativeapp.init import nativeapp_init
from snowcli.output.decorators import with_output
from snowcli.output.types import CommandResult, MessageResult

from snowcli.cli.appify.metadata import MetadataDumper
from snowcli.cli.appify.generate import modify_yml

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

    with modify_yml(project.path / "snowflake.yml") as snowflake_yml:
        native_app = snowflake_yml["native_app"]

        # include referenced stages in our app stage
        artifacts = native_app["artifacts"].data
        for stage_id in dumper.referenced_stage_ids:
            stage_path = dumper.get_stage_path(stage_id).relative_to(project.path)
            artifacts.append(str(stage_path))
        native_app["artifacts"] = as_document(artifacts)

    return MessageResult(f"Created Native Application project from {db}.")
