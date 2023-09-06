from typing import Optional
import logging
import typer

from pathlib import Path
from textwrap import dedent

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.output.decorators import with_output, catch_error
from snowcli.output.printing import OutputData
from snowcli.cli.stage.diff import stage_diff, DiffResult

from .manager import NativeAppManager
from .artifacts import ArtifactError


from .manager import NativeAppManager
from .artifacts import ArtifactError

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    hidden=False,
    name="app",
    help="Manage Native Apps in Snowflake",
)

log = logging.getLogger(__name__)

ProjectArgument = typer.Option(
    None,
    "-p",
    "--project",
    help="Path where the Native Apps project resides. Defaults to current working directory",
    show_default=False,
)


@app.command("init")
@with_output
def nativeapp_init(
    name: str = typer.Argument(
        ..., help="Name of the Native Apps project to be initiated."
    ),
    template: str = typer.Option(
        None, help="A git URL to use as template for the Native Apps project."
    ),
) -> OutputData:
    """
    Initialize a Native Apps project, optionally with a --template.
    """

    NativeAppManager().nativeapp_init(name, template)
    return OutputData.from_string(
        f"Native Apps project {name} has been created in your local directory."
    )
    pass


@app.command("bundle", hidden=True)
@with_output
@catch_error(ArtifactError, exit_code=1)
def nativeapp_bundle(
    project_path: Optional[str] = ProjectArgument,
) -> OutputData:
    """
    Prepares a local folder with configured app artifacts.
    """
    manager = NativeAppManager(project_path)
    manager.build_bundle()
    return OutputData.from_string(f"Bundle generated at {manager.deploy_root}")


@app.command("diff", hidden=True)
@with_output
@global_options_with_connection
def stage_diff(
    stage_fqn: str = typer.Argument(None, help="Name of stage"),
    folder_name: str = typer.Argument(None, help="Path to local folder"),
    **options,
) -> OutputData:
    """
    Diffs a stage with a local folder
    """
    diff: DiffResult = stage_diff(Path(folder_name), stage_fqn)
    output = f"""\
        only local:  {', '.join(diff.only_local)}
        only stage:  {', '.join(diff.only_on_stage)}
        mod/unknown: {', '.join(diff.modified)}
        unmodified:  {', '.join(diff.unmodified)}"""

    return OutputData.from_string(dedent(output))
