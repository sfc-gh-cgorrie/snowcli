from snowcli.api.plugin.command import (
    plugin_hook_impl,
    CommandSpec,
    SNOWCLI_ROOT_COMMAND_PATH,
)
from snowcli.cli.snowpark import app as snowpark_app


@plugin_hook_impl
def command_spec():
    return CommandSpec(
        parent_command_path=SNOWCLI_ROOT_COMMAND_PATH, typer_instance=snowpark_app
    )