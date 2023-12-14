from snowcli.api.plugin.command import (
    SNOWCLI_ROOT_COMMAND_PATH,
    CommandSpec,
    CommandType,
    plugin_hook_impl,
)
from snowcli.cli.appify import commands


@plugin_hook_impl
def command_spec():
    return CommandSpec(
        parent_command_path=SNOWCLI_ROOT_COMMAND_PATH,
        command_type=CommandType.SINGLE_COMMAND,
        typer_instance=commands.app,
    )
