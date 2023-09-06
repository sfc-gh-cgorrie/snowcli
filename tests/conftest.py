from __future__ import annotations
from dataclasses import dataclass

import functools
import json
from typing import Any, Dict, List, Optional

from typer import Typer
from typer.testing import CliRunner


@dataclass
class CommandResult:
    exit_code: int
    json: Optional[List[Dict[str, Any]]] = None
    output: Optional[str] = None


class SnowCLIRunner(CliRunner):
    def __init__(self, app: Typer, test_snowcli_config: str):
        super().__init__()
        self.app = app
        self.test_snowcli_config = test_snowcli_config

    @functools.wraps(CliRunner.invoke)
    def invoke(self, *a, **kw):
        kw.update(catch_exceptions=False)
        return super().invoke(self.app, *a, **kw)

    def invoke_with_config(self, *args, **kwargs):
        return self.invoke(
            ["--config-file", self.test_snowcli_config, *args[0]],
            **kwargs,
        )

    def invoke_with_format(self, *args, **kwargs) -> CommandResult:
        result = self._invoke(
            [
                *args[0],
                "--format",
                "JSON",
            ],
            **kwargs,
        )
        if result.output == "" or result.output.strip() == "Done":
            return CommandResult(result.exit_code, json=[])
        return CommandResult(result.exit_code, json.loads(result.output))
