from __future__ import annotations

import os
from pathlib import Path
from typing import List
import logging
import sys
from typing import Optional
from snowcli.exception import MissingConfiguration
from snowcli.cli.project.definition import load_project_definition

log = logging.getLogger(__name__)


class DefinitionManager:
    BASE_CONFIG_FILENAME = "snowflake.yml"
    USER_CONFIG_FILENAME = "snowflake.local.yml"

    config: dict
    project_path: Path

    def __init__(self) -> None:
        pass

    def _find_project_path(self) -> Path:
        project_path_temp = Path(os.getcwd())
        for arg in sys.argv:
            if arg.lower().startswith("--project=") or arg.lower().startswith("-p="):
                arg_val_arr = arg.split("=")
                project_path_temp = Path(os.path.abspath(arg_val_arr[1]))
        return project_path_temp

    def _find_config_files(self, project_path: Path) -> Optional[List[Path]]:
        parent_path = project_path
        starting_mount = project_path.is_mount()
        while parent_path:
            parent_path_str = str(parent_path)
            if (
                project_path.is_mount() != starting_mount
                or parent_path_str == "/"
                or parent_path_str == str(Path.home())
            ):
                return None
            base_config_file_path = self._is_base_config_file_available(parent_path)
            if base_config_file_path:
                user_config_file_path = self._is_user_config_file_available(parent_path)
                if user_config_file_path:
                    return [base_config_file_path, user_config_file_path]
                return [base_config_file_path]
            parent_path = parent_path.parent.absolute()
        return None

    def _is_config_available(
        self, config_filename, project_path: Path
    ) -> Optional[Path]:
        config_file_path = Path(str(project_path) + "/" + config_filename)
        if config_file_path.is_file():
            return config_file_path
        return None

    def _is_base_config_file_available(self, project_path: Path) -> Optional[Path]:
        return self._is_config_available(self.BASE_CONFIG_FILENAME, project_path)

    def _is_user_config_file_available(self, project_path: Path) -> Optional[Path]:
        return self._is_config_available(self.USER_CONFIG_FILENAME, project_path)

    def load_project_definition(self) -> dict:
        if self.config:
            return self.config

        project_path = self._find_project_path()
        self.project_path = project_path
        config_files = self._find_config_files(project_path)
        if not config_files:
            raise MissingConfiguration(
                f"Cannot find native app project configuration. Please provide or run this command in a valid native app project directory."
            )

        self.config = load_project_definition(config_files)
        return self.config
