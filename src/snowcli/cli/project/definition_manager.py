from __future__ import annotations

import os
from pathlib import Path
from typing import List
import logging
import functools
from typing import Optional
from snowcli.exception import MissingConfiguration
from snowcli.cli.project.definition import load_project_definition

log = logging.getLogger(__name__)


class DefinitionManager:
    BASE_CONFIG_FILENAME = "snowflake.yml"
    USER_CONFIG_FILENAME = "snowflake.local.yml"

    project_definition: dict
    project_path: Path
    project_path_arg: str

    def __init__(self, project: str = "") -> None:
        self.project_path_arg = project
        pass

    def _find_project_path(self) -> Path:
        search_path = Path(os.getcwd())
        if self.project_path_arg and len(self.project_path_arg) > 0:
            project_path = Path(os.path.abspath(self.project_path_arg))

        return project_path

    def _find_config_files(self, project_path: Path) -> Optional[List[Path]]:
        parent_path = project_path
        starting_mount = project_path.is_mount()
        while parent_path:
            if (
                project_path.is_mount() != starting_mount
                or parent_path.parent == parent_path
                or parent_path == Path.home()
            ):
                return None
            base_config_file_path = self._is_base_config_file_available(parent_path)
            if base_config_file_path:
                user_config_file_path = self._is_user_config_file_available(parent_path)
                if user_config_file_path:
                    return [base_config_file_path, user_config_file_path]
                return [base_config_file_path]
            parent_path = parent_path.parent
        return None

    def _config_if_available(
        self, config_filename, project_path: Path
    ) -> Optional[Path]:
        config_file_path = Path(project_path) / config_filename
        if config_file_path.is_file():
            return config_file_path
        return None

    def _is_base_config_file_available(self, project_path: Path) -> Optional[Path]:
        return self._config_if_available(self.BASE_CONFIG_FILENAME, project_path)

    def _is_user_config_file_available(self, project_path: Path) -> Optional[Path]:
        return self._config_if_available(self.USER_CONFIG_FILENAME, project_path)

    @functools.cached_property
    def get_project_definition(self) -> dict:
        project_path = self._find_project_path()
        self.project_path = project_path
        config_files = self._find_config_files(project_path)
        if not config_files:
            raise MissingConfiguration(
                f"Cannot find native app project configuration. Please provide or run this command in a valid native app project directory."
            )
        self.project_definition = load_project_definition(config_files)
        return self.project_definition
