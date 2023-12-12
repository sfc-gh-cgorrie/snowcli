from __future__ import annotations

import logging
from functools import cached_property
from pathlib import Path
from typing import Callable, List, Literal, Optional

from click.exceptions import ClickException
from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.object.stage.manager import StageManager
from snowcli.cli.project.definition_manager import DefinitionManager


log = logging.getLogger(__name__)

REFERENCES_BY_NAME_JSON = "references_by_name.json"
REFERENCES_OBJECT_TYPES = ["function", "table", "view"]


class MetadataDumper(SqlExecutionMixin):
    """
    Dumps a Snowflake database as folders and files in a local filesystem.
    Schemas become directories, and other objects become sql files with their DDL.
    Dependencies between objects that use the reference framework are stored in a JSON file.
    Stages are dumped in entirety and become directories.
    """

    database: str
    target_path: DefinitionManager

    def __init__(self, database: str, target_path: Path):
        super().__init__()
        self.database = database
        self.target_path = target_path
