from typing import Generator
from contextlib import contextmanager
from pathlib import Path
from strictyaml import YAML, load

from snowcli.cli.project.schemas.project_definition import project_schema


@contextmanager
def modify_yml(path: Path) -> Generator[YAML, None, None]:
    """
    Read, then write back modifications made to a YAML file.
    """
    with open(path, "r") as f:
        yml = load(f.read(), schema=project_schema)

    yield yml

    with open(path, "w") as f:
        f.write(yml.as_yaml())
