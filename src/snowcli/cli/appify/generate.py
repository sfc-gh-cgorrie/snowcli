from typing import Generator, List, Tuple
from contextlib import contextmanager
from pathlib import Path
from strictyaml import YAML, load

from snowcli.cli.project.util import to_identifier
from snowcli.cli.project.schemas.project_definition import project_schema

APP_PUBLIC = "app_public"


@contextmanager
def modify_yml(path: Path) -> Generator[YAML, None, None]:
    """
    Read, then write back modifications made to a project definition file.
    """
    with open(path, "r") as f:
        yml = load(f.read(), schema=project_schema)

    yield yml

    with open(path, "w") as f:
        f.write(yml.as_yaml())


def get_ordering(reference_json: Path) -> List[Tuple[str, str]]:
    """
    Return a list of (schema, object name) tuples that represent a
    depth-first search of the DAG that represents their dependencies.
    """
    return []


def generate_setup_statements(
    stages_path: Path,
    metadata_path: Path,
    reference_json: Path,
) -> Generator[str, None, None]:
    """
    Generator that yields all the statements necessary to build the setup script.
    """
    yield f"create application role if not exists {APP_PUBLIC}"

    schemas = [f.name for f in sorted(metadata_path.iterdir()) if f.is_dir()]
    for schema in schemas:
        yield f"create or alter versioned schema {to_identifier(schema)}"
        yield f"grant usage on schema {to_identifier(schema)} to application role {APP_PUBLIC}"
