import hashlib
from typing import Union, Generator, Dict, List, Tuple
from pathlib import Path
from unittest import mock
from tempfile import TemporaryDirectory

from contextlib import contextmanager

from tests.testing_utils.fixtures import *

from snowcli.cli.stage.diff import stage_diff

STAGE_MANAGER = "snowcli.cli.stage.manager.StageManager"

FILE_CONTENTS = {
    "README.md": "This is a README\n",
    "ui/streamlit.py": "# this is a streamlit\n",
    "my.jar": b"083hgia2r92tha",
}
DEFAULT_LAST_MODIFIED = "Tue, 5 Sep 2023 17:59:21 GMT"
STAGE_LS_COLUMNS = ["name", "size", "md5", "last_modified"]


def md5_of(contents: Union[str, bytes]) -> str:
    hash = hashlib.md5()
    if isinstance(contents, bytes):
        hash.update(contents)
    else:
        hash.update(contents.encode("UTF-8"))
    return hash.hexdigest()


def stage_contents(
    files: Dict[str, Union[str, bytes]], last_modified=DEFAULT_LAST_MODIFIED
) -> List[Tuple[str, int, bytes, str]]:
    """
    Return file contents as they would be listed by a SNOWFLAKE_SSE stage
    if they were uploaded with the given structure and contents.
    """
    return [
        (f"stage/{relpath}", len(contents), md5_of(contents), last_modified)
        for (relpath, contents) in files.items()
    ]


@contextmanager
def temp_local_dir(files: Dict[str, Union[str, bytes]]) -> Generator[Path, None, None]:
    """
    Creates a temporary local directory structure from a dictionary
    of local paths and their file contents (either strings to be encoded
    as UTF-8, or binary bytes).
    """
    with TemporaryDirectory() as tmpdir:
        for relpath, contents in files.items():
            path = Path(tmpdir, relpath)
            path.parent.mkdir(parents=True, exist_ok=True)
            mode = "wb" if isinstance(contents, bytes) else "w"
            encoding = None if isinstance(contents, bytes) else "UTF-8"
            with open(path, mode=mode, encoding=encoding) as fh:
                fh.write(contents)

        yield Path(tmpdir)


@mock.patch(f"{STAGE_MANAGER}.list")
def test_empty_stage(mock_list, mock_cursor):
    mock_list.return_value = mock_cursor(rows=[], columns=STAGE_LS_COLUMNS)

    with temp_local_dir(FILE_CONTENTS) as local_path:
        diff_result = stage_diff(local_path, "a.b.c")
        assert len(diff_result.only_on_stage) == 0
        assert len(diff_result.modified) == 0
        assert len(diff_result.unmodified) == 0
        assert sorted(diff_result.only_local) == sorted(FILE_CONTENTS.keys())


@mock.patch(f"{STAGE_MANAGER}.list")
def test_empty_dir(mock_list, mock_cursor):
    mock_list.return_value = mock_cursor(
        rows=stage_contents(FILE_CONTENTS),
        columns=STAGE_LS_COLUMNS,
    )

    with temp_local_dir({}) as local_path:
        diff_result = stage_diff(local_path, "a.b.c")
        assert sorted(diff_result.only_on_stage) == sorted(FILE_CONTENTS.keys())
        assert len(diff_result.modified) == 0
        assert len(diff_result.unmodified) == 0
        assert len(diff_result.only_local) == 0


@mock.patch(f"{STAGE_MANAGER}.list")
def test_identical_stage(mock_list, mock_cursor):
    mock_list.return_value = mock_cursor(
        rows=stage_contents(FILE_CONTENTS),
        columns=STAGE_LS_COLUMNS,
    )

    with temp_local_dir(FILE_CONTENTS) as local_path:
        diff_result = stage_diff(local_path, "a.b.c")
        assert len(diff_result.only_on_stage) == 0
        assert len(diff_result.modified) == 0
        assert sorted(diff_result.unmodified) == sorted(FILE_CONTENTS.keys())
        assert len(diff_result.only_local) == 0


@mock.patch(f"{STAGE_MANAGER}.list")
def test_new_local_file(mock_list, mock_cursor):
    mock_list.return_value = mock_cursor(
        rows=stage_contents(FILE_CONTENTS),
        columns=STAGE_LS_COLUMNS,
    )

    with temp_local_dir(
        {**FILE_CONTENTS, "a/new/README.md": "### I am a new markdown readme"}
    ) as local_path:
        diff_result = stage_diff(local_path, "a.b.c")
        assert len(diff_result.only_on_stage) == 0
        assert len(diff_result.modified) == 0
        assert sorted(diff_result.unmodified) == sorted(FILE_CONTENTS.keys())
        assert diff_result.only_local == ["a/new/README.md"]


@mock.patch(f"{STAGE_MANAGER}.list")
def test_modified_file(mock_list, mock_cursor):
    mock_list.return_value = mock_cursor(
        rows=stage_contents(FILE_CONTENTS),
        columns=STAGE_LS_COLUMNS,
    )

    with temp_local_dir(
        {
            **FILE_CONTENTS,
            "README.md": "This is a modification to the existing README",
        }
    ) as local_path:
        diff_result = stage_diff(local_path, "a.b.c")
        assert len(diff_result.only_on_stage) == 0
        assert sorted(diff_result.modified) == ["README.md"]
        assert sorted(diff_result.unmodified) == ["my.jar", "ui/streamlit.py"]
        assert len(diff_result.only_local) == 0
