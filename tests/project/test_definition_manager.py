from unittest import mock
from pathlib import Path
from tests.project.fixtures import *
from tests.testing_utils.fixtures import *
import sys
import typer

from snowcli.cli.project.definition_manager import DefinitionManager

definition_manager = DefinitionManager()


@mock.patch("os.getcwd")
def test_no_project_parameter_provided(mock_getcwd):
    mock_getcwd.return_value = "/hello/world"
    project_path = definition_manager._find_project_path()
    assert project_path == Path("/hello/world")


@mock.patch("os.getcwd")
@mock.patch("os.path.abspath")
def test_double_dash_project_parameter_provided(mock_abs, mock_getcwd):
    mock_getcwd.return_value = "/hello/world"
    mock_abs.return_value = "/hello/world/test"
    definition_manager = DefinitionManager("/hello/world/test")
    project_path = definition_manager._find_project_path()
    assert project_path == Path("/hello/world/test")


@mock.patch("os.getcwd")
@mock.patch("os.path.abspath")
def test_dash_p_parameter_provided(mock_abs, mock_getcwd):
    mock_getcwd.return_value = "/hello/world"
    mock_abs.return_value = "/hello/world/test/again"
    definition_manager = DefinitionManager("/hello/world/test/again")
    project_path = definition_manager._find_project_path()
    assert project_path == Path("/hello/world/test/again")


@mock.patch("os.getcwd")
@mock.patch("os.path.abspath")
def test_dash_p_with_relative_parameter_provided(mock_abs, mock_getcwd):
    mock_getcwd.return_value = "/hello/world"
    mock_abs.return_value = "/hello/world/relative"
    definition_manager = DefinitionManager("./relative")
    project_path = definition_manager._find_project_path()
    assert project_path == Path("/hello/world/relative")


def test_find_config_files_reached_root():
    results = definition_manager._find_config_files(Path("/tmp"))
    assert results == None


def test_find_config_files_reached_file_system_border():
    results = definition_manager._find_config_files(Path("/mnt/temp"))
    assert results == None


@mock.patch("pathlib.Path.home")
def test_find_config_files_reached_home(path_home):
    path_home.return_value = "/usr/user1"
    results = definition_manager._find_config_files(Path("/usr/user1/project"))
    assert results == None
