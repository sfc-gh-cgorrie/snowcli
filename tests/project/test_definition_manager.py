import pytest
from typing import Optional, List
from unittest import mock
from tests.project.fixtures import *
from tests.testing_utils.fixtures import *
import sys

from snowcli.cli.project.definition_manager import DefinitionManager

definition_manager = DefinitionManager()


def test_no_project_parameter_provided():
    testargs = ["snow", "app", "init"]
    with mock.patch.object(sys, "argv", testargs):
        with mock.patch.object(os, "getcwd", "/hello/world"):
            project_path = definition_manager._find_project_path()
            assert project_path == "/hello/world"


def test_double_dash_project_parameter_provided():
    testargs = ["snow", "app", "init", "--project=/hello/world/test"]
    with mock.patch.object(sys, "argv", testargs):
        with mock.patch.object(os, "getcwd", "/hello/world"):
            project_path = definition_manager._find_project_path()
            assert project_path == "/hello/world/test"


def test_dash_p_parameter_provided():
    testargs = ["snow", "app", "init", "-p=/hello/world/test/again"]
    with mock.patch.object(sys, "argv", testargs):
        with mock.patch.object(os, "getcwd", "/hello/world"):
            project_path = definition_manager._find_project_path()
            assert project_path == "/hello/world/test/again"


@mock.patch("os.path.abspath")
def test_dash_p_with_replative_parameter_provided(mock_abs):
    testargs = ["snow", "app", "init", "-p=./relative"]
    mock_abs.return_value = "/hello/world/relative"
    with mock.patch.object(sys, "argv", testargs):
        with mock.patch.object(os, "getcwd", "/hello/world"):
            project_path = definition_manager._find_project_path()
            assert project_path == "/hello/world/relative"


def test_find_config_files_reached_root():
    results = definition_manager._find_config_files(Path("/tmp"))
    assert results == None


def test_find_config_files_reached_file_system_border():
    results = definition_manager._find_config_files(Path("/mnt/temp"))
    assert results == None


# def test_find_config_files_reached_home(path_home):
#     with mock.patch.object(Path, "home", "/usr/user1"):
#         results = definition_manager._find_config_files(Path("/usr/user1/project"))
#         assert results == None
