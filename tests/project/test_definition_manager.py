from unittest import TestCase, mock
from pathlib import Path
from unittest.case import _AssertRaisesContext
from src.snowcli.exception import MissingConfiguration
from tests.project.fixtures import *
from tests.testing_utils.fixtures import *
import sys
import typer
import unittest

from snowcli.cli.project.definition_manager import DefinitionManager


class MyTest(TestCase):
    @mock.patch("os.getcwd", return_value="/hello/world")
    @mock.patch(
        "snowcli.cli.project.definition_manager.DefinitionManager.find_config_files",
        return_value=[Path("/hello/world")],
    )
    def test_no_project_parameter_provided(self, mock_config_files, mock_getcwd):
        definition_manager = DefinitionManager()
        mock_config_files.assert_called_with(Path("/hello/world"))
        assert definition_manager._project_config_paths == [Path("/hello/world")]

    @mock.patch("os.getcwd", return_value="/hello/world")
    @mock.patch(
        "snowcli.cli.project.definition_manager.DefinitionManager.find_config_files",
        return_value=[Path("/hello/world")],
    )
    @mock.patch("os.path.abspath", return_value="/hello/world/test")
    def test_double_dash_project_parameter_provided(
        self, mock_abs, mock_config_files, mock_getcwd
    ):
        definition_manager = DefinitionManager("/hello/world/test")
        mock_config_files.assert_called_with(Path("/hello/world/test"))
        assert definition_manager._project_config_paths == [Path("/hello/world")]

    @mock.patch("os.getcwd", return_value="/hello/world")
    @mock.patch(
        "snowcli.cli.project.definition_manager.DefinitionManager.find_config_files",
        return_value=[Path("/hello/world")],
    )
    @mock.patch("os.path.abspath", return_value="/hello/world/test/again")
    def test_dash_p_parameter_provided(self, mock_abs, mock_config_files, mock_getcwd):
        definition_manager = DefinitionManager("/hello/world/test/again")
        mock_config_files.assert_called_with(Path("/hello/world/test/again"))
        assert definition_manager._project_config_paths == [Path("/hello/world")]

    @mock.patch("os.getcwd", return_value="/hello/world")
    @mock.patch(
        "snowcli.cli.project.definition_manager.DefinitionManager.find_config_files",
        return_value=[Path("/hello/world")],
    )
    @mock.patch("os.path.abspath", return_value="/hello/world/relative")
    def test_dash_p_with_relative_parameter_provided(
        self, mock_abs, mock_config_files, mock_getcwd
    ):
        mock_getcwd.return_value = "/hello/world"
        definition_manager = DefinitionManager("./relative")
        mock_abs.assert_called_with("./relative")
        mock_config_files.assert_called_with(Path("/hello/world/relative"))
        assert definition_manager._project_config_paths == [Path("/hello/world")]

    @mock.patch("os.getcwd", return_value="/hello/world")
    @mock.patch(
        "snowcli.cli.project.definition_manager.DefinitionManager._base_config_file_if_available",
        return_value=None,
    )
    @mock.patch("os.path.abspath", return_value="/tmp")
    def test_find_config_files_reached_root(
        self, mock_abs, mock_config_files, mock_getcwd
    ):
        with pytest.raises(Exception) as exception:
            definition_manager = DefinitionManager("/tmp")
            assert definition_manager.project_root == None
        assert (
            str(exception.value)
            == "Cannot find native app project configuration. Please provide or run this command in a valid native app project directory."
        )

    @mock.patch("os.getcwd", return_value="/hello/world")
    @mock.patch(
        "snowcli.cli.project.definition_manager.DefinitionManager._base_config_file_if_available",
        return_value=None,
    )
    @mock.patch("os.path.abspath", return_value="/usr/user1/project")
    @mock.patch("pathlib.Path.home", return_value="/usr/user1")
    def test_find_config_files_reached_home(
        self, mock_abs, mock_config_files, mock_getcwd, path_home
    ):
        with pytest.raises(Exception) as exception:
            definition_manager = DefinitionManager("/usr/user1/project")
            assert definition_manager.project_root == None
        assert (
            str(exception.value)
            == "Cannot find native app project configuration. Please provide or run this command in a valid native app project directory."
        )
