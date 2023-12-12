import os
import shutil
import traceback
from unittest.mock import MagicMock, patch, Mock
import pytest

import click
from click.testing import CliRunner

from brickflow import BrickflowProjectDeploymentSettings, BrickflowEnvVars
from brickflow.cli import (
    cli,
    exec_command,
)
from brickflow.cli.bundles import (
    bundle_download_path,
    download_and_unzip_databricks_cli,
    get_force_lock_flag,
    bundle_deploy,
    bundle_destroy,
)
from brickflow.cli.projects import handle_libraries


def fake_run(*_, **__):
    click.echo("hello world")


# TODO: Add more tests to the cli
class TestCli:
    def test_no_command_error(self):
        runner = CliRunner()
        non_existent_command = "non_existent_command"
        result = runner.invoke(cli, ["non_existent_command"])  # noqa
        assert result.exit_code == 2
        assert result.output.strip().endswith(
            f"Error: No such command '{non_existent_command}'."
        )

    @patch("webbrowser.open")
    def test_docs(self, browser: Mock):
        runner = CliRunner()
        browser.return_value = None
        result = runner.invoke(cli, ["docs"])  # noqa
        assert result.exit_code == 0, traceback.print_exception(*result.exc_info)
        assert result.output.strip().startswith("Opening browser for docs...")
        browser.assert_called_once_with(
            "https://engineering.nike.com/brickflow/", new=2
        )

    def test_force_arg(self):
        with patch.dict(
            os.environ, {BrickflowEnvVars.BRICKFLOW_BUNDLE_CLI_VERSION.value: "0.203.0"}
        ):
            assert get_force_lock_flag() == "--force-lock"
        with patch.dict(
            os.environ, {BrickflowEnvVars.BRICKFLOW_BUNDLE_CLI_VERSION.value: "auto"}
        ):
            assert get_force_lock_flag() == "--force-lock"
        with patch.dict(
            os.environ,
            {BrickflowEnvVars.BRICKFLOW_BUNDLE_CLI_VERSION.value: "something else"},
        ):
            assert get_force_lock_flag() == "--force-lock"
        with patch.dict(
            os.environ, {BrickflowEnvVars.BRICKFLOW_BUNDLE_CLI_VERSION.value: "0.202.0"}
        ):
            assert get_force_lock_flag() == "--force"

    def test_install_cli(self):
        expected_version = "0.200.0"
        url = bundle_download_path(expected_version)
        file_path = download_and_unzip_databricks_cli(url, expected_version)
        assert url is not None
        version_value = exec_command(file_path, "--version", [], capture_output=True)
        assert (
            version_value.strip() == f"Databricks CLI v{expected_version}"
        ), version_value
        directory_path = ".databricks"
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)

    def test_projects_handle_libraries(self):
        bpd = BrickflowProjectDeploymentSettings()
        bpd.brickflow_auto_add_libraries = None
        handle_libraries(skip_libraries=True)
        assert bpd.brickflow_auto_add_libraries is False
        handle_libraries(skip_libraries=False)
        assert bpd.brickflow_auto_add_libraries is True
        bpd.brickflow_auto_add_libraries = None

    @pytest.mark.parametrize(
        "function, command, workflows_dir",
        [
            (bundle_deploy, "deploy", "/path/to/deploy/workflows"),
            (bundle_destroy, "destroy", "/path/to/destroy/workflows"),
        ],
    )
    def test_bundle_commands(self, function, command, workflows_dir, mocker):
        # Mock the helper functions
        mock_get_bundles_project_env = mocker.patch(
            "brickflow.cli.bundles.get_bundles_project_env", return_value="test_env"
        )
        mock_get_force_lock_flag = mocker.patch(
            "brickflow.cli.bundles.get_force_lock_flag", return_value="--force-lock"
        )
        mock_exec_command: MagicMock = mocker.patch(
            "brickflow.cli.bundles.exec_command"
        )

        # Call the function under test
        function(force_acquire_lock=True, workflows_dir=workflows_dir)

        # Assert that the helper functions were called with the expected arguments
        mock_get_bundles_project_env.assert_called_once()
        mock_get_force_lock_flag.assert_called_once()

        # Assert that exec_command was called with the expected arguments
        mock_exec_command.assert_any_call(
            "test_bundle_cli", "bundle", [command, "-e", "test_env", "--force-lock"]
        )
