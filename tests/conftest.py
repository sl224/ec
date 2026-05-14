from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

FIXTURE_ROOT_ENV = "E2UDE_TEST_FIXTURE_ROOT"
MSSQL_SERVER_ENV = "E2UDE_TEST_MSSQL_SERVER"
MSSQL_DATABASE_ENV = "E2UDE_TEST_MSSQL_DATABASE"
MSSQL_DRIVER_ENV = "E2UDE_TEST_MSSQL_DRIVER"
REPO_ROOT = Path(__file__).resolve().parents[1]


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "local_integration: requires developer-local fixtures or database configuration",
    )
    config.addinivalue_line(
        "markers",
        "local_mssql: requires a developer-local SQL Server instance",
    )


@pytest.fixture(scope="session")
def repo_root():
    return REPO_ROOT


@pytest.fixture(scope="session")
def optional_local_fixture_root():
    configured_root = os.environ.get(FIXTURE_ROOT_ENV)
    if not configured_root:
        return None

    path = Path(configured_root).expanduser().resolve()
    return path if path.exists() else None


@pytest.fixture(scope="session")
def local_fixture_root(optional_local_fixture_root):
    if optional_local_fixture_root is None:
        pytest.skip(
            f"Set {FIXTURE_ROOT_ENV} to run developer-local fixture integration tests."
        )
    return optional_local_fixture_root


@pytest.fixture(scope="session")
def local_mssql_server():
    server = os.environ.get(MSSQL_SERVER_ENV)
    if not server:
        pytest.skip(
            f"Set {MSSQL_SERVER_ENV} to run developer-local SQL Server integration tests."
        )
    return server


@pytest.fixture(scope="session")
def local_mssql_database():
    return os.environ.get(MSSQL_DATABASE_ENV, "AnalyticsDataMart")


@pytest.fixture(scope="session")
def local_mssql_driver():
    return os.environ.get(MSSQL_DRIVER_ENV, "ODBC Driver 17 for SQL Server")


@pytest.fixture
def run_repo_python(repo_root):
    def _run(script, extra_env=None):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(repo_root / "src") + os.pathsep + env.get(
            "PYTHONPATH", ""
        )
        if extra_env:
            env.update({key: str(value) for key, value in extra_env.items()})

        return subprocess.run(
            [sys.executable, "-c", script],
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )

    return _run


@pytest.fixture
def run_repo_command(repo_root):
    def _run(args, extra_env=None):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(repo_root / "src") + os.pathsep + env.get(
            "PYTHONPATH", ""
        )
        if extra_env:
            env.update({key: str(value) for key, value in extra_env.items()})

        return subprocess.run(
            args,
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )

    return _run


def _toml_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Path):
        value = str(value)
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    raise TypeError(f"Unsupported TOML value: {value!r}")


@pytest.fixture
def write_app_config(tmp_path):
    def _write(
        *,
        database: dict[str, Any],
        paths: dict[str, Any] | None = None,
        logging: dict[str, Any] | None = None,
    ) -> Path:
        config_path = tmp_path / "e2ude_config.toml"
        lines = ["[database]"]

        for key, value in database.items():
            lines.append(f"{key} = {_toml_literal(value)}")

        if paths:
            lines.extend(["", "[paths]"])
            for key, value in paths.items():
                lines.append(f"{key} = {_toml_literal(value)}")

        if logging:
            lines.extend(["", "[logging]"])
            for key, value in logging.items():
                lines.append(f"{key} = {_toml_literal(value)}")

        config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return config_path

    return _write
