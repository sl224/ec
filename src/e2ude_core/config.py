import sys
import os
import tempfile
from typing import Union, Literal, Tuple, Callable, Type, Optional
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
    TomlConfigSettingsSource,
    InitSettingsSource,
    EnvSettingsSource,
    DotEnvSettingsSource,
    SecretsSettingsSource,
)


class LoggingConfig(BaseModel):
    level: str = "INFO"
    log_to_file: bool = False
    log_file: str = "e2ude_core.log"
    rotation_size_mb: int = 10
    rotation_backup_count: int = 5
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


class SQLiteConfig(BaseModel):
    type: Literal["sqlite3"] = "sqlite3"
    db_location: str = "e2ude_core.sqlite3"
    in_memory: bool = False
    pool_size: Optional[int] = None
    max_overflow: int = 32
    pool_timeout: int = 30


class MSSQLConfig(BaseModel):
    type: Literal["mssql"] = "mssql"
    # server_name: str = "localhost"
    server_name: str = "RSSC30-DB0140"
    db_name: str = "AnalyticsDataMart"
    driver: str = "ODBC Driver 17 for SQL Server"
    trusted_connection: str = "yes"
    schema_name: str = "e2ude_core_dev"
    pool_size: Optional[int] = None
    max_overflow: int = 32
    pool_timeout: int = 30
    pool_pre_ping: bool = True


DatabaseConfig = Union[SQLiteConfig, MSSQLConfig]


def _default_staging_root() -> Path:
    return Path(tempfile.gettempdir()) / "e2ude_core_staging"


class PathsConfig(BaseModel):
    scan_root: Path | None = None
    staging_root: Path = Field(default_factory=_default_staging_root)


class RuntimeConfig(BaseModel):
    discovery_mode: Literal["incremental", "reconcile"] = "incremental"
    discovery_workers: int = Field(default=1024, gt=0)
    pipeline_buffer_size: int = Field(default=60, gt=0)
    unzip_workers: int = Field(default=60, gt=0)
    process_workers: int = Field(default=8, gt=0)
    db_write_workers: int = Field(default=4, gt=0)


class DiagnosticsConfig(BaseModel):
    enable_viztracer: bool = False


class AppSettings(BaseSettings):
    """Runtime settings."""

    logging: LoggingConfig = LoggingConfig()
    database: DatabaseConfig = Field(default=SQLiteConfig(), discriminator="type")
    paths: PathsConfig = PathsConfig()
    runtime: RuntimeConfig = RuntimeConfig()
    diagnostics: DiagnosticsConfig = DiagnosticsConfig()

    model_config = SettingsConfigDict(
        env_prefix="E2UDE_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: InitSettingsSource,
        env_settings: EnvSettingsSource,
        dotenv_settings: DotEnvSettingsSource,
        file_secret_settings: SecretsSettingsSource,
    ) -> Tuple[Callable, ...]:
        sources = [init_settings, env_settings, dotenv_settings]

        repo_default_config = (
            Path(__file__).resolve().parents[2] / "e2ude_config.defaults.toml"
        )

        # User config overrides repo defaults when present.
        user_config_path = os.getenv("E2UDE_CONFIG_PATH", "e2ude_config.toml")
        user_config = Path(user_config_path)

        if user_config.is_file():
            sources.append(
                TomlConfigSettingsSource(settings_cls, toml_file=user_config)
            )
        elif "E2UDE_CONFIG_PATH" in os.environ:
            print(
                f"WARNING: Config file specified but not found: {user_config}",
                file=sys.stderr,
            )

        if repo_default_config.is_file():
            sources.append(
                TomlConfigSettingsSource(
                    settings_cls,
                    toml_file=repo_default_config,
                )
            )

        sources.append(file_secret_settings)
        return tuple(sources)


# Settings singleton.
try:
    settings = AppSettings()
except Exception as e:
    print(f"CRITICAL CONFIG ERROR: {e}", file=sys.stderr)
    sys.exit(1)
