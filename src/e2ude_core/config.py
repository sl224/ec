import sys
import os
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

# --- Configuration Models ---


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
    # Optional: If None, inherits from global worker_threads
    pool_size: Optional[int] = None
    max_overflow: int = 32
    pool_timeout: int = 30


class MSSQLConfig(BaseModel):
    type: Literal["mssql"] = "mssql"
    server_name: str = "localhost"
    db_name: str = "AnalyticsDataMart"
    driver: str = "ODBC Driver 17 for SQL Server"
    trusted_connection: str = "yes"
    schema_name: str = "e2ude_core_dev"
    # Optional: If None, inherits from global worker_threads
    pool_size: Optional[int] = None
    max_overflow: int = 32
    pool_timeout: int = 30
    pool_pre_ping: bool = True


DatabaseConfig = Union[SQLiteConfig, MSSQLConfig]

# --- The Settings Manager ---


class AppSettings(BaseSettings):
    """
    Application Configuration.
    """
    # Global Concurrency Setting
    # This controls both the ThreadPoolExecutor size AND the default DB pool size.
    worker_threads: int = 64

    logging: LoggingConfig = LoggingConfig()
    database: DatabaseConfig = Field(default=SQLiteConfig(), discriminator="type")

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

        # Locate User Config
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

        sources.append(file_secret_settings)
        return tuple(sources)


# Singleton Instance
try:
    settings = AppSettings()
except Exception as e:
    print(f"CRITICAL CONFIG ERROR: {e}", file=sys.stderr)
    sys.exit(1)