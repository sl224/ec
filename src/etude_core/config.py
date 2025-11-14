import sys
from typing import Union, Literal, Tuple, Callable, Type
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

# Define the absolute path to the config file.
# This finds the directory where this file lives, then points to `global_config.toml`.
BASE_DIR = Path(__file__).resolve().parent.parent.parent
TOML_PATH = BASE_DIR / "global_config.toml"

# Add a debug check to warn if the config file is missing.
if not TOML_PATH.is_file():
    print(f"WARNING: Config file not found at path: {TOML_PATH}", file=sys.stderr)
    print(f"WARNING: Current working directory: {Path.cwd()}", file=sys.stderr)
# ---


class LoggingConfig(BaseModel):
    level: str = "INFO"
    log_to_file: bool = False
    log_file: str = "scan_job.log"
    rotation_size_mb: int = 10
    rotation_backup_count: int = 5
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


# Define the discriminated union for database configurations.
# Pydantic uses the 'type' field to select the correct model.


class SQLiteConfig(BaseSettings):
    type: Literal["sqlite3"] = "sqlite3"
    db_location: str = "etude_core.sqlite3"
    in_memory: bool = False


class MSSQLConfig(BaseModel):
    type: Literal["mssql"] = "mssql"
    server_name: str = "YOUR_SERVER_NAME"
    db_name: str = "YOUR_DATABASE_NAME"
    driver: str = "{ODBC Driver 17 for SQL Server}"
    trusted_connection: str = "yes"


# A type that can be any of the supported database configurations.
DatabaseConfig = Union[SQLiteConfig, MSSQLConfig]


class AppSettings(BaseSettings):
    """
    Main settings class that loads configuration from various sources.
    Uses defaults if the file or keys are missing.
    """

    logging: LoggingConfig = LoggingConfig()
    database: DatabaseConfig = Field(default=SQLiteConfig(), discriminator="type")

    model_config = SettingsConfigDict(
        extra="forbid",
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
        """
        Define the priority order for loading settings sources.
        Our custom TOML file is inserted with high priority.
        """
        return (
            init_settings,
            TomlConfigSettingsSource(settings_cls, toml_file=TOML_PATH),
            # The rest are the default sources
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )


# Create a singleton instance of the settings to be used throughout the app.
try:
    settings = AppSettings()
except Exception as e:
    print(f"Failed to load configuration: {e}")
