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

# --- Define the absolute path to the config file ---
# This finds the directory where 'global_config.py' lives
BASE_DIR = Path(__file__).resolve().parent
# This creates a full, absolute path to 'global_config.toml'
TOML_PATH = BASE_DIR / "global_config.toml"
# ---

# --- Add a debug check ---
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


# --- 2. Define the Discriminated Union for the Database ---
# Defines the different database connection types the application supports.
# Pydantic will use the 'type' field to determine which model to use.


class SQLiteConfig(BaseSettings):
    type: Literal["sqlite3"] = "sqlite3"
    db_location: str = "db.sqlite3"
    in_memory: bool = False


class MSSQLConfig(BaseModel):
    type: Literal["mssql"] = "mssql"
    server_name: str = "YOUR_SERVER_NAME"
    db_name: str = "YOUR_DATABASE_NAME"
    driver: str = "{ODBC Driver 17 for SQL Server}"
    trusted_connection: str = "yes"


# Create a new type that can be ANY of the above configs
DatabaseConfig = Union[SQLiteConfig, MSSQLConfig]


# --- 3. Define the Main "Loader" (BaseSettings) ---
# This is the main settings class that loads configuration from the TOML file.
# It provides default values to prevent the application from crashing if the
# config file is missing or incomplete.


class AppSettings(BaseSettings):
    """
    Loads all application settings from the TOML file.
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
        Define the priority order for loading settings.
        We are inserting our TOML file source right after
        the initial settings.
        """
        return (
            init_settings,
            # Add our TOML file as a source
            TomlConfigSettingsSource(settings_cls, toml_file=TOML_PATH),
            # The rest are the default sources
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )


# --- 4. Create the Singleton Instance ---
try:
    settings = AppSettings()
    # If you want to verify, uncomment this temporarily:
    # print(f"--- DEBUG: Loading config from {TOML_PATH} ---")
    # print(settings.model_dump_json(indent=2))
    # print("-----------------------------------------------")
except Exception as e:
    print(f"Failed to load configuration: {e}")
