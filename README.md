# E2UDE Core ETL

**E2UDE Core** is a robust, idempotent ETL framework designed to process complex file archives (like MCData, Segments, Maintenance Logs) and load them into a SQL database (MSSQL/SQLite) using a **Hash-Centric Deduplication strategy**.

## Architecture Overview

This system is designed to be **"Dagster Lite,"** providing orchestration, state management, and auditability without the heavy infrastructure overhead of a full platform.

### Key Concepts

#### Hash-Centric Deduplication (HCD)
Data is keyed by the **MD5 hash** of the source file content, not the filename or date.
* **Benefit:** If the same file appears in 50 different zip archives (common in cumulative logs), it is parsed and stored once.
* **`metadata_hash_registry`**: The canonical list of unique file contents.
* **`metadata_file`**: The link between a specific Zip Archive instance and the Content Hash.

#### Atomic Execution
* **Handlers:** Each file type (e.g., `MCDATA`, `TMPTR_LOG`) has a dedicated Handler that inherits from `BaseHandler`.
* **Atomic Replace:** When processing a file, the system performs an atomic transaction: `DELETE` existing data for this hash -> `INSERT` new data. This ensures idempotency.
* **Metadata Scan:** A specialized handler that scans archives to populate the registry before deep processing begins.

#### Orchestration
* **Session Management:** Every execution run is tracked in `processing_sessions`.
* **Job Tracking:** Every individual file processing task is logged in `processing_jobs`.
* **Smart Skipping:** The system checks the database state before doing work. If a file hash has already been successfully processed by the current logic version, it is skipped.

---

## Design Philosophy: Why "Dagster Lite"?

We deliberately chose a custom, lightweight architecture over off-the-shelf orchestrators like Dagster or Airflow for specific domain reasons:

### 1. Direct Data Visibility (The "Killer Feature")
* **The Problem with Standard Orchestrators:** Tools like Dagster typically abstract their run history into an internal database (often SQLite or a siloed Postgres schema). Joining "Job Success" data with "Business Data" requires complex cross-database queries or API calls.
* **The E2UDE Advantage:** Our `processing_jobs` and `processing_sessions` tables live in the **same database schema** as your actual data (`rsmdata_*`).
* **Benefit:** You can write a single SQL query to join metadata, file lineage, and raw sensor data.
* **Benefit:** Debugging is instant. You can trace a specific row in `rsmdata_tmptr` back to the exact `job_id`, `git_hash`, and `zip_file` that produced it using standard foreign keys.

### 2. Domain-Specific Deduplication
Generic orchestrators trigger based on "Events" or "Time". Our system triggers based on **Content Hashing**. We natively understand that `FolderA/File.csv` and `FolderB/File.csv` are identical if their hashes match, preventing billions of rows of redundant storage—a feature that would require complex custom logic to implement in Dagster.

### 3. Infrastructure Simplicity
* No web servers, no daemon processes, no complex deployment manifests.
* The entire "Platform" is a single Python library and a database schema. It runs anywhere Python runs.

---

## Environment Setup (Windows/Linux/Mac)

### Prerequisites
* **Python:** Requires Python 3.13+.
* **UV:** This project uses `uv` for ultra-fast package management.
    * Install UV: `pip install uv` (or follow official docs).
* **ODBC Driver:** (For MSSQL) Install "ODBC Driver 17 for SQL Server".

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <repo_url>
    cd e2ude_core
    ```

2.  **Sync Dependencies:**
    Create the virtual environment and sync lockfile dependencies.
    ```bash
    uv sync
    ```

3.  **Install Project in Editable Mode:**
    Install the project into the environment created by uv sync.
    ```bash
    uv pip install -e .
    ```

4.  **Configure the Application:**
    Create a `e2ude_config.toml` in the project root (copy from provided example).
    * **Database:** Configure your connection string.
    * **Schema:** Set `schema_name = "e2ude_core_dev"` for Blue/Green deployment support.

5.  **Set up Pre-Commit Hooks (Dev Only):**
    Ensure code quality checks run before every commit.
    ```bash
    uv run pre-commit install
    ```

---

## Usage

### Running the Pipeline
To process data, run the main entry point module. By default, it will attempt to connect to the DB configured in `global_config.toml`.

```bash
uv run -m e2ude_core.main
