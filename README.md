## Environment Setup (Windows)

### Prerequisites

1.  **Python:** This project requires **Python 3.13**.
2.  **UV:** This project uses `uv` for package management. Install it if you don't have it.
3.  **ODBC Driver:** You must have "ODBC Driver 17 for SQL Server" installed for the database connection to work.

### Installation Steps

1.  **Clone the repository:**
    ```powershell
    git clone https://bitbucket.northgrum.com/scm/psai/e2ude_core.git
    cd e2ude_core
    ```

2.  **Create virtual environment and install dependencies:**
    `uv` creates the `.venv` directory by default when you run `pip install`.

    ```powershell
    # Install main project dependencies
    uv pip install -e .
    ```

3.  **Configure the database:**
    Create a `global_config.toml` in the project root. Copy the contents from `global_config.toml` and ensure the `[database]` section points to your local or dev database.

4.  **Set up pre-commit hooks (Optional but Recommended):**
    ```powershell
    pre-commit install
    ```

---

## How to Run the Program

Run the ETL pipeline using `uv`:

```powershell
uv run -m e2ude_core.main
```

# Architecture Summary

**Objective:** A robust, idempotent ETL system for processing zipped archives. The system extracts files, scans metadata, and loads data into a SQL database using a **Hash-Centric Deduplication** strategy.

---

## 1. Core Design Philosophy: Hash-Centric Architecture

The core philosophy remains unchanged, as confirmed by the codebase.

### Separation of Instance vs. Content
* **File Instance (`FileID`):** Represents a specific file path in a specific zip folder. Used for Audit Logs and Lineage (`metadata_file` table).
* **File Content (`HashID`):** Represents the MD5 hash of the file. Used for Data Deduplication and Job Control (`metadata_hash_registry` table).

### Data Deduplication
Leaf data tables (e.g., `rsmdata_tmptr`) are keyed by `hash_id`. Data is stored once per unique content.

### Idempotency & Skip Logic
* Jobs are safe to re-run.
* The `job_scope` context manager checks `check_for_completed_job(pipeline_id, hash_id, dataset_key)`.
* If a **COMPLETED** job is found for that specific hash and table (`dataset_key`), the job is skipped.

### Atomic Replace
The `FileHandler`'s `_atomic_upload` method performs a transaction: `DELETE FROM table WHERE hash_id = X` &rarr; `INSERT new data`.

---

## 2. Component Architecture

### A. The Handler (`src/e2ude_core/pipelines/base.py`)
A generic, configurable class (`FileHandler`) that orchestrates the ETL.
* **Responsibility:** Validation, Transaction Management, Atomic Writes.
* **Inputs:** A `parser_func` and a `table_config` (which is now a `List[Type[HashVerifiableModel]]`).
* **Contract:** The `run` method can be restricted to a subset of its models using the `keys_to_process: List[str]` argument.

### B. The Parsers (`src/e2ude_core/pipelines/parsers/`)
* **Purity:** Pure Python functions with no SQL imports.
* **Return Type:** Must return a `Dict[Type[Base], pd.DataFrame]`. The dictionary key is the SQLAlchemy Model class itself.

### C. The Orchestrator (`src/e2ude_core/registry.py`)
* **Responsibility:** Wires `FileType` strings to configured `FileHandler` instances.
* **Registry:** A dictionary (`HANDLER_REGISTRY`) that holds the master configuration.

### D. Job Management (`src/e2ude_core/orchestration/` and `src/e2ude_core/db/models/manager.py`)
* **`ProcessingJob` Table:** Logs `file_id`, `hash_id`, `pipeline_id`, and the new granular `dataset_key` (the table name).
* **`job_scope` Context:** Manages the lifecycle of a `ProcessingJob`, handling skip logic, status updates, and error capture.

---

## 3. Data Models & Schema (`src/e2ude_core/db/`)

* **`metadata_hash_registry`:** Unique list of all file contents (MD5s).
* **`metadata_file`:** The "Rosetta Stone" linking `folder_id`, `relative_path`, and `hash_id`.
* **Leaf Tables (e.g., `rsmdata_tmptr`):** Keyed by `hash_id`.
* **Schema:** The system is configured to use a specific schema (e.g., `e2ude_core`) when run against MSSQL, managed by `DEFAULT_SCHEMA` and the `schema_fkey` helper.
