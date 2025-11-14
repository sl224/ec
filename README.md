## 2. Environment Setup (Windows)

### Prerequisites

1.  **Python:** This project requires **Python 3.13**.
2.  **UV:** This project uses `uv` for package management. Install it if you don't have it.
3.  **ODBC Driver:** You must have "ODBC Driver 17 for SQL Server" installed for the database connection to work.

### Installation Steps

1.  **Clone the repository:**
    ```powershell
    git clone <your-repo-url>
    cd <your-repo-name>
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

## 3. How to Run the Program

Run the ETL pipeline using `uv`:

```powershell
uv run -m etude_core.main
```
