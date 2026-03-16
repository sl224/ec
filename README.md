# E2UDE Core ETL

`e2ude_core` processes TransportRSM archives into SQL tables.

## Production Refresh

Production refresh runs read from `\\Rsiny1-ilsfs\RSM` and require an explicit target schema choice on every run.

```powershell
.\scripts\refresh-data.ps1 -Target dev -Preview
.\scripts\refresh-data.ps1 -Target dev
.\scripts\refresh-data.ps1 -Target prod
```

See [docs/data-refresh.md](docs/data-refresh.md) for the refresh setup and run path.

## Development

- [docs/plugin-development.md](docs/plugin-development.md)
- [docs/architecture.md](docs/architecture.md)

Useful local commands:

```powershell
uv run python scripts/preview_parser.py C:\temp\sample_MCData
uv run python scripts/run_fixture_zip_e2e.py C:\local\e2ude_fixtures\169871\2023\11\169871_20231107_024218_987_TransportRSM.fpkg.e2d.zip
```

## Setup

Prerequisites:

- Python 3.13+
- `uv`
- ODBC Driver 17 for SQL Server when targeting MSSQL

Install:

```bash
git clone <repo_url>
cd e2ude_core
uv sync
uv pip install -e .
uv run pre-commit install
```

Local configuration:

- Use `e2ude_config.toml` in the repo root, or
- use `e2ude_config.local.toml` with `E2UDE_CONFIG_PATH`

The committed defaults file sets the shared scan root to `\\Rsiny1-ilsfs\RSM`.

`e2ude_config.example.toml` is for local development and validation, not the routine production refresh.
`e2ude_config.refresh.example.toml` shows the shape expected on any machine used for routine refreshes; `scripts/refresh-data.ps1` overrides only the schema so operators choose `dev` or `prod` each time.
On any machine with share access, SQL access, and enough local staging space, the normal flow after pulling is:

```powershell
.\scripts\refresh-data.ps1 -Target dev
```

Runtime tuning now lives under `[runtime]` and `[diagnostics]` in config. If you need an env override, use the nested Pydantic names such as `E2UDE_RUNTIME__PROCESS_WORKERS` or `E2UDE_PATHS__STAGING_ROOT`.

## Code Layout

| Path | Purpose |
| --- | --- |
| `src/e2ude_core/main.py` | Process entry point |
| `src/e2ude_core/orchestration/state.py` | Folder state and work planning |
| `src/e2ude_core/orchestration/workflow.py` | Per-folder execution and returned folder result |
| `src/e2ude_core/runtime_files.py` | File type and handler specs |
| `src/e2ude_core/services/file_catalog.py` | File typing and hashing |
| `scripts/refresh-data.ps1` | Refresh entry point for dev/prod target selection |
| `scripts/preview_parser.py` | Single-file parser preview |
| `scripts/run_fixture_zip_e2e.py` | Single-zip end-to-end validation |

When adding or changing a handled file type, update `src/e2ude_core/runtime_files.py`.
