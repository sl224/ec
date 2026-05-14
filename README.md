# E2UDE Core ETL

`e2ude_core` processes TransportRSM archives into SQL tables.

## Production Refresh

Production refresh runs read from `\\Rsiny1-ilsfs\RSM` and require an explicit target schema choice on every run.

```powershell
uv run e2ude refresh --env dev --preview
uv run e2ude refresh --env dev
uv run e2ude refresh --env prod --confirm e2ude_core
uv run e2ude refresh --schema e2ude_candidate_mytest --preview

.\scripts\refresh-data.ps1 -Target dev -Preview
.\scripts\refresh-data.ps1 -Target dev
```

See [docs/data-refresh.md](docs/data-refresh.md) for the refresh setup and run path.
See [docs/cli-workflows.md](docs/cli-workflows.md) for concrete CLI use cases.

## Development

- [docs/plugin-development.md](docs/plugin-development.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/cli-workflows.md](docs/cli-workflows.md)

Useful local commands:

```powershell
uv run e2ude parser list
uv run e2ude parser preview C:\temp\sample_MCData
uv run e2ude parser preview C:\temp\mystery_input.txt --as segments
uv run e2ude parser status --env dev
uv run e2ude parser backfill segments --schema e2ude_candidate_mytest --dry-run
uv run python scripts/run_fixture_zip_e2e.py C:\local\e2ude_fixtures\169871\2023\11\169871_20231107_024218_987_TransportRSM.fpkg.e2d.zip
uv run python scripts/measure_discovery.py --scan-root C:\path\to\fixture_or_share
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
`e2ude_config.refresh.example.toml` shows the shape expected on any machine used for routine refreshes. `e2ude refresh --env dev` writes to the shared SQL Server development schema, `e2ude refresh --env prod --confirm e2ude_core` writes to production, and `--schema` intentionally points a run at a candidate or experiment schema. `scripts/refresh-data.ps1` remains a PowerShell wrapper around that CLI.
If you do not set `staging_root`, runs stage under the local OS temp area in an `e2ude_core_staging` folder. Override it only when you want a specific disk. On any machine with share access, SQL access, and enough local temp space, the normal flow after pulling is:

```powershell
uv run e2ude refresh --env dev
```

Runtime tuning now lives under `[runtime]` and `[diagnostics]` in config. Discovery now defaults to `discovery_mode = "incremental"`, which relists known archive directories every run and uses directory membership checks for non-archive frontier directories. `discovery_mode = "reconcile"` still forces a full source walk. If you need an env override, use the nested Pydantic names such as `E2UDE_RUNTIME__PROCESS_WORKERS`, `E2UDE_RUNTIME__DISCOVERY_MODE`, or `E2UDE_PATHS__STAGING_ROOT`.

## Code Layout

| Path | Purpose |
| --- | --- |
| `src/e2ude_core/cli.py` | Operator and parser-development CLI |
| `src/e2ude_core/main.py` | Process entry point |
| `src/e2ude_core/orchestration/state.py` | Archive inventory/work state and planning |
| `src/e2ude_core/orchestration/workflow.py` | Per-archive execution and returned archive result |
| `src/e2ude_core/runtime_files.py` | File type and parser specs |
| `src/e2ude_core/services/file_catalog.py` | File typing and hashing |
| `scripts/refresh-data.ps1` | Compatibility wrapper for refresh target/schema selection |
| `scripts/preview_parser.py` | Compatibility wrapper for single-file parser preview |
| `scripts/run_fixture_zip_e2e.py` | Single-zip end-to-end validation |
| `scripts/measure_discovery.py` | Discovery baseline and incremental-vs-reconcile measurement |

When adding or changing a parsed file type, update `src/e2ude_core/runtime_files.py`.
