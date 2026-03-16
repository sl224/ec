# Production Refresh

## Standard Run

Routine refreshes can run from any machine that has:

- scan root: `\\Rsiny1-ilsfs\RSM`
- destination database: the shared MSSQL database
- target schema: chosen explicitly on every run
- entry point: `scripts/refresh-data.ps1`
- enough local disk for staging

```powershell
.\scripts\refresh-data.ps1 -Target dev -Preview
.\scripts\refresh-data.ps1 -Target dev
.\scripts\refresh-data.ps1 -Target prod
```

The wrapper sets `E2UDE_DATABASE__SCHEMA_NAME` to:

- `e2ude_core_dev` for `-Target dev`
- `e2ude_core` for `-Target prod`

Do not use plain `uv run -m e2ude_core.main` for routine refreshes. The wrapper exists so operators always choose the target schema deliberately.

After pulling the latest changes, the normal incremental ingest into `e2ude_core_dev` is:

```powershell
.\scripts\refresh-data.ps1 -Target dev
```

Use `-Preview` first if you want to confirm the resolved config path and target schema without starting the ingest.

## Refresh Config

Place a machine-local `e2ude_config.toml` in the repo root of whatever machine you are using for the refresh.

Use [e2ude_config.refresh.example.toml](../e2ude_config.refresh.example.toml) as the template. It should contain the real MSSQL server, database, driver, and any machine-specific staging root. The wrapper overrides only the schema, so the base config can stay fixed while operators choose `dev` or `prod` per run.

## Concurrent Runs

Multiple operators can run refreshes at the same time without intentionally sharing a work queue.

- Concurrent runs are tolerated by the current MSSQL locking and manifest checks.
- Overlapping runs may still stage and parse the same folders redundantly.
- One operator at a time is still the most efficient choice when possible.

## What The Run Does

`src/e2ude_core/main.py`:

1. discovers transport zips
2. registers folders
3. asks the planner which folders still need work
4. stages required files
5. runs metadata scans where needed
6. runs parser jobs for missing or stale outputs

Planning is in `src/e2ude_core/orchestration/state.py`.
Per-folder execution and folder-level result reporting are in `src/e2ude_core/orchestration/workflow.py`.

## Overrides

Config-first runtime overrides are preferred. If you need an env override, use the nested Pydantic names:

- `E2UDE_PATHS__SCAN_ROOT`
- `E2UDE_PATHS__STAGING_ROOT`
- `E2UDE_RUNTIME__DISCOVERY_WORKERS`
- `E2UDE_RUNTIME__PIPELINE_BUFFER_SIZE`
- `E2UDE_RUNTIME__UNZIP_WORKERS`
- `E2UDE_RUNTIME__PROCESS_WORKERS`
- `E2UDE_RUNTIME__DB_WRITE_WORKERS`
- `E2UDE_DIAGNOSTICS__ENABLE_VIZTRACER`

For routine refreshes, keep the standard scan root and use the wrapper target instead of setting `E2UDE_DATABASE__SCHEMA_NAME` by hand unless you are troubleshooting.

## Troubleshooting

If the run finds no work:

- confirm the run is still pointed at `\\Rsiny1-ilsfs\RSM`
- confirm you chose the intended wrapper target (`dev` or `prod`)
- use `.\scripts\refresh-data.ps1 -Target dev -Preview` to confirm the resolved target before a real run
- confirm archive names still match `*TransportRSM.fpkg.e2d.zip`
- inspect `src/e2ude_core/orchestration/state.py`

If a folder is scanned but not parsed:

- inspect `processing_sessions`
- inspect `processing_jobs`
- inspect `metadata_artifact_manifest`

If outputs are missing:

- preview one extracted file with `scripts/preview_parser.py`
- run one archive with `scripts/run_fixture_zip_e2e.py`
- verify the file type and handler version in `src/e2ude_core/runtime_files.py`

## Non-Prod Validation

SQLite, fixture mirrors, and candidate MSSQL schemas are for development and validation. Use [docs/plugin-development.md](plugin-development.md) for those flows.
