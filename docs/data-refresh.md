# Production Refresh

## Standard Run

Routine refreshes can run from any machine that has:

- scan root: `\\Rsiny1-ilsfs\RSM`
- destination database: the shared MSSQL database
- target schema: chosen explicitly on every run
- entry point: `scripts/refresh-data.ps1`
- enough free space in the local OS temp area, unless you override staging

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

Use [e2ude_config.refresh.example.toml](../e2ude_config.refresh.example.toml) as the template. It should contain the real MSSQL server and database settings. `staging_root` is optional; if omitted, runs stage under the local OS temp area in an `e2ude_core_staging` folder. Set it only when you want staging on a specific disk. The wrapper overrides only the schema, so the base config can stay fixed while operators choose `dev` or `prod` per run.

## Concurrent Runs

Multiple operators can run refreshes at the same time without intentionally sharing a work queue.

- Concurrent runs are tolerated by the current MSSQL locking and manifest checks.
- Overlapping runs may still stage and parse the same folders redundantly.
- One operator at a time is still the most efficient choice when possible.

## What The Run Does

`src/e2ude_core/main.py`:

1. enumerates source archives with correctness-first discovery and records directory snapshots
2. upserts archive inventory and source facts
3. asks the planner which archives still need work
4. stages required files
5. runs metadata scans where needed
6. runs parser jobs for missing or stale outputs

The planner is still incremental because unchanged archives are skipped after their stored source facts are compared against the latest discovered source facts in `metadata_archive`.
The discovery path is also incremental again, but only with safe signals:

- known archive directories are relisted every run
- non-archive frontier directories are checked by directory membership signals
- `reconcile` still does a full source walk

Planning is in `src/e2ude_core/orchestration/state.py`.
Per-archive execution and archive-level result reporting are in `src/e2ude_core/orchestration/workflow.py`.

## Overrides

Config-first runtime overrides are preferred. If you need an env override, use the nested Pydantic names:

- `E2UDE_PATHS__SCAN_ROOT`
- `E2UDE_PATHS__STAGING_ROOT`
- `E2UDE_RUNTIME__DISCOVERY_MODE`
- `E2UDE_RUNTIME__DISCOVERY_WORKERS`
- `E2UDE_RUNTIME__PIPELINE_BUFFER_SIZE`
- `E2UDE_RUNTIME__UNZIP_WORKERS`
- `E2UDE_RUNTIME__PROCESS_WORKERS`
- `E2UDE_RUNTIME__DB_WRITE_WORKERS`
- `E2UDE_DIAGNOSTICS__ENABLE_VIZTRACER`

For routine refreshes, keep the standard scan root and use the wrapper target instead of setting `E2UDE_DATABASE__SCHEMA_NAME` by hand unless you are troubleshooting.

If you want to force staging onto a specific drive for one run:

```powershell
.\scripts\refresh-data.ps1 -Target dev -StagingRoot D:\E2UDE_STAGING
```

If you need to force a full source walk:

```powershell
$env:E2UDE_RUNTIME__DISCOVERY_MODE = "reconcile"
.\scripts\refresh-data.ps1 -Target dev
```

`incremental` no longer relies on directory-`mtime` subtree skipping for known archive directories, so in-place archive edits are still discovered.

## Troubleshooting

If the run finds no work:

- confirm the run is still pointed at `\\Rsiny1-ilsfs\RSM`
- confirm you chose the intended wrapper target (`dev` or `prod`)
- use `.\scripts\refresh-data.ps1 -Target dev -Preview` to confirm the resolved target before a real run
- confirm archive names still match `*TransportRSM.fpkg.e2d.zip`
- inspect `src/e2ude_core/orchestration/state.py`

If an archive is scanned but not parsed:

- inspect `processing_sessions`
- inspect `processing_jobs`
- inspect `metadata_artifact_manifest`

If outputs are missing:

- preview one extracted file with `scripts/preview_parser.py`
- run one archive with `scripts/run_fixture_zip_e2e.py`
- verify the file type and handler version in `src/e2ude_core/runtime_files.py`

## Non-Prod Validation

SQLite, fixture mirrors, and candidate MSSQL schemas are for development and validation. Use [docs/plugin-development.md](plugin-development.md) for those flows.
