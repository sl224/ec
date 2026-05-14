# Production Refresh

## Standard Run

Routine refreshes can run from any machine that has:

- scan root: `\\Rsiny1-ilsfs\RSM`
- destination database: the shared MSSQL database
- target schema: chosen explicitly on every run
- entry point: `uv run e2ude refresh`
- enough free space in the local OS temp area, unless you override staging

```powershell
uv run e2ude refresh --env dev --preview
uv run e2ude refresh --env dev
uv run e2ude refresh --env prod --confirm e2ude_core
uv run e2ude refresh --schema e2ude_candidate_mytest --preview

.\scripts\refresh-data.ps1 -Target dev -Preview
.\scripts\refresh-data.ps1 -Target dev
```

The CLI sets `E2UDE_DATABASE__SCHEMA_NAME` to:

- `e2ude_core_dev` for `--env dev`
- `e2ude_core` for `--env prod`

`--schema` overrides that target default when you intentionally want a custom candidate schema. The PowerShell wrapper remains for operators who prefer `-Target dev`/`-Target prod`; it calls the CLI path.

Do not use plain `uv run -m e2ude_core.main` for routine refreshes. The CLI exists so operators always choose the target schema deliberately and see the resolved target before writes begin.

For command-by-command examples, see [CLI Workflows](cli-workflows.md).

After pulling the latest changes, the normal incremental ingest into `e2ude_core_dev` is:

```powershell
uv run e2ude refresh --env dev
```

Use `--preview` first if you want to confirm the resolved backend, server, database, and schema without starting the ingest.

## Refresh Config

Place a machine-local `e2ude_config.toml` in the repo root of whatever machine you are using for the refresh.

Use [e2ude_config.refresh.example.toml](../e2ude_config.refresh.example.toml) as the template. It should contain the real MSSQL server and database settings. `staging_root` is optional; if omitted, runs stage under the local OS temp area in an `e2ude_core_staging` folder. Set it only when you want staging on a specific disk. The CLI overrides only the database backend/schema choice, so the base config can stay fixed while operators choose `--env dev`, `--env prod`, a deliberate `--schema`, or a local `--sqlite` file per run.

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

## Incremental State

The planner uses current desired state, not audit history:

- `metadata_archive` stores source archive facts and metadata scan freshness.
- `metadata_file` stores files discovered inside each archive.
- `metadata_hash_registry` stores unique file content hashes.
- `metadata_artifact_manifest` stores valid parser outputs by `hash_id`, target table, and parser version.
- `processing_sessions` and `processing_jobs` record what happened for debugging.

Parser output is current when the manifest has a row for the file `hash_id` and
target table at the current parser version. Successful empty outputs still get
manifest rows with `row_count = 0`.

## Overrides

Config-first runtime overrides are preferred. If you need an env override, use the nested Pydantic names:

- `E2UDE_PATHS__SCAN_ROOT`
- `E2UDE_PATHS__STAGING_ROOT`
- `E2UDE_RUNTIME__DISCOVERY_MODE`
- `E2UDE_RUNTIME__DISCOVERY_WORKERS`
- `E2UDE_RUNTIME__PIPELINE_BUFFER_SIZE`
- `E2UDE_RUNTIME__UNZIP_WORKERS`
- `E2UDE_RUNTIME__PROCESS_WORKERS`
- `E2UDE_DIAGNOSTICS__ENABLE_VIZTRACER`

For routine refreshes, keep the standard scan root and use the CLI target instead of setting `E2UDE_DATABASE__SCHEMA_NAME` by hand unless you are troubleshooting.
For parser experiments, prefer CLI flags over env overrides:

```powershell
uv run e2ude parser list
uv run e2ude parser status --env dev
uv run e2ude parser preview C:\temp\sample_MCData
uv run e2ude parser preview C:\temp\mystery_input.txt --as segments
uv run e2ude parser backfill segments --schema e2ude_candidate_segments --dry-run
uv run e2ude parser backfill segments --schema e2ude_candidate_segments --limit 50
uv run e2ude parser retry-failed segments --schema e2ude_candidate_segments --dry-run
```

Parser backfill is catalog-driven. If there are no `metadata_file` rows for that parser, run a refresh/scan first; backfill will not invent catalog facts from a local sample file.

If you want to force staging onto a specific drive for one run:

```powershell
uv run e2ude refresh --env dev --staging-root D:\E2UDE_STAGING
```

If you need to force a full source walk:

```powershell
$env:E2UDE_RUNTIME__DISCOVERY_MODE = "reconcile"
uv run e2ude refresh --env dev
```

`incremental` no longer relies on directory-`mtime` subtree skipping for known archive directories, so in-place archive edits are still discovered.

## Troubleshooting

If the run finds no work:

- confirm the run is still pointed at `\\Rsiny1-ilsfs\RSM`
- confirm you chose the intended target (`--env dev`, `--env prod`, `--schema`, or `--sqlite`)
- use `uv run e2ude refresh --env dev --preview` to confirm the resolved target before a real run
- confirm archive names still match `*TransportRSM.fpkg.e2d.zip`
- inspect `src/e2ude_core/orchestration/state.py`

If an archive is scanned but not parsed:

- inspect `processing_sessions`
- inspect `processing_jobs`
- inspect `metadata_artifact_manifest`

If outputs are missing:

- list parser/catalog state with `uv run e2ude parser status --env dev`
- preview one extracted file with `uv run e2ude parser preview C:\path\to\file --as parser`
- plan the missing parser with `uv run e2ude parser backfill parser --env dev --dry-run`
- run one archive with `scripts/run_fixture_zip_e2e.py`
- verify the file type and parser version in `src/e2ude_core/runtime_files.py`

## Non-Prod Validation

SQLite, fixture mirrors, and candidate MSSQL schemas are for development and validation. Use [docs/plugin-development.md](plugin-development.md) for those flows.
