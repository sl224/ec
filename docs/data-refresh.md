# Production Refresh

## Standard Run

Routine refreshes can run from any machine that has:

- scan root: `\\Rsiny1-ilsfs\RSM`
- destination database: the shared MSSQL database
- target schema: chosen explicitly on every run
- entry point: `uv run e2ude refresh`
- enough free space in the local OS temp area for selected parser inputs

```powershell
uv run e2ude refresh --env dev --preview
uv run e2ude refresh --env dev
uv run e2ude refresh --env prod --confirm e2ude_core
uv run e2ude refresh --schema e2ude_candidate_mytest --preview
```

The CLI sets `E2UDE_DATABASE__SCHEMA_NAME` to:

- `e2ude_core_dev` for `--env dev`
- `e2ude_core` for `--env prod`

`--schema` writes to a named MSSQL schema for candidate builds and experiments.

Do not use plain `uv run -m e2ude_core.main` for routine refreshes. Use the CLI so the target schema is explicit and printed before writes begin.

For command-by-command examples, see [CLI Workflows](cli-workflows.md).

After pulling the latest changes, the normal incremental ingest into `e2ude_core_dev` is:

```powershell
uv run e2ude refresh --env dev
```

Use `--preview` first if you want to confirm the resolved backend, server, database, and schema without starting the ingest.

## Refresh Config

Place a machine-local `e2ude_config.toml` in the repo root of whatever machine you are using for the refresh.

Use [e2ude_config.refresh.example.toml](../e2ude_config.refresh.example.toml) as the template. It should contain the real MSSQL server and database settings. `staging_root` is optional; if omitted, runs extract selected parser inputs under the local OS temp area in an `e2ude_core_staging` folder. Set it only when staging should use a specific disk. The CLI overrides the database backend/schema choice, so the base config can stay fixed while operators choose `--env dev`, `--env prod`, `--schema`, or `--sqlite` per run.

## Concurrent Runs

Multiple operators can run refreshes at the same time without intentionally sharing a work queue.

- Concurrent runs are tolerated by the current MSSQL locking and manifest checks.
- Overlapping runs may still catalog or hash the same archive members redundantly.
- One operator at a time is still the most efficient choice when possible.

## What The Run Does

`src/e2ude_core/main.py`:

1. enumerates archive locators under the configured scan root
2. records locator facts and marks missing locators absent
3. asks the planner which archives still need work
4. catalogs zip members for uncataloged archives without extracting leaf files
5. extracts and hashes only members needed by active parsers
6. runs parser jobs for missing or stale outputs

The planner is incremental because parser outputs are keyed by content hash.
Moving an archive creates a new locator observation and marks the old locator
absent after a full scan. The moved archive may be cataloged again, but parser
output still dedupes by member `content_hash`.
Discovery walks the configured scan root on every refresh:

- directory scans only identify archive locators
- zip contents are read later, only for archives that need catalog or parser work
- presence reconciliation happens after a complete scan

Planning is in `src/e2ude_core/orchestration/state.py`.
Per-archive execution and archive-level result reporting are in `src/e2ude_core/orchestration/workflow.py`.

## Incremental State

The planner uses current desired state, not audit history:

- `metadata_archive` stores discovered archive locators. `locator_key` is unique, `locator_path` is the last observed path, and `archive_key` is a non-unique filename label.
- `metadata_file` stores zip members discovered inside each archive. `content_hash` is nullable until a parser needs that member.
- `metadata_artifact_manifest` stores valid parser outputs by `content_hash`, logical artifact key, target table, and parser version.
- `processing_sessions` and `processing_jobs` record what happened for debugging.

Parser output is current when the manifest has a row for the file `content_hash` and
artifact key at the current parser version, and the row points at the parser's
current target table. Successful empty outputs still get manifest rows with
`row_count = 0`.

## Overrides

Config-first runtime overrides are preferred. If you need an env override, use the nested Pydantic names:

- `E2UDE_PATHS__SCAN_ROOT`
- `E2UDE_PATHS__STAGING_ROOT`
- `E2UDE_RUNTIME__DISCOVERY_WORKERS`
- `E2UDE_RUNTIME__PROCESS_WORKERS`
- `E2UDE_DIAGNOSTICS__ENABLE_VIZTRACER`

For routine refreshes, keep the standard scan root and use the CLI target instead of setting `E2UDE_DATABASE__SCHEMA_NAME` by hand unless you are troubleshooting.
For parser experiments, prefer CLI flags over env overrides:

```powershell
uv run e2ude parser list
uv run e2ude parser status --env dev
uv run e2ude parser preview C:\temp\sample_MCData
uv run e2ude parser preview C:\temp\mystery_input.txt --as segments
uv run e2ude parser backfill segments --schema e2ude_candidate_segments --plan
uv run e2ude parser backfill segments --schema e2ude_candidate_segments --limit 50
```

For schema rebuilds where existing content-addressed parser output is still
valid, seed a candidate before running refresh:

```powershell
uv run e2ude schema seed --from e2ude_core_dev --to e2ude_candidate_seeded --plan
uv run e2ude schema seed --from e2ude_core_dev --to e2ude_candidate_seeded --yes
uv run e2ude refresh --schema e2ude_candidate_seeded
```

Seeding never copies source archive inventory rows. The current scan owns
`metadata_archive`; source catalog and parser facts are reused only through
`catalog_signature` and `content_hash`.

Use `--plan` first. It reports how much catalog and parser output can be reused
without writing to the destination schema.

Parser backfill is catalog-driven. If there are no `metadata_file` rows for that parser, run a refresh first; backfill will not invent catalog facts from a local sample file.
After a parser failure, fix the problem and rerun `parser backfill` or `refresh`; missing/stale manifest rows remain pending until they complete.

If you want to force staging onto a specific drive for one run:

```powershell
uv run e2ude refresh --env dev --staging-root D:\E2UDE_STAGING
```

Raw archive payloads are assumed immutable. If an archive moves, refresh records
a new locator and marks the old locator absent after the full scan completes.
Parser output still dedupes by file `content_hash`. If the same locator appears
with a different size or member catalog, refresh stops instead of rewriting
existing catalog facts.

## Troubleshooting

If the run finds no work:

- confirm the run is still pointed at `\\Rsiny1-ilsfs\RSM`
- confirm you chose the intended target (`--env dev`, `--env prod`, `--schema`, or `--sqlite`)
- use `uv run e2ude refresh --env dev --preview` to confirm the resolved target before a real run
- confirm archive names still match `*TransportRSM.fpkg.e2d.zip`
- inspect `src/e2ude_core/orchestration/state.py`

If an archive is cataloged but not parsed:

- inspect `processing_sessions`
- inspect `processing_jobs`
- inspect `metadata_artifact_manifest`

If outputs are missing:

- list parser/catalog state with `uv run e2ude parser status --env dev`
- preview one extracted file with `uv run e2ude parser preview C:\path\to\file --as parser`
- plan the missing parser with `uv run e2ude parser backfill parser --env dev --plan`
- run one archive with `scripts/run_fixture_zip_e2e.py`
- verify the file type and parser version in `src/e2ude_core/runtime_files.py`

## Non-Prod Validation

SQLite, fixture mirrors, and candidate MSSQL schemas are for development and validation. Use [docs/plugin-development.md](plugin-development.md) for those flows.
