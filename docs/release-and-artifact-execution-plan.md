# Release And Artifact Execution Plan

Goal: keep the useful content-addressed design, make prod refresh safer, and
avoid building a release workflow engine.

This plan intentionally chooses a simpler operating model:

```text
direct incremental refresh for dev
optional manual blue/green for prod
no stateful release sessions
no delta merge-back
no custom migration framework
```

The best patch should make the current system more honest and inspectable, not
create a second orchestration product inside the ETL.

## Current Facts

- `prod` is physical MSSQL schema `e2ude_core`.
- `dev` is physical MSSQL schema `e2ude_core_dev`.
- Parsed leaf tables are content addressed by `hash_id`.
- `metadata_hash_registry` maps `hash_id` to file digest.
- `metadata_file` maps archive file instances to `hash_id`.
- Parsed data tables carry `hash_id` as part of their primary key.
- `metadata_artifact_manifest` currently records:
  - `hash_id`
  - `target_table`
  - `handler_version`
  - `row_count`
  - `created_at`
- `processing_sessions` and `processing_jobs` are audit/debug rows, not skip
  state.
- Existing ingest is mostly resumable because planning is driven by archive
  state and artifact manifest state.

## Decisions

- Keep explicit `handler_version` as the semantic invalidation knob.
- Do not add parser code hashing.
- Do not add output schema signatures for now.
- Do not build our own Alembic.
- Do not build a stateful release workflow with `start/status/resume/use`.
- Do not build row-level delta merge-back into prod.
- Do not build row-level copy-forward until measurements prove it is needed.
- Add only artifact metadata that removes current ambiguity.
- Use simple, explicit schema operations for blue/green.

## Supported Workflows

### 1. Normal Dev Refresh

Use this when shared dev can be updated in place:

```powershell
uv run e2ude refresh --env dev
```

Behavior:

- writes directly to `e2ude_core_dev`
- processes only missing/stale work
- uses existing resumable ingest behavior

### 2. Parser Experiment

Use this when developing a parser without disturbing shared dev:

```powershell
uv run e2ude parser preview C:\temp\sample_TMPTR_LOG --as tmptr_log
uv run e2ude refresh --schema e2ude_candidate_tmptr
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr --dry-run
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr
```

Behavior:

- candidate schema is an experiment sandbox
- parser preview is DB-free
- parser backfill can target one parser
- dev/prod remain untouched

### 3. Direct Prod Refresh

Use only when direct mutation is acceptable:

```powershell
uv run e2ude refresh --env prod --confirm e2ude_core
```

Behavior:

- writes directly to `e2ude_core`
- processes only missing/stale work
- no clean schema-level rollback

Guardrail:

- prod refresh must require exact confirmation
- output must print server, database, schema, and command before writes

### 4. Manual Blue/Green Prod Refresh

Use when prod should not be touched until validation passes:

```powershell
uv run e2ude schema clone prod e2ude_candidate_20260506
uv run e2ude refresh --schema e2ude_candidate_20260506
uv run e2ude schema check e2ude_candidate_20260506
uv run e2ude schema promote e2ude_candidate_20260506 prod --yes --confirm e2ude_core
```

Behavior:

- clone creates current runtime tables in a candidate schema
- clone copies compatible, non-audit source rows into candidate
- refresh updates candidate incrementally
- check validates candidate
- promote archives current prod and transfers candidate tables into prod
- publish is table/schema transfer, not row merge

If the shell dies:

- rerun the failed step if safe
- or clean up the candidate manually:

```powershell
uv run e2ude schema cleanup e2ude_candidate_20260506 --dry-run
uv run e2ude schema cleanup e2ude_candidate_20260506 --yes --confirm e2ude_candidate_20260506
```

No release ledger. No multi-user adoption. No hidden state machine.

## Rejected Workflows

### Delta Candidate Merge-Back

```text
candidate contains only new incremental rows
publish merges those rows back into prod
```

Rejected because it would require custom merge logic for:

- archive metadata
- directory snapshots
- hash registry
- file metadata
- artifact manifest
- every parsed leaf table
- partial failure recovery

If blue/green full candidate is too expensive, prefer direct prod refresh with
confirmation before building a delta merge system.

### Stateful Release Builder

```powershell
e2ude release start prod
e2ude release clone
e2ude release refresh
e2ude release resume
e2ude release publish
```

Rejected for now because it requires:

- durable release ledger
- per-table step tracking
- multi-user release adoption semantics
- resume logic
- publish interruption recovery
- local active-release pointers or DB selection rules

Those are real release-system concerns. They are not needed yet.

### Row-Level Copy-Forward

```text
copy compatible parsed rows from one schema into another by hash digest
```

Deferred as a separate parser-level feature. The implemented schema clone already
does the simpler useful thing: create the current runtime schema and copy
compatible source rows by table, preserving content-addressed `hash_id` values
when the source table is compatible. It does not do per-parser row matching or
cross-schema transformation logic.

## Stage 1: Harden Artifact Manifest

Problem:

`metadata_artifact_manifest` can currently say "this hash/table/version exists",
but it cannot distinguish:

```text
not processed
```

from:

```text
processed successfully and produced zero rows
```

Patch:

- Add `row_count` to `metadata_artifact_manifest`.
- Add `created_at` to `metadata_artifact_manifest`.
- Keep primary key as `(hash_id, target_table)`.
- Do not add `pipeline_id`.
- Do not add output schema signatures.

Runtime behavior:

- Parser upload writes/replaces one manifest row per expected output table.
- Non-empty output records actual row count.
- Empty successful output deletes stale rows for that `hash_id/table` and writes
  `row_count = 0`.
- Missing expected parser output should be an explicit parser contract error or
  an explicit zero-row decision. Do not silently leave stale data.

Tests:

- Non-empty output records row count.
- Empty output records `row_count = 0`.
- Empty output deletes stale rows from a previous run.
- Planner skips a hash/table when current manifest row has `row_count = 0`.
- Existing parser and ingest tests still pass.

## Stage 2: Artifact And Parser Status

Add read-only visibility before more mutation.

Commands:

```powershell
uv run e2ude parser status --env dev
uv run e2ude parser status tmptr_log --schema e2ude_candidate_tmptr
uv run e2ude artifacts status --schema e2ude_candidate_tmptr
```

Output should show:

- parser id
- file type
- handler version
- target tables
- cataloged files
- distinct hashes
- complete current artifacts
- missing/stale artifacts
- total materialized rows from manifest
- failed/running jobs

Tests:

- Status matches manifest counts for SQLite fixture data.
- Zero-row artifacts count as complete.
- Missing/stale artifact counts remain correct.

## Stage 3: Force And Invalidate

Problem:

If parser logic changes but table shape does not, the primary answer is still:

```text
bump handler_version
```

But users need an experiment escape hatch.

Commands:

```powershell
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr --force --dry-run
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr --force
uv run e2ude artifacts invalidate tmptr_log --schema e2ude_candidate_tmptr --dry-run
uv run e2ude artifacts invalidate tmptr_log --schema e2ude_candidate_tmptr --yes
```

Rules:

- `--force` ignores current manifest version for the selected parser and
  overwrites artifacts for matching hashes.
- `artifacts invalidate` deletes manifest rows for selected parser output
  tables.
- Do not delete leaf rows by default.
- Add `--delete-rows` later only if needed.

Tests:

- `--force --dry-run` shows hashes that would otherwise be skipped.
- `--force` rewrites manifest row counts.
- `invalidate` makes the next backfill plan see work.

## Stage 4: Env Aliases And Prod Guardrail

Add simple env aliases:

```text
dev  -> e2ude_core_dev
prod -> e2ude_core
```

Commands:

```powershell
uv run e2ude env status dev
uv run e2ude env status prod
uv run e2ude refresh --env dev
uv run e2ude refresh --env prod --confirm e2ude_core
```

Rules:

- `--env` and `--schema` are mutually exclusive.
- `--schema` is for explicit schema names and experiments.
- `refresh --env prod` refuses without exact confirmation.
- All write commands print resolved target before work starts.

Tests:

- Env aliases resolve correctly.
- Direct prod refresh refuses without exact confirmation.
- Dev refresh target preview works.

## Stage 5: Schema Clone

Command:

```powershell
uv run e2ude schema clone prod e2ude_candidate_20260506
uv run e2ude schema clone e2ude_core e2ude_candidate_20260506
uv run e2ude schema clone prod e2ude_candidate_20260506 --replace --yes
```

Behavior:

- Creates candidate schema with current runtime tables and constraints.
- Copies compatible source rows into candidate with SQL Server-side operations.
- Omits `processing_sessions` and `processing_jobs`; they are audit history.
- Skips incompatible tables instead of inventing migrations.
- Does not mutate source schema.
- If candidate exists, refuse unless `--replace`.

Implementation preference:

- Keep it explicit and table-oriented.
- No release ledger.
- No resumable session tracking.
- If interrupted, operator can inspect/check/cleanup and rerun with `--replace`.

Tests:

- Clone refuses protected destination names.
- Clone creates expected table set.
- Clone row counts match source for compatible copied tables.
- Clone refuses non-empty destination unless explicitly replaced.

## Legacy Prod Seed

Use this when the source schema predates the `folder_id` to `archive_id`
control-plane change:

```powershell
uv run python scripts/seed_legacy_schema.py e2ude_core e2ude_candidate_legacy_seed
uv run python scripts/seed_legacy_schema.py e2ude_core e2ude_candidate_legacy_seed --yes
uv run e2ude schema check e2ude_candidate_legacy_seed
```

Behavior:

- Reads legacy source schema only.
- Creates the destination with current runtime tables.
- Copies `metadata_archive` and `metadata_discovery_directory`.
- Converts `metadata_hash_registry.md5` from hex text to `varbinary(16)` when
  needed.
- Maps old `metadata_file.folder_id` through `metadata_folder.FolderPath` to the
  current archive row, then writes `metadata_file.archive_id`. The default is
  `--folder-map auto`, which tries exact path first and then a unique archive
  filename match. If those do not match, it derives current archive rows directly
  from legacy `metadata_folder`. Path matching uses Windows path normalization
  without touching the filesystem.
- Copies compatible parsed leaf tables by `hash_id`.
- Rebuilds `metadata_artifact_manifest.row_count` from copied leaf rows at the
  current handler versions.
- Omits old processing audit tables.

Safety:

- Does not modify the source schema.
- Refuses protected destination schemas.
- Refuses non-empty destinations unless `--replace --confirm-dest NAME` is used.
- Refuses unmapped file rows unless `--allow-unmapped-files` is explicit.

## Stage 6: Schema Check

Command:

```powershell
uv run e2ude schema check e2ude_candidate_20260506
uv run e2ude schema check prod
uv run e2ude schema check dev
```

Checks:

- schema exists
- runtime tables exist
- required runtime columns exist
- no running jobs
- failed jobs are reported as audit/debug information
- parser artifact completeness
- archive state summary
- top table row counts

Tests:

- Missing schema is reported clearly.
- Missing runtime table/column is reported clearly.
- Running and failed jobs are reported clearly.
- Missing/stale artifacts are reported clearly.

## Stage 7: Schema Promote And Rollback

Existing promote should become the blessed low-level blue/green publish command.

Commands:

```powershell
uv run e2ude schema promote e2ude_candidate_20260506 prod --dry-run
uv run e2ude schema promote e2ude_candidate_20260506 prod --yes --confirm e2ude_core

uv run e2ude schema promote e2ude_core_archive_20260506_143012 prod --dry-run
uv run e2ude schema promote e2ude_core_archive_20260506_143012 prod --yes --confirm e2ude_core
```

Behavior:

- Source schema must be complete.
- Target env schema is archived automatically.
- Source candidate tables transfer into target schema.
- Publish is schema/table transfer, not row merge.
- Archive schema name is printed for rollback.

Tests:

- Promote refuses incomplete candidate.
- Promote requires exact confirmation for prod/dev targets.
- Promote archives existing target tables.
- Promote transfers candidate tables into target schema.
- Rollback uses the same command with archive schema as source.

## Stage 8: Interrupted State Review

Current ingest:

- Discovery/register operations are transactional enough to retry.
- If scan metadata commits but scan-complete mark does not, next run rescans.
- If scan-complete commits but parser work does not, archive remains
  `NEEDS_PROCESSING`.
- Parser table uploads are transactional per table.
- Completed parser tables are recorded in manifest and skipped later.
- Missing parser tables are planned later.
- Stale running jobs are culled at startup.

Current gaps:

- Empty successful outputs need manifest rows with `row_count = 0`.
- Stale staging directories may remain after process death, but this is not a DB
  correctness issue.
- Direct prod refresh can leave partial new artifacts by design; this is why it
  needs confirmation.

Schema operations:

- Clone interruption can leave a partial candidate. Operator should run
  `schema check`, `schema cleanup`, or `schema clone --replace`.
- Promote interruption is the scariest path. Keep promote short, table-transfer
  based, and protected by confirmation.
- Do not add resume logic until real operational pain justifies it.

## Stage 9: Documentation And Script Surface

Docs should teach:

```text
dev:
  e2ude refresh --env dev

parser experiment:
  e2ude parser preview
  e2ude parser backfill --schema candidate

prod direct:
  e2ude refresh --env prod --confirm e2ude_core

prod blue/green:
  e2ude schema clone prod candidate
  e2ude refresh --schema candidate
  e2ude schema check candidate
  e2ude schema promote candidate prod --yes --confirm e2ude_core
```

Script cleanup:

- Keep `scripts/refresh-data.ps1` as a compatibility wrapper.
- Update wrapper to use env aliases once implemented.
- Keep old schema scripts until CLI schema commands are covered by local MSSQL
  tests.
- Eventually make old schema scripts wrappers or remove them.

## Open Questions

- Should direct prod refresh remain available after blue/green is implemented?
- Should promote drop the empty source candidate schema after table transfer?
- How long should archive schemas be retained?

## Non-Goals

- No Alembic replacement.
- No automatic semantic parser-change detection.
- No output schema signature.
- No stateful release workflow engine.
- No multi-user release adoption.
- No delta candidate merge-back.
- No row-level copy-forward in the first implementation.
- No hidden migration or column transformation logic.

## Implementation Progress

- [x] Manifest hardening: `row_count`, `created_at`, empty-output manifests.
- [x] Parser/artifact status commands.
- [x] Parser backfill `--force`.
- [x] Artifact manifest invalidation.
- [x] Env aliases and direct-prod confirmation.
- [x] Schema check command.
- [x] Schema clone command for compatible runtime tables.
- [x] Schema promote command in the main CLI.
- [x] SQLite CLI e2e tests for parser/artifact workflows.
- [x] Local-MSSQL-gated CLI e2e test for clone/check/promote.
- [ ] Decide whether old schema scripts become wrappers or are deleted.
- [ ] Decide archive schema retention policy.
