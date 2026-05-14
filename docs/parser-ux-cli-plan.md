# Parser UX And CLI Execution Plan

Goal: make parser development, targeted parser backfill, and routine refresh clear,
fast, and hard to point at the wrong database.

This plan is about operator/developer UX without adding a CLI framework, a second
handler registry, or a new control plane.

## Present Problems

- `--target dev` does not say whether the run writes to local SQLite or shared SQL
  Server.
- Parser authors have to know parser names or infer them from production path
  globs.
- Local test files often do not match production RSM path patterns.
- Adding a new parser can make the workflow feel like a whole-lake refresh even
  when the relevant catalog facts already exist.
- Parser failures are hard to iterate on because staged files are cleaned up.
- Related operations are split across PowerShell and several Python scripts.

## Design Invariants

- Runtime file specs remain the only handler table.
- DB write commands always print the resolved target before doing work.
- Parser subset runs do not mark an archive globally complete.
- Parser backfill is driven by `metadata_file` plus `metadata_artifact_manifest`,
  not by whole-archive handler generation.
- New CLI code uses stdlib `argparse`; no Click, Typer, plugin framework, or
  command framework.
- Existing scripts may remain temporarily as thin wrappers, but duplicated logic
  should move toward one CLI path.
- The CLI should be built from three plain data operations:
  - target resolution
  - parser resolution
  - parser work planning
- These operations should be boring functions, not a command framework.

## Target Vocabulary

Use explicit destinations:

- `--env dev`: shared SQL Server, schema `e2ude_core_dev`
- `--env prod`: shared SQL Server, schema `e2ude_core`
- `--schema NAME`: custom SQL Server schema
- `--sqlite PATH`: local SQLite file

Every DB-writing command prints a banner:

```text
backend   mssql
server    RSSC30-DB0140
database  AnalyticsDataMart
schema    e2ude_candidate_status
class     disposable
command   trial status
```

## Commands

```powershell
e2ude parser list
e2ude parser status --env dev

e2ude parser preview C:\temp\sample_MCData
e2ude parser preview C:\temp\weird_status_sample.txt --as status
e2ude parser preview C:\temp\TMPTR_LOG

e2ude parser trial status --schema e2ude_candidate_status --limit 50
e2ude parser trial --from-file C:\temp\sample_Status.txt --schema e2ude_candidate_status

e2ude parser backfill status --schema e2ude_candidate_status --dry-run
e2ude parser backfill status --schema e2ude_candidate_status

e2ude refresh --env dev
e2ude refresh --env prod --confirm e2ude_core
e2ude refresh --schema e2ude_candidate_status

e2ude schema cleanup e2ude_candidate_status --preview
e2ude schema promote e2ude_candidate_status prod --preview
```

## Parser Selection

Parser resolution order:

1. Explicit `--as`.
2. Exact parser id, file type, or unique prefix.
3. Production path pattern match.
4. Local filename hint derived from pattern basename, for example `TMPTR_LOG`.
5. Helpful failure that lists likely parser ids and local hints.

Do not try every parser until one succeeds. Parser failure is not a reliable
wrong-parser signal, and some parsers may accept bad input too easily.

## Stages

### Stage 1: Target Clarity

- [x] Add `src/e2ude_core/cli.py`.
- [x] Add `[project.scripts] e2ude = "e2ude_core.cli:main"`.
- [x] Implement `refresh` with `--env`, `--schema`, `--sqlite`, and `--preview`.
- [x] Print the target banner before DB writes.
- [x] Keep `scripts/refresh-data.ps1` as a temporary thin wrapper.

Tests:

- [x] CLI preview prints MSSQL target banner for `--env dev`.
- [x] CLI preview prints schema override.
- [x] CLI preview prints SQLite file target.
- [x] Unsafe schema names are rejected before execution.

### Stage 2: Parser Listing

- [x] Implement `e2ude parser list`.
- [x] Show parser id, file type, version, local hint, patterns, and output tables.
- [x] Add `--counts` to join against DB catalog/manifest when a target is provided.

Tests:

- [x] Parser listing includes every handled runtime spec.
- [x] Counts report files, distinct hashes, complete artifacts, and missing/stale
      artifacts.

### Stage 3: Local Preview

- [x] Move `scripts/preview_parser.py` logic behind `e2ude parser preview`.
- [x] Support `--as` for explicit parser/file-type selection.
- [x] Support local filename hints for files that do not match production paths.
- [x] Keep JSON output available for scriptability.

Tests:

- [x] Explicit `--as` works for arbitrary local filenames.
- [x] Production pattern detection still works.
- [x] Local hint detection works for `TMPTR_LOG`.
- [ ] Ambiguous/unknown selection prints useful choices.

### Stage 4: Parser Trial

- [x] Implement `e2ude parser trial PARSER`.
- [x] Implement `e2ude parser trial --from-file FILE` as parser inference for cataloged work.
- [x] Select matching `metadata_file.file_type` rows.
- [x] Prefer distinct hashes.
- [x] Stage only needed files by exact cataloged relative path.
- [x] Run only the selected parser.
- [x] Write audit session/job rows and row counts.
- [x] Do not mark archive processing complete for subset runs.

Tests:

- [x] Trial/query path selects only matching file types.
- [x] Subset run does not mark archive globally complete.
- [x] Subset run records rows uploaded in audit tables.

### Stage 5: Parser Backfill

- [x] Implement `e2ude parser backfill PARSER --dry-run`.
- [x] Implement `e2ude parser backfill PARSER`.
- [x] Drive work from:
  - `metadata_file.file_type`
  - `metadata_file.hash_id`
  - expected output models from runtime specs
  - `metadata_artifact_manifest`
- [x] Process only missing/stale artifacts for the selected parser.
- [x] Avoid staging archives that do not contain matching pending files.

Tests:

- [x] Backfill plan excludes unrelated file types.
- [ ] Backfill skips hashes with complete current manifest rows.
- [ ] Backfill processes stale manifest versions.
- [x] Backfill does not mark unrelated archive work complete.

### Stage 6: Failure Debugging

- [x] On parser failure, preserve the failing staged file or copy it to a failure
      directory.
- [x] Print archive id, file id, hash id, parser id, relative path, job id, and
      failure location.
- [x] Add `e2ude parser retry-failed PARSER`.

Tests:

- [ ] Failed parser trial leaves a reproducible failure artifact.
- [x] Retry-failed selects only failed audit rows for the requested parser.

### Stage 7: Script Cleanup

- [x] Delete broken `scripts/gen_rsm_fixture.py`.
- [x] Fold `preview_parser.py` into CLI as a thin wrapper.
- [x] Leave `run_fixture_zip_e2e.py` as a focused compatibility script.
- [x] Leave `measure_discovery.py` as a focused compatibility script.
- [x] Fold `cleanup_mssql_schema.py` and `promote_schema.py` into CLI schema
      commands or leave them as thin wrappers.
- [x] Update docs to teach the CLI first.

Tests:

- [x] Existing script behavior remains covered before wrappers are deleted.

## Plan Review

### Gaps

- Parser subset completion needs an explicit invariant: subset runs must update
  artifacts, jobs, and row counts, but not broad archive completion state.
- New parser patterns that were not previously cataloged still require a catalog
  pass. Backfill can be narrow only after `metadata_file` knows those files exist.
- A backfill may need to stage from archives whose catalog is stale. The plan
  should either require current scan generation or run a targeted scan first.
- Local filename hints help preview, but they cannot safely replace explicit
  parser selection for ambiguous names.
- Counts should distinguish files from distinct hashes; parser cost follows
  hashes more than file instances.

### Cross-Cutting Simplifications

- One target resolver can serve refresh, trial, backfill, schema cleanup, schema
  promote, fixture E2E, and discovery measurement. It should return plain data:
  backend, config path, sqlite path, schema, target class, and resolved settings.
- One parser resolver can serve `parsers`, `preview`, `trial`, `backfill`, and
  `retry-failed`. It should read only runtime specs and return one spec.
- One target banner can replace repeated print/config explanations in scripts.
- One "parser work query" can serve parser counts, trial planning, backfill
  planning, and retry planning by changing only filters and limits.
- One staging primitive should accept explicit relative paths to extract. That
  solves trial, backfill, failure reproduction, and future targeted refresh.
- The CLI is a delivery mechanism, not the architecture. The architecture win is
  making the three pieces of hidden state visible and reusable.
- Parser backfill should have a clear "catalog first" failure mode. If a parser
  has no `metadata_file` rows, the command should say that catalog rows are
  missing rather than pretending there is no work.

### Simplicity Review

- Avoid building a CLI application framework. A command table of plain functions
  is enough.
- Avoid making parser UX interactive by default. Helpful errors and explicit
  choices are more scriptable.
- Avoid a second parser registry. The runtime specs already contain parser id,
  file type, patterns, version, function, and output tables.
- Avoid treating `--target dev` as a product concept. Show the real backend,
  server, database, and schema every time.
- Avoid making archive state more complex to support parser trials. Let subset
  operations be artifact/job operations until full archive completion is truly
  proven.

### Implementation Status

Implemented in branch `simplify-etl-control-plane`:

1. `e2ude` CLI with target resolution, parser resolution, preview, refresh,
   parser listing, parser counts, parser trial/backfill, retry-failed, and
   schema cleanup/promote.
2. `scripts/refresh-data.ps1` and `scripts/preview_parser.py` as compatibility
   wrappers around the CLI path.
3. Targeted parser work remains artifact/job-driven and does not update broad
   archive completion.
4. Full pytest coverage passed locally, with local integration tests skipped
   unless fixture/MSSQL env vars are configured.

Remaining follow-up candidates:

1. Add explicit tests for stale manifest version reprocessing.
2. Add explicit tests for parser failure artifact copying.
3. Decide whether the older MSSQL schema scripts should become wrappers after
   local-MSSQL integration coverage is exercised.

### Full Execution Rule

If executing the whole plan, keep the implementation in the same order:

1. Create target/parser resolution first.
2. Add parser work planning in read-only mode.
3. Only then add trial/backfill execution.
4. Keep subset execution artifact-driven; do not update broad archive completion.
5. Prefer thin wrappers over duplicated scripts until tests prove the CLI path.
