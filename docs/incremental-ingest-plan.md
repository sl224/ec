# Incremental Ingest Plan

## Discovery Correctness Fix

Problem:

- directory `mtime` is not a safe generic subtree skip signal
- editing a file in place often changes the file `mtime` but not the parent directory `mtime`
- the current incremental discovery path can therefore miss modified archives inside an "unchanged" directory

Goal:

- restore correctness first
- then rebuild a cheaper incremental scanner around safe source signals

### Execution Plan

#### Phase A: Immediate Safe Rollback

- [x] Remove directory-`mtime` subtree skipping from incremental discovery
- [x] Make incremental discovery fall back to correctness-first source enumeration
- [x] Keep `reconcile` mode available, but do not rely on it to cover routine in-place archive edits

Success criteria:

- a normal run cannot miss an in-place edit of a known archive just because its parent directory `mtime` stayed the same

#### Phase B: Regression Coverage

- [x] Add a regression where a known archive is modified in place
- [x] Preserve the parent directory `mtime` in the test setup
- [x] Prove the archive is still rediscovered and marked changed
- [x] Keep add/remove directory coverage alongside the new in-place edit coverage

Success criteria:

- discovery tests fail under directory-`mtime` subtree skipping and pass under the corrected implementation

#### Phase C: Measure The Safe Baseline

- [x] Add a reusable discovery measurement script
- [x] Capture discovery timing and counts on the fixture mirror after the safe rollback
- [x] Record:
  - directories walked
  - archives enumerated
  - archives changed
  - end-to-end discovery time
- [ ] Capture the same baseline against the real UNC source tree when that path is reachable from the refresh environment

Success criteria:

- we have a repeatable baseline, and we can compare reconcile vs incremental safely

Latest captured baseline via `scripts/measure_discovery.py --scan-root C:\Users\shan\workspace\e2ude_core\fixtures`:

- reconcile: `0.027s`, `6` scanned dirs, `25` archives enumerated, `25` archive rows changed
- incremental: `0.009s`, `4` scanned dirs, `3` archive-dir scans, `1` frontier-dir scan, `2` skipped dirs, `25` archives enumerated, `0` archive rows changed
- real UNC baseline: blocked in this environment because `\\Rsiny1-ilsfs\RSM` was not reachable here at measurement time

#### Phase D: Scanner V2

- [x] Design scanner v2 around safe source signals
- [x] Prefer relisting known archive-containing directories over subtree skipping by directory `mtime`
- [x] Compare listed child file `(size, mtime)` against `metadata_archive`
- [x] Keep new-directory discovery as an explicit problem with an explicit strategy
- [x] Keep `reconcile` mode as a backstop, not as the primary correctness mechanism

Success criteria:

- scanner v2 is cheaper than the safe baseline without reintroducing the in-place edit bug

## Branch Progress

Magic-wand branch status:

- [x] Replace the old archive registry with canonical `metadata_archive` inventory/work-state rows
- [x] Rename the hot path around `archive_id` instead of `folder_id`
- [x] Move planner selection off audit reconstruction and onto archive work state
- [x] Update scan/process completion to write archive state transactionally
- [x] Delete stale file-catalog rows on rescan so the archive catalog reflects current contents
- [x] Roll back unsafe directory-`mtime` subtree skipping
- [x] Rebuild safe incremental discovery after the directory-`mtime` rollback
- [x] Add reusable discovery measurement support
- [ ] Replace archive registration lookups with set-based MSSQL registration
- [ ] Add run-local dedupe before parsing
- [ ] Revisit staging after measuring SMB behavior

## Goal

Make a routine refresh scale with changed archives, not the full historical corpus.

The desired steady-state cost is closer to:

- discover changed source archives cheaply
- register only changed/new source archives
- plan work from a small control-plane query
- stage/scan/parse only changed or invalidated content

and much less like:

- walk the entire share
- re-register every archive
- rebuild readiness state from audit tables
- prove "nothing changed" expensively on every run

## Design Principles

1. Make skip decisions as early as possible.
2. Keep one clear control plane for hot-path planning.
3. Keep audit/history tables out of the hot path.
4. Prefer set-based DB operations over giant parameterized `IN (...)` queries.
5. Add reconciliation explicitly instead of relying on heuristics that can silently miss work.
6. Only add durable state when it removes more work than it adds invalidation complexity.

## Why Not Add Multiple New Sources Of Truth At Once

In the current codebase, adding several new derived state stores at once would increase drift risk faster than it would reduce complexity.

Today the runtime already depends on:

- `metadata_archive`
- `metadata_file`
- `metadata_artifact_manifest`
- `processing_sessions`
- `processing_jobs`

If we immediately add all of the following:

- a discovery inventory
- a folder runtime state cache
- a hash artifact cache
- archive claims

we create several places that all need exact invalidation rules from day one.

For a migration plan, the cleaner move is:

- pick one control-plane direction
- make that authoritative
- let later steps simplify around it

That is different from a clean-slate design.

## If We Had A Clean Slate

No, the plan would not be the same.

Without backward-compat constraints, I would not layer multiple new state stores onto the current structure. I would design one explicit ingest control plane from the start.

### Clean-Slate Target Shape

#### 1. Canonical Archive Inventory

One row per source archive path.

Suggested fields:

- `archive_id`
- `source_path`
- `buno`
- `archive_datetime`
- `file_size_bytes`
- `source_mtime`
- `source_fingerprint`
- `first_seen_at`
- `last_seen_at`
- `is_present`

This replaces the idea of a separate "discovery inventory" plus ad hoc folder registration.

#### 2. Canonical Archive Work State

One row per archive describing what work is required right now.

Suggested fields:

- `archive_id`
- `required_scan_version`
- `completed_scan_version`
- `required_handler_generation`
- `completed_handler_generation`
- `catalog_hash`
- `work_state`
- `last_success_at`
- `last_error_at`
- `last_error_message`
- `claimed_by`
- `claimed_at`
- `claim_expires_at`

This replaces planner reconstruction from audit tables and avoids needing a separate folder-claims table.

#### 3. File Catalog

Only for archives that need or have had scan work.

Suggested fields:

- `archive_id`
- `relative_path`
- `file_type`
- `hash_id`
- `file_size_bytes`

#### 4. Artifact Manifest

Keep content-addressed artifact tracking.

This is already a good idea because dedupe by `hash_id` is genuinely valuable across archives.

#### 5. Audit Log

Keep `processing_sessions` and `processing_jobs` only as an audit/history surface.

They should not be the primary planning/control plane.

### Clean-Slate Execution Model

1. Incremental discovery updates archive inventory from source facts.
2. Source fact change invalidates archive work state.
3. Planner selects archives directly from archive work state.
4. Workers claim archives before staging.
5. Scan and parse update canonical work state transactionally.
6. Audit tables record what happened, but do not decide what should happen.

If we were starting over, this would be the target.

## Pragmatic Migration Plan

This plan is optimized for real value with low-bloat migration from the current system.

### Phase 0: Instrument The Current Pipeline

- [ ] Add timing and count metrics for:
  - discovery
  - folder registration
  - planner selection
  - staging
  - metadata scan
  - parse/upload
- [ ] Record:
  - zips discovered
  - folders inserted
  - folders selected for work
  - bytes staged
  - files scanned
  - files parsed
  - uploads skipped by manifest
- [ ] Write one sample refresh performance report from the real environment

Why first:

- we should know whether the biggest cost is share walk, DB registration, planner reconstruction, or staging/parsing
- this keeps later work honest

### Phase 1: Choose A Single Control-Plane Direction

- [x] Decide whether to extend `metadata_folder` into the canonical archive inventory/work-state row, or introduce one replacement table
- [x] Do not introduce both a discovery inventory table and a separate folder runtime state table in the same step
- [x] Document the chosen control-plane ownership clearly

Recommended pragmatic direction:

- replace `metadata_folder` with `metadata_archive` as the canonical archive inventory/work-state row
- add source facts and current work-state fields there or in one adjacent authoritative table

Why:

- it reduces migration churn
- it avoids proliferating durable state stores

### Phase 2: Make MSSQL Folder Registration Set-Based

- [ ] Replace path batch lookups in `register_archives_bulk()` with temp-table or staging-table registration for MSSQL
- [ ] `MERGE` or equivalent set-based upsert source rows into `metadata_archive`
- [ ] Join back to ids in one set-based step
- [ ] Keep the current chunked path for SQLite/dev fallback

Why:

- this is already a proven hot spot
- it is a direct improvement with low conceptual overhead

### Phase 3: Make Discovery Incremental Using The Chosen Control Plane

- [x] Persist source facts for each archive:
  - path
  - size
  - mtime
  - presence
- [x] On each run, only mark archives as changed/new when those source facts differ
- [x] Add an explicit reconcile mode that does a full source walk and inventory reconciliation
- [x] Keep normal incremental mode cheap and safe

Important:

- do not make "recent folders only" the default without a reconciliation path
- late backfills and corrected older drops must still be discoverable

### Phase 4: Move Planner Selection Off Audit Reconstruction

- [x] Stop deriving hot-path readiness primarily from `processing_sessions` and `processing_jobs`
- [x] Make planner selection query the canonical archive work state directly
- [x] Update work state transactionally when:
  - metadata scan completes
  - artifacts are materialized
  - scanner/handler generation changes invalidate existing work

Why:

- planner cost should be cheap and direct
- audit tables are good history, but poor primary state

### Phase 5: Add Run-Local Dedupe Before Parsing

- [ ] Deduplicate pending work within a run by `(hash_id, file_type, required_models)`
- [ ] Skip parser invocation when another work item in the same run already proves the same artifact set
- [ ] Keep `ArtifactManifest` as the durable cross-run dedupe source
- [ ] Do not add a second durable hash-status cache unless metrics prove it is necessary

Why:

- this cuts duplicate parse CPU with very little new complexity
- it avoids creating another table that mirrors manifest state

### Phase 6: Re-evaluate Staging Strategy With Measurements

- [ ] Measure:
  - source zip size
  - selected member count
  - time spent copying full zips
  - time spent extracting selected members
  - SMB throughput behavior
- [ ] Compare:
  - current whole-zip local copy then selective extract
  - direct selective extract from the network zip
- [ ] Only change staging strategy if the measurements clearly favor it

Why:

- direct-from-network extraction sounds attractive but may be worse on SMB
- this should be benchmark-driven, not assumed

### Phase 7: Add Folder Claims Only If Multi-Operator Refresh Is A Real Requirement

- [ ] If operators truly need to contribute concurrently, add DB-backed archive claims
- [ ] Claim work before staging
- [ ] Expire stale claims safely
- [ ] Keep claim state in the canonical archive work state, not in yet another independent table if possible

Why later:

- one-at-a-time refresh remains the simplest model
- claims add real state complexity and should only be added if they solve a real workflow problem

## Recommended Execution Order

1. Phase 0: instrumentation
2. Phase 1: choose one control-plane direction
3. Phase 2: MSSQL set-based folder registration
4. Phase 3: incremental discovery
5. Phase 4: planner on canonical work state
6. Phase 5: run-local dedupe before parse
7. Phase 6: benchmark-driven staging changes
8. Phase 7: optional claims

## What We Should Not Do Early

- [ ] add several new durable caches at once
- [ ] rely on recency heuristics without reconciliation
- [ ] add multi-operator claims before single-run efficiency is fixed
- [ ] optimize parser internals before fixing front-of-pipeline waste
- [ ] change staging mechanics without measuring SMB behavior

## Definition Of Success

A routine incremental refresh should eventually look like:

- cheap source fact check
- very small changed-archive set
- cheap planner query
- limited staging
- scan/parse only for genuinely changed or invalidated content

And it should no longer look like:

- full share walk
- re-registration of nearly everything
- hot-path derivation from audit tables
- expensive proof that "nothing changed"
