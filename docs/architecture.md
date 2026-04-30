# Architecture

## Flow

The runtime path is:

1. `src/e2ude_core/main.py` relists known archive directories, checks the non-archive directory frontier for membership changes, records directory snapshots in `metadata_discovery_directory`, and upserts archive inventory/work state.
2. `src/e2ude_core/orchestration/state.py` decides which archives need work and builds per-archive plans.
3. `src/e2ude_core/orchestration/pipeline.py` stages the files needed for active handlers and owns worker cleanup.
4. `src/e2ude_core/orchestration/workflow.py` runs one archive end to end and returns an archive-level result.
5. `src/e2ude_core/pipelines/base.py` writes target tables and artifact metadata.

## Key Modules

| Area | File |
| --- | --- |
| Entry point | `src/e2ude_core/main.py` |
| Runtime file specs | `src/e2ude_core/runtime_files.py` |
| Handler lookup | `src/e2ude_core/registry.py` |
| File typing and hashing | `src/e2ude_core/services/file_catalog.py` |
| Archive planning | `src/e2ude_core/orchestration/state.py` |
| Archive execution | `src/e2ude_core/orchestration/workflow.py` |
| Session/job persistence | `src/e2ude_core/orchestration/managers.py` |
| Parser execution and upload | `src/e2ude_core/pipelines/base.py` |

## Core Tables

- `metadata_archive`
- `metadata_discovery_directory`
- `metadata_hash_registry`
- `metadata_file`
- `metadata_artifact_manifest`
- `processing_sessions`
- `processing_jobs`
- `rsmdata_*`

## Handler Registration

Handled file types are defined in `src/e2ude_core/runtime_files.py`.

That file controls:

- file type names
- path patterns
- parser functions
- handler versions
- expected output models

`src/e2ude_core/registry.py` is derived from those specs. Do not add handlers there directly.

## Read Order

For refresh work:

1. `README.md`
2. `docs/data-refresh.md`
3. `src/e2ude_core/main.py`
4. `src/e2ude_core/orchestration/state.py`
5. `src/e2ude_core/orchestration/pipeline.py`
6. `src/e2ude_core/orchestration/workflow.py`

For parser work:

1. `README.md`
2. `docs/plugin-development.md`
3. `src/e2ude_core/runtime_files.py`
4. `src/e2ude_core/pipelines/parsers/segments.py`
5. `src/e2ude_core/pipelines/parsers/tmptr.py`
