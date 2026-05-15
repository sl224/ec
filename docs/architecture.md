# Architecture

## Flow

The runtime path is:

1. `src/e2ude_core/main.py` walks the configured scan root, discovers archive locators, and reconciles the archive inventory.
2. `src/e2ude_core/orchestration/state.py` decides which archives need work and builds per-archive plans.
3. `src/e2ude_core/orchestration/pipeline.py` submits archive work to worker processes.
4. `src/e2ude_core/orchestration/workflow.py` catalogs one archive, extracts only needed members, hashes on demand, and runs parser work.
5. `src/e2ude_core/pipelines/base.py` writes target tables and artifact metadata.

## Key Modules

| Area | File |
| --- | --- |
| Entry point | `src/e2ude_core/main.py` |
| Runtime file specs | `src/e2ude_core/runtime_files.py` |
| Zip catalog and extraction | `src/e2ude_core/services/zip_io.py` |
| Catalog and hashing | `src/e2ude_core/orchestration/catalog.py` |
| Archive planning | `src/e2ude_core/orchestration/state.py` |
| Archive execution | `src/e2ude_core/orchestration/workflow.py` |
| Session/job persistence | `src/e2ude_core/orchestration/runs.py` |
| Parser execution and upload | `src/e2ude_core/pipelines/base.py` |

## Core Tables

- `metadata_archive`
- `metadata_file`
- `metadata_artifact_manifest`
- `processing_sessions`
- `processing_jobs`
- `rsmdata_*`

## Control Plane

Incremental ingest is driven by desired state:

```text
metadata_archive
  -> metadata_file
  -> metadata_artifact_manifest
  -> rsmdata_* leaf tables
```

Rules:

- `metadata_archive.locator_key` is the normalized locator identity for a discovered zip path.
- `metadata_archive.locator_path` is the last observed filesystem path for that locator.
- `metadata_archive.archive_key` is a non-unique domain label from the TransportRSM filename.
- `metadata_file` is the per-archive zip-member catalog. `content_hash` is nullable until a parser needs that member.
- `content_hash` is the stable content-addressed identity in manifests, jobs, and parser leaf tables.
- `metadata_artifact_manifest` decides whether parser output for a hash/artifact key is current. `target_table` records the current physical table for that artifact.
- `processing_sessions` and `processing_jobs` are audit/debug rows only.

Do not reconstruct planner truth from audit rows. A job can explain a failure,
but the manifest decides whether valid output exists.

Archive moves are treated as new locator observations. The moved archive is
cataloged again, but parser output still dedupes by `content_hash`. If the same
locator is observed later with a different size, refresh stops instead of
silently rewriting cataloged facts.

## Parser Registration

Handled file types are defined in `src/e2ude_core/runtime_files.py`.

That file controls:

- file type names
- path patterns
- parser functions
- parser versions
- expected output models

Do not add a second parser registry. The runtime file specs are the parser table.

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
