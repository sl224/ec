# CLI Workflows

## Target Selection

Every write command must name its destination.

| Use case | Command | Writes to |
| --- | --- | --- |
| Preview shared dev | `uv run e2ude refresh --env dev --preview` | nothing |
| Refresh shared dev | `uv run e2ude refresh --env dev` | `e2ude_core_dev` |
| Refresh prod directly | `uv run e2ude refresh --env prod --confirm e2ude_core` | `e2ude_core` |
| Use candidate schema | `uv run e2ude refresh --schema e2ude_candidate_tmptr` | named MSSQL schema |
| Use local SQLite | `uv run e2ude refresh --sqlite local.sqlite3 --preview` | local file |

Rules:

- `--env dev` and `--env prod` are shared MSSQL aliases.
- `--schema NAME` is a custom MSSQL schema for experiments or candidates.
- `--sqlite PATH` is local-only.
- Prod writes require exact confirmation.

## Normal Dev Refresh

```powershell
uv run e2ude refresh --env dev --preview
uv run e2ude refresh --env dev
```

The refresh is incremental. It updates archive inventory, scans changed archives,
and parses only missing or stale content-hash artifacts.

## Direct Prod Refresh

Use only when direct prod mutation is acceptable.

```powershell
uv run e2ude refresh --env prod --preview
uv run e2ude refresh --env prod --confirm e2ude_core
```

There is no schema-level rollback for a direct prod refresh.

## Prod Candidate Refresh

Use when prod should stay untouched until validation passes.

```powershell
uv run e2ude schema clone prod e2ude_candidate_20260506
uv run e2ude refresh --schema e2ude_candidate_20260506
uv run e2ude schema check e2ude_candidate_20260506
uv run e2ude schema promote e2ude_candidate_20260506 prod --yes --confirm e2ude_core
```

`schema promote` transfers tables. It does not row-merge candidate changes back
into prod.

## Parser Development

Find parser ids:

```powershell
uv run e2ude parser list
```

Preview one local file without a database:

```powershell
uv run e2ude parser preview C:\temp\sample_TMPTR_LOG
uv run e2ude parser preview C:\temp\sample.txt --as segments
```

Build and test one parser in a candidate schema:

```powershell
uv run e2ude refresh --schema e2ude_candidate_segments
uv run e2ude parser status segments --schema e2ude_candidate_segments
uv run e2ude parser backfill segments --schema e2ude_candidate_segments --dry-run
uv run e2ude parser backfill segments --schema e2ude_candidate_segments --limit 50
```

`parser backfill` is catalog-driven. If the file was never scanned into
`metadata_file`, run a refresh first.

## Parser Rebuilds

Preferred path after parser logic changes:

```text
bump handler version in src/e2ude_core/runtime_files.py
```

Then run:

```powershell
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr --dry-run
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr
```

Force a rebuild without changing the version:

```powershell
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr --force --dry-run
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr --force
```

Invalidate manifest rows so the next plan sees work:

```powershell
uv run e2ude artifacts invalidate tmptr_log --schema e2ude_candidate_tmptr --dry-run
uv run e2ude artifacts invalidate tmptr_log --schema e2ude_candidate_tmptr --yes
```

## Failure Triage

Inspect parser completeness:

```powershell
uv run e2ude parser status --env dev
uv run e2ude artifacts status tmptr_log --schema e2ude_candidate_tmptr
```

Retry failed audit rows:

```powershell
uv run e2ude parser retry-failed tmptr_log --schema e2ude_candidate_tmptr --dry-run
uv run e2ude parser retry-failed tmptr_log --schema e2ude_candidate_tmptr
```

Audit tables explain failures. They do not control incremental skips.

## Legacy Prod Seed

Use when old prod has `metadata_folder` / `folder_id` and the current code
expects `metadata_archive` / `archive_id`.

```powershell
uv run python scripts/seed_legacy_schema.py e2ude_core e2ude_candidate_legacy_seed
uv run python scripts/seed_legacy_schema.py e2ude_core e2ude_candidate_legacy_seed --yes
uv run e2ude schema check e2ude_candidate_legacy_seed
```

The seed script reads the source schema only. It writes a new current-shape
schema, maps `folder_id` to `archive_id`, converts legacy MD5 text when needed,
copies compatible hash-addressed leaf tables, and rebuilds artifact manifest
row counts.

The default folder mapping is `auto`: first exact path, then unique archive file
name. Use `--folder-map exact-path` or `--folder-map archive-name` only when you
need to force one strategy.
