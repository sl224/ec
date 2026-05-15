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

- `--env dev` and `--env prod` select the shared MSSQL schemas.
- `--schema NAME` is a custom MSSQL schema for experiments or candidates.
- `--sqlite PATH` is local-only.
- Prod writes require exact confirmation.

## Normal Dev Refresh

```powershell
uv run e2ude refresh --env dev --preview
uv run e2ude refresh --env dev
```

The refresh is incremental. It updates archive inventory, catalogs uncataloged
zip members, hashes only parser-relevant members, and parses only missing or
stale content-hash artifacts.

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
uv run e2ude refresh --schema e2ude_candidate_20260506
uv run e2ude schema check e2ude_candidate_20260506
uv run e2ude schema promote e2ude_candidate_20260506 prod --yes --confirm e2ude_core
```

This builds the candidate from the source archive share, not from prod rows.
`schema promote` transfers tables. It does not merge candidate rows into prod.
Keep the archived prod schema until the promoted schema has been validated.

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
uv run e2ude parser backfill segments --schema e2ude_candidate_segments --plan
uv run e2ude parser backfill segments --schema e2ude_candidate_segments --limit 50
```

`parser backfill` is catalog-driven. If the file was never cataloged into
`metadata_file`, run a refresh first.

## Parser Rebuilds

Preferred path after parser logic changes:

```text
bump parser version in src/e2ude_core/runtime_files.py
```

Then run:

```powershell
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr --plan
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr
```

Force a rebuild without changing the version:

```powershell
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr --force --plan
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr --force
```

Invalidate manifest rows so the next plan sees work:

```powershell
uv run e2ude parser invalidate tmptr_log --schema e2ude_candidate_tmptr --plan
uv run e2ude parser invalidate tmptr_log --schema e2ude_candidate_tmptr --yes
```

## Failure Triage

Inspect parser completeness:

```powershell
uv run e2ude parser status --env dev
uv run e2ude parser status tmptr_log --schema e2ude_candidate_tmptr
```

After fixing a parser bug or transient data issue, rerun the same missing work:

```powershell
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr --plan
uv run e2ude parser backfill tmptr_log --schema e2ude_candidate_tmptr
```

Audit tables explain previous failures. They do not decide what needs work.

## Clean Rebuild

Use this when the current schema should be replaced and reprocessing cost is acceptable.

```powershell
uv run e2ude refresh --schema e2ude_candidate_rebuild_20260514 --preview
uv run e2ude refresh --schema e2ude_candidate_rebuild_20260514
uv run e2ude schema check e2ude_candidate_rebuild_20260514
uv run e2ude schema promote e2ude_candidate_rebuild_20260514 prod --yes --confirm e2ude_core
```

The clean rebuild path uses the archive share plus parser versions in
`src/e2ude_core/runtime_files.py`.

## Seeded Rebuild

Use this when rebuilding schema shape but existing content-addressed output is
still valid. It is the faster rebuild path: create a fresh candidate, reuse
catalog and parser facts that still match current code, then let `refresh`
process only the gaps.

```powershell
uv run e2ude schema seed --from e2ude_core_dev --to e2ude_candidate_seeded --plan
uv run e2ude schema seed --from e2ude_core_dev --to e2ude_candidate_seeded --yes
uv run e2ude refresh --schema e2ude_candidate_seeded
uv run e2ude schema check e2ude_candidate_seeded
uv run e2ude schema promote e2ude_candidate_seeded prod --yes --confirm e2ude_core
```

`--plan` reports reusable catalog rows, parser rows, and current scan coverage
without writing anything.

`schema seed --yes`:

- creates the destination runtime schema from current code
- scans the current archive share
- registers current archive locators in the destination
- reuses source catalog rows by locator or catalog signature
- copies compatible parser rows by `content_hash`

It does not copy source archive inventory rows or processing audit rows. The
destination `metadata_archive` always comes from the current scan. `refresh`
handles anything that could not be reused.
