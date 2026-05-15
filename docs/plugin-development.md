# Parser Development

## Entry Points

When you add or change a handled file type, start in `src/e2ude_core/runtime_files.py`.

That file defines:

- file type
- match patterns
- parser function
- parser version
- expected output models

Those specs are also the parser lookup table used by planning, preview, upload,
and table creation.

## Workflow

1. Add or update the parser under `src/e2ude_core/pipelines/parsers/`.
2. Update the spec in `src/e2ude_core/runtime_files.py`.
3. Add or update the SQLAlchemy models if the output shape changed.
4. Add or update tests.
5. Preview one file with `e2ude parser preview`.
6. Run one archive with `scripts/run_fixture_zip_e2e.py`.
7. Use a candidate MSSQL schema only if you need non-local validation.

## Validation

For the full operator command set, see [CLI Workflows](cli-workflows.md).

Preview one file:

```powershell
uv run e2ude parser list
uv run e2ude parser preview C:\temp\sample_MCData
uv run e2ude parser preview C:\temp\mystery_input.txt --as segments
```

The preview command can infer normal production-pattern filenames and local hints such as `TMPTR_LOG`. Use `--as` when a local file name is arbitrary.

Run one archive:

```powershell
$env:E2UDE_CONFIG_PATH = ".\e2ude_config.local.toml"
uv run python scripts/run_fixture_zip_e2e.py C:\local\e2ude_fixtures\169871\2023\11\169871_20231107_024218_987_TransportRSM.fpkg.e2d.zip
```

Backfill only one parser from cataloged files:

```powershell
uv run e2ude parser backfill segments --schema e2ude_candidate_segments --plan
uv run e2ude parser backfill segments --schema e2ude_candidate_segments --limit 50
```

Backfill writes audit rows and artifact manifest rows. A normal refresh uses the
same artifact planner after archive cataloging.

## Versioning

Bump the parser version in `src/e2ude_core/runtime_files.py` when existing hashes should be reprocessed for that output.

The planner compares parser versions against `metadata_artifact_manifest` by content hash and logical artifact key. If the stored version is behind, or the artifact now writes to a different target table, the file hash is scheduled again for that parser.

## Reference Files

| Task | File |
| --- | --- |
| Runtime file spec | `src/e2ude_core/runtime_files.py` |
| Parser lookup | `src/e2ude_core/runtime_files.py` |
| Parser CLI | `src/e2ude_core/cli.py` |
| Zip catalog and extraction | `src/e2ude_core/services/zip_io.py` |
| Catalog and hashing | `src/e2ude_core/orchestration/catalog.py` |
| Example parser | `src/e2ude_core/pipelines/parsers/segments.py` |
| Example parser | `src/e2ude_core/pipelines/parsers/tmptr.py` |
| End-to-end helper | `scripts/run_fixture_zip_e2e.py` |
| Regression coverage | `tests/test_runtime_regressions.py` |
