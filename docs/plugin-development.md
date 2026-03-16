# Plugin Development

## Entry Points

When you add or change a handled file type, start in `src/e2ude_core/runtime_files.py`.

That file defines:

- file type
- match patterns
- parser function
- handler version
- expected output models

`src/e2ude_core/registry.py` is built from those specs.

## Workflow

1. Add or update the parser under `src/e2ude_core/pipelines/parsers/`.
2. Update the spec in `src/e2ude_core/runtime_files.py`.
3. Add or update the SQLAlchemy models if the output shape changed.
4. Add or update tests.
5. Preview one file with `scripts/preview_parser.py`.
6. Run one archive with `scripts/run_fixture_zip_e2e.py`.
7. Use a candidate MSSQL schema only if you need non-local validation.

## Validation

Preview one file:

```powershell
uv run python scripts/preview_parser.py C:\temp\sample_MCData
uv run python scripts/preview_parser.py C:\temp\mystery_input.txt --file-type SEGMENTS
```

Run one archive:

```powershell
$env:E2UDE_CONFIG_PATH = ".\e2ude_config.local.toml"
uv run python scripts/run_fixture_zip_e2e.py C:\local\e2ude_fixtures\169871\2023\11\169871_20231107_024218_987_TransportRSM.fpkg.e2d.zip
```

## Versioning

Bump the handler version in `src/e2ude_core/runtime_files.py` when existing hashes should be reprocessed for that output.

The planner compares handler versions against `metadata_artifact_manifest`. If the stored version is behind, the folder is scheduled again.

## Reference Files

| Task | File |
| --- | --- |
| Runtime file spec | `src/e2ude_core/runtime_files.py` |
| Handler lookup | `src/e2ude_core/registry.py` |
| File typing and hashing | `src/e2ude_core/services/file_catalog.py` |
| Example parser | `src/e2ude_core/pipelines/parsers/segments.py` |
| Example parser | `src/e2ude_core/pipelines/parsers/tmptr.py` |
| Preview helper | `scripts/preview_parser.py` |
| End-to-end helper | `scripts/run_fixture_zip_e2e.py` |
| Regression coverage | `tests/test_runtime_regressions.py` |
