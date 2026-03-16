from __future__ import annotations

from enum import StrEnum
from fnmatch import fnmatch
import re


SAFE_SCHEMA_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PROTECTED_SHARED_SCHEMAS = frozenset({"e2ude_core", "e2ude_core_dev"})
DISPOSABLE_SCHEMA_PATTERNS = (
    "e2ude_tmp_*",
    "e2ude_core_fixture_*",
    "e2ude_candidate_*",
    "e2ude_archive_*",
)


class SchemaClassification(StrEnum):
    PROTECTED_SHARED = "PROTECTED_SHARED"
    DISPOSABLE = "DISPOSABLE"
    CUSTOM = "CUSTOM"


def validate_schema_name(schema_name: str) -> str:
    if not SAFE_SCHEMA_RE.fullmatch(schema_name):
        raise ValueError(
            f"Refusing unsafe schema name {schema_name!r}. Use only letters, digits, and underscores."
        )
    return schema_name


def is_protected_schema(schema_name: str) -> bool:
    return schema_name.casefold() in {
        candidate.casefold() for candidate in PROTECTED_SHARED_SCHEMAS
    }


def is_disposable_schema(schema_name: str) -> bool:
    lowered = schema_name.casefold()
    return any(
        fnmatch(lowered, pattern.casefold()) for pattern in DISPOSABLE_SCHEMA_PATTERNS
    )


def schema_classification(schema_name: str) -> SchemaClassification:
    if is_protected_schema(schema_name):
        return SchemaClassification.PROTECTED_SHARED
    if is_disposable_schema(schema_name):
        return SchemaClassification.DISPOSABLE
    return SchemaClassification.CUSTOM


def require_exact_confirmation(
    *,
    expected_schema: str,
    provided_schema: str | None,
    flag_name: str,
) -> None:
    if provided_schema != expected_schema:
        raise SystemExit(
            f"Refusing destructive action without {flag_name} {expected_schema!r}."
        )


def format_target_banner(db_settings, *, schema_name: str) -> str:
    server_name = getattr(db_settings, "server_name", "<unknown-server>")
    db_name = getattr(db_settings, "db_name", "<unknown-db>")
    classification = schema_classification(schema_name)
    return (
        f"Target server=[{server_name}] database=[{db_name}] "
        f"schema=[{schema_name}] classification=[{classification.value}]"
    )
