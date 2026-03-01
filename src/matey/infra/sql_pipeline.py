from __future__ import annotations

import difflib
from urllib.parse import urlsplit

from matey.app.protocols import ISqlPipeline
from matey.domain.lockfile import digest_text_blake2b256
from matey.domain.model import Engine, PreparedSql, SqlComparison, SqlSource


class SqlPipeline(ISqlPipeline):
    def prepare(self, *, engine: Engine, source: SqlSource) -> PreparedSql:
        normalized = _normalize_text(source.text)
        normalized = _normalize_by_origin(engine=engine, normalized=normalized, source=source)
        return PreparedSql(normalized=normalized, digest=digest_text_blake2b256(normalized))

    def compare(self, *, engine: Engine, expected: SqlSource, actual: SqlSource) -> SqlComparison:
        expected_prepared = self.prepare(engine=engine, source=expected)
        actual_prepared = self.prepare(engine=engine, source=actual)
        equal = expected_prepared.normalized == actual_prepared.normalized
        diff_text: str | None = None
        if not equal:
            diff = difflib.unified_diff(
                expected_prepared.normalized.splitlines(keepends=True),
                actual_prepared.normalized.splitlines(keepends=True),
                fromfile="expected",
                tofile="actual",
            )
            diff_text = "".join(diff)
        return SqlComparison(expected=expected_prepared, actual=actual_prepared, equal=equal, diff=diff_text)


def _normalize_text(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return ""
    return normalized + "\n"


def _normalize_by_origin(*, engine: Engine, normalized: str, source: SqlSource) -> str:
    if not normalized:
        return ""
    if engine != Engine.BIGQUERY:
        return normalized
    if source.origin == "artifact":
        return normalized
    if source.context_url is None:
        return normalized
    return _normalize_bigquery_dataset_qualifiers(normalized, source.context_url)


def _normalize_bigquery_dataset_qualifiers(text: str, url: str) -> str:
    parsed = urlsplit(url)
    project = parsed.netloc
    parts = [part for part in parsed.path.split("/") if part]
    dataset: str | None = None
    if len(parts) == 1:
        dataset = parts[0]
    elif len(parts) >= 2:
        dataset = parts[-1]

    if not project or not dataset:
        return text

    # Normalize scratch dataset names to deterministic token for comparison/digest stability.
    return text.replace(f"`{project}.{dataset}.", f"`{project}.__MATEY_DATASET__.").replace(
        f" {project}.{dataset}.",
        f" {project}.__MATEY_DATASET__.",
    )
