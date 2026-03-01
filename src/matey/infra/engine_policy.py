from __future__ import annotations

import re
from urllib.parse import SplitResult, urlsplit, urlunsplit

from matey.app.protocols import EngineClassifierPolicy, EnginePolicy, IEnginePolicyRegistry
from matey.domain.engine import Engine
from matey.domain.errors import EngineInferenceError

_BIGQUERY_MULTI_REGION = {"us", "eu"}


class EnginePolicyRegistry(IEnginePolicyRegistry):
    def __init__(self) -> None:
        self._policies: dict[Engine, EnginePolicy] = {
            Engine.POSTGRES: EnginePolicy(
                wait_required=True,
                requires_test_url_for_index0=False,
                build_scratch_url=_replace_path_segment,
                classifier=EngineClassifierPolicy(
                    missing_db_positive=(r"does not exist", r"3d000"),
                    missing_db_negative=(r"connection refused", r"i/o timeout", r"no such host", r"28p01"),
                    create_exists=(),
                    create_fatal=(r"permission denied", r"access denied"),
                ),
            ),
            Engine.MYSQL: EnginePolicy(
                wait_required=True,
                requires_test_url_for_index0=False,
                build_scratch_url=_replace_path_segment,
                classifier=EngineClassifierPolicy(
                    missing_db_positive=(r"unknown database", r"\b1049\b"),
                    missing_db_negative=(r"connection refused", r"i/o timeout", r"no such host", r"\b1045\b"),
                    create_exists=(),
                    create_fatal=(r"permission denied", r"access denied"),
                ),
            ),
            Engine.SQLITE: EnginePolicy(
                wait_required=False,
                requires_test_url_for_index0=False,
                build_scratch_url=_sqlite_scratch_url,
                classifier=EngineClassifierPolicy(
                    missing_db_positive=(
                        r"unable to open database file",
                        r"cannot open database file",
                        r"no such file or directory",
                    ),
                    missing_db_negative=(r"permission denied",),
                    create_exists=(),
                    create_fatal=(r"permission denied",),
                ),
            ),
            Engine.CLICKHOUSE: EnginePolicy(
                wait_required=True,
                requires_test_url_for_index0=False,
                build_scratch_url=_replace_path_segment,
                classifier=EngineClassifierPolicy(
                    missing_db_positive=(r"database .* does not exist", r"code:\s*81"),
                    missing_db_negative=(r"connection refused", r"i/o timeout", r"no such host", r"code:\s*516"),
                    create_exists=(),
                    create_fatal=(r"permission denied", r"access denied"),
                ),
            ),
            Engine.BIGQUERY: EnginePolicy(
                wait_required=False,
                requires_test_url_for_index0=True,
                build_scratch_url=_bigquery_scratch_url,
                classifier=EngineClassifierPolicy(
                    missing_db_positive=(),
                    missing_db_negative=(),
                    create_exists=(r"already exists", r"alreadyexists"),
                    create_fatal=(
                        r"permission denied",
                        r"access denied",
                        r"project .* not found",
                        r"notfound",
                        r"location",
                        r"quota",
                        r"ratelimit",
                        r"invalid",
                        r"badrequest",
                    ),
                ),
            ),
        }

    def get(self, engine: Engine) -> EnginePolicy:
        return self._policies[engine]


def detect_engine_from_url(url: str) -> Engine:
    scheme = urlsplit(url).scheme.lower()
    if scheme in {"postgres", "postgresql"}:
        return Engine.POSTGRES
    if scheme == "mysql":
        return Engine.MYSQL
    if scheme in {"sqlite", "sqlite3"}:
        return Engine.SQLITE
    if scheme.startswith("clickhouse"):
        return Engine.CLICKHOUSE
    if scheme == "bigquery":
        return Engine.BIGQUERY
    raise EngineInferenceError(f"Unsupported database URL scheme: {scheme}")


def classify_missing_db(policy: EnginePolicy, detail_text: str) -> bool:
    text = detail_text.lower()
    if any(re.search(pattern, text) for pattern in policy.classifier.missing_db_negative):
        return False
    return any(re.search(pattern, text) for pattern in policy.classifier.missing_db_positive)


def classify_create_outcome(policy: EnginePolicy, detail_text: str) -> str:
    text = detail_text.lower()
    if any(re.search(pattern, text) for pattern in policy.classifier.create_fatal):
        return "fatal"
    if any(re.search(pattern, text) for pattern in policy.classifier.create_exists):
        return "exists"
    return "ok"


def _replace_path_segment(base_url: str, scratch_name: str) -> str:
    parsed = urlsplit(base_url)
    rebuilt = SplitResult(
        scheme=parsed.scheme,
        netloc=parsed.netloc,
        path=f"/{scratch_name}",
        query=parsed.query,
        fragment=parsed.fragment,
    )
    return urlunsplit(rebuilt)


def _sqlite_scratch_url(base_url: str, scratch_name: str) -> str:
    parsed = urlsplit(base_url)
    path = parsed.path or parsed.netloc
    if not path:
        return f"sqlite3:{scratch_name}.sqlite3"
    from pathlib import Path

    base = Path(path)
    parent = base.parent if base.suffix else base
    scratch = parent / f"{scratch_name}.sqlite3"
    return f"sqlite3:{scratch.as_posix()}"


def _is_location_like(token: str) -> bool:
    lowered = token.lower()
    return lowered in _BIGQUERY_MULTI_REGION or "-" in lowered


def _bigquery_scratch_url(base_url: str, scratch_name: str) -> str:
    parsed = urlsplit(base_url)
    if not parsed.netloc:
        raise EngineInferenceError("BigQuery scratch base URL must include project host")

    segments = [seg for seg in parsed.path.split("/") if seg]
    if len(segments) > 2:
        raise EngineInferenceError(
            "BigQuery scratch base URL must be one of: "
            "bigquery://<project>, bigquery://<project>/<location>, "
            "bigquery://<project>/<location>/<dataset> or bigquery://<project>/<dataset>."
        )

    if len(segments) == 0:
        scratch_segments = [scratch_name]
    elif len(segments) == 1:
        if _is_location_like(segments[0]):
            scratch_segments = [segments[0], scratch_name]
        else:
            scratch_segments = [scratch_name]
    else:
        scratch_segments = [segments[0], scratch_name]

    rebuilt = SplitResult(
        scheme=parsed.scheme,
        netloc=parsed.netloc,
        path=f"/{'/'.join(scratch_segments)}",
        query=parsed.query,
        fragment=parsed.fragment,
    )
    return urlunsplit(rebuilt)
