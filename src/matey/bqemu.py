from __future__ import annotations

from urllib.parse import SplitResult, parse_qsl, urlencode, urlsplit, urlunsplit

DEFAULT_BIGQUERY_EMULATOR_IMAGE = "ghcr.io/recidiviz/bigquery-emulator:latest"
DEFAULT_BIGQUERY_EMULATOR_PROJECT = "matey"
DEFAULT_BIGQUERY_EMULATOR_LOCATION = "us"
_BIGQUERY_MULTI_REGION = {"us", "eu"}


class BigQueryEmulatorUrlError(ValueError):
    pass


def is_bigquery_emulator_url(url: str) -> bool:
    return urlsplit(url).scheme == "bigquery-emulator"


def build_bigquery_emulator_url(
    *,
    hostport: str,
    project: str,
    dataset: str,
    location: str | None = None,
    query: str = "",
    fragment: str = "",
) -> str:
    path_parts = [project]
    if location is not None:
        path_parts.append(location)
    path_parts.append(dataset)
    return urlunsplit(
        SplitResult(
            scheme="bigquery-emulator",
            netloc=hostport,
            path=f"/{'/'.join(path_parts)}",
            query=query,
            fragment=fragment,
        )
    )


def parse_bigquery_emulator_url(url: str) -> tuple[str, str, str | None, str]:
    parsed = urlsplit(url)
    if parsed.scheme != "bigquery-emulator" or not parsed.netloc:
        raise BigQueryEmulatorUrlError(f"Invalid bigquery-emulator URL: {url!r}")
    segments = [segment for segment in parsed.path.split("/") if segment]
    if len(segments) == 2:
        project, dataset = segments
        return parsed.netloc, project, None, dataset
    if len(segments) == 3:
        project, location, dataset = segments
        return parsed.netloc, project, location, dataset
    raise BigQueryEmulatorUrlError(
        "BigQuery emulator URL must be one of: "
        "bigquery-emulator://<host:port>/<project>/<dataset> or "
        "bigquery-emulator://<host:port>/<project>/<location>/<dataset>."
    )


def rewrite_bigquery_emulator_url(*, base_url: str, scratch_name: str) -> str:
    parsed = urlsplit(base_url)
    if parsed.scheme != "bigquery-emulator" or not parsed.netloc:
        raise BigQueryEmulatorUrlError(
            "BigQuery emulator scratch base URL must include host:port."
        )

    segments = [segment for segment in parsed.path.split("/") if segment]
    if len(segments) == 0 or len(segments) > 3:
        raise BigQueryEmulatorUrlError(
            "BigQuery emulator scratch base URL must be one of: "
            "bigquery-emulator://<host:port>/<project>, "
            "bigquery-emulator://<host:port>/<project>/<dataset>, "
            "bigquery-emulator://<host:port>/<project>/<location>/<dataset>."
        )

    project = segments[0]
    if len(segments) == 1:
        location: str | None = None
    elif len(segments) == 2:
        if is_bigquery_location_like(segments[1]):
            raise BigQueryEmulatorUrlError(
                "Ambiguous BigQuery emulator scratch base URL. Use an explicit dataset "
                "(bigquery-emulator://<host:port>/<project>/<dataset>) or explicit "
                "location+dataset (bigquery-emulator://<host:port>/<project>/<location>/<dataset>)."
            )
        location = None
    else:
        location = segments[1]

    return build_bigquery_emulator_url(
        hostport=parsed.netloc,
        project=project,
        dataset=scratch_name,
        location=location,
        query=parsed.query,
        fragment=parsed.fragment,
    )


def to_dbmate_bigquery_url(url: str) -> str:
    if not is_bigquery_emulator_url(url):
        return url

    hostport, project, location, dataset = parse_bigquery_emulator_url(url)
    remainder = [dataset] if location is None else [location, dataset]
    parsed = urlsplit(url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params.setdefault("disable_auth", "true")
    params.setdefault("endpoint", f"http://{hostport}")
    return urlunsplit(
        SplitResult(
            scheme="bigquery",
            netloc=project,
            path=f"/{'/'.join(remainder)}",
            query=urlencode(params),
            fragment=parsed.fragment,
        )
    )


def is_bigquery_location_like(token: str) -> bool:
    lowered = token.lower()
    return lowered in _BIGQUERY_MULTI_REGION or "-" in lowered


__all__ = [
    "DEFAULT_BIGQUERY_EMULATOR_IMAGE",
    "DEFAULT_BIGQUERY_EMULATOR_LOCATION",
    "DEFAULT_BIGQUERY_EMULATOR_PROJECT",
    "BigQueryEmulatorUrlError",
    "build_bigquery_emulator_url",
    "is_bigquery_emulator_url",
    "is_bigquery_location_like",
    "parse_bigquery_emulator_url",
    "rewrite_bigquery_emulator_url",
    "to_dbmate_bigquery_url",
]
