from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import tomlkit
from tomlkit.items import AoT, Table

from matey.config import (
    DEFAULT_CONFIG_VALUES,
    Config,
    ConfigError,
    normalize_target_names,
    target_default_dir,
    target_env_stem,
)

TemplateProvider = Literal["github", "gitlab", "buildkite"]


def _github_template() -> str:
    return (
        "name: matey-schema\n"
        "on:\n"
        "  pull_request:\n"
        "jobs:\n"
        "  schema:\n"
        "    runs-on: ubuntu-latest\n"
        "    steps:\n"
        "      - uses: actions/checkout@v4\n"
        "      - uses: prefix-dev/setup-pixi@v0\n"
        "      - run: pixi install\n"
        "      - run: pixi run matey schema status --all\n"
        '      - run: pixi run matey schema plan --all --base "${{ github.base_ref }}"\n'
    )


def _gitlab_template() -> str:
    return (
        "stages:\n"
        "  - validate\n"
        "\n"
        "matey_schema:\n"
        "  stage: validate\n"
        "  image: ghcr.io/prefix-dev/pixi:latest\n"
        "  script:\n"
        "    - pixi install\n"
        "    - pixi run matey schema status --all\n"
        '    - pixi run matey schema plan --all --base "$CI_MERGE_REQUEST_TARGET_BRANCH_NAME"\n'
    )


def _buildkite_template() -> str:
    return (
        "steps:\n"
        '  - label: ":matey: schema"\n'
        "    command:\n"
        "      - pixi install\n"
        "      - pixi run matey schema status --all\n"
        '      - pixi run matey schema plan --all --base "$BUILDKITE_PULL_REQUEST_BASE_BRANCH"\n'
    )


_CI_TEMPLATES: dict[TemplateProvider, Callable[[], str]] = {
    "github": _github_template,
    "gitlab": _gitlab_template,
    "buildkite": _buildkite_template,
}
_CI_TEMPLATE_PATHS: dict[TemplateProvider, Path] = {
    "github": Path(".github/workflows/matey-schema.yml"),
    "gitlab": Path("matey.gitlab-ci.yml"),
    "buildkite": Path(".buildkite/matey-schema.yml"),
}


@dataclass(frozen=True, slots=True)
class ConfigDocument:
    defaults: dict[str, str]
    targets: dict[str, dict[str, str]]


def render_config_template(targets: tuple[str, ...]) -> str:
    try:
        normalized_targets = normalize_target_names(targets)
    except ConfigError as error:
        raise ValueError(str(error)) from error
    _require_unique_env_stems(normalized_targets)
    return render_config_document(
        ConfigDocument(
            defaults=dict(DEFAULT_CONFIG_VALUES),
            targets={
                target: {
                    "dir": target_default_dir(DEFAULT_CONFIG_VALUES["dir"], target),
                    "url_env": f"{target_env_stem(target)}_DATABASE_URL",
                    "test_url_env": f"{target_env_stem(target)}_TEST_DATABASE_URL",
                }
                for target in normalized_targets
            },
        )
    )


def render_ci_template(provider: TemplateProvider) -> str:
    try:
        render = _CI_TEMPLATES[provider]
    except KeyError as error:
        raise ValueError(f"Unsupported CI provider: {provider!r}") from error
    return render()


def default_ci_template_path(provider: TemplateProvider) -> Path:
    try:
        return _CI_TEMPLATE_PATHS[provider]
    except KeyError as error:
        raise ValueError(f"Unsupported CI provider: {provider!r}") from error


def render_updated_config(
    *,
    current: Config | None,
    repo_root: Path,
    config_root: Path,
    existing_text: str | None,
    target_name: str | None,
    dir_value: str | None,
    url_env: str | None,
    test_url_env: str | None,
) -> tuple[str, Config]:
    document, normalized_target = _updated_config_document(
        current=current,
        config_root=config_root,
        target_name=target_name,
        dir_value=dir_value,
        url_env=url_env,
        test_url_env=test_url_env,
    )
    config = Config.from_sources(
        repo_root=repo_root,
        defaults=document.defaults,
        targets=document.targets,
    )
    if existing_text is None:
        return render_config_document(document), config
    return (
        update_config_text(
            existing_text=existing_text,
            document=document,
            target_name=normalized_target,
        ),
        config,
    )


def render_config_document(document: ConfigDocument) -> str:
    lines = [
        f'dir = "{document.defaults["dir"]}"',
        f'url_env = "{document.defaults["url_env"]}"',
        f'test_url_env = "{document.defaults["test_url_env"]}"',
    ]
    for name in sorted(document.targets):
        row = document.targets[name]
        implicit_dir = target_default_dir(document.defaults["dir"], name)
        lines.extend(
            [
                "",
                f"[{name}]",
            ]
        )
        if row["dir"] != implicit_dir:
            lines.append(f'dir = "{row["dir"]}"')
        lines.extend(
            [
                f'url_env = "{row["url_env"]}"',
                f'test_url_env = "{row["test_url_env"]}"',
            ]
        )
    return "\n".join(lines) + "\n"


def write_text_file(path: Path, content: str, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _require_unique_env_stems(targets: tuple[str, ...]) -> None:
    seen: dict[str, str] = {}
    for target in targets:
        stem = target_env_stem(target)
        previous = seen.get(stem)
        if previous is not None:
            raise ValueError(
                f"Targets {previous!r} and {target!r} normalize to the same env stem {stem!r}."
            )
        seen[stem] = target


def _updated_config_document(
    *,
    current: Config | None,
    config_root: Path,
    target_name: str | None,
    dir_value: str | None,
    url_env: str | None,
    test_url_env: str | None,
) -> tuple[ConfigDocument, str | None]:
    defaults = _current_defaults(current, config_root=config_root)
    targets = _current_targets(current, config_root=config_root)
    normalized_target: str | None = None

    if target_name is None:
        defaults["dir"] = dir_value or defaults["dir"]
        defaults["url_env"] = url_env or defaults["url_env"]
        defaults["test_url_env"] = test_url_env or defaults["test_url_env"]
    else:
        try:
            normalized_target = normalize_target_names((target_name,))[0]
        except ConfigError as error:
            raise ValueError(str(error)) from error
        row = dict(targets.get(normalized_target, {}))
        stem = target_env_stem(normalized_target)
        row["dir"] = dir_value or row.get("dir") or target_default_dir(defaults["dir"], normalized_target)
        row["url_env"] = url_env or row.get("url_env") or f"{stem}_DATABASE_URL"
        row["test_url_env"] = test_url_env or row.get("test_url_env") or f"{stem}_TEST_DATABASE_URL"
        targets[normalized_target] = row

    return ConfigDocument(defaults=defaults, targets=targets), normalized_target


def _current_defaults(current: Config | None, *, config_root: Path) -> dict[str, str]:
    if current is None:
        return dict(DEFAULT_CONFIG_VALUES)
    try:
        rel_dir = current.default_target.dir.relative_to(config_root).as_posix()
    except ValueError:
        rel_dir = current.default_target.dir.as_posix()
    return {
        "dir": rel_dir,
        "url_env": current.default_target.url_env,
        "test_url_env": current.default_target.test_url_env,
    }


def _current_targets(current: Config | None, *, config_root: Path) -> dict[str, dict[str, str]]:
    if current is None:
        return {}
    rows: dict[str, dict[str, str]] = {}
    for name, target in current.targets.items():
        try:
            rel_dir = target.dir.relative_to(config_root).as_posix()
        except ValueError:
            rel_dir = target.dir.as_posix()
        rows[name] = {
            "dir": rel_dir,
            "url_env": target.url_env,
            "test_url_env": target.test_url_env,
        }
    return rows


def update_config_text(
    *,
    existing_text: str,
    document: ConfigDocument,
    target_name: str | None,
) -> str:
    parsed = tomlkit.parse(existing_text)
    _set_scalar(parsed, "dir", document.defaults["dir"])
    _set_scalar(parsed, "url_env", document.defaults["url_env"])
    _set_scalar(parsed, "test_url_env", document.defaults["test_url_env"])
    if target_name is None:
        return tomlkit.dumps(parsed)

    row = document.targets[target_name]
    implicit_dir = target_default_dir(document.defaults["dir"], target_name)
    target_table = parsed.get(target_name)
    if not isinstance(target_table, Table):
        target_table = tomlkit.table()
        if parsed and not isinstance(list(parsed.values())[-1], AoT):
            parsed.append(tomlkit.nl())
        parsed.add(target_name, target_table)

    if row["dir"] == implicit_dir:
        if "dir" in target_table:
            del target_table["dir"]
    else:
        target_table["dir"] = row["dir"]
    target_table["url_env"] = row["url_env"]
    target_table["test_url_env"] = row["test_url_env"]
    return tomlkit.dumps(parsed)


def _set_scalar(doc: Table, key: str, value: str) -> None:
    doc[key] = value


__all__ = [
    "ConfigDocument",
    "TemplateProvider",
    "default_ci_template_path",
    "render_ci_template",
    "render_config_document",
    "render_config_template",
    "render_updated_config",
    "update_config_text",
    "write_text_file",
]
