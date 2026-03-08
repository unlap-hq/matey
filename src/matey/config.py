from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any, Literal

from matey.paths import (
    PathBoundaryError,
    RelativePathError,
    describe_path_boundary_error,
    normalize_relative_posix_path,
    safe_descendant,
)

WORKSPACE_CONFIG_FILE = "matey.toml"
TARGET_CONFIG_FILE = "config.toml"
_TARGET_REQUIRED_KEYS = frozenset({"engine", "url_env", "test_url_env"})
_ENV_NAME_PATTERN = __import__("re").compile(r"^[A-Z_][A-Z0-9_]*$")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CodegenConfig:
    enabled: bool
    generator: str
    options: str | None


@dataclass(frozen=True, slots=True)
class TargetConfig:
    name: str
    dir: Path
    url_env: str
    test_url_env: str
    engine: str = ""
    codegen: CodegenConfig | None = None

    @property
    def schema(self) -> Path:
        return self.dir / "schema.sql"

    @property
    def migrations(self) -> Path:
        return self.dir / "migrations"

    @property
    def checkpoints(self) -> Path:
        return self.dir / "checkpoints"

    @property
    def lockfile(self) -> Path:
        return self.dir / "schema.lock.toml"

    @property
    def config_path(self) -> Path:
        return self.dir / TARGET_CONFIG_FILE


@dataclass(frozen=True, slots=True)
class WorkspaceConfig:
    repo_root: Path
    targets: tuple[Path, ...]
    source_path: Path | None
    source_kind: Literal["workspace", "pyproject", "none"]


class Config:
    def __init__(self, workspace: WorkspaceConfig, targets: tuple[TargetConfig, ...]) -> None:
        ordered = tuple(sorted(targets, key=lambda item: item.name))
        self._workspace = workspace
        self._targets = MappingProxyType({target.name: target for target in ordered})

    @property
    def workspace(self) -> WorkspaceConfig:
        return self._workspace

    @property
    def targets(self) -> MappingProxyType[str, TargetConfig]:
        return self._targets

    @classmethod
    def load(
        cls,
        repo_root: Path,
        config_path: Path | None = None,
        config_root: Path | None = None,
    ) -> Config:
        del config_root
        workspace = load_workspace(repo_root.resolve(), config_path=config_path)
        targets = load_targets_from_workspace(workspace)
        return cls(workspace, targets)

    def select(
        self,
        *,
        path: str | None = None,
        all_targets: bool = False,
    ) -> tuple[TargetConfig, ...]:
        if path is not None and all_targets:
            raise ConfigError("Cannot combine --path with --all.")

        if path is not None:
            normalized = normalize_target_path_ref(path)
            target = self._targets.get(normalized)
            if target is None:
                available = ", ".join(self._targets.keys()) or "(none)"
                raise ConfigError(
                    f"Target path {normalized!r} is not configured in workspace. "
                    f"Available paths: {available}"
                )
            return (target,)

        if all_targets:
            if not self._targets:
                raise ConfigError("No targets configured in workspace. Add one with `matey init --path ...`.")
            return tuple(self._targets.values())

        if len(self._targets) == 1:
            return tuple(self._targets.values())
        if not self._targets:
            raise ConfigError("No targets configured. Pass --path or initialize a target first.")
        available = ", ".join(self._targets.keys())
        raise ConfigError(
            "Multiple targets configured; choose one with --path or use --all. "
            f"Available paths: {available}"
        )


def load_workspace(repo_root: Path, config_path: Path | None = None) -> WorkspaceConfig:
    root = repo_root.resolve()
    if config_path is not None:
        resolved = config_path if config_path.is_absolute() else (root / config_path)
        resolved = resolved.resolve()
        if not resolved.exists():
            raise ConfigError(f"Config file not found: {resolved}")
        if resolved.name == "pyproject.toml":
            return _load_workspace_from_pyproject(root, resolved)
        return _load_workspace_from_file(root, resolved)

    workspace_path = root / WORKSPACE_CONFIG_FILE
    if workspace_path.exists():
        return _load_workspace_from_file(root, workspace_path)

    pyproject_path = root / "pyproject.toml"
    if pyproject_path.exists():
        pyproject_workspace = _load_workspace_from_pyproject(root, pyproject_path, allow_missing=True)
        if pyproject_workspace is not None:
            return pyproject_workspace

    return WorkspaceConfig(repo_root=root, targets=(), source_path=None, source_kind="none")


def load_target(*, path: str | Path, repo_root: Path) -> TargetConfig:
    root = repo_root.resolve()
    target_root = _resolve_target_path(root=root, path=path, label="target path", allow_missing_leaf=False)
    config_path = target_root / TARGET_CONFIG_FILE
    if not config_path.exists():
        raise ConfigError(f"Target config not found: {config_path}")
    doc = _load_toml(config_path, label=str(config_path))
    return _target_from_doc(repo_root=root, target_root=target_root, doc=doc, source=str(config_path))


def load_targets_from_workspace(workspace: WorkspaceConfig) -> tuple[TargetConfig, ...]:
    return tuple(load_target(path=path, repo_root=workspace.repo_root) for path in workspace.targets)


def _load_workspace_from_file(repo_root: Path, path: Path) -> WorkspaceConfig:
    doc = _load_toml(path, label=str(path))
    targets = _extract_workspace_targets(doc, repo_root=repo_root, source=str(path))
    return WorkspaceConfig(repo_root=repo_root, targets=targets, source_path=path, source_kind="workspace")


def _load_workspace_from_pyproject(repo_root: Path, path: Path, allow_missing: bool = False) -> WorkspaceConfig | None:
    doc = _load_toml(path, label=str(path))
    tool = doc.get("tool")
    if tool is None:
        if allow_missing:
            return None
        raise ConfigError("Invalid pyproject.toml: missing [tool.matey] section.")
    if not isinstance(tool, dict):
        raise ConfigError("Invalid pyproject.toml: [tool] must be a table.")
    section = tool.get("matey")
    if section is None:
        if allow_missing:
            return None
        raise ConfigError("Invalid pyproject.toml: [tool.matey] must be a table.")
    if not isinstance(section, dict):
        raise ConfigError("Invalid pyproject.toml: [tool.matey] must be a table.")
    targets = _extract_workspace_targets(section, repo_root=repo_root, source="pyproject.toml [tool.matey]")
    return WorkspaceConfig(repo_root=repo_root, targets=targets, source_path=path, source_kind="pyproject")


def _extract_workspace_targets(doc: dict[str, Any], *, repo_root: Path, source: str) -> tuple[Path, ...]:
    unsupported = set(doc) - {"targets"}
    if unsupported:
        rendered = ", ".join(sorted(repr(key) for key in unsupported))
        raise ConfigError(f"{source}: unsupported workspace keys: {rendered}.")
    raw_targets = doc.get("targets", [])
    if not isinstance(raw_targets, list) or not all(isinstance(item, str) for item in raw_targets):
        raise ConfigError(f"{source}: 'targets' must be an array of strings.")
    paths: list[Path] = []
    seen: set[Path] = set()
    for item in raw_targets:
        target_path = _resolve_target_path(root=repo_root, path=item, label="workspace target", allow_missing_leaf=True)
        if target_path in seen:
            raise ConfigError(f"{source}: duplicate target path {item!r}.")
        seen.add(target_path)
        paths.append(target_path)
    return tuple(paths)


def _target_from_doc(*, repo_root: Path, target_root: Path, doc: dict[str, Any], source: str) -> TargetConfig:
    unsupported = set(doc) - (_TARGET_REQUIRED_KEYS | {"codegen"})
    if unsupported:
        rendered = ", ".join(sorted(repr(key) for key in unsupported))
        raise ConfigError(f"{source}: unsupported target keys: {rendered}.")

    values: dict[str, str] = {}
    for key in _TARGET_REQUIRED_KEYS:
        value = doc.get(key)
        if not isinstance(value, str):
            raise ConfigError(f"{source}: {key!r} must be a string.")
        values[key] = value

    _require_env_name(values["url_env"], source=f"{source}: url_env")
    _require_env_name(values["test_url_env"], source=f"{source}: test_url_env")

    codegen = _parse_codegen(doc.get("codegen"), source=source)
    try:
        name = target_root.relative_to(repo_root).as_posix()
    except ValueError as error:
        raise ConfigError(f"{source}: target path must stay inside repo root.") from error
    return TargetConfig(
        name=name or ".",
        dir=target_root,
        engine=values["engine"],
        url_env=values["url_env"],
        test_url_env=values["test_url_env"],
        codegen=codegen,
    )


def _parse_codegen(value: Any, *, source: str) -> CodegenConfig | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ConfigError(f"{source}: [codegen] must be a table.")
    unsupported = set(value) - {"enabled", "generator", "options"}
    if unsupported:
        rendered = ", ".join(sorted(repr(key) for key in unsupported))
        raise ConfigError(f"{source}: unsupported [codegen] keys: {rendered}.")
    enabled_raw = value.get("enabled", True)
    if not isinstance(enabled_raw, bool):
        raise ConfigError(f"{source}: codegen.enabled must be a boolean.")
    generator_raw = value.get("generator", "tables")
    options_raw = value.get("options")
    if not isinstance(generator_raw, str):
        raise ConfigError(f"{source}: codegen.generator must be a string.")
    if options_raw is not None and not isinstance(options_raw, str):
        raise ConfigError(f"{source}: codegen.options must be a string.")
    return CodegenConfig(
        enabled=enabled_raw,
        generator=generator_raw,
        options=options_raw,
    )


def _resolve_target_path(*, root: Path, path: str | Path, label: str, allow_missing_leaf: bool) -> Path:
    raw = path if isinstance(path, Path) else Path(path)
    if raw.is_absolute():
        candidate = raw
    else:
        try:
            normalized = normalize_target_path_ref(str(path), label=label)
        except RelativePathError as error:
            raise ConfigError(str(error)) from error
        candidate = root / Path(normalized)
    try:
        return safe_descendant(
            root=root,
            candidate=candidate,
            label=label,
            allow_missing_leaf=allow_missing_leaf,
            expected_kind="dir",
        )
    except PathBoundaryError as error:
        raise ConfigError(
            describe_path_boundary_error(
                error,
                path=candidate,
                symlink_message=f"{label} uses symlinked path segment",
            )
        ) from error


def _load_toml(path: Path, *, label: str) -> dict[str, Any]:
    try:
        parsed = tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ConfigError(f"Unable to read {label}: {error.strerror or error}") from error
    except UnicodeDecodeError as error:
        raise ConfigError(f"Unable to decode {label} as UTF-8.") from error
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"Unable to parse {label}: {error}") from error
    if not isinstance(parsed, dict):
        raise ConfigError(f"Invalid TOML in {label}: expected a top-level table.")
    return parsed


def target_env_stem(path: str) -> str:
    normalized = PurePosixPath(path).as_posix().strip()
    if not normalized or normalized == ".":
        return "DEFAULT"
    stem = normalized.replace("/", "_").replace("-", "_").upper()
    if stem and stem[0].isdigit():
        stem = f"_{stem}"
    return stem


def _require_env_name(name: str, *, source: str) -> None:
    if not _ENV_NAME_PATTERN.fullmatch(name):
        raise ConfigError(
            f"{source}: invalid environment variable name {name!r}; expected [A-Z_][A-Z0-9_]*."
        )


def normalize_target_path_ref(path: str, *, label: str = "target path") -> str:
    if path in ("", "."):
        return "."
    return normalize_relative_posix_path(path, label=label)


__all__ = [
    "TARGET_CONFIG_FILE",
    "WORKSPACE_CONFIG_FILE",
    "CodegenConfig",
    "Config",
    "ConfigError",
    "TargetConfig",
    "WorkspaceConfig",
    "load_target",
    "load_targets_from_workspace",
    "load_workspace",
    "normalize_target_path_ref",
    "target_env_stem",
]
