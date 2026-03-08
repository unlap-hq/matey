from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal

import pygit2
import tomlkit
from tomlkit.items import Table

from matey.paths import (
    PathBoundaryError,
    describe_path_boundary_error,
    normalize_target_path_ref,
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
    root: Path
    url_env: str
    test_url_env: str
    engine: str = ""
    codegen: CodegenConfig | None = None

    @classmethod
    def load(cls, *, path: str | Path, workspace_root: Path) -> TargetConfig:
        root = workspace_root.resolve()
        target_root = _resolve_target_path(
            root=root,
            path=path,
            label="target path",
            allow_missing_leaf=False,
        )
        config_path = target_root / TARGET_CONFIG_FILE
        if not config_path.exists():
            raise ConfigError(f"Target config not found: {config_path}")
        doc = _load_toml(config_path, label=str(config_path))
        return _target_from_doc(
            workspace_root=root,
            target_root=target_root,
            doc=doc,
            source=str(config_path),
        )

    @property
    def config_path(self) -> Path:
        return self.root / TARGET_CONFIG_FILE

    @property
    def schema(self) -> Path:
        return self.root / "schema.sql"

    @property
    def lockfile(self) -> Path:
        return self.root / "schema.lock.toml"

    @property
    def migrations(self) -> Path:
        return self.root / "migrations"

    @property
    def checkpoints(self) -> Path:
        return self.root / "checkpoints"

    @property
    def models(self) -> Path:
        return self.root / "models.py"


@dataclass(slots=True)
class ConfigEditor:
    kind: Literal["workspace", "pyproject"]

    def render_workspace(self, *, target_paths: tuple[str, ...]) -> str:
        doc = tomlkit.document()
        section = self._workspace_section(doc, create=True)
        section["targets"] = tuple(
            sorted(
                normalize_target_path_ref(path, label="target path")
                for path in target_paths
            )
        )
        return tomlkit.dumps(doc)

    def update_workspace(self, *, existing_text: str, target_path: str | None) -> str:
        parsed = tomlkit.parse(existing_text)
        section = self._workspace_section(parsed, create=True)
        targets = section.get("targets")
        values = [str(item) for item in targets] if isinstance(targets, list) else []
        normalized = (
            normalize_target_path_ref(target_path, label="target path")
            if target_path is not None
            else None
        )
        if normalized is not None and normalized not in values:
            values.append(normalized)
        section["targets"] = sorted(values)
        return tomlkit.dumps(parsed)

    def render_target(
        self,
        *,
        engine: str,
        url_env: str,
        test_url_env: str,
        codegen: CodegenConfig | None = None,
    ) -> str:
        doc = tomlkit.document()
        doc["engine"] = engine
        doc["url_env"] = url_env
        doc["test_url_env"] = test_url_env
        _set_codegen(doc, codegen)
        return tomlkit.dumps(doc)

    def update_target(
        self,
        *,
        existing_text: str,
        engine: str,
        url_env: str,
        test_url_env: str,
        codegen: CodegenConfig | None = None,
    ) -> str:
        parsed = tomlkit.parse(existing_text)
        parsed["engine"] = engine
        parsed["url_env"] = url_env
        parsed["test_url_env"] = test_url_env
        _set_codegen(parsed, codegen)
        return tomlkit.dumps(parsed)

    def _workspace_section(self, doc: Table, *, create: bool) -> Table:
        if self.kind == "workspace":
            return doc
        tool = doc.get("tool")
        if not isinstance(tool, Table):
            if not create:
                raise ConfigError("pyproject.toml is missing [tool] table.")
            tool = tomlkit.table()
            doc["tool"] = tool
        matey = tool.get("matey")
        if not isinstance(matey, Table):
            if not create:
                raise ConfigError("pyproject.toml is missing [tool.matey] table.")
            matey = tomlkit.table()
            tool["matey"] = matey
        return matey


@dataclass(frozen=True, slots=True)
class Workspace:
    root: Path
    config_path: Path
    config_kind: Literal["workspace", "pyproject", "none"]
    targets: tuple[TargetConfig, ...]

    @classmethod
    def load(
        cls,
        *,
        root: Path,
        config_path: Path | None,
        config_kind: Literal["workspace", "pyproject", "none"],
    ) -> Workspace:
        resolved_root = root.resolve()
        resolved_config_path = (
            resolved_root / WORKSPACE_CONFIG_FILE
            if config_path is None and config_kind == "none"
            else (config_path if config_path is None or config_path.is_absolute() else (resolved_root / config_path))
        )
        if resolved_config_path is None:
            raise ConfigError("config_path is required when config_kind is not 'none'.")
        return cls._from_paths(resolved_root, resolved_config_path, config_kind)

    @classmethod
    def discover(
        cls,
        *,
        start: Path,
        workspace: Path | None = None,
        allow_create_fallback: bool = False,
    ) -> Workspace:
        if workspace is not None:
            resolved = workspace if workspace.is_absolute() else (start / workspace)
            resolved = resolved.resolve()
            if not resolved.is_dir():
                raise ConfigError("--workspace must point to a directory.")
            return cls._from_root(resolved, create_default=True)

        cwd = start.resolve()
        if local := cls._discover_existing(cwd):
            return cls._from_paths(cwd, local[0], local[1])

        repo_root = _find_repo_root_or_none(cwd)
        if repo_root is not None:
            if discovered := cls._discover_existing(repo_root):
                return cls._from_paths(repo_root, discovered[0], discovered[1])
            return cls._from_root(repo_root, create_default=True)

        if allow_create_fallback:
            return cls._from_root(cwd, create_default=True)

        raise ConfigError(
            "Path is not inside a git repository and no local workspace config was found."
        )

    @classmethod
    def _from_root(cls, root: Path, *, create_default: bool) -> Workspace:
        if discovered := cls._discover_existing(root):
            return cls._from_paths(root, discovered[0], discovered[1])
        config_path = root / WORKSPACE_CONFIG_FILE
        del create_default
        return cls._from_paths(root, config_path, "none")

    @classmethod
    def _from_paths(
        cls,
        root: Path,
        config_path: Path,
        config_kind: Literal["workspace", "pyproject", "none"],
    ) -> Workspace:
        root = root.resolve()
        if config_kind == "none":
            targets: tuple[TargetConfig, ...] = ()
        else:
            targets = tuple(
                sorted(
                    (
                        TargetConfig.load(
                            path=path.relative_to(root).as_posix() or ".",
                            workspace_root=root,
                        )
                        for path in _parse_workspace_targets(
                            root,
                            config_path=config_path,
                            config_kind=config_kind,
                        )
                    ),
                    key=lambda target: target.name,
                )
            )
        return cls(
            root=root,
            config_path=config_path,
            config_kind=config_kind,
            targets=targets,
        )

    @staticmethod
    def _discover_existing(root: Path) -> tuple[Path, Literal["workspace", "pyproject"]] | None:
        for candidate in (root / WORKSPACE_CONFIG_FILE, root / "pyproject.toml"):
            if not candidate.exists():
                continue
            if candidate.name == "pyproject.toml":
                if _pyproject_has_matey(candidate):
                    return candidate, "pyproject"
                continue
            return candidate, "workspace"
        return None

    def render_updated(self, *, target_path: str | None, existing_text: str | None = None) -> str:
        editor = ConfigEditor("pyproject" if self.config_kind == "pyproject" else "workspace")
        if existing_text is None:
            existing_text = self.config_path.read_text(encoding="utf-8") if self.config_path.exists() else None
        if existing_text is None:
            return editor.render_workspace(target_paths=(target_path,) if target_path is not None else ())
        return editor.update_workspace(existing_text=existing_text, target_path=target_path)

    @property
    def target_paths(self) -> tuple[str, ...]:
        return tuple(target.name for target in self.targets)

    def select(
        self,
        *,
        path: str | None = None,
        all_targets: bool = False,
        require_single: bool = False,
    ) -> tuple[TargetConfig, ...]:
        if path is not None and all_targets:
            raise ConfigError("Cannot combine --path with --all.")

        if path is not None:
            normalized = normalize_target_path_ref(path)
            target_by_path = {target.name: target for target in self.targets}
            if normalized not in target_by_path:
                available = ", ".join(self.target_paths) or "(none)"
                raise ConfigError(
                    f"Target path {normalized!r} is not configured in workspace. Available paths: {available}"
                )
            selected = (target_by_path[normalized],)
        elif all_targets:
            if not self.targets:
                raise ConfigError("No targets configured in workspace. Add one with `matey init --path ...`.")
            selected = self.targets
        else:
            if len(self.targets) == 1:
                selected = self.targets
            elif not self.targets:
                raise ConfigError("No targets configured. Pass --path or initialize a target first.")
            else:
                available = ", ".join(self.target_paths)
                raise ConfigError(
                    "Multiple targets configured; choose one with --path or use --all. "
                    f"Available paths: {available}"
                )

        if require_single and len(selected) != 1:
            raise ConfigError("This command requires exactly one resolved target.")
        return selected


def default_target_config_values(path: str) -> tuple[str, str]:
    stem = target_env_stem(path)
    if stem == "DEFAULT":
        return "DATABASE_URL", "TEST_DATABASE_URL"
    return f"{stem}_DATABASE_URL", f"{stem}_TEST_DATABASE_URL"


def target_env_stem(path: str) -> str:
    normalized = PurePosixPath(path).as_posix().strip()
    if not normalized or normalized == ".":
        return "DEFAULT"
    stem = normalized.replace("/", "_").replace("-", "_").upper()
    if stem and stem[0].isdigit():
        stem = f"_{stem}"
    return stem


def _parse_workspace_targets(
    repo_root: Path,
    *,
    config_path: Path,
    config_kind: Literal["workspace", "pyproject"],
) -> tuple[Path, ...]:
    doc = _load_toml(config_path, label=str(config_path))
    if config_kind == "pyproject":
        tool = doc.get("tool")
        if tool is None or not isinstance(tool, dict):
            raise ConfigError("Invalid pyproject.toml: missing [tool.matey] section.")
        section = tool.get("matey")
        if section is None or not isinstance(section, dict):
            raise ConfigError("Invalid pyproject.toml: [tool.matey] must be a table.")
        doc = section
        source = "pyproject.toml [tool.matey]"
    else:
        source = str(config_path)
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
        target_path = _resolve_target_path(
            root=repo_root,
            path=item,
            label="workspace target",
            allow_missing_leaf=True,
        )
        if target_path in seen:
            raise ConfigError(f"{source}: duplicate target path {item!r}.")
        seen.add(target_path)
        paths.append(target_path)
    return tuple(paths)


def _pyproject_has_matey(path: Path) -> bool:
    doc = _load_toml(path, label=str(path))
    tool = doc.get("tool")
    return isinstance(tool, dict) and isinstance(tool.get("matey"), dict)


def _find_repo_root_or_none(start: Path) -> Path | None:
    try:
        discovered = pygit2.discover_repository(str(start))
    except (KeyError, pygit2.GitError):
        return None
    if discovered is None:
        return None
    return Path(discovered).resolve().parent


def _target_from_doc(*, workspace_root: Path, target_root: Path, doc: dict[str, Any], source: str) -> TargetConfig:
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
        name = target_root.relative_to(workspace_root).as_posix()
    except ValueError as error:
        raise ConfigError(f"{source}: target path must stay inside workspace root.") from error
    return TargetConfig(
        name=name or ".",
        root=target_root,
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
    return CodegenConfig(enabled=enabled_raw, generator=generator_raw, options=options_raw)


def _resolve_target_path(*, root: Path, path: str | Path, label: str, allow_missing_leaf: bool) -> Path:
    raw = path if isinstance(path, Path) else Path(path)
    if raw.is_absolute():
        candidate = raw
    else:
        normalized = normalize_target_path_ref(str(path), label=label)
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


def _require_env_name(name: str, *, source: str) -> None:
    if not _ENV_NAME_PATTERN.fullmatch(name):
        raise ConfigError(
            f"{source}: invalid environment variable name {name!r}; expected [A-Z_][A-Z0-9_]*."
        )


def _set_codegen(doc: Table, codegen: CodegenConfig | None) -> None:
    if codegen is None:
        return
    table = doc.get("codegen")
    if not isinstance(table, Table):
        table = tomlkit.table()
        doc["codegen"] = table
    table["enabled"] = codegen.enabled
    table["generator"] = codegen.generator
    if codegen.options is None:
        if "options" in table:
            del table["options"]
    else:
        table["options"] = codegen.options


__all__ = [
    "TARGET_CONFIG_FILE",
    "WORKSPACE_CONFIG_FILE",
    "CodegenConfig",
    "ConfigEditor",
    "ConfigError",
    "TargetConfig",
    "Workspace",
    "default_target_config_values",
    "normalize_target_path_ref",
    "target_env_stem",
]
