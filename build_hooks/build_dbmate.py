from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

try:
    from hatchling.builders.hooks.plugin.interface import BuildHookInterface
except Exception:  # pragma: no cover - hatchling import is only required for wheel builds
    BuildHookInterface = object  # type: ignore[assignment]


DEFAULT_DBMATE_MODULE = "github.com/amacneil/dbmate/v2"
DEFAULT_DBMATE_VERSION = "v2.31.0"
DEFAULT_DBMATE_SOURCE = "go-install"
DEFAULT_DBMATE_CGO_ENABLED = "1"
DEFAULT_GO_LICENSES_MODULE = "github.com/google/go-licenses/v2"
DEFAULT_GO_LICENSES_VERSION = "v2.0.1"
DEFAULT_GO_LICENSES_DISALLOWED_TYPES = "forbidden,restricted,unknown"
DEFAULT_GO_LICENSES_ENFORCE = False


class _BuildEnv:
    def __init__(
        self,
        *,
        dbmate_source: str,
        dbmate_module: str,
        dbmate_version: str,
        dbmate_cgo_enabled: str,
        go_licenses_module: str,
        go_licenses_version: str,
        go_licenses_disallowed_types: str,
        go_licenses_enforce: bool,
    ) -> None:
        self.dbmate_source = dbmate_source
        self.dbmate_module = dbmate_module
        self.dbmate_version = dbmate_version
        self.dbmate_cgo_enabled = dbmate_cgo_enabled
        self.go_licenses_module = go_licenses_module
        self.go_licenses_version = go_licenses_version
        self.go_licenses_disallowed_types = go_licenses_disallowed_types
        self.go_licenses_enforce = go_licenses_enforce


def _optional(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _parse_bool(raw: str | None, *, name: str, default: bool) -> bool:
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value for {name}: {raw!r}")


def _load_build_env(environ: Mapping[str, str]) -> _BuildEnv:
    source = _optional(environ.get("MATEY_DBMATE_SOURCE")) or DEFAULT_DBMATE_SOURCE
    module = _optional(environ.get("MATEY_DBMATE_MODULE")) or DEFAULT_DBMATE_MODULE
    version = _optional(environ.get("MATEY_DBMATE_VERSION")) or DEFAULT_DBMATE_VERSION
    cgo_enabled = _optional(environ.get("MATEY_DBMATE_CGO_ENABLED")) or DEFAULT_DBMATE_CGO_ENABLED
    go_licenses_module = (
        _optional(environ.get("MATEY_GO_LICENSES_MODULE")) or DEFAULT_GO_LICENSES_MODULE
    )
    go_licenses_version = (
        _optional(environ.get("MATEY_GO_LICENSES_VERSION")) or DEFAULT_GO_LICENSES_VERSION
    )
    go_licenses_disallowed_types = (
        _optional(environ.get("MATEY_GO_LICENSES_DISALLOWED_TYPES"))
        or DEFAULT_GO_LICENSES_DISALLOWED_TYPES
    )
    go_licenses_enforce = _parse_bool(
        _optional(environ.get("MATEY_GO_LICENSES_ENFORCE")),
        name="MATEY_GO_LICENSES_ENFORCE",
        default=DEFAULT_GO_LICENSES_ENFORCE,
    )

    return _BuildEnv(
        dbmate_source=source,
        dbmate_module=module,
        dbmate_version=version,
        dbmate_cgo_enabled=cgo_enabled,
        go_licenses_module=go_licenses_module,
        go_licenses_version=go_licenses_version,
        go_licenses_disallowed_types=go_licenses_disallowed_types,
        go_licenses_enforce=go_licenses_enforce,
    )


def _run(command: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _run_capture(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def _go_env_value(name: str, base_env: dict[str, str]) -> str:
    return _run(["go", "env", name], env=base_env)


def _is_windows_host() -> bool:
    return os.name == "nt"


def _tool_binary_name(name: str) -> str:
    return f"{name}.exe" if _is_windows_host() else name


def _go_bin_dir(root: Path) -> Path:
    bin_dir = root / ".matey" / "go" / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    return bin_dir


def _remove_tree(path: Path) -> None:
    def _onerror(func: Any, value: str, _exc_info: Any) -> None:
        Path(value).chmod(0o755)
        func(value)

    if path.exists():
        shutil.rmtree(path, onerror=_onerror)


def _build_env(root: Path, *, base_environ: Mapping[str, str], cgo_enabled: str) -> dict[str, str]:
    cache_root = root / ".matey" / "go"
    gopath = cache_root / "gopath"
    gocache = cache_root / "cache"
    gopath.mkdir(parents=True, exist_ok=True)
    gocache.mkdir(parents=True, exist_ok=True)

    env = dict(base_environ)
    env.setdefault("GOPATH", str(gopath))
    env.setdefault("GOCACHE", str(gocache))
    env.setdefault("GOMODCACHE", str(gopath / "pkg" / "mod"))
    # Default to CGO-enabled builds so sqlite driver support is available by default.
    # Set MATEY_DBMATE_CGO_ENABLED=0 when a pure-Go fallback is needed.
    env["CGO_ENABLED"] = cgo_enabled
    if env.get("CGO_ENABLED") == "1":
        # In some conda/pixi environments Go defaults to a non-existent compiler
        # wrapper (for example x86_64-conda-linux-gnu-cc). Fall back to clang
        # toolchain when available.
        cc = env.get("CC")
        cxx = env.get("CXX")
        if (not cc or shutil.which(cc) is None) and shutil.which("clang") is not None:
            env["CC"] = "clang"
        if (not cxx or shutil.which(cxx) is None) and shutil.which("clang++") is not None:
            env["CXX"] = "clang++"
    return env


def _target_platform(env: dict[str, str]) -> tuple[str, str]:
    goos = env.get("GOOS") or _go_env_value("GOOS", env)
    goarch = env.get("GOARCH") or _go_env_value("GOARCH", env)
    return goos, goarch


def _output_binary_path(root: Path, goos: str, goarch: str) -> Path:
    binary_name = "dbmate.exe" if goos == "windows" else "dbmate"
    output_dir = root / "src" / "matey" / "_vendor" / "dbmate" / f"{goos}-{goarch}"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / binary_name


def _module_name_from_go_mod(module_root: Path) -> str | None:
    go_mod = module_root / "go.mod"
    if not go_mod.exists():
        return None
    for line in go_mod.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("module "):
            parts = stripped.split(maxsplit=1)
            if len(parts) == 2 and parts[1].strip():
                return parts[1].strip()
    return None


def _find_license_file(module_root: Path) -> Path:
    for candidate in ("LICENSE", "LICENSE.txt", "COPYING"):
        candidate_path = module_root / candidate
        if candidate_path.exists():
            return candidate_path
    raise RuntimeError(f"No license file found in module root: {module_root}")


class _BuiltSourceInfo:
    def __init__(
        self,
        *,
        module_ref: str,
        module_version: str,
        package_ref: str,
        module_root: Path,
        dbmate_license_file: Path,
        package_cwd: Path | None,
    ) -> None:
        self.module_ref = module_ref
        self.module_version = module_version
        self.package_ref = package_ref
        self.module_root = module_root
        self.dbmate_license_file = dbmate_license_file
        self.package_cwd = package_cwd


def _build_from_vendor(
    root: Path,
    output_binary: Path,
    env: dict[str, str],
    *,
    fallback_module_ref: str,
    fallback_module_version: str,
) -> _BuiltSourceInfo:
    module_root = root / "vendor" / "dbmate"
    if not (module_root / "go.mod").exists():
        raise RuntimeError(
            "MATEY_DBMATE_SOURCE=vendor requested but vendor/dbmate/go.mod is missing."
        )

    build_target = "./cmd/dbmate" if (module_root / "cmd" / "dbmate").exists() else "."

    _run(
        [
            "go",
            "build",
            "-trimpath",
            "-ldflags",
            "-s -w",
            "-o",
            str(output_binary),
            build_target,
        ],
        cwd=module_root,
        env=env,
    )
    module_ref = _module_name_from_go_mod(module_root) or fallback_module_ref
    return _BuiltSourceInfo(
        module_ref=module_ref,
        module_version=fallback_module_version,
        package_ref=build_target,
        module_root=module_root,
        dbmate_license_file=_find_license_file(module_root),
        package_cwd=module_root,
    )


def _build_from_go_install(
    root: Path, output_binary: Path, env: dict[str, str], *, module: str, version: str
) -> _BuiltSourceInfo:
    bin_dir = _go_bin_dir(root)
    env = env.copy()
    env["GOBIN"] = str(bin_dir)

    _run(["go", "install", f"{module}@{version}"], env=env)

    binary_name = output_binary.name
    installed_binary = bin_dir / binary_name
    if not installed_binary.exists():
        raise RuntimeError(
            f"Expected built dbmate binary at {installed_binary}, but it was not found."
        )

    shutil.copy2(installed_binary, output_binary)
    module_root = Path(_run(["go", "list", "-f", "{{.Dir}}", "-m", f"{module}@{version}"], env=env))
    package_ref = "./cmd/dbmate" if (module_root / "cmd" / "dbmate").exists() else "."
    return _BuiltSourceInfo(
        module_ref=module,
        module_version=version,
        package_ref=package_ref,
        module_root=module_root,
        dbmate_license_file=_find_license_file(module_root),
        package_cwd=module_root,
    )


def _host_go_tool_env(env: dict[str, str]) -> dict[str, str]:
    tool_env = env.copy()
    tool_env.pop("GOOS", None)
    tool_env.pop("GOARCH", None)
    return tool_env


def _install_go_licenses(
    root: Path,
    *,
    env: dict[str, str],
    module: str,
    version: str,
) -> Path:
    tool_env = _host_go_tool_env(env)
    bin_dir = _go_bin_dir(root)
    tool_env["GOBIN"] = str(bin_dir)
    _run(["go", "install", f"{module}@{version}"], env=tool_env)
    tool_binary = bin_dir / _tool_binary_name("go-licenses")
    if not tool_binary.exists():
        raise RuntimeError(f"Expected go-licenses binary at {tool_binary}, but it was not found.")
    return tool_binary


def _third_party_license_dir(output_binary: Path) -> Path:
    output_dir = output_binary.parent / "THIRD_PARTY_LICENSES"
    _remove_tree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def _write_dbmate_license(*, source_license: Path, third_party_dir: Path) -> Path:
    target = third_party_dir / "DBMATE_LICENSE"
    shutil.copyfile(source_license, target)
    target.chmod(0o644)
    return target


def _write_third_party_notices(
    *,
    third_party_dir: Path,
    source_info: _BuiltSourceInfo,
    go_licenses_module: str,
    go_licenses_version: str,
    disallowed_types: str,
    enforce: bool,
    dbmate_license_filename: str,
) -> None:
    notices = (
        "Third-party notices for bundled dbmate binary.\n"
        "\n"
        f"Component: dbmate\n"
        f"Module: {source_info.module_ref}\n"
        f"Version: {source_info.module_version}\n"
        "License: MIT\n"
        "Source: https://github.com/amacneil/dbmate\n"
        f"License file: {dbmate_license_filename}\n"
        "\n"
        f"go-licenses tool: {go_licenses_module}@{go_licenses_version}\n"
        f"go-licenses policy disallowed types: {disallowed_types}\n"
        f"go-licenses policy enforce: {enforce}\n"
        f"go-licenses package ref: {source_info.package_ref}\n"
        "go-licenses report file: go-licenses-report.txt\n"
        "go-licenses check output: go-licenses-check.txt\n"
        "go-licenses save output: go-licenses-save.txt\n"
        "go-licenses saved licenses: go-licenses-save/\n"
    )
    (third_party_dir / "THIRD_PARTY_NOTICES.txt").write_text(notices, encoding="utf-8")


def _collect_go_dependency_licenses(
    *,
    root: Path,
    output_binary: Path,
    source_info: _BuiltSourceInfo,
    env: dict[str, str],
    go_licenses_module: str,
    go_licenses_version: str,
    go_licenses_disallowed_types: str,
    go_licenses_enforce: bool,
) -> None:
    go_licenses_binary = _install_go_licenses(
        root,
        env=env,
        module=go_licenses_module,
        version=go_licenses_version,
    )
    third_party_dir = _third_party_license_dir(output_binary)
    save_dir = third_party_dir / "go-licenses-save"

    check_result = _run_capture(
        [
            str(go_licenses_binary),
            "check",
            source_info.package_ref,
            f"--disallowed_types={go_licenses_disallowed_types}",
        ],
        cwd=source_info.package_cwd,
        env=env,
    )
    check_output = "\n".join(
        text
        for text in ((check_result.stdout or "").strip(), (check_result.stderr or "").strip())
        if text
    )
    check_file = third_party_dir / "go-licenses-check.txt"
    check_file.write_text(check_output + ("\n" if check_output else ""), encoding="utf-8")
    if check_result.returncode != 0 and go_licenses_enforce:
        raise RuntimeError(
            f"go-licenses check failed and MATEY_GO_LICENSES_ENFORCE is enabled. See {check_file}"
        )

    report_result = _run_capture(
        [str(go_licenses_binary), "report", source_info.package_ref],
        cwd=source_info.package_cwd,
        env=env,
    )
    report_output = "\n".join(
        text
        for text in ((report_result.stdout or "").strip(), (report_result.stderr or "").strip())
        if text
    )
    report_file = third_party_dir / "go-licenses-report.txt"
    report_file.write_text(report_output + ("\n" if report_output else ""), encoding="utf-8")
    if report_result.returncode != 0 and go_licenses_enforce:
        raise RuntimeError(
            f"go-licenses report failed and MATEY_GO_LICENSES_ENFORCE is enabled. See {report_file}"
        )

    save_result = _run_capture(
        [
            str(go_licenses_binary),
            "save",
            source_info.package_ref,
            "--save_path",
            str(save_dir),
        ],
        cwd=source_info.package_cwd,
        env=env,
    )
    save_output = "\n".join(
        text
        for text in ((save_result.stdout or "").strip(), (save_result.stderr or "").strip())
        if text
    )
    save_output_file = third_party_dir / "go-licenses-save.txt"
    save_output_file.write_text(save_output + ("\n" if save_output else ""), encoding="utf-8")
    if save_result.returncode != 0 and go_licenses_enforce:
        raise RuntimeError(
            "go-licenses save failed and MATEY_GO_LICENSES_ENFORCE is enabled. "
            f"See {save_output_file}"
        )

    copied_license = _write_dbmate_license(
        source_license=source_info.dbmate_license_file,
        third_party_dir=third_party_dir,
    )
    _write_third_party_notices(
        third_party_dir=third_party_dir,
        source_info=source_info,
        go_licenses_module=go_licenses_module,
        go_licenses_version=go_licenses_version,
        disallowed_types=go_licenses_disallowed_types,
        enforce=go_licenses_enforce,
        dbmate_license_filename=copied_license.name,
    )


def build_dbmate(root: Path, *, environ: Mapping[str, str] | None = None) -> Path:
    base_environ = dict(os.environ) if environ is None else dict(environ)
    build_env = _load_build_env(base_environ)
    env = _build_env(root, base_environ=base_environ, cgo_enabled=build_env.dbmate_cgo_enabled)
    goos, goarch = _target_platform(env)
    output_binary = _output_binary_path(root, goos, goarch)
    source = build_env.dbmate_source

    if source == "vendor":
        source_info = _build_from_vendor(
            root,
            output_binary,
            env,
            fallback_module_ref=build_env.dbmate_module,
            fallback_module_version=build_env.dbmate_version,
        )
    elif source == "go-install":
        source_info = _build_from_go_install(
            root,
            output_binary,
            env,
            module=build_env.dbmate_module,
            version=build_env.dbmate_version,
        )
    else:
        raise RuntimeError(
            f"Unsupported MATEY_DBMATE_SOURCE={source!r}. Expected 'vendor' or 'go-install'."
        )

    _collect_go_dependency_licenses(
        root=root,
        output_binary=output_binary,
        source_info=source_info,
        env=env,
        go_licenses_module=build_env.go_licenses_module,
        go_licenses_version=build_env.go_licenses_version,
        go_licenses_disallowed_types=build_env.go_licenses_disallowed_types,
        go_licenses_enforce=build_env.go_licenses_enforce,
    )

    output_binary.chmod(0o755)
    return output_binary


class CustomBuildHook(BuildHookInterface):  # type: ignore[misc]
    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        del version
        build_dbmate(Path(self.root))
        build_data["pure_python"] = False
        build_data["infer_tag"] = True


if __name__ == "__main__":
    built = build_dbmate(Path(__file__).resolve().parents[1])
    print(f"Built bundled dbmate binary: {built}")
