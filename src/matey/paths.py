from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Literal


class PathBoundaryError(ValueError):
    def __init__(self, message: str, *, kind: str) -> None:
        super().__init__(message)
        self.kind = kind


class RelativePathError(ValueError):
    def __init__(self, message: str, *, kind: str) -> None:
        super().__init__(message)
        self.kind = kind


def absolute_path(path: Path) -> Path:
    return path if path.is_absolute() else (Path.cwd() / path)


def normalize_relative_posix_path(path: str, *, label: str) -> str:
    normalized = PurePosixPath(path).as_posix()
    candidate = PurePosixPath(normalized)
    if not normalized or normalized == ".":
        raise RelativePathError(
            f"{label} cannot be empty or current-directory.",
            kind="empty",
        )
    if candidate.is_absolute():
        raise RelativePathError(
            f"{label} must be relative, got absolute path {path!r}.",
            kind="absolute",
        )
    if any(part in {"..", "."} for part in candidate.parts):
        raise RelativePathError(
            f"{label} contains unsupported traversal or dot segment.",
            kind="dot_segment",
        )
    return normalized


def normalize_target_path_ref(path: str, *, label: str = "target path") -> str:
    if path in ("", "."):
        return "."
    return normalize_relative_posix_path(path, label=label)


def ensure_non_symlink_path(
    path: Path,
    *,
    label: str,
    allow_missing_leaf: bool,
    expected_kind: Literal["file", "dir"] | None = None,
) -> Path:
    candidate = absolute_path(path)
    current = Path(candidate.anchor) if candidate.anchor else Path()
    parts = candidate.parts[1:] if candidate.anchor else candidate.parts
    missing_prefix = False

    for index, part in enumerate(parts):
        current = current / part
        is_leaf = index == len(parts) - 1

        if missing_prefix:
            continue

        if current.is_symlink():
            if is_leaf:
                raise PathBoundaryError(
                    f"Path is a symlinked file or directory: {candidate}",
                    kind="symlink_leaf",
                )
            raise PathBoundaryError(
                f"Path uses a symlinked intermediate directory: {candidate}",
                kind="symlink_intermediate",
            )

        if not current.exists():
            if is_leaf and not allow_missing_leaf:
                raise PathBoundaryError(
                    f"{label} does not exist: {current}.",
                    kind="missing",
                )
            missing_prefix = True
            continue

        if not is_leaf and not current.is_dir():
            raise PathBoundaryError(
                f"{label} uses non-directory path segment {current}.",
                kind="non_directory_segment",
            )

        if is_leaf:
            if expected_kind == "file" and not current.is_file():
                raise PathBoundaryError(
                    f"{label} is not a file: {current}.",
                    kind="not_file",
                )
            if expected_kind == "dir" and not current.is_dir():
                raise PathBoundaryError(
                    f"{label} is not a directory: {current}.",
                    kind="not_dir",
                )

    return candidate


def safe_descendant(
    *,
    root: Path,
    candidate: Path,
    label: str,
    allow_missing_leaf: bool,
    expected_kind: Literal["file", "dir"] | None = None,
) -> Path:
    root_path = ensure_non_symlink_path(
        root,
        label=f"{label} root",
        allow_missing_leaf=True,
        expected_kind="dir",
    )
    candidate_path = candidate if candidate.is_absolute() else (root_path / candidate)
    candidate_path = ensure_non_symlink_path(
        candidate_path,
        label=label,
        allow_missing_leaf=allow_missing_leaf,
        expected_kind=expected_kind,
    )
    try:
        candidate_path.relative_to(root_path)
    except ValueError as error:
        raise PathBoundaryError(
            f"Path is outside target directory: {candidate_path}",
            kind="outside",
        ) from error
    return candidate_path


def safe_relative_descendant(
    *,
    root: Path,
    candidate: Path,
    label: str,
    allow_missing_leaf: bool,
    expected_kind: Literal["file", "dir"] | None = None,
) -> str:
    return (
        safe_descendant(
            root=root,
            candidate=candidate,
            label=label,
            allow_missing_leaf=allow_missing_leaf,
            expected_kind=expected_kind,
        )
        .relative_to(absolute_path(root))
        .as_posix()
    )


def describe_path_boundary_error(
    error: PathBoundaryError,
    *,
    path: Path | None = None,
    symlink_message: str | None = None,
) -> str:
    if symlink_message is not None and error.kind in {"symlink_leaf", "symlink_intermediate"}:
        if path is None:
            return symlink_message
        return f"{symlink_message}: {path}"
    return str(error)


__all__ = [
    "PathBoundaryError",
    "RelativePathError",
    "absolute_path",
    "describe_path_boundary_error",
    "ensure_non_symlink_path",
    "normalize_relative_posix_path",
    "normalize_target_path_ref",
    "safe_descendant",
    "safe_relative_descendant",
]
