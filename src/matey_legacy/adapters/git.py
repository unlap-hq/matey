from __future__ import annotations

import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from matey.core import SchemaValidationError
from matey.settings.env import RuntimeEnv


def _run_git(
    repo_root: Path,
    args: list[str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=False,
        capture_output=True,
        text=True,
    )


def _resolve_repo_root(cwd: Path | None = None) -> Path:
    resolved_cwd = (cwd or Path.cwd()).resolve()
    result = subprocess.run(
        ["git", "-C", str(resolved_cwd), "rev-parse", "--show-toplevel"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        details = (result.stderr or result.stdout or "").strip()
        raise SchemaValidationError(f"Unable to resolve git repository root. {details}".strip())
    root = (result.stdout or "").strip()
    if not root:
        raise SchemaValidationError("Unable to resolve git repository root.")
    return Path(root)


def _detect_base_ref(
    *,
    explicit_base_branch: str | None,
    repo_root: Path,
    runtime_env: RuntimeEnv,
) -> str:
    if explicit_base_branch:
        return explicit_base_branch

    for value in (
        runtime_env.github_base_ref,
        runtime_env.gitlab_base_ref,
        runtime_env.buildkite_base_ref,
    ):
        if value:
            return value

    upstream = _run_git(repo_root, ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"])
    if upstream.returncode == 0:
        value = (upstream.stdout or "").strip()
        if value:
            return value

    return "origin/main"


def _resolve_merge_base(repo_root: Path, base_ref: str) -> str:
    merge_base = _run_git(repo_root, ["merge-base", base_ref, "HEAD"])
    if merge_base.returncode != 0:
        details = (merge_base.stderr or merge_base.stdout or "").strip()
        raise SchemaValidationError(
            f"Unable to compute merge-base for base ref '{base_ref}'. {details}".strip()
        )
    value = (merge_base.stdout or "").strip()
    if not value:
        raise SchemaValidationError(f"Git merge-base returned empty output for base ref '{base_ref}'.")
    return value


@contextmanager
def _temporary_worktree(repo_root: Path, commit: str) -> Iterator[Path]:
    worktree_dir = Path(tempfile.mkdtemp(prefix="matey-worktree-"))
    add_result = _run_git(repo_root, ["worktree", "add", "--detach", str(worktree_dir), commit])
    if add_result.returncode != 0:
        details = (add_result.stderr or add_result.stdout or "").strip()
        shutil.rmtree(worktree_dir, ignore_errors=True)
        raise SchemaValidationError(f"Unable to create git worktree at {commit}. {details}".strip())
    try:
        yield worktree_dir
    finally:
        remove_result = _run_git(repo_root, ["worktree", "remove", "--force", str(worktree_dir)])
        if remove_result.returncode != 0:
            shutil.rmtree(worktree_dir, ignore_errors=True)


def _map_to_worktree(path: Path, repo_root: Path, worktree_root: Path) -> Path:
    try:
        relative = path.resolve().relative_to(repo_root.resolve())
    except ValueError as error:
        raise SchemaValidationError(
            f"Cannot resolve baseline path for {path}; it is outside git repo {repo_root}."
        ) from error
    return worktree_root / relative
