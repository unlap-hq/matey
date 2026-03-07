from __future__ import annotations

import gc
import importlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from matey.tx import TxError, commit_artifacts, recover_artifacts, serialized_target

tx_store_mod = importlib.import_module("matey.tx.store")
tx_journal_mod = importlib.import_module("matey.tx.journal")
tx_locking_mod = importlib.import_module("matey.tx.locking")


def _write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _manifest_json(*, writes: list[str], deletes: list[str], created_ns: int = 1) -> str:
    return json.dumps(
        {
            "version": 1,
            "created_ns": created_ns,
            "writes": writes,
            "deletes": deletes,
        }
    )


def _spawn_lock_worker(
    target: Path, hold_seconds: float, output_path: Path
) -> subprocess.Popen[str]:
    code = (
        "import sys, time\n"
        "from pathlib import Path\n"
        "from matey.tx import serialized_target\n"
        "target = Path(sys.argv[1])\n"
        "hold = float(sys.argv[2])\n"
        "output = Path(sys.argv[3])\n"
        "start = time.monotonic()\n"
        "with serialized_target(target):\n"
        "    acquired = time.monotonic()\n"
        "    output.write_text(str(acquired - start), encoding='utf-8')\n"
        "    time.sleep(hold)\n"
    )
    project_root = Path(__file__).resolve().parents[2]
    src_path = str(project_root / "src")
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = src_path if not existing else f"{src_path}:{existing}"
    return subprocess.Popen(
        [sys.executable, "-c", code, str(target), str(hold_seconds), str(output_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


def test_commit_artifacts_writes_and_deletes(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    a_path = target / "a.txt"
    stale_path = target / "stale.txt"
    new_path = target / "new.txt"
    _write(a_path, b"old-a")
    _write(stale_path, b"old-stale")

    changed = commit_artifacts(
        target,
        writes={
            a_path: b"new-a",
            new_path: b"new-file",
        },
        deletes=(stale_path,),
    )

    assert a_path.read_bytes() == b"new-a"
    assert new_path.read_bytes() == b"new-file"
    assert not stale_path.exists()
    assert changed == tuple(sorted((a_path, new_path, stale_path), key=lambda p: p.as_posix()))


def test_commit_artifacts_rejects_paths_outside_target(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)
    outside = (tmp_path / "outside.txt").resolve()

    with pytest.raises(TxError, match="outside target directory"):
        commit_artifacts(target, writes={outside: b"x"}, deletes=())


def test_commit_artifacts_rejects_symlinked_intermediate_path(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)
    foreign = tmp_path / "foreign"
    foreign.mkdir(parents=True, exist_ok=True)
    (target / "linked").symlink_to(foreign, target_is_directory=True)

    with pytest.raises(TxError, match="symlinked intermediate directory"):
        commit_artifacts(
            target,
            writes={target / "linked" / "outside.txt": b"x"},
            deletes=(),
        )


def test_commit_artifacts_rejects_symlinked_leaf_path(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)
    referent = target / "real.txt"
    referent.write_text("real", encoding="utf-8")
    leaf = target / "leaf.txt"
    leaf.symlink_to(referent)

    with pytest.raises(TxError, match="symlinked file or directory"):
        commit_artifacts(
            target,
            writes={},
            deletes=(leaf,),
        )

    assert referent.read_text(encoding="utf-8") == "real"
    assert leaf.is_symlink()


def test_commit_artifacts_rejects_overlapping_write_and_delete(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)
    shared = target / "shared.txt"
    _write(shared, b"old")

    with pytest.raises(TxError, match="both writes and deletes"):
        commit_artifacts(
            target,
            writes={shared: b"new"},
            deletes=(shared,),
        )


def test_recover_artifacts_drops_prepared_tx(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    tx_dir = target / ".matey" / "tx" / "tx-prepared"
    tx_dir.mkdir(parents=True, exist_ok=True)
    (tx_dir / "state").write_text("prepared\n", encoding="utf-8")
    (tx_dir / "manifest.json").write_text(
        _manifest_json(writes=["a.txt"], deletes=[]),
        encoding="utf-8",
    )
    _write(tx_dir / "staged" / "a.txt", b"new")

    recover_artifacts(target)

    assert not tx_dir.exists()
    assert not (target / ".matey" / "tx").exists()


def test_recover_artifacts_rolls_back_applying_tx(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    orig_path = target / "orig.txt"
    deleted_path = target / "delete-me.txt"
    created_path = target / "created.txt"

    # Simulate partially applied state in target filesystem.
    _write(orig_path, b"mutated-orig")
    _write(created_path, b"created-new")
    # deleted_path was removed by the interrupted apply.

    tx_dir = target / ".matey" / "tx" / "tx-applying"
    tx_dir.mkdir(parents=True, exist_ok=True)
    (tx_dir / "state").write_text("applying\n", encoding="utf-8")
    (tx_dir / "manifest.json").write_text(
        _manifest_json(
            writes=["orig.txt", "created.txt"],
            deletes=["delete-me.txt"],
        ),
        encoding="utf-8",
    )

    # Backup contains the pre-apply originals.
    _write(tx_dir / "backup" / "orig.txt", b"original-orig")
    _write(tx_dir / "backup" / "delete-me.txt", b"original-delete")

    recover_artifacts(target)

    assert orig_path.read_bytes() == b"original-orig"
    assert deleted_path.read_bytes() == b"original-delete"
    assert not created_path.exists()
    assert not tx_dir.exists()
    assert not (target / ".matey" / "tx").exists()


def test_commit_failure_keeps_journal_and_is_recoverable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    stable_path = target / "stable.txt"
    delete_path = target / "delete.txt"
    _write(stable_path, b"stable-before")
    _write(delete_path, b"delete-before")

    def _failing_apply(*, target_root: Path, tx_dir: Path, manifest: tx_journal_mod.TxManifest) -> None:
        # Simulate partial mutation before crash.
        first = tx_journal_mod.absolute_target_path(target_root, manifest.writes[0])
        first.write_bytes(b"mutated-during-apply")
        raise RuntimeError("boom")

    monkeypatch.setattr(tx_store_mod, "apply_tx", _failing_apply)

    with pytest.raises(RuntimeError, match="boom"):
        commit_artifacts(
            target,
            writes={stable_path: b"stable-after"},
            deletes=(delete_path,),
        )

    tx_root = target / ".matey" / "tx"
    pending = sorted(entry for entry in tx_root.iterdir() if entry.is_dir())
    assert len(pending) == 1
    assert (pending[0] / "state").read_text(encoding="utf-8").strip() == "applying"

    recover_artifacts(target)

    assert stable_path.read_bytes() == b"stable-before"
    assert delete_path.read_bytes() == b"delete-before"
    assert not tx_root.exists()


def test_recover_artifacts_rejects_multiple_applying_transactions(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    for name in ("tx-applying-a", "tx-applying-b"):
        tx_dir = target / ".matey" / "tx" / name
        tx_dir.mkdir(parents=True, exist_ok=True)
        (tx_dir / "state").write_text("applying\n", encoding="utf-8")
        (tx_dir / "manifest.json").write_text(
            _manifest_json(writes=["file.txt"], deletes=[]),
            encoding="utf-8",
        )

    with pytest.raises(TxError, match="Multiple applying transactions found"):
        recover_artifacts(target)


def test_recover_artifacts_rejects_non_directory_tx_root(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    tx_root = target / ".matey" / "tx"
    tx_root.parent.mkdir(parents=True, exist_ok=True)
    tx_root.write_text("not a directory", encoding="utf-8")

    with pytest.raises(TxError, match="Transaction root is not a directory"):
        recover_artifacts(target)


def test_recover_artifacts_rejects_symlinked_tx_root(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    meta_root = target / ".matey"
    meta_root.mkdir(parents=True, exist_ok=True)
    foreign = tmp_path / "foreign-tx-root"
    foreign.mkdir(parents=True, exist_ok=True)
    (meta_root / "tx").symlink_to(foreign, target_is_directory=True)

    with pytest.raises(TxError, match="Transaction root is symlinked"):
        recover_artifacts(target)


def test_recover_artifacts_rejects_symlinked_tx_entry(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    tx_root = target / ".matey" / "tx"
    tx_root.mkdir(parents=True, exist_ok=True)
    foreign = tmp_path / "foreign"
    foreign.mkdir(parents=True, exist_ok=True)
    (tx_root / "linked").symlink_to(foreign, target_is_directory=True)

    with pytest.raises(TxError, match="symlinked entry"):
        recover_artifacts(target)


def test_recover_artifacts_rejects_non_directory_tx_entry(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    tx_root = target / ".matey" / "tx"
    tx_root.mkdir(parents=True, exist_ok=True)
    (tx_root / "junk").write_text("junk", encoding="utf-8")

    with pytest.raises(TxError, match="non-directory entry"):
        recover_artifacts(target)


def test_recover_artifacts_rejects_manifest_paths_in_tx_namespace(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    tx_dir = target / ".matey" / "tx" / "tx-applying"
    tx_dir.mkdir(parents=True, exist_ok=True)
    (tx_dir / "state").write_text("applying\n", encoding="utf-8")
    (tx_dir / "manifest.json").write_text(
        _manifest_json(writes=[".matey/tx/forbidden"], deletes=[]),
        encoding="utf-8",
    )

    with pytest.raises(TxError, match="reserved for tx journal internals"):
        recover_artifacts(target)


def test_recover_artifacts_rejects_manifest_paths_in_tx_lock_file(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    tx_dir = target / ".matey" / "tx" / "tx-applying"
    tx_dir.mkdir(parents=True, exist_ok=True)
    (tx_dir / "state").write_text("applying\n", encoding="utf-8")
    (tx_dir / "manifest.json").write_text(
        _manifest_json(writes=[".matey/tx.lock"], deletes=[]),
        encoding="utf-8",
    )

    with pytest.raises(TxError, match="reserved for tx journal internals"):
        recover_artifacts(target)


def test_recover_artifacts_recovers_valid_transactions_before_raising_invalid(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    restored_path = target / "restored.txt"
    restored_path.write_bytes(b"mutated")

    valid_tx = target / ".matey" / "tx" / "000-valid"
    valid_tx.mkdir(parents=True, exist_ok=True)
    (valid_tx / "state").write_text("applying\n", encoding="utf-8")
    (valid_tx / "manifest.json").write_text(
        _manifest_json(writes=["restored.txt"], deletes=[]),
        encoding="utf-8",
    )
    _write(valid_tx / "backup" / "restored.txt", b"original")

    invalid_tx = target / ".matey" / "tx" / "001-invalid"
    invalid_tx.mkdir(parents=True, exist_ok=True)
    (invalid_tx / "state").write_text("applying\n", encoding="utf-8")
    (invalid_tx / "manifest.json").write_text("{not json", encoding="utf-8")

    with pytest.raises(TxError, match="invalid journal entries"):
        recover_artifacts(target)

    assert restored_path.read_bytes() == b"original"
    assert not valid_tx.exists()
    assert invalid_tx.exists()


def test_recover_artifacts_rejects_manifest_missing_created_ns(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    tx_dir = target / ".matey" / "tx" / "tx-applying"
    tx_dir.mkdir(parents=True, exist_ok=True)
    (tx_dir / "state").write_text("applying\n", encoding="utf-8")
    (tx_dir / "manifest.json").write_text(
        json.dumps({"version": 1, "writes": ["file.txt"], "deletes": []}),
        encoding="utf-8",
    )

    with pytest.raises(TxError, match="created_ns"):
        recover_artifacts(target)


def test_recover_artifacts_rejects_symlinked_manifest_file(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    tx_dir = target / ".matey" / "tx" / "tx-applying"
    tx_dir.mkdir(parents=True, exist_ok=True)
    (tx_dir / "state").write_text("applying\n", encoding="utf-8")
    foreign = tmp_path / "foreign.json"
    foreign.write_text(_manifest_json(writes=["file.txt"], deletes=[]), encoding="utf-8")
    (tx_dir / "manifest.json").symlink_to(foreign)

    with pytest.raises(TxError, match="symlinked journal path"):
        recover_artifacts(target)


def test_recover_artifacts_rejects_symlinked_backup_file(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    tx_dir = target / ".matey" / "tx" / "tx-applying"
    tx_dir.mkdir(parents=True, exist_ok=True)
    (tx_dir / "state").write_text("applying\n", encoding="utf-8")
    (tx_dir / "manifest.json").write_text(
        _manifest_json(writes=["file.txt"], deletes=[]),
        encoding="utf-8",
    )
    foreign = tmp_path / "foreign.bak"
    foreign.write_text("backup", encoding="utf-8")
    (tx_dir / "backup").mkdir(parents=True, exist_ok=True)
    (tx_dir / "backup" / "file.txt").symlink_to(foreign)

    with pytest.raises(TxError, match="symlinked journal path"):
        recover_artifacts(target)


def test_serialized_target_blocks_concurrent_processes(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    first_out = tmp_path / "first.out"
    second_out = tmp_path / "second.out"
    first = _spawn_lock_worker(target, 1.0, first_out)
    time.sleep(0.2)
    second = _spawn_lock_worker(target, 0.0, second_out)

    first_stdout, first_stderr = first.communicate(timeout=15)
    second_stdout, second_stderr = second.communicate(timeout=15)
    assert first.returncode == 0, f"first worker failed: {first_stderr or first_stdout}"
    assert second.returncode == 0, f"second worker failed: {second_stderr or second_stdout}"

    first_wait = float(first_out.read_text(encoding="utf-8"))
    second_wait = float(second_out.read_text(encoding="utf-8"))
    assert first_wait < 0.3
    assert second_wait >= 0.5


def test_serialized_target_is_reentrant_in_same_process(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    with serialized_target(target), serialized_target(target):
        pass


def test_target_rlock_cache_is_weakly_evicted(tmp_path: Path) -> None:
    lock_path = (tmp_path / "target" / ".matey" / "tx.lock").resolve()
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    lock = tx_locking_mod.target_rlock(lock_path)
    assert lock_path in tx_locking_mod._RLOCKS_BY_PATH

    del lock
    gc.collect()

    assert lock_path not in tx_locking_mod._RLOCKS_BY_PATH


def test_serialized_target_does_not_create_repo_lock_file(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    with serialized_target(target):
        pass

    assert not (target / ".matey" / "tx.lock").exists()


def test_recover_artifacts_aggregates_manifest_read_oserror(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    valid_tx_dir = target / ".matey" / "tx" / "tx-valid"
    valid_tx_dir.mkdir(parents=True, exist_ok=True)
    (valid_tx_dir / "state").write_text("prepared\n", encoding="utf-8")
    (valid_tx_dir / "manifest.json").write_text(
        _manifest_json(writes=["a.txt"], deletes=[]),
        encoding="utf-8",
    )

    invalid_tx_dir = target / ".matey" / "tx" / "tx-invalid"
    invalid_tx_dir.mkdir(parents=True, exist_ok=True)
    bad_manifest = invalid_tx_dir / "manifest.json"
    bad_manifest.write_text(
        _manifest_json(writes=["b.txt"], deletes=[]),
        encoding="utf-8",
    )
    (invalid_tx_dir / "state").write_text("prepared\n", encoding="utf-8")

    original_read_text = Path.read_text

    def _read_text(self: Path, *args, **kwargs) -> str:
        if self == bad_manifest:
            raise OSError("boom")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _read_text)

    with pytest.raises(TxError, match="invalid journal entries"):
        recover_artifacts(target)

    assert not valid_tx_dir.exists()


def test_recover_artifacts_aggregates_manifest_invalid_utf8(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    tx_dir = target / ".matey" / "tx" / "tx-invalid"
    tx_dir.mkdir(parents=True, exist_ok=True)
    (tx_dir / "state").write_text("prepared\n", encoding="utf-8")
    (tx_dir / "manifest.json").write_bytes(b"\xff\xfe\x00")

    with pytest.raises(TxError, match="invalid journal entries"):
        recover_artifacts(target)
