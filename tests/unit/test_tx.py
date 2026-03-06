from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

import matey.tx as tx_mod
from matey.tx import TxError, commit_artifacts, recover_artifacts, serialized_target


def _write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


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
        json.dumps({"version": 1, "writes": ["a.txt"], "deletes": []}),
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
        json.dumps(
            {
                "version": 1,
                "writes": ["orig.txt", "created.txt"],
                "deletes": ["delete-me.txt"],
            }
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

    def _failing_apply(*, target_root: Path, tx_dir: Path, manifest: tx_mod._TxManifest) -> None:
        # Simulate partial mutation before crash.
        first = tx_mod._absolute_target_path(target_root, manifest.writes[0])
        first.write_bytes(b"mutated-during-apply")
        raise RuntimeError("boom")

    monkeypatch.setattr(tx_mod, "_apply_tx", _failing_apply)

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
            json.dumps({"version": 1, "writes": ["file.txt"], "deletes": []}),
            encoding="utf-8",
        )

    with pytest.raises(TxError, match="Multiple applying transactions found"):
        recover_artifacts(target)


def test_recover_artifacts_rejects_manifest_paths_in_tx_namespace(tmp_path: Path) -> None:
    target = (tmp_path / "target").resolve()
    target.mkdir(parents=True, exist_ok=True)

    tx_dir = target / ".matey" / "tx" / "tx-applying"
    tx_dir.mkdir(parents=True, exist_ok=True)
    (tx_dir / "state").write_text("applying\n", encoding="utf-8")
    (tx_dir / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "writes": [".matey/tx/forbidden"],
                "deletes": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(TxError, match="reserved for tx journal internals"):
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
