from pathlib import Path

from matey.app.protocols import ArtifactDelete, ArtifactWrite
from matey.domain.model import TargetKey
from matey.infra.artifact_store import SqliteArtifactStore


def test_artifact_store_begin_apply_finalize_and_recover(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    target = repo / "db"
    target.mkdir(parents=True)
    target_file = target / "schema.sql"
    target_file.write_text("old\n", encoding="utf-8")

    store = SqliteArtifactStore(repo_root=repo)
    key = TargetKey("abc")

    txid = store.begin(
        target_key=key,
        target_root=target,
        writes=(ArtifactWrite(rel_path="schema.sql", content=b"new\n"),),
        deletes=(ArtifactDelete(rel_path="orphan.sql"),),
    )
    store.apply(txid=txid)
    assert target_file.read_text(encoding="utf-8") == "new\n"
    store.finalize(txid=txid)

    # Recovery on empty pending set is a no-op.
    store.recover_pending(target_key=key, target_root=target)
