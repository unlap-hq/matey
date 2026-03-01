from pathlib import Path

from matey.domain.target import derive_target_key


def test_target_key_is_stable_for_same_repo_and_path(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    db = repo / "db"
    db.mkdir(parents=True)

    key1 = derive_target_key(repo_root=repo, db_dir=db)
    key2 = derive_target_key(repo_root=repo, db_dir=db)
    assert key1 == key2
