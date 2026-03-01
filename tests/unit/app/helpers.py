from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path

from matey.app.config_engine import TargetRuntime
from matey.app.protocols import (
    CmdResult,
    ScratchHandle,
    WorktreeChange,
)
from matey.app.runtime import AppContext
from matey.domain.lockfile import (
    LockStep,
    SchemaLock,
    digest_bytes_blake2b256,
    lock_chain_seed,
    lock_chain_step,
)
from matey.domain.model import (
    ConfigDefaults,
    Engine,
    ResolvedTargetConfig,
    SqlSource,
    derive_target_key,
)
from matey.infra.engine_policy import EnginePolicyRegistry
from matey.infra.sql_pipeline import SqlPipeline


def cmd_result(*, exit_code: int = 0, stdout: str = "", stderr: str = "") -> CmdResult:
    return CmdResult(argv=("dbmate",), exit_code=exit_code, stdout=stdout, stderr=stderr)


class FakeEnv:
    def __init__(self, values: dict[str, str] | None = None) -> None:
        self._values = values or {}

    def get(self, key: str, default: str | None = None) -> str | None:
        return self._values.get(key, default)

    def require(self, key: str) -> str:
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value


class FakeGit:
    def __init__(self, *, repo_root: Path) -> None:
        self._root = repo_root.resolve()
        self.head = "head"
        self.merge_base_value = "base"
        self.resolved_refs: dict[str, str] = {}
        self.local_changes = False
        self.local_change_rows: tuple[WorktreeChange, ...] = ()
        self.tree_paths: dict[tuple[str, str], tuple[Path, ...]] = {}
        self.blobs: dict[tuple[str, str], bytes] = {}

    def repo_root(self) -> Path:
        return self._root

    def head_commit(self) -> str:
        return self.head

    def resolve_ref(self, ref: str) -> str:
        return self.resolved_refs.get(ref, ref)

    def merge_base(self, left_ref: str, right_ref: str) -> str:
        del left_ref, right_ref
        return self.merge_base_value

    def read_blob_bytes(self, commit: str, rel_path: Path) -> bytes | None:
        return self.blobs.get((commit, rel_path.as_posix()))

    def list_tree_paths(self, commit: str, rel_dir: Path) -> tuple[Path, ...]:
        return self.tree_paths.get((commit, rel_dir.as_posix()), ())

    def has_local_changes(self, *, rel_paths: tuple[Path, ...]) -> bool:
        del rel_paths
        return self.local_changes

    def list_local_changes(self, *, rel_paths: tuple[Path, ...]) -> tuple[WorktreeChange, ...]:
        del rel_paths
        return self.local_change_rows


class FakeScratch:
    def __init__(self) -> None:
        self.prepared: list[ScratchHandle] = []
        self.cleaned: list[ScratchHandle] = []

    def prepare(
        self,
        *,
        engine: Engine,
        scratch_name: str,
        purpose: str,
        test_base_url: str | None,
        keep: bool,
    ) -> ScratchHandle:
        del keep
        if test_base_url and test_base_url.strip():
            url = test_base_url.strip()
        else:
            url = f"sqlite3:/tmp/{scratch_name}.sqlite3"
        handle = ScratchHandle(
            engine=engine,
            url=url,
            scratch_name=scratch_name,
            purpose=purpose,
            auto_provisioned=False,
            cleanup_required=False,
        )
        self.prepared.append(handle)
        return handle

    def cleanup(self, handle: ScratchHandle) -> None:
        self.cleaned.append(handle)


class FakeArtifactStore:
    def recover_pending(self, *, target_key, target_root: Path) -> None:
        del target_key, target_root

    def begin(self, *, target_key, target_root: Path, writes, deletes) -> str:
        del target_key, target_root, writes, deletes
        return "txid"

    def apply(self, *, txid: str) -> None:
        del txid

    def finalize(self, *, txid: str) -> None:
        del txid


class FakeScope:
    @contextmanager
    def open(self, *, target_key, target_root: Path):
        del target_key, target_root
        yield


class FakeProcess:
    def run(self, argv: tuple[str, ...], cwd: Path | None = None) -> CmdResult:
        del argv, cwd
        return cmd_result()


class ScriptedDbmate:
    def __init__(self) -> None:
        self._queues: dict[str, list[CmdResult]] = defaultdict(list)
        self.calls: list[str] = []

    def queue(self, method: str, *results: CmdResult) -> None:
        self._queues[method].extend(results)

    def _next(self, method: str) -> CmdResult:
        queue = self._queues.get(method)
        if queue:
            return queue.pop(0)
        if method == "status":
            return cmd_result(stdout="applied: 0\n")
        if method == "dump":
            return cmd_result(stdout="-- schema\n")
        return cmd_result()

    def _record(self, method: str) -> CmdResult:
        self.calls.append(method)
        return self._next(method)

    def new(self, name: str, migrations_dir: Path) -> CmdResult:
        del name, migrations_dir
        return self._record("new")

    def wait(self, url: str, timeout_seconds: int) -> CmdResult:
        del url, timeout_seconds
        return self._record("wait")

    def create(self, url: str, migrations_dir: Path) -> CmdResult:
        del url, migrations_dir
        return self._record("create")

    def drop(self, url: str, migrations_dir: Path) -> CmdResult:
        del url, migrations_dir
        return self._record("drop")

    def up(self, url: str, migrations_dir: Path, no_dump_schema: bool = True) -> CmdResult:
        del url, migrations_dir, no_dump_schema
        return self._record("up")

    def migrate(self, url: str, migrations_dir: Path, no_dump_schema: bool = True) -> CmdResult:
        del url, migrations_dir, no_dump_schema
        return self._record("migrate")

    def rollback(self, url: str, migrations_dir: Path, steps: int, no_dump_schema: bool = True) -> CmdResult:
        del url, migrations_dir, steps, no_dump_schema
        return self._record("rollback")

    def load_schema(
        self,
        url: str,
        schema_path: Path,
        migrations_dir: Path,
        no_dump_schema: bool = True,
    ) -> CmdResult:
        del url, schema_path, migrations_dir, no_dump_schema
        return self._record("load_schema")

    def dump(self, url: str, migrations_dir: Path) -> CmdResult:
        del url, migrations_dir
        return self._record("dump")

    def status(self, url: str, migrations_dir: Path) -> CmdResult:
        del url, migrations_dir
        return self._record("status")

    def raw(self, argv_suffix: tuple[str, ...], url: str, migrations_dir: Path) -> CmdResult:
        del argv_suffix, url, migrations_dir
        return self._record("raw")

    @staticmethod
    def to_output(result: CmdResult):
        from matey.domain.dbmate_output import DbmateOutput

        return DbmateOutput(exit_code=result.exit_code, stdout=result.stdout, stderr=result.stderr)


def build_runtime(*, repo_root: Path, target_name: str = "core") -> TargetRuntime:
    from matey.app.config_engine import build_target_runtime

    db_dir = repo_root / "db" / target_name
    db_dir.mkdir(parents=True, exist_ok=True)
    (db_dir / "migrations").mkdir(parents=True, exist_ok=True)
    (db_dir / "checkpoints").mkdir(parents=True, exist_ok=True)
    return build_target_runtime(
        resolved=ResolvedTargetConfig(
            name=target_name,
            db_dir=db_dir,
            url_env="MATEY_URL",
            test_url_env="MATEY_TEST_URL",
        )
    )


def write_lock_for_runtime(
    *,
    runtime: TargetRuntime,
    repo_root: Path,
    engine: Engine,
    schema_sql: str,
) -> SchemaLock:
    migrations_rel = tuple(
        sorted(
            (Path("migrations") / path.name).as_posix()
            for path in runtime.paths.migrations_dir.iterdir()
            if path.is_file() and path.suffix == ".sql"
        )
    )
    pipeline = SqlPipeline()
    target_key = derive_target_key(repo_root=repo_root, db_dir=runtime.paths.db_dir)
    chain = lock_chain_seed(engine, target_key)
    steps: list[LockStep] = []

    for index, migration_rel in enumerate(migrations_rel, start=1):
        migration_path = runtime.paths.db_dir / migration_rel
        migration_bytes = migration_path.read_bytes()
        migration_digest = digest_bytes_blake2b256(migration_bytes)
        version = migration_path.stem.split("_", 1)[0]
        chain = lock_chain_step(chain, version, migration_rel, migration_digest)
        checkpoint_rel = (Path("checkpoints") / f"{migration_path.stem}.sql").as_posix()
        checkpoint_path = runtime.paths.db_dir / checkpoint_rel
        checkpoint_text = checkpoint_path.read_text(encoding="utf-8")
        checkpoint_digest = digest_bytes_blake2b256(checkpoint_text.encode("utf-8"))
        schema_digest = pipeline.prepare(
            engine=engine,
            source=SqlSource(text=checkpoint_text, origin="artifact"),
        ).digest
        steps.append(
            LockStep(
                index=index,
                version=version,
                migration_file=migration_rel,
                migration_digest=migration_digest,
                checkpoint_file=checkpoint_rel,
                checkpoint_digest=checkpoint_digest,
                schema_digest=schema_digest,
                chain_hash=chain,
            )
        )

    schema_digest = pipeline.prepare(
        engine=engine,
        source=SqlSource(text=schema_sql, origin="artifact"),
    ).digest
    lock = SchemaLock(
        lock_version=0,
        hash_algorithm="blake2b-256",
        canonicalizer="matey-sql-v0",
        engine=engine.value,
        target=runtime.target_id.name,
        schema_file="schema.sql",
        migrations_dir="migrations",
        checkpoints_dir="checkpoints",
        head_index=len(steps),
        head_chain_hash=chain,
        head_schema_digest=schema_digest,
        steps=tuple(steps),
    )
    runtime.paths.lock_file.write_text(lock.to_toml(), encoding="utf-8")
    runtime.paths.schema_file.write_text(schema_sql, encoding="utf-8")
    return lock


def build_context(
    *,
    repo_root: Path,
    git: FakeGit | None = None,
    env: FakeEnv | None = None,
    dbmate: ScriptedDbmate | None = None,
    scratch: FakeScratch | None = None,
) -> AppContext:
    from matey.infra.runtime_io import LocalFileSystem

    return AppContext(
        fs=LocalFileSystem(),
        proc=FakeProcess(),
        env=env or FakeEnv(),
        git=git or FakeGit(repo_root=repo_root),
        dbmate=dbmate or ScriptedDbmate(),
        sql_pipeline=SqlPipeline(),
        engine_policies=EnginePolicyRegistry(),
        scratch=scratch or FakeScratch(),
        artifact_store=FakeArtifactStore(),
        scope=FakeScope(),
    )


def default_defaults() -> ConfigDefaults:
    return ConfigDefaults()
