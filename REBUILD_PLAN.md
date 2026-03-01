# Matey Rebuild Plan

## 0. Objective

Rebuild `src/matey` from first principles with a strict architecture and deterministic behavior.

This document is the implementation contract.

Hard requirements:

1. No functional reuse of `matey_legacy` interfaces.
2. Strong boundaries: `domain -> app -> infra -> cli`.
3. Deterministic replay + lock/checkpoint model.
4. Explicitly typed interfaces and errors.
5. DRY command orchestration with one SQL pipeline and one command scope.

---

## 1. Design Principles (Non-Negotiable)

1. Determinism over convenience.
2. Single public API per concept.
3. Pure domain + app policy; infra performs side effects only.
4. No hidden global state.
5. All command control flow is typed (no ad-hoc string branching outside explicit classifiers/parsers).
6. Crash-consistent artifact apply for generated artifacts.
7. No compatibility shims.

---

## 2. Dependency Usage Contract (`pyproject.toml`)

1. `typer`
   - CLI declaration only.
2. `mashumaro[toml]`
   - Typed TOML read/write for config + lockfile.
3. `typed-settings[dotenv]`
   - Single environment ingress.
4. `pygit2`
   - Ref resolution, merge-base, blob/tree reads, worktree status.
5. `testcontainers[postgres,mysql,clickhouse]`
   - Containerized scratch for non-sqlite/non-bigquery engines.
6. `portalocker`
   - Cross-process lock backend for command scope.
7. `sqlite3` (stdlib)
   - Artifact transaction journal store under `.matey/tx.db`.
8. `tomli-w`
   - Not required for core artifacts; optional fallback only.

---

## 3. Target Package Structure

```text
src/matey/
  __main__.py

  cli/
    app.py
    options.py
    render.py
    groups/
      schema.py
      db.py
      config.py
      ci.py

  domain/
    errors.py
    constants.py
    target.py
    engine.py
    sql.py
    dbmate_output.py
    digest.py
    migration.py
    lockfile.py
    config.py
    plan.py
    result.py

  app/
    protocols.py
    context.py
    kernel.py
    scope.py
    schema_engine.py
    db_engine.py
    ci_engine.py
    config_engine.py

  infra/
    env.py
    fs.py
    proc.py
    git.py
    dbmate.py
    locking.py
    sql_pipeline.py
    engine_policy.py
    artifact_store.py
    scratch/
      factory.py
      sqlite.py
      containerized.py
      bigquery.py
```

Rules:

1. `domain` is pure.
2. `app` owns command policy.
3. `infra` owns implementation details.
4. `cli` only maps args to app and renders outputs.

---

## 4. Interface Contracts (Authoritative)

All protocols live in `app/protocols.py` and are prefixed with `I`.

```python
from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, Protocol

from matey.domain.engine import Engine
from matey.domain.target import TargetKey
from matey.domain.sql import SqlSource, PreparedSql, SqlComparison


@dataclass(frozen=True)
class CmdResult:
    argv: tuple[str, ...]
    exit_code: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class ArtifactWrite:
    rel_path: str
    content: bytes


@dataclass(frozen=True)
class ArtifactDelete:
    rel_path: str


@dataclass(frozen=True)
class WorktreeChange:
    rel_path: str
    staged: bool
    unstaged: bool
    untracked: bool


@dataclass(frozen=True)
class ScratchHandle:
    engine: Engine
    url: str
    scratch_name: str
    purpose: str
    auto_provisioned: bool
    cleanup_required: bool


class IProcessRunner(Protocol):
    def run(self, argv: tuple[str, ...], cwd: Path | None = None) -> CmdResult: ...


class IFileSystem(Protocol):
    def read_bytes(self, path: Path) -> bytes: ...
    def read_text(self, path: Path) -> str: ...
    def write_bytes_atomic(self, path: Path, data: bytes) -> None: ...
    def write_text_atomic(self, path: Path, data: str) -> None: ...
    def exists(self, path: Path) -> bool: ...
    def mkdir(self, path: Path, parents: bool = False) -> None: ...
    def list_files(self, path: Path) -> tuple[Path, ...]: ...


class IEnvProvider(Protocol):
    def get(self, key: str, default: str | None = None) -> str | None: ...
    def require(self, key: str) -> str: ...


class IGitRepo(Protocol):
    def repo_root(self) -> Path: ...
    def head_commit(self) -> str: ...
    def resolve_ref(self, ref: str) -> str: ...
    def merge_base(self, left_ref: str, right_ref: str) -> str: ...
    def read_blob_bytes(self, commit: str, rel_path: Path) -> bytes | None: ...
    def list_tree_paths(self, commit: str, rel_dir: Path) -> tuple[Path, ...]: ...
    def has_local_changes(self, *, rel_paths: tuple[Path, ...]) -> bool: ...
    def list_local_changes(self, *, rel_paths: tuple[Path, ...]) -> tuple[WorktreeChange, ...]: ...


class ISqlPipeline(Protocol):
    def prepare(self, *, engine: Engine, source: SqlSource) -> PreparedSql: ...
    def compare(self, *, engine: Engine, expected: SqlSource, actual: SqlSource) -> SqlComparison: ...


@dataclass(frozen=True)
class EngineClassifierPolicy:
    missing_db_positive: tuple[str, ...]
    missing_db_negative: tuple[str, ...]
    create_exists: tuple[str, ...]
    create_fatal: tuple[str, ...]


@dataclass(frozen=True)
class EnginePolicy:
    wait_required: bool
    requires_test_url_for_index0: bool
    build_scratch_url: Callable[[str, str], str]
    classifier: EngineClassifierPolicy


class IEnginePolicyRegistry(Protocol):
    def get(self, engine: Engine) -> EnginePolicy: ...


class IScratchManager(Protocol):
    def prepare(
        self,
        *,
        engine: Engine,
        scratch_name: str,
        purpose: str,
        test_base_url: str | None,
        keep: bool,
    ) -> ScratchHandle: ...
    def cleanup(self, handle: ScratchHandle) -> None: ...


class IDbmateGateway(Protocol):
    def new(self, name: str, migrations_dir: Path) -> CmdResult: ...
    def wait(self, url: str, timeout_seconds: int) -> CmdResult: ...
    def create(self, url: str, migrations_dir: Path) -> CmdResult: ...
    def drop(self, url: str, migrations_dir: Path) -> CmdResult: ...
    def up(self, url: str, migrations_dir: Path, no_dump_schema: bool = True) -> CmdResult: ...
    def migrate(self, url: str, migrations_dir: Path, no_dump_schema: bool = True) -> CmdResult: ...
    def rollback(self, url: str, migrations_dir: Path, steps: int, no_dump_schema: bool = True) -> CmdResult: ...
    def load_schema(self, url: str, schema_path: Path, migrations_dir: Path, no_dump_schema: bool = True) -> CmdResult: ...
    def dump(self, url: str, migrations_dir: Path) -> CmdResult: ...
    def status(self, url: str, migrations_dir: Path) -> CmdResult: ...
    def raw(self, argv_suffix: tuple[str, ...], url: str, migrations_dir: Path) -> CmdResult: ...


class IArtifactStore(Protocol):
    def recover_pending(self, *, target_key: TargetKey, target_root: Path) -> None: ...
    def begin(
        self,
        *,
        target_key: TargetKey,
        target_root: Path,
        writes: tuple[ArtifactWrite, ...],
        deletes: tuple[ArtifactDelete, ...],
    ) -> str: ...
    def apply(self, *, txid: str) -> None: ...
    def finalize(self, *, txid: str) -> None: ...


class ICommandScope(Protocol):
    def open(self, *, target_key: TargetKey, target_root: Path) -> AbstractContextManager[None]: ...
```

Contract rules:

1. App-layer orchestration must use `ICommandScope` for every command.
2. SQL normalization/comparison/digest prep must go through `ISqlPipeline` only.
3. Engine-specific branching in app layer is forbidden; app consumes `IEnginePolicyRegistry`.
4. Artifact mutation during `schema apply` must go through `IArtifactStore` only.
5. `TargetKey` (not target display name) is the identity key for tx/recovery/locking semantics.
6. `IDbmateGateway` is side-effect transport only; dbmate text parsing is pure domain logic in `domain/dbmate_output.py`.
7. Classifier behavior comes from policy tables (`EngineClassifierPolicy`) and may not be hardcoded in command engines.

---

## 5. Domain Model Specification

## 5.1 Constants (`domain/constants.py`)

```python
LOCK_VERSION = 0
HASH_ALGORITHM = "blake2b-256"
CANONICALIZER = "matey-sql-v0"
LOCK_FILENAME = "schema.lock.toml"
SCHEMA_FILENAME = "schema.sql"
MIGRATIONS_DIRNAME = "migrations"
CHECKPOINTS_DIRNAME = "checkpoints"
CHAIN_PREFIX = "matey-lock-v0"
```

## 5.2 Target Model (`domain/target.py`)

```python
@dataclass(frozen=True)
class TargetId:
    name: str


@dataclass(frozen=True)
class TargetPaths:
    db_dir: Path
    migrations_dir: Path
    checkpoints_dir: Path
    schema_file: Path
    lock_file: Path


@dataclass(frozen=True)
class TargetKey:
    value: str
```

Rules:

1. `TargetKey` is derived from canonical repo-root + repo-relative `db_dir` path.
2. `TargetKey` is stable across process restarts.
3. No app/infra identity operations may use display target name alone.

## 5.3 Engine Model (`domain/engine.py`)

```python
class Engine(Enum):
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    CLICKHOUSE = "clickhouse"
    BIGQUERY = "bigquery"
```

## 5.4 SQL Model (`domain/sql.py`)

```python
@dataclass(frozen=True)
class SqlSource:
    text: str
    origin: Literal["artifact", "scratch_dump", "live_dump"]
    context_url: str | None = None


@dataclass(frozen=True)
class PreparedSql:
    normalized: str
    digest: str


@dataclass(frozen=True)
class SqlComparison:
    expected: PreparedSql
    actual: PreparedSql
    equal: bool
    diff: str | None
```

Rules:

1. Public SQL API is only `ISqlPipeline.prepare` and `ISqlPipeline.compare`.
2. There is no public `normalize_dump_sql` or public `canonicalize_sql` API.
3. All equality/diff decisions used by command exits come from `ISqlPipeline.compare`.
4. All lock/checkpoint schema digests of dump-derived SQL come from `ISqlPipeline.prepare`.

## 5.5 Dbmate Output Model (`domain/dbmate_output.py`)

```python
@dataclass(frozen=True)
class DbStatusSnapshot:
    applied_files: tuple[str, ...]
    applied_count: int


@dataclass(frozen=True)
class DbmateOutput:
    exit_code: int
    stdout: str
    stderr: str
```

Pure functions:

```python
def parse_status_output(text: str) -> DbStatusSnapshot: ...
def extract_status_text(output: DbmateOutput) -> str: ...
def extract_dump_sql(output: DbmateOutput) -> str: ...
```

Rules:

1. Parsing/extraction is deterministic and side-effect free.
2. Extraction/parse failures raise typed errors; command engines never parse dbmate output ad-hoc.
3. Guarded db flows (`up|migrate|down|drift|plan`) must use this module.
4. App layer maps `IDbmateGateway` transport output into `DbmateOutput` before parser/extraction calls.

## 5.6 Digest (`domain/digest.py`)

Functions:

```python
def digest_bytes_blake2b256(payload: bytes) -> str: ...
def lock_chain_seed(engine: Engine, target_key: TargetKey) -> str: ...
def lock_chain_step(prev: str, version: str, migration_file: str, migration_digest: str) -> str: ...
```

Rules:

1. Migration/checkpoint file digests are raw byte digests.
2. Schema digests are taken from `PreparedSql.digest` from `ISqlPipeline.prepare`.
3. Merge-base migration comparisons use blob bytes from git.

## 5.7 Migration Parsing (`domain/migration.py`)

```python
@dataclass(frozen=True)
class MigrationFile:
    version: str
    filename: str
    rel_path: str


@dataclass(frozen=True)
class DownSectionState:
    marker_present: bool
    has_executable_sql: bool
```

Rules:

1. Include only regular `*.sql` files.
2. Deterministic lexicographic ordering by filename.
3. `has_executable_sql` ignores whitespace, line comments, block comments, and bare delimiters.

## 5.8 Lockfile Model (`domain/lockfile.py`)

Use `DataClassTOMLMixin`.

```python
@dataclass(frozen=True)
class LockStep(DataClassTOMLMixin):
    index: int
    version: str
    migration_file: str
    migration_digest: str
    checkpoint_file: str
    checkpoint_digest: str
    schema_digest: str
    chain_hash: str


@dataclass(frozen=True)
class SchemaLock(DataClassTOMLMixin):
    lock_version: int
    hash_algorithm: str
    canonicalizer: str
    engine: str
    target: str
    schema_file: str
    migrations_dir: str
    checkpoints_dir: str
    head_index: int
    head_chain_hash: str
    head_schema_digest: str
    steps: tuple[LockStep, ...]
```

Validation rules:

1. `lock_version/hash_algorithm/canonicalizer` strict match.
2. Contiguous step indexes from 1..N.
3. Unique migration versions and files.
4. `migration_file` under `migrations_dir`.
5. `checkpoint_file` under `checkpoints_dir`.
6. Path traversal and absolute paths forbidden.
7. `head_index == len(steps)`.

## 5.9 Config Model (`domain/config.py`)

Typed input + normalized runtime target model.

```python
@dataclass(frozen=True)
class ConfigDefaults(DataClassTOMLMixin):
    dir: str = "db"
    url_env: str = "MATEY_URL"
    test_url_env: str = "MATEY_TEST_URL"
    base_ref: str | None = None


@dataclass(frozen=True)
class ConfigTarget(DataClassTOMLMixin):
    dir: str | None = None
    url_env: str | None = None
    test_url_env: str | None = None


@dataclass(frozen=True)
class MateyConfig(DataClassTOMLMixin):
    defaults: ConfigDefaults = ConfigDefaults()
    targets: dict[str, ConfigTarget] | None = None
```

## 5.10 Plan/Result Types (`domain/plan.py`, `domain/result.py`)

Use narrow dataclasses per command family.
No dict payloads.

Operation contexts:

```python
@dataclass(frozen=True)
class SchemaOpContext:
    target_id: TargetId
    target_key: TargetKey
    target_paths: TargetPaths
    repo_root: Path
    base_ref: str | None
    replay_engine: Engine
    test_url: str | None
    clean: bool
    keep_scratch: bool
    run_nonce: str


@dataclass(frozen=True)
class DbOpContext:
    target_id: TargetId
    target_key: TargetKey
    target_paths: TargetPaths
    live_engine: Engine
    live_url: str
    test_url: str | None
    keep_scratch: bool
    run_nonce: str
```

Rules:

1. Each command builds exactly one op context and passes it through helpers.
2. Helper APIs must not accept scattered raw tuples for target/url/flags.

---

## 6. Environment Ingress (`infra/env.py`)

Single ingress using `typed-settings[dotenv]`.

Rules:

1. No raw `os.environ` reads outside `infra/env.py`.
2. Precedence is deterministic:
   - CLI override
   - process env
   - `.env`
   - defaults
3. Engine inference consumes resolved URL values only (non-empty string), never env key names.

---

## 7. Infrastructure Implementations

## 7.1 Filesystem (`infra/fs.py`)

1. Atomic single-file writes via temp + replace.
2. Path resolution helper ensures writes stay under target root.

## 7.2 Process Runner (`infra/proc.py`)

1. Full stdout/stderr capture.
2. No logging/printing side effects.

## 7.3 Git (`infra/git.py`)

Use `pygit2` only.

Capabilities:

1. Repo discovery.
2. Ref resolution and merge-base.
3. Blob and tree reads from arbitrary commit.
4. Worktree status for selected target paths (staged/unstaged/untracked, excluding ignored).

## 7.4 Dbmate Gateway (`infra/dbmate.py`)

1. Implements `IDbmateGateway` (typed command transport).
2. Public gateway methods are explicit by verb and forward through one private `_run_dbmate(...)` helper for DRY argv construction.
3. No parsing/extraction logic in infra gateway.
4. `domain/dbmate_output.py` performs output extraction/parsing.

## 7.5 SQL Pipeline (`infra/sql_pipeline.py`)

Single implementation of `ISqlPipeline`.

Internal steps (not public API):

1. Base text normalization (line endings, trim policy, trailing newline policy).
2. Origin-aware normalization (artifact/scratch_dump/live_dump).
3. Engine-aware dump adaptation (for example BigQuery scratch dataset normalization when comparing dump-origin SQL).
4. Digest generation from normalized text.
5. Unified diff generation from normalized text.

Constraint:

- Command/app code may only call `prepare` / `compare`.

## 7.6 Engine Policy Registry (`infra/engine_policy.py`)

Data-driven registry mapping `Engine -> EnginePolicy`.

Policy includes:

1. scratch URL transformation.
2. wait requirement.
3. structured classifier tables (`EngineClassifierPolicy`):
   - `missing_db_positive`
   - `missing_db_negative`
   - `create_exists`
   - `create_fatal`
5. index-0 baseline requirement.

SQLite missing-db classifier includes both:

1. `unable to open database file`
2. `cannot open database file`

## 7.7 Scratch (`infra/scratch/*`)

Managers:

1. SQLite scratch.
2. Containerized scratch (postgres/mysql/clickhouse).
3. BigQuery scratch.

API rules:

1. Caller supplies `scratch_name` + `purpose`.
2. Names: deterministic phase prefix + run nonce.
3. Return `ScratchHandle` with `auto_provisioned` and cleanup requirement.

BigQuery rules:

1. Explicit scratch base URL required.
2. Preserve scheme/host/query/fragment; rewrite only dataset segment.
3. No wait step.
4. Drop scratch dataset on cleanup unless keep.

## 7.8 Artifact Store (`infra/artifact_store.py`)

Purpose: crash-consistent multi-file apply for `schema apply`.

Storage:

1. `.matey/tx.db` (sqlite journal)
2. `.matey/tx/<txid>/staged/...`
3. `.matey/tx/<txid>/backup/...`

State machine:

1. `prepared`
2. `applying`
3. `committed`

Recovery contract:

1. `recover_pending(target_key, target_root)` runs before command logic via command scope.
2. `prepared` or `applying` => rollback to pre-apply.
3. `committed` => cleanup finalize only.
4. Recovery must be idempotent.

Durability ordering:

1. Persist staged + backups.
2. Fsync staged/backup and directories.
3. Persist journal row `prepared`.
4. Transition to `applying`.
5. Perform file mutations.
6. Fsync affected paths.
7. Transition to `committed`.
8. Finalize cleanup.

Guarantee:

- Application-level crash consistency (recover to fully pre-apply or fully post-apply), not OS-level multi-file atomic rename.

## 7.9 Locking + Command Scope (`infra/locking.py`, `app/scope.py`)

1. Use `portalocker` backend.
2. One lock key per target path.
3. Reentrant in-process depth tracked by lock manager.
4. `ICommandScope.open(...)`:
   - acquires lock
   - runs artifact-store recovery prelude
   - yields
   - releases lock

No command may manage lock/recovery directly.

---

## 8. Application Kernel and Context

`app/context.py` wires all protocols.
`app/kernel.py` builds context without globals.

```python
@dataclass(frozen=True)
class AppContext:
    fs: IFileSystem
    proc: IProcessRunner
    env: IEnvProvider
    git: IGitRepo
    dbmate: IDbmateGateway
    sql_pipeline: ISqlPipeline
    engine_policies: IEnginePolicyRegistry
    scratch: IScratchManager
    artifact_store: IArtifactStore
    scope: ICommandScope
```

---

## 9. Shared Runtime Rules

1. Schema commands read head from current worktree.
2. Base comes from merge-base commit content via git blob/tree APIs.
3. Base-ref resolution order:
   - explicit `--base`
   - `GITHUB_BASE_REF`
   - `CI_MERGE_REQUEST_TARGET_BRANCH_NAME`
   - `BUILDKITE_PULL_REQUEST_BASE_BRANCH`
   - configured default base ref
   - `refs/remotes/origin/HEAD`
   - `origin/main`
   - `origin/master`
4. Stacked branch flows are explicit: pass `--base` (or CI target branch env).
5. Fail-closed base guard (fallback-derived base only):
   - if merge-base equals `HEAD`, branch is not trunk, and no local target-file changes, fail with explicit guidance to pass `--base`.
6. Engine inference for schema replay:
   - non-empty CLI `--test-url`
   - non-empty CLI `--url`
   - non-empty resolved value of `test_url_env`
   - non-empty resolved value of `url_env`
   - lockfile engine if lock exists and parses
   - else fail
7. If both URL-derived engine and lock engine exist, mismatch is typed error.
8. Lockfile is derived artifact written only by `schema apply`.
9. `--clean` is manual-only; never implicit/automatic.

---

## 10. Command Semantics

## 10.1 `matey schema status`

Goal: artifact health only (engine inference free).

Algorithm:

1. Enter `ICommandScope` for target.
2. Build `SchemaOpContext` once for this command.
3. Enumerate workspace migrations deterministically.
4. Parse lockfile if present.
5. Evaluate shape/integrity.
6. Build expected lock view from workspace migrations + deterministic checkpoint mapping.
7. Compute row-level states:
   - `new-in-worktree`
   - `migration-changed`
   - `checkpoint-missing`
   - `checkpoint-changed`
   - `chain-mismatch`
   - `orphan-checkpoint`
8. Result:
   - `up-to-date` when lock exists, valid, and matches expected lock bytes.
   - `stale` otherwise when evaluable.
   - `error` on parse/config/runtime failure.

Output:

1. Marker-first rows plus summary (style-aligned with db status format, but schema semantics).
2. No engine operations.

Exit:

1. `0` up-to-date
2. `1` stale or error

## 10.2 `matey schema plan [--clean]`

Goal: read-only A vs B replay + down-roundtrip.

Definitions:

1. `A`: workspace `schema.sql` prepared via SQL pipeline as artifact source.
2. `B`: replay scratch dump prepared via SQL pipeline as scratch dump source.

Algorithm:

1. Enter `ICommandScope`.
2. Build `SchemaOpContext` once for this command.
3. Load head migrations/checkpoints/schema from workspace.
4. Head coherence checks:
   - migration chain parseable + digestible.
   - existing checkpoints must map to head migrations.
   - orphan checkpoints recorded as diagnostics.
   - missing head tail checkpoints do not fail plan.
5. Build replay plan:
   - `--clean`: no base reads, divergence=1, empty anchor.
   - default:
     1. resolve base ref
     2. read base migrations/checkpoints/schema from merge-base
     3. enforce base coherence matrix:
        - case A: no migrations + no checkpoints + no schema file => valid empty base (`divergence=1`, empty anchor)
        - case B: schema-only (schema file exists, migrations/checkpoints absent) => fail (`ReplayError`)
        - case C: checkpoints exist while migrations absent => fail (`ReplayError`)
        - case D: migrations exist:
          - all migrations must be parseable and digestible from git blob bytes
          - deterministic checkpoint must exist for every base migration (`checkpoints/<migration_stem>.sql`)
          - extra checkpoints not mapped to a base migration are invalid and fail
          - base schema file is informational only and is not used as replay anchor
     4. compute divergence from base/head migration file+digest sequences
     5. anchor at base deterministic checkpoint for `divergence-1` when divergence>1
6. Resolve replay engine using shared rule 9.6.
7. Replay scratch:
   - prepare/create/wait if policy says wait
   - load anchor when present
   - replay tail migrations
   - dump schema (`B_raw`)
8. Compare using `ISqlPipeline.compare`:
   - expected: `SqlSource(origin="artifact", text=A_text)`
   - actual: `SqlSource(origin="scratch_dump", text=B_raw, context_url=scratch_url)`
9. Down-roundtrip scratch (separate scratch):
   - initialize from same anchor
   - stepwise single-migration temp dirs
   - for each tail step:
     1. detect executable down SQL
     2. if executable: baseline dump -> apply one -> rollback one -> compare baseline vs rollback dump -> reapply one
     3. if not executable: apply one and continue
10. Plan gates:
   - fail on replay errors
   - fail on down-roundtrip errors
   - do not fail solely for orphan checkpoints
   - do not fail solely for missing tail checkpoints

## 10.3 `matey schema plan` subcommands

1. `schema plan` summary:
   - print replay/down summary + `A==B` state
   - exit non-zero when `A!=B`
2. `schema plan diff`:
   - print same comparison diff
   - exit non-zero when `A!=B`
3. `schema plan sql`:
   - print normalized `B`
   - exit zero if replay/down gates pass

## 10.4 `matey schema apply [--clean]`

Goal: authoritative artifact mutation from plan result.

Algorithm:

1. Enter `ICommandScope`.
2. Build `SchemaOpContext` once for this command.
3. Run full `schema plan` pipeline in require-held mode.
4. Failure gates:
   - fail on replay/down errors
   - do not fail on `A!=B`
5. Checkpoint capture:
   - default mode:
     1. unchanged-prefix checkpoints can be reused only with lock metadata integrity verification
     2. tail checkpoints captured from checkpoint scratch progression
     3. metadata missing/unusable or prefix mismatch => `CheckpointIntegrityError`, require `--clean`
   - `--clean` mode:
     1. regenerate full checkpoint chain
6. Build write set:
   - `schema.sql` from normalized `B`
   - deterministic checkpoint files
   - rebuilt lockfile
   - orphan checkpoint delete set
7. Execute artifact transaction:
   - `begin -> apply -> finalize`

No `--force`.

## 10.5 `matey db up|migrate|down`

Goal: guarded live mutations with pre/post schema checks.

Algorithm:

1. Enter `ICommandScope`.
2. Build `DbOpContext` once for this command.
3. Require `schema status = up-to-date` before mutation.
4. Resolve live URL (`--url` > resolved `url_env` value).
5. Resolve live engine from live URL.
6. If lock engine exists and mismatches live engine, fail typed.
7. Run preflight by verb using:
   - typed `IDbmateGateway` methods (`status/create/up/migrate/rollback/dump/load`)
   - `domain/dbmate_output.py` for status/dump parsing
   - `EnginePolicy.classifier` tables for missing-db and create outcome classification
   - `up`:
     1. non-bigquery: run status; if missing-db classifier positive, run create then rerun status
     2. bigquery: run create first and classify outcome:
        - `ok`/`exists` => continue
        - `fatal` => fail typed
        then run status
   - `migrate`: status only; no create-if-needed
   - `down`: status only
8. Parse status and require lock-prefix match.
9. Precheck compare:
   - if index>0: live dump vs checkpoint at current index via SQL pipeline compare
   - if index=0: compare live dump vs engine empty-baseline scratch dump
     - bigquery requires non-empty `--test-url` or resolved `test_url_env` value
10. Execute mutation:
   - `up -> dbmate up`
   - `migrate -> dbmate migrate`
   - `down [N] -> dbmate rollback N` (`N>0`, default 1)
11. Post status parse + expected post index gate.
12. Postcheck compare using same rules as precheck at resulting index.

## 10.6 `matey db drift|plan`

1. `db drift`:
   - build/reuse `DbOpContext`
   - require schema status up-to-date
   - parse live status and prefix gate
   - compare live dump vs expected schema at current live index
   - fail on diff
2. `db plan`:
   - build/reuse `DbOpContext`
   - require schema status up-to-date
   - parse live status and prefix gate
   - compare live dump vs head expected schema
   - fail on diff
3. `db plan diff`:
   - same compute path as `db plan`, render diff
4. `db plan sql`:
   - offline artifact output only (no live connection/status)
   - require schema status up-to-date
   - failure semantics:
     - if schema status is `stale`, fail typed `SchemaMismatchError` (artifacts not ready)
     - if schema status is `error`, fail with underlying typed status evaluation error (`LockfileError`/`ConfigError`/`ReplayError`)

## 10.7 Remaining `matey db` commands

1. `db new`: workspace migration file creation only.
2. `db create`: direct create.
3. `db wait`: direct wait.
4. `db status`: direct passthrough output.
5. `db load`: direct load.
6. `db dump`: direct dump.
7. `db drop`: direct drop.
8. `dbmate` passthrough: direct forward with lock scope, no policy checks.

---

## 11. Dbmate Output + Classifier Contracts

Parser (`domain/dbmate_output.py`):

1. Parse status rows: `^\[(?P<mark>[ X])\]\s+(?P<file>.+?)\s*$`.
2. Parse optional `applied: <int>` summary.
3. Mark `X` rows define applied sequence.
4. Summary count mismatch => parse error.
5. Applied sequence must be exact lock prefix for guarded flows.
6. Guarded command engines must use `parse_status_output(...)`; ad-hoc parsing is forbidden.
7. Guarded dump extraction must use `extract_dump_sql(...)`; ad-hoc extraction is forbidden.

Classifier (`EnginePolicy.classifier` tables):

1. Input to classifier evaluation is normalized lowercase command detail text (`stderr` fallback `stdout`).
2. Missing-db positives table:
   - postgres: `does not exist`, `3d000`
   - mysql: `unknown database`, `1049`
   - clickhouse: `database ... does not exist`, `code: 81`
   - sqlite: `unable to open database file`, `cannot open database file`, `no such file or directory`
3. Missing-db negatives table:
   - `connection refused`, `i/o timeout`, `no such host`
   - auth failures, permission denied, quota/invalid-request style errors
4. Unknown errors default to non-missing (fail closed).
5. BigQuery never uses status-based missing-db create-if-needed logic.
6. BigQuery preflight-create tables (for `db up`):
   - `create_exists`: `already exists`, HTTP/gRPC duplicate-already-exists equivalents
   - `create_fatal`: `permission denied`, `access denied`, `project ... not found`, location mismatch errors, quota/rate-limit exceeded, invalid/bad request
7. All signatures must be backed by captured fixture text in classifier unit tests.

---

## 12. Lockfile Format (v0)

```toml
lock_version = 0
hash_algorithm = "blake2b-256"
canonicalizer = "matey-sql-v0"
engine = "postgres"
target = "core"
schema_file = "schema.sql"
migrations_dir = "migrations"
checkpoints_dir = "checkpoints"
head_index = 3
head_chain_hash = "..."
head_schema_digest = "..."

[[steps]]
index = 1
version = "202601010101"
migration_file = "migrations/202601010101_create_widgets.sql"
migration_digest = "..."
checkpoint_file = "checkpoints/202601010101_create_widgets.sql"
checkpoint_digest = "..."
schema_digest = "..."
chain_hash = "..."
```

Rules:

1. Paths are target-relative.
2. No absolute paths.
3. No path traversal.
4. One step per migration.

---

## 13. Config Resolution Semantics

Precedence:

1. CLI flags
2. `matey.toml`
3. `pyproject.toml [tool.matey]`
4. defaults

Target selection:

1. `--target NAME` for single target
2. `--all` for configured target set
3. otherwise:
   - if one configured target => use it
   - else typed selection error

---

## 14. CLI Surface and Ordering

Root groups:

1. `db`
2. `schema`
3. `config`
4. `ci`

`schema`:

1. `status`
2. `plan`
3. `apply`

`schema plan`:

1. default summary
2. `diff`
3. `sql`

`db`:

1. `new`
2. `create`
3. `wait`
4. `up`
5. `migrate`
6. `status`
7. `drift`
8. `plan`
9. `load`
10. `dump`
11. `down`
12. `drop`
13. `dbmate`

`db plan`:

1. default summary
2. `diff`
3. `sql`

Help constraints:

1. No runoff paragraph blobs.
2. Root help includes subgroup command lists.
3. Group text generated from single metadata source.

---

## 15. Concurrency and Safety

1. Every command runs inside `ICommandScope`.
2. `ICommandScope` performs lock + artifact recovery prelude exactly once.
3. Reentrant lock semantics are internal to lock manager.
4. Commands never call artifact recovery directly.
5. `ICommandScope` must validate `target_key <-> target_root` identity before lock/recovery:
   - derive canonical key from `target_root`
   - if derived key != provided key, fail typed `TargetIdentityError`
   - no lock acquisition or recovery side effects may run on identity mismatch

---

## 16. Error Taxonomy and Exit Semantics

Typed errors (`domain/errors.py`):

1. `MateyError`
2. `ConfigError`
3. `TargetSelectionError`
4. `LockfileError`
5. `ReplayError`
6. `SchemaMismatchError`
7. `LiveDriftError`
8. `LiveHistoryMismatchError`
9. `ArtifactTransactionError`
10. `ArtifactRecoveryError`
11. `BigQueryPreflightError`
12. `CheckpointIntegrityError`
13. `EngineInferenceError`
14. `TargetIdentityError`
15. `ExternalCommandError`
16. `CliUsageError`

Exit mapping:

1. `CliUsageError -> 2`
2. other `MateyError -> 1`
3. unexpected -> `70`

---

## 17. Testing Strategy

## 17.1 Unit

`domain`:

1. migration parsing
2. lock shape/path invariants
3. chain digest determinism
4. target key determinism
5. dbmate status parser grammar + applied summary consistency
6. dbmate dump/status extraction error handling

`app`:

1. schema status stale reason matrix
2. schema plan divergence/anchor resolution
3. clean-mode short-circuit of base reads
4. down stepwise roundtrip semantics
5. db mutation pre/post checks
6. index-0 baseline behavior by engine
7. fail-closed base guard + local-change bypass
8. schema status gate before db mutation
9. db plan prefix gate before schema compare
10. base coherence matrix cases (A/B/C/D) are enforced exactly
11. `db plan sql` fails with typed stale/error semantics without touching live DB
12. `SchemaOpContext`/`DbOpContext` construction and propagation (no argument fanout bypass)

`infra`:

1. git status semantics for selected rel paths
2. sql pipeline prepare/compare behavior across origins
3. engine policy classifiers (including sqlite phrase pair)
4. bigquery preflight-create classifier examples (`exists` vs `fatal`) from fixtures
5. artifact store state machine + recovery idempotency
6. dbmate gateway method argv building is deterministic and routed through shared private `_run_dbmate(...)`

`cli`:

1. command wiring
2. option mapping
3. exit mapping
4. help ordering/metadata generation

## 17.2 Integration

Schema:

1. postgres/mysql/clickhouse/sqlite replay + apply
2. bigquery replay/apply (env-gated)
3. schema apply crash recovery (`prepared`, `applying`)
4. plan tolerates missing tail checkpoints/orphans
5. apply regenerates/prunes deterministically

DB:

1. up/migrate/down pre/post checks
2. create-if-needed classifier behavior
3. drift at current index
4. plan vs head
5. plan sql offline behavior
6. bigquery preflight create outcomes (`ok`, `exists`, `fatal`)

---

## 18. Build/Packaging Constraints

1. Build hooks do not import runtime `src/matey` modules.
2. Bundled dbmate remains under `src/matey/_vendor/dbmate/**`.
3. Wheel/sdist contain runtime-required artifacts only.

---

## 19. Implementation Slices

## Slice 1: Domain foundation

1. `constants`, `target`, `engine`, `sql`, `dbmate_output`, `migration`, `lockfile`, `digest`, `config`, `plan`, `result`, `errors`
2. unit tests green

## Slice 2: Infra primitives

1. `env`, `fs`, `proc`, `git`, `dbmate`, `sql_pipeline`, `engine_policy`, `scratch`, `artifact_store`, `locking`
2. infra unit tests green

## Slice 3: App scope + schema engine

1. `scope`, `schema_engine`
2. schema command semantics green

## Slice 4: App db engine

1. `db_engine`
2. db guarded semantics green

## Slice 5: CLI wiring

1. cli groups + rendering + help metadata
2. CLI tests green

## Slice 6: Integration + packaging hardening

1. integration matrix
2. wheel/sdist smoke

---

## 20. Elegance Review Checklist

1. One public API per concept.
2. No command duplicates lock/recovery logic.
3. No command duplicates compare/normalize logic.
4. No engine-specific branching outside policy registry.
5. No stringly target identity in locking/tx paths.
6. No CLI/business logic coupling.
7. Deterministic outputs across repeated runs.
8. No ad-hoc dbmate output parsing in app/infra command flows.

---

## 21. Explicitly Forbidden

1. Legacy interface reuse.
2. Public helper proliferation for SQL normalization.
3. Ad-hoc `if engine == ...` in app commands.
4. App-layer direct file mutation during schema apply.
5. Direct `os.environ` reads outside env ingress.
6. Ad-hoc status/dump parsing outside `domain/dbmate_output.py`.

---

## 22. Refinement Invariants

The transport/parser/context refinement in this plan is structural only:

1. No CLI surface changes.
2. No command behavior changes.
3. No exit code mapping changes.
4. No lock/checkpoint algorithm changes.
5. No replay/diff semantics changes.

---

## 23. Immediate Next Step

Implement Slice 1 exactly as specified and block all further work until domain tests pass.
