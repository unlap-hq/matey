# matey

[![Checks](https://github.com/unlap-hq/matey/actions/workflows/checks.yml/badge.svg)](https://github.com/unlap-hq/matey/actions/workflows/checks.yml)
[![PyPI version](https://img.shields.io/pypi/v/matey.svg)](https://pypi.org/project/matey/)
[![Python versions](https://img.shields.io/pypi/pyversions/matey.svg)](https://pypi.org/project/matey/)
[![License](https://img.shields.io/pypi/l/matey.svg)](LICENSE)

`matey` is a database migration system for teams that want more than “run some SQL files”.

Install:

```bash
pip install matey
```

It keeps schema artifacts in git, validates migration replay in scratch databases, bootstraps empty databases to head, manages live data as explicit artifacts, and can generate SQLAlchemy models from the validated schema.

## Supported engines

Current engine set:

- `sqlite`
- `postgres`
- `mysql`
- `clickhouse`
- `bigquery`
- `bigquery-emulator`

Notes:

- `bigquery-emulator` is supported, but it has narrower semantics than real BigQuery.

## Feature matrix

| Feature | matey | dbmate | Flyway | Liquibase | Alembic |
|---|---:|---:|---:|---:|---:|
| SQL-file migrations | Yes | Yes | Yes | Yes | Partial |
| Repo-native multi-target workspace | Yes | No | No | No | No |
| Validated replay in scratch DBs | Yes | No | No | No | No |
| Canonical committed `schema.sql` | Yes | Yes | No | No | No |
| Verified intermediate schema states | Yes | No | No | No | No |
| Fast bootstrap without replay | Yes | No | Partial | Partial | No |
| Live data artifact workflow | Yes | No | No | Partial | No |
| SQLAlchemy codegen from validated schema | Yes | No | No | No | Yes |
| BigQuery support | Yes | Yes | Yes | Yes | No |
| BigQuery emulator support | Yes | Partial | No | No | No |
| ClickHouse support | Yes | Yes | Yes | Partial | No |
| Migration autogeneration | No | No | No | No | Partial |

## How matey works

Matey has two levels of configuration:

- a **workspace config** at the repo root
- a **target config** inside each database target directory

A target is just a path in your repo. There are no logical target names to map back to directories.

A typical workflow looks like this:

1. write a migration under `migrations/`
2. run `matey schema apply`
3. matey replays the migration chain into a scratch database
4. if replay succeeds, matey writes schema artifacts (`schema.sql`, `schema.lock.toml`, checkpoints)
5. if codegen is enabled, matey also writes `models.py`
6. later, `matey db ...` commands compare live database state against the **current worktree target state**

That scratch replay step is the main difference from a plain migration runner.

## Scratch replay

`matey schema plan` and `matey schema apply` do not just trust the migration files. They:

1. create or lease a scratch database
2. load the best available starting point (zero, checkpoint, or bootstrap state)
3. replay the current migration chain into that scratch database
4. dump and compare the resulting schema
5. write artifacts only after replay succeeds

This is why matey can detect problems like:

- broken migration ordering
- incompatible checkpoint/schema artifacts
- drift between committed artifacts and actual replay behavior
- backend-specific issues that only show up when SQL is really executed

Useful flags:

- `--test-url` points scratch at an explicit test database base
- `--clean` forces replay from empty
- `--keep-scratch` leaves the scratch DB behind for debugging

## Workspace and target layout

### Workspace config

The workspace config lives at the repo root in either:

- `matey.toml`
- or `pyproject.toml` under `[tool.matey]`

It is just an explicit list of target paths:

```toml
targets = [
  ".",
  "db/core",
  "services/analytics/db",
]
```

### Target layout

Each target directory contains its own database artifacts and config.

```text
<target>/
  config.toml
  schema.sql
  schema.lock.toml
  migrations/
  checkpoints/
  data/
    data.toml
    *.jsonl
  models.py
```

Target-local config lives in `config.toml`.

```toml
engine = "postgres"
url_env = "CORE_DATABASE_URL"
test_url_env = "CORE_TEST_DATABASE_URL"

[codegen]
enabled = true
generator = "tables"
#  options = "..."
```

## Quickstart

### 1. Install matey

```bash
pip install matey
```

### 2. Initialize the current directory as a target

```bash
matey init \
  --engine postgres \
  --url-env DATABASE_URL \
  --test-url-env TEST_DATABASE_URL
```

That will:

- create or update the workspace `matey.toml`
- add the current directory as a target (`"."`)
- write `config.toml`
- initialize:
  - `schema.sql`
  - `schema.lock.toml`
  - `migrations/`
  - `checkpoints/`
  - `data/`

To also scaffold CI in the resolved workspace root:

```bash
matey init \
  --engine postgres \
  --url-env DATABASE_URL \
  --test-url-env TEST_DATABASE_URL \
  --ci github
```

### 3. Create a migration

```bash
matey db new "create users"
```

### 4. Validate replay and write schema artifacts

```bash
matey schema apply
```

### 5. Apply migrations to a live database

```bash
matey db up
```

## Everyday workflows

### Start a new target

Use this when you are setting up a database target for the first time.

```bash
matey init \
  --path db/core \
  --engine postgres \
  --url-env CORE_DATABASE_URL \
  --test-url-env CORE_TEST_DATABASE_URL \
  --ci github

matey db new --path db/core "create users"
matey schema apply --path db/core
```

Result:
- the target directory is initialized
- the workspace target list is updated
- optional CI is written at the workspace root when `--ci` is provided
- a migration file exists
- schema artifacts are regenerated from validated replay
- `models.py` is generated if codegen is enabled

### Move schema forward safely

Use this when you have added or edited migrations and want to update both repo artifacts and a live database.

```bash
matey schema apply --path db/core
matey db up --path db/core
```

Result:
- `schema apply` replays the migration chain in scratch and refreshes artifacts
- `db up` applies pending migrations to the live database

### Inspect before changing anything

Use this when you want to understand what matey thinks will happen.

```bash
matey lint --all
matey schema plan --path db/core
matey db plan --path db/core
```

Use `--sql` or `--diff` when you want the replayed schema text or a diff instead of a summary.

### Bootstrap a new database to head

Use this when you need to bring up an empty database quickly without replaying the entire migration chain live.

```bash
matey db bootstrap --path db/core
```

Result:
- matey loads the committed head schema into the empty database
- verifies `dbmate status`
- verifies the resulting live schema against the current worktree head

### Roll back and verify

Use this when you want to step backward and still keep matey’s safety checks.

```bash
matey db down --path db/core --steps [N]
```

Result:
- matey rolls back the requested migrations
- verifies the resulting live schema against the expected worktree state
- can verify all the way back to the explicit zero baseline

## Live data

Matey keeps live data separate from schema migrations.

Why:
- schema migrations change structure
- data files describe explicit current-state table contents
- data workflows stay easier to reason about than data hidden inside migration history

Each target can contain:

```text
data/
  data.toml
  roles.jsonl
  permissions.jsonl
```

`data.toml` declares one or more named data sets.

Example:

```toml
[core]
files = [
  { name = "roles", table = "roles", mode = "replace", order_by = ["id"] },
  { name = "permissions", table = "permissions", mode = "upsert", on = ["id"], order_by = ["id"] },
]

[demo]
files = [
  { name = "roles", table = "roles", mode = "replace", order_by = ["id"] },
  { name = "permissions", table = "permissions", mode = "upsert", on = ["id"], order_by = ["id"] },
  { name = "demo_users", table = "users", mode = "insert", order_by = ["id"] },
]
```

One file maps to one target table.

Supported data modes:
- `replace`: target table contents become exactly the file contents
- `upsert`: insert new rows and update existing rows by the declared key columns
- `insert`: append rows only

Why `order_by` matters:
- export needs deterministic row ordering
- otherwise repeated exports can churn in git for no semantic reason

Data commands:

```bash
matey data export --path db/core --set core
matey data apply --path db/core --set core
```

Both commands require the live database to already match the current worktree head schema.

## SQLAlchemy codegen

Codegen is target-local and deterministic relative to the validated replay state.
It does **not** introspect the live production database.

Target-local config:

```toml
[codegen]
enabled = true
generator = "tables"
#  options = "..."
```

Generated file:

```text
<target>/models.py
```

That means `models.py` is always derived from the same validated scratch replay that produced the schema artifacts.
It is regenerated automatically by `schema apply` when `[codegen].enabled = true`.

## Non-goals

Matey intentionally does **not** try to be:

- Atlas-style migration autogeneration
- a declarative schema source-of-truth system
- a generic ETL/import/export framework

The source of truth remains:

- explicit SQL migrations
- committed schema artifacts
- explicit data manifests/files

## Developer notes

This project bundles a compiled `dbmate` binary into the wheel at build time.

For development on this repository, use:

```bash
pixi install
```

Build hook:
- `build_hooks/build_dbmate.py`

Useful environment variables:

- `MATEY_DBMATE_SOURCE=vendor|go-install`
- `MATEY_DBMATE_MODULE=...`
- `MATEY_DBMATE_VERSION=...`
- `MATEY_DBMATE_CGO_ENABLED=1|0`
- `MATEY_GO_LICENSES_MODULE=...`
- `MATEY_GO_LICENSES_VERSION=...`
- `MATEY_GO_LICENSES_DISALLOWED_TYPES=...`
- `MATEY_GO_LICENSES_ENFORCE=true|false`

Per-platform wheel build notices are written under:

```text
src/matey/_vendor/dbmate/<goos>-<goarch>/THIRD_PARTY_LICENSES/
```

## License

Apache-2.0. See [LICENSE](LICENSE).
