# matey

`matey` is an opinionated CLI wrapper around `dbmate` for repeatable migrations and schema safety.

This scaffold bundles a compiled `dbmate` binary into the wheel at build time.

## Local bootstrap

1. Install dev environment:

   ```bash
   pixi install
   ```

2. Build bundled `dbmate` + wheel:

   ```bash
   pixi run build-wheel
   ```

3. Run CLI:

   ```bash
   pixi run python -m matey --version
   ```

## Build configuration

The build hook lives at `build_hooks/build_dbmate.py`.

- Default source strategy is `go install` from:
  - module: `github.com/amacneil/dbmate/v2`
  - version: `v2.31.0`
- License collection runs during dbmate build using:
  - `github.com/google/go-licenses/v2@v2.0.1`
  - policy check output is always written to `go-licenses-check.txt`
  - save command output is always written to `go-licenses-save.txt`
  - strict policy failure is opt-in via `MATEY_GO_LICENSES_ENFORCE=true`
- Override with environment variables:
  - `MATEY_DBMATE_SOURCE=vendor|go-install`
  - `MATEY_DBMATE_MODULE=...`
  - `MATEY_DBMATE_VERSION=...`
  - `MATEY_DBMATE_CGO_ENABLED=1|0` (default `1`; set `0` for pure-Go fallback)
  - `MATEY_GO_LICENSES_MODULE=...`
  - `MATEY_GO_LICENSES_VERSION=...`
  - `MATEY_GO_LICENSES_DISALLOWED_TYPES=...`
  - `MATEY_GO_LICENSES_ENFORCE=true|false` (default `false`)
  - `MATEY_DBMATE_BIN=...` (runtime override for execution)

If you vendor dbmate source under `vendor/dbmate`, set:

```bash
MATEY_DBMATE_SOURCE=vendor
```

Per platform wheel build, third-party notices are written under:

```text
src/matey/_vendor/dbmate/<goos>-<goarch>/THIRD_PARTY_LICENSES/
```
