from __future__ import annotations

from matey.env import (
    DEFAULT_DBMATE_CGO_ENABLED,
    DEFAULT_DBMATE_MODULE,
    DEFAULT_DBMATE_SOURCE,
    DEFAULT_DBMATE_VERSION,
    DEFAULT_GO_LICENSES_DISALLOWED_TYPES,
    DEFAULT_GO_LICENSES_ENFORCE,
    DEFAULT_GO_LICENSES_MODULE,
    DEFAULT_GO_LICENSES_VERSION,
    load_build_env,
    load_runtime_env,
)


def test_load_runtime_env_defaults() -> None:
    env = load_runtime_env(environ={})
    assert env.dbmate_bin is None
    assert env.dbmate_wait_timeout == "45s"
    assert env.github_base_ref is None
    assert env.gitlab_base_ref is None
    assert env.buildkite_base_ref is None


def test_load_runtime_env_parses_and_trims_values() -> None:
    env = load_runtime_env(
        environ={
            "MATEY_DBMATE_BIN": " /tmp/dbmate ",
            "MATEY_DBMATE_WAIT_TIMEOUT": " 90s ",
            "GITHUB_BASE_REF": " main ",
            "CI_MERGE_REQUEST_TARGET_BRANCH_NAME": " release ",
            "BUILDKITE_PULL_REQUEST_BASE_BRANCH": " trunk ",
        }
    )
    assert env.dbmate_bin == "/tmp/dbmate"
    assert env.dbmate_wait_timeout == "90s"
    assert env.github_base_ref == "main"
    assert env.gitlab_base_ref == "release"
    assert env.buildkite_base_ref == "trunk"


def test_load_build_env_defaults() -> None:
    env = load_build_env(environ={})
    assert env.dbmate_source == DEFAULT_DBMATE_SOURCE
    assert env.dbmate_module == DEFAULT_DBMATE_MODULE
    assert env.dbmate_version == DEFAULT_DBMATE_VERSION
    assert env.dbmate_cgo_enabled == DEFAULT_DBMATE_CGO_ENABLED
    assert env.go_licenses_module == DEFAULT_GO_LICENSES_MODULE
    assert env.go_licenses_version == DEFAULT_GO_LICENSES_VERSION
    assert env.go_licenses_disallowed_types == DEFAULT_GO_LICENSES_DISALLOWED_TYPES
    assert env.go_licenses_enforce is DEFAULT_GO_LICENSES_ENFORCE


def test_load_build_env_parses_and_trims_values() -> None:
    env = load_build_env(
        environ={
            "MATEY_DBMATE_SOURCE": " vendor ",
            "MATEY_DBMATE_MODULE": " github.com/example/dbmate ",
            "MATEY_DBMATE_VERSION": " v2.99.1 ",
            "MATEY_DBMATE_CGO_ENABLED": " 0 ",
            "MATEY_GO_LICENSES_MODULE": " github.com/example/go-licenses ",
            "MATEY_GO_LICENSES_VERSION": " v3.1.0 ",
            "MATEY_GO_LICENSES_DISALLOWED_TYPES": " forbidden,unknown ",
            "MATEY_GO_LICENSES_ENFORCE": " true ",
        }
    )
    assert env.dbmate_source == "vendor"
    assert env.dbmate_module == "github.com/example/dbmate"
    assert env.dbmate_version == "v2.99.1"
    assert env.dbmate_cgo_enabled == "0"
    assert env.go_licenses_module == "github.com/example/go-licenses"
    assert env.go_licenses_version == "v3.1.0"
    assert env.go_licenses_disallowed_types == "forbidden,unknown"
    assert env.go_licenses_enforce is True
