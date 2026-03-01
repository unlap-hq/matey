from __future__ import annotations

from matey.app.protocols import ScratchHandle
from matey.domain.engine import Engine
from matey.domain.errors import ReplayError


class BigQueryScratchManager:
    def prepare(
        self,
        *,
        scratch_name: str,
        purpose: str,
        test_base_url: str | None,
        build_scratch_url,
    ) -> ScratchHandle:
        if not test_base_url or not test_base_url.strip():
            raise ReplayError(
                "BigQuery scratch requires a non-empty --test-url or resolved test_url_env value."
            )

        url = build_scratch_url(test_base_url.strip(), scratch_name)
        return ScratchHandle(
            engine=Engine.BIGQUERY,
            url=url,
            scratch_name=scratch_name,
            purpose=purpose,
            auto_provisioned=False,
            cleanup_required=True,
        )

    def cleanup(self, handle: ScratchHandle) -> None:
        # BigQuery dataset cleanup is handled by dbmate drop in command engines.
        return
