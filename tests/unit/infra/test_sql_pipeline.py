from matey.domain.engine import Engine
from matey.domain.sql import SqlSource
from matey.infra.sql_pipeline import SqlPipeline


def test_sql_pipeline_compare_equal_after_bigquery_dataset_normalization() -> None:
    pipeline = SqlPipeline()
    expected = SqlSource(text="SELECT * FROM `proj.__MATEY_DATASET__.t`\n", origin="artifact")
    actual = SqlSource(
        text="SELECT * FROM `proj.real_dataset.t`\n",
        origin="scratch_dump",
        context_url="bigquery://proj/us/real_dataset",
    )
    comparison = pipeline.compare(engine=Engine.BIGQUERY, expected=expected, actual=actual)
    assert comparison.equal is True
