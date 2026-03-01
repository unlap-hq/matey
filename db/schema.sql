
--
-- Database schema
--

CREATE DATABASE IF NOT EXISTS matey_core_checkpoint_6aca83d7e6;

CREATE TABLE matey_core_checkpoint_6aca83d7e6.schema_migrations
(
    `version` String,
    `ts` DateTime DEFAULT now(),
    `applied` UInt8 DEFAULT 1
)
ENGINE = ReplacingMergeTree(ts)
PRIMARY KEY version
ORDER BY version
SETTINGS index_granularity = 8192;

CREATE TABLE matey_core_checkpoint_6aca83d7e6.t
(
    `id` Int32
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;


--
-- Dbmate schema migrations
--

INSERT INTO matey_core_checkpoint_6aca83d7e6.schema_migrations (version) VALUES
    ('001');
