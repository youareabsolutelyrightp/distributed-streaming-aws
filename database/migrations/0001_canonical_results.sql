CREATE TABLE IF NOT EXISTS canonical_results (
    run_id text NOT NULL,
    output_id text NOT NULL,
    canonical_digest text NOT NULL,
    query_id text NOT NULL CHECK (query_id IN ('query1', 'query2')),
    grading_mode text NOT NULL,
    effective_time timestamptz NOT NULL,
    terminal boolean NOT NULL,
    result jsonb NOT NULL,
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    kafka_append_time timestamptz NOT NULL,
    serialization_started_epoch_us bigint,
    serialization_completed_epoch_us bigint,
    kafka_key_bytes integer,
    kafka_value_bytes integer,
    kafka_header_bytes integer,
    transport_trace jsonb,
    kafka_value_raw bytea,
    materialized_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, output_id)
);

ALTER TABLE canonical_results
    ADD COLUMN IF NOT EXISTS canonical_digest text;

UPDATE canonical_results
    SET canonical_digest = output_id
    WHERE canonical_digest IS NULL;

ALTER TABLE canonical_results
    ALTER COLUMN canonical_digest SET NOT NULL;

ALTER TABLE canonical_results
    ADD COLUMN IF NOT EXISTS serialization_started_epoch_us bigint;

ALTER TABLE canonical_results
    ADD COLUMN IF NOT EXISTS serialization_completed_epoch_us bigint;

ALTER TABLE canonical_results ADD COLUMN IF NOT EXISTS kafka_key_bytes integer;
ALTER TABLE canonical_results ADD COLUMN IF NOT EXISTS kafka_value_bytes integer;
ALTER TABLE canonical_results ADD COLUMN IF NOT EXISTS kafka_header_bytes integer;
ALTER TABLE canonical_results ADD COLUMN IF NOT EXISTS transport_trace jsonb;
ALTER TABLE canonical_results ADD COLUMN IF NOT EXISTS kafka_value_raw bytea;

CREATE INDEX IF NOT EXISTS canonical_results_run_query_time
    ON canonical_results (run_id, query_id, effective_time DESC);
