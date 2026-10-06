CREATE TABLE IF NOT EXISTS public.api_ingestion_checkpoints (
    dataset_id VARCHAR(255) PRIMARY KEY,
    strategy_type VARCHAR(50) NOT NULL,
    checkpoint_payload JSONB NOT NULL,
    version BIGINT NOT NULL,
    last_batch_id VARCHAR(255),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT api_ingestion_checkpoints_dataset_id_nonempty
        CHECK (btrim(dataset_id) <> ''),

    CONSTRAINT api_ingestion_checkpoints_strategy_type_nonempty
        CHECK (btrim(strategy_type) <> ''),

    CONSTRAINT api_ingestion_checkpoints_payload_object
        CHECK (jsonb_typeof(checkpoint_payload) = 'object'),

    CONSTRAINT api_ingestion_checkpoints_version_positive
        CHECK (version >= 1)
);
