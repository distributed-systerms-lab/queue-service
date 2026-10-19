CREATE TABLE IF NOT EXISTS processed_messages (
    queue_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    fingerprint BYTEA NOT NULL CHECK (octet_length(fingerprint) = 32),
    message_type TEXT NOT NULL,
    payload BYTEA NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (queue_name, message_id)
);
