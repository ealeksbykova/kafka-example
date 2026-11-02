CREATE TABLE processed_order_events
(
    -- Business primary key: the order id from the event. Redelivery of the same order is a no-op.
    order_id     UUID PRIMARY KEY,
    -- Producer generated id of the event; kept for tracing and troubleshooting.
    event_id     UUID           NOT NULL,
    customer_id  VARCHAR(64)    NOT NULL,
    product      VARCHAR(255)   NOT NULL,
    quantity     INTEGER        NOT NULL,
    total_price  NUMERIC(19, 2) NOT NULL,
    currency     VARCHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    occurred_at  TIMESTAMPTZ    NOT NULL,
    processed_at TIMESTAMPTZ    NOT NULL
);

-- A given event must never create two rows, even if the order id changed (defensive).
CREATE UNIQUE INDEX uq_processed_order_events_event_id ON processed_order_events (event_id);
CREATE INDEX idx_processed_order_events_customer ON processed_order_events (customer_id);

-- Records that could not be processed, so a poison message is inspectable instead of lost.
CREATE TABLE dead_letter_events
(
    id           BIGSERIAL PRIMARY KEY,
    topic        VARCHAR(255) NOT NULL,
    record_key   VARCHAR(255),
    payload      TEXT,
    reason       TEXT,
    partition    INTEGER,
    record_offset BIGINT,
    received_at  TIMESTAMPTZ  NOT NULL
);

-- Counters per processing outcome; used by the REST API, the e2e test and dashboards.
CREATE TABLE consumer_processing_stats
(
    outcome      VARCHAR(32) PRIMARY KEY,
    occurrences  BIGINT      NOT NULL DEFAULT 0,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO consumer_processing_stats (outcome, occurrences)
VALUES ('CREATED', 0),
       ('DUPLICATE_IGNORED', 0),
       ('FAILED', 0);
