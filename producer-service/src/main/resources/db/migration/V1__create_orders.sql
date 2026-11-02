CREATE TABLE orders
(
    id          UUID PRIMARY KEY,
    customer_id VARCHAR(64)  NOT NULL,
    product     VARCHAR(255) NOT NULL,
    quantity    INTEGER      NOT NULL CHECK (quantity > 0),
    total_price NUMERIC(19, 2) NOT NULL CHECK (total_price >= 0),
    currency    VARCHAR(3)   NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status      VARCHAR(20)  NOT NULL,
    event_id    UUID,
    created_at  TIMESTAMPTZ  NOT NULL,
    published_at TIMESTAMPTZ
);

-- Supports "my orders" style queries without scanning the table.
CREATE INDEX idx_orders_customer_created_at ON orders (customer_id, created_at DESC);

-- Same event must never be published twice; also used by the e2e test to look up the event id.
CREATE UNIQUE INDEX uq_orders_event_id ON orders (event_id) WHERE event_id IS NOT NULL;
