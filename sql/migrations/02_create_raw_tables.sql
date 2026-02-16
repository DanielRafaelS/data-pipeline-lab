CREATE TABLE IF NOT EXISTS raw.products (
    product_id      INTEGER PRIMARY KEY,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE raw.products IS 'Raw product data ingested from source API.';
COMMENT ON COLUMN raw.products.payload IS 'Full JSON payload returned by source API.';

CREATE INDEX IF NOT EXISTS idx_raw_products_ingested_at
    ON raw.products (ingested_at);


CREATE TABLE IF NOT EXISTS raw.users (
    user_id         INTEGER PRIMARY KEY,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE raw.users IS 'Raw user data ingested from source API.';
CREATE INDEX IF NOT EXISTS idx_raw_users_ingested_at
    ON raw.users (ingested_at);


CREATE TABLE IF NOT EXISTS raw.carts (
    cart_id         INTEGER PRIMARY KEY,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE raw.carts IS 'Raw cart data ingested from source API.';
CREATE INDEX IF NOT EXISTS idx_raw_carts_ingested_at
    ON raw.carts (ingested_at);
