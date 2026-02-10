CREATE TABLE IF NOT EXISTS raw.products (
    product_id      INTEGER PRIMARY KEY,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.users (
    user_id         INTEGER PRIMARY KEY,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.carts (
    cart_id         INTEGER PRIMARY KEY,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMP NOT NULL DEFAULT NOW()
);
