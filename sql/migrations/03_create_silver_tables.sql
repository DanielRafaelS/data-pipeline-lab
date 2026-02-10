CREATE TABLE IF NOT EXISTS silver.products (
    product_id      INTEGER PRIMARY KEY,
    title           TEXT,
    category        TEXT,
    price           NUMERIC(10,2),
    rating_rate     NUMERIC(3,2),
    rating_count    INTEGER,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.users (
    user_id         INTEGER PRIMARY KEY,
    email           TEXT,
    username        TEXT,
    first_name      TEXT,
    last_name       TEXT,
    city            TEXT,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.carts (
    cart_id         INTEGER PRIMARY KEY,
    user_id         INTEGER,
    cart_date       DATE,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.cart_items (
    cart_id         INTEGER,
    product_id      INTEGER,
    quantity        INTEGER,
    PRIMARY KEY (cart_id, product_id)
);
