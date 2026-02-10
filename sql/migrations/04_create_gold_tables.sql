CREATE TABLE IF NOT EXISTS gold.dim_user (
    user_key        SERIAL PRIMARY KEY,
    user_id         INTEGER UNIQUE,
    email           TEXT,
    username        TEXT,
    first_name      TEXT,
    last_name       TEXT,
    city            TEXT
);

CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_key     SERIAL PRIMARY KEY,
    product_id      INTEGER UNIQUE,
    title           TEXT,
    category        TEXT,
    price           NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key        DATE PRIMARY KEY,
    year            INTEGER,
    month           INTEGER,
    day             INTEGER
);

-- =====================
-- Fact
-- =====================

CREATE TABLE IF NOT EXISTS gold.fact_sales (
    sale_id         SERIAL PRIMARY KEY,
    user_key        INTEGER REFERENCES gold.dim_user(user_key),
    product_key     INTEGER REFERENCES gold.dim_product(product_key),
    date_key        DATE REFERENCES gold.dim_date(date_key),
    quantity        INTEGER,
    unit_price     NUMERIC(10,2),
    total_amount   NUMERIC(10,2)
);
