-- GOLD LAYER - DIMENSIONAL MODEL

CREATE SCHEMA IF NOT EXISTS gold;

-- DIMENSION: USER (Type 1 SCD)
CREATE TABLE IF NOT EXISTS gold.dim_user (
    user_key        BIGSERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL UNIQUE,
    email           TEXT NOT NULL,
    username        TEXT NOT NULL,
    first_name      TEXT,
    last_name       TEXT,
    city            TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE gold.dim_user IS 'User dimension (Type 1 Slowly Changing Dimension).';

CREATE INDEX IF NOT EXISTS idx_dim_user_user_id
    ON gold.dim_user(user_id);



-- DIMENSION: PRODUCT (Type 1 SCD)
CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_key     BIGSERIAL PRIMARY KEY,
    product_id      INTEGER NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    category        TEXT NOT NULL,
    price           NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE gold.dim_product IS 'Product dimension (Type 1 Slowly Changing Dimension).';

CREATE INDEX IF NOT EXISTS idx_dim_product_product_id
    ON gold.dim_product(product_id);



-- DIMENSION: DATE (Calendar Dimension)
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key        DATE PRIMARY KEY,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    day             INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
    month_name      TEXT NOT NULL,
    quarter         INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE gold.dim_date IS 'Calendar dimension used for time-based analytics.';


-- FACT TABLE: SALES
-- Grain: 1 row per (user, product, date)
CREATE TABLE IF NOT EXISTS gold.fact_sales (
    sale_id         BIGSERIAL PRIMARY KEY,

    user_key        BIGINT NOT NULL,
    product_key     BIGINT NOT NULL,
    date_key        DATE NOT NULL,

    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    total_amount    NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Referential integrity
    CONSTRAINT fk_fact_user
        FOREIGN KEY (user_key)
        REFERENCES gold.dim_user(user_key)
        ON DELETE RESTRICT,

    CONSTRAINT fk_fact_product
        FOREIGN KEY (product_key)
        REFERENCES gold.dim_product(product_key)
        ON DELETE RESTRICT,

    CONSTRAINT fk_fact_date
        FOREIGN KEY (date_key)
        REFERENCES gold.dim_date(date_key)
        ON DELETE RESTRICT,

    -- Grain enforcement
    CONSTRAINT uq_fact_sales_grain
        UNIQUE (user_key, product_key, date_key),

    -- Business rule consistency
    CONSTRAINT chk_fact_sales_math
        CHECK (total_amount = quantity * unit_price)
);


CREATE INDEX IF NOT EXISTS idx_fact_sales_user
    ON gold.fact_sales(user_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_product
    ON gold.fact_sales(product_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date
    ON gold.fact_sales(date_key);
