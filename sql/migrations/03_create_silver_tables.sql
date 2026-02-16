-- PRODUCTS
CREATE TABLE IF NOT EXISTS silver.products (
    product_id      INTEGER PRIMARY KEY,
    title           TEXT NOT NULL,
    category        TEXT NOT NULL,
    price           NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    price_bucket    TEXT NOT NULL,
    rating_rate     NUMERIC(3,2) CHECK (rating_rate >= 0),
    rating_count    INTEGER CHECK (rating_count >= 0),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE silver.products IS 'Cleaned and normalized product data.';


-- USERS
CREATE TABLE IF NOT EXISTS silver.users (
    user_id         INTEGER PRIMARY KEY,
    email           TEXT NOT NULL,
    username        TEXT NOT NULL,
    first_name      TEXT,
    last_name       TEXT,
    city            TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE silver.users IS 'Cleaned and normalized user data.';

CREATE INDEX IF NOT EXISTS idx_silver_users_email
    ON silver.users (email);


-- CARTS
CREATE TABLE IF NOT EXISTS silver.carts (
    cart_id         INTEGER PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    cart_date       DATE NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_carts_user
        FOREIGN KEY (user_id)
        REFERENCES silver.users(user_id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_silver_carts_user
    ON silver.carts (user_id);

CREATE INDEX IF NOT EXISTS idx_silver_carts_date
    ON silver.carts (cart_date);


-- CART ITEMS
CREATE TABLE IF NOT EXISTS silver.cart_items (
    cart_id         INTEGER NOT NULL,
    product_id      INTEGER NOT NULL,
    quantity        INTEGER NOT NULL CHECK (quantity > 0),

    PRIMARY KEY (cart_id, product_id),

    CONSTRAINT fk_cart_items_cart
        FOREIGN KEY (cart_id)
        REFERENCES silver.carts(cart_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_cart_items_product
        FOREIGN KEY (product_id)
        REFERENCES silver.products(product_id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_silver_cart_items_product
    ON silver.cart_items (product_id);
