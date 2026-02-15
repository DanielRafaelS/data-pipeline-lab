from typing import List, Dict

from etl.db import fetch_all, execute_many

# PRODUCTS
def transform_products() -> int:
    rows = fetch_all("SELECT product_id, payload FROM raw.products")

    prepared = []
    for row in rows:
        data = row["payload"]

        prepared.append(
            (
                row["product_id"],
                data.get("title"),
                data.get("category"),
                float(data.get("price", 0)),
                float(data.get("rating", {}).get("rate", 0)),
                int(data.get("rating", {}).get("count", 0)),
            )
        )

    query = """
        INSERT INTO silver.products (
            product_id,
            title,
            category,
            price,
            rating_rate,
            rating_count
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            category = EXCLUDED.category,
            price = EXCLUDED.price,
            rating_rate = EXCLUDED.rating_rate,
            rating_count = EXCLUDED.rating_count,
            updated_at = NOW();
    """

    execute_many(query, prepared)
    return len(prepared)

# USERS
def transform_users() -> int:
    rows = fetch_all("SELECT user_id, payload FROM raw.users")

    prepared = []
    for row in rows:
        data = row["payload"]

        prepared.append(
            (
                row["user_id"],
                data.get("email"),
                data.get("username"),
                data.get("name", {}).get("firstname"),
                data.get("name", {}).get("lastname"),
                data.get("address", {}).get("city"),
            )
        )

    query = """
        INSERT INTO silver.users (
            user_id,
            email,
            username,
            first_name,
            last_name,
            city
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id)
        DO UPDATE SET
            email = EXCLUDED.email,
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            city = EXCLUDED.city,
            updated_at = NOW();
    """

    execute_many(query, prepared)
    return len(prepared)

# CARTS + CART ITEMS
def transform_carts() -> int:
    rows = fetch_all("SELECT cart_id, payload FROM raw.carts")

    carts_prepared = []
    items_prepared = []

    for row in rows:
        data = row["payload"]

        cart_id = row["cart_id"]
        user_id = data.get("userId")
        cart_date = data.get("date", "")[:10]

        carts_prepared.append(
            (
                cart_id,
                user_id,
                cart_date,
            )
        )

        for item in data.get("products", []):
            items_prepared.append(
                (
                    cart_id,
                    item.get("productId"),
                    item.get("quantity"),
                )
            )

    carts_query = """
        INSERT INTO silver.carts (
            cart_id,
            user_id,
            cart_date
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (cart_id)
        DO UPDATE SET
            user_id = EXCLUDED.user_id,
            cart_date = EXCLUDED.cart_date,
            updated_at = NOW();
    """

    items_query = """
        INSERT INTO silver.cart_items (
            cart_id,
            product_id,
            quantity
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (cart_id, product_id)
        DO UPDATE SET
            quantity = EXCLUDED.quantity;
    """

    execute_many(carts_query, carts_prepared)
    execute_many(items_query, items_prepared)

    return len(carts_prepared)
