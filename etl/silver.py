from decimal import Decimal
from datetime import datetime
from typing import List, Tuple

from etl.db import fetch_all, execute_many

# PRODUCTS
def transform_products() -> int:

    """
        Transform raw product records into the silver layer.

        This function reads product payloads from the raw.products table,
        applies structural normalization, basic data quality validations,
        explicit type casting, and derives analytical attributes before
        loading them into silver.products.

        Transformations applied:
        - Explicit type casting for numeric fields (price, rating_rate, rating_count)
        - Text normalization (trimmed title, lowercase category)
        - Validation rules:
        • product_id must be present
        • price must be >= 0
        • rating_count coerced to >= 0
        - Derived attribute:
        • price_bucket (low, mid, high) based on price ranges

        The load operation is idempotent, using ON CONFLICT (product_id)
        to ensure safe re-execution without duplication.

        Returns:
        int: Number of successfully processed product records.

        Raises:
        ValueError: If no valid product records are available for loading.
    """

    rows = fetch_all("SELECT product_id, payload FROM raw.products")

    prepared: List[Tuple] = []

    for row in rows:
        product_id = row["product_id"]
        data = row["payload"]

        if not product_id:
            continue

        title = (data.get("title") or "").strip()
        category = (data.get("category") or "").strip().lower()

        price = Decimal(str(data.get("price", 0)))
        rating_rate = Decimal(str(data.get("rating", {}).get("rate", 0)))
        rating_count = int(data.get("rating", {}).get("count", 0))

        #  Basic validations 
        if price < 0:
            continue

        if rating_count < 0:
            rating_count = 0

        #  Derived column 
        if price < 50:
            price_bucket = "low"
        elif price <= 150:
            price_bucket = "mid"
        else:
            price_bucket = "high"

        prepared.append(
            (
                product_id,
                title,
                category,
                price,
                rating_rate,
                rating_count,
                price_bucket,
            )
        )

    if not prepared:
        raise ValueError("No valid product records to load into silver layer.")

    query = """
        INSERT INTO silver.products (
            product_id,
            title,
            category,
            price,
            rating_rate,
            rating_count,
            price_bucket
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            category = EXCLUDED.category,
            price = EXCLUDED.price,
            rating_rate = EXCLUDED.rating_rate,
            rating_count = EXCLUDED.rating_count,
            price_bucket = EXCLUDED.price_bucket,
            updated_at = NOW();
    """

    execute_many(query, prepared)
    return len(prepared)

# USERS
def transform_users() -> int:
    """
        Transform raw user records from the bronze layer into the silver layer.

        This function reads user payloads from the raw.users table,
        extracts and normalizes relevant attributes, applies minimal
        data quality validations, and loads the cleaned data into
        silver.users.

        Transformations applied:
        - Explicit extraction of nested fields (first_name, last_name, city)
        - Text normalization (strip whitespace; lowercase email)
        - Validation rule:
            • user_id must be present

        The load operation is idempotent through the use of
        ON CONFLICT (user_id), ensuring safe re-execution without
        data duplication.

        Returns:
            int: Number of successfully processed user records.

        Raises:
            ValueError: If no valid user records are available for loading.
    """

    rows = fetch_all("SELECT user_id, payload FROM raw.users")

    prepared = []

    for row in rows:
        user_id = row["user_id"]
        data = row["payload"]

        if not user_id:
            continue

        email = (data.get("email") or "").strip().lower()
        username = (data.get("username") or "").strip()
        first_name = (data.get("name", {}).get("firstname") or "").strip()
        last_name = (data.get("name", {}).get("lastname") or "").strip()
        city = (data.get("address", {}).get("city") or "").strip()

        prepared.append(
            (
                user_id,
                email,
                username,
                first_name,
                last_name,
                city,
            )
        )

    if not prepared:
        raise ValueError("No valid user records to load into silver layer.")

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
        cart_id = row["cart_id"]
        data = row["payload"]

        if not cart_id:
            continue

        user_id = data.get("userId")
        raw_date = data.get("date")

        if not user_id or not raw_date:
            continue

        # Normalize date (YYYY-MM-DD)
        try:
            cart_date = datetime.fromisoformat(raw_date.replace("Z", "")).date()
        except Exception:
            continue

        carts_prepared.append(
            (
                cart_id,
                user_id,
                cart_date,
            )
        )

        for item in data.get("products", []):
            product_id = item.get("productId")
            quantity = item.get("quantity")

            if not product_id:
                continue

            quantity = int(quantity or 0)

            if quantity <= 0:
                continue

            items_prepared.append(
                (
                    cart_id,
                    product_id,
                    quantity,
                )
            )

    if not carts_prepared:
        raise ValueError("No valid cart records to load into silver layer.")

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