from datetime import datetime
from typing import List, Tuple

from etl.db import fetch_all, execute_many


# DIM USER
def load_dim_user() -> int:
    rows = fetch_all("""
        SELECT user_id, email, username, first_name, last_name, city
        FROM silver.users
    """)

    prepared = [
        (
            row["user_id"],
            row["email"],
            row["username"],
            row["first_name"],
            row["last_name"],
            row["city"],
        )
        for row in rows
    ]

    query = """
        INSERT INTO gold.dim_user (
            user_id, email, username, first_name, last_name, city
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id)
        DO UPDATE SET
            email = EXCLUDED.email,
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            city = EXCLUDED.city;
    """

    execute_many(query, prepared)
    return len(prepared)


# DIM PRODUCT
def load_dim_product() -> int:
    rows = fetch_all("""
        SELECT product_id, title, category, price
        FROM silver.products
    """)

    prepared = [
        (
            row["product_id"],
            row["title"],
            row["category"],
            row["price"],
        )
        for row in rows
    ]

    query = """
        INSERT INTO gold.dim_product (
            product_id, title, category, price
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (product_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            category = EXCLUDED.category,
            price = EXCLUDED.price;
    """

    execute_many(query, prepared)
    return len(prepared)


# DIM DATE
def load_dim_date() -> int:
    rows = fetch_all("""
        SELECT DISTINCT cart_date
        FROM silver.carts
    """)

    prepared: List[Tuple] = []

    for row in rows:
        date_value = row["cart_date"]
        if date_value is None:
            continue

        dt = datetime.strptime(str(date_value), "%Y-%m-%d")

        prepared.append(
            (
                date_value,
                dt.year,
                dt.month,
                dt.day,
            )
        )

    query = """
        INSERT INTO gold.dim_date (
            date_key, year, month, day
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date_key)
        DO NOTHING;
    """

    execute_many(query, prepared)
    return len(prepared)

# FACT SALES
def load_fact_sales() -> int:
    rows = fetch_all("""
        SELECT
            c.cart_id,
            c.user_id,
            c.cart_date,
            ci.product_id,
            ci.quantity,
            p.price
        FROM silver.carts c
        JOIN silver.cart_items ci ON c.cart_id = ci.cart_id
        JOIN silver.products p ON ci.product_id = p.product_id
    """)

    prepared = []

    for row in rows:
        total_amount = row["quantity"] * row["price"]

        prepared.append(
            (
                row["user_id"],
                row["product_id"],
                row["cart_date"],
                row["quantity"],
                row["price"],
                total_amount,
            )
        )

    query = """
        INSERT INTO gold.fact_sales (
            user_key,
            product_key,
            date_key,
            quantity,
            unit_price,
            total_amount
        )
        SELECT
            u.user_key,
            p.product_key,
            %s,
            %s,
            %s,
            %s
        FROM gold.dim_user u
        JOIN gold.dim_product p
            ON p.product_id = %s
        WHERE u.user_id = %s;
    """

    final_prepared = [
        (
            row[2],      # date_key
            row[3],      # quantity
            row[4],      # unit_price
            row[5],      # total_amount
            row[1],      # product_id
            row[0],      # user_id
        )
        for row in prepared
    ]

    execute_many(query, final_prepared)
    return len(final_prepared)
