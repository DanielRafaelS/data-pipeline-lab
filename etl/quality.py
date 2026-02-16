from typing import Tuple
from etl.db import fetch_all


class DataQualityError(Exception):
    """Raised when a data quality validation fails."""
    pass


def _assert_zero(query: str, message: str) -> None:
    """
    Execute a validation query that must return zero rows.

    Raises:
        DataQualityError: If query returns any record.
    """
    result = fetch_all(query)
    if result:
        raise DataQualityError(message)


# SILVER VALIDATIONS
def validate_silver_products() -> None:
    """
    Ensures no invalid prices or negative rating counts exist.
    """
    _assert_zero(
        """
        SELECT product_id
        FROM silver.products
        WHERE price < 0
           OR rating_count < 0;
        """,
        "Invalid product pricing or rating_count detected."
    )


def validate_silver_users() -> None:
    """
    Ensures critical user fields are not null.
    """
    _assert_zero(
        """
        SELECT user_id
        FROM silver.users
        WHERE email IS NULL
           OR username IS NULL;
        """,
        "Null email or username detected in silver.users."
    )


def validate_silver_cart_items() -> None:
    """
    Ensures cart item quantities are positive.
    """
    _assert_zero(
        """
        SELECT cart_id
        FROM silver.cart_items
        WHERE quantity <= 0;
        """,
        "Non-positive quantity detected in silver.cart_items."
    )
