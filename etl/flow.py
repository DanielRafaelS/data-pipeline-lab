from prefect import flow, task
import logging

from etl.bronze import (
    load_products_raw,
    load_users_raw,
    load_carts_raw,
)
from etl.silver import (
    transform_products,
    transform_users,
    transform_carts,
)
from etl.gold import (
    load_dim_user,
    load_dim_product,
    load_dim_date,
    load_fact_sales,
)
from etl.config import get_settings

# Logging
def _configure_logging():
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

# Tasks
@task
def bronze_layer():
    load_products_raw()
    load_users_raw()
    load_carts_raw()


@task
def silver_layer():
    transform_products()
    transform_users()
    transform_carts()


@task
def gold_layer():
    load_dim_user()
    load_dim_product()
    load_dim_date()
    load_fact_sales()

# Flow
@flow(name="medallion-etl")
def etl_flow():
    _configure_logging()

    bronze = bronze_layer()
    silver = silver_layer(wait_for=[bronze])
    gold = gold_layer(wait_for=[silver])

    return gold
