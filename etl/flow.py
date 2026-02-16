import time
import logging

from prefect import flow, task, get_run_logger

from etl.config import get_settings
from etl.bronze import load_products_raw, load_users_raw, load_carts_raw
from etl.silver import transform_products, transform_users, transform_carts
from etl.gold import (
    load_dim_user,
    load_dim_product,
    load_dim_date,
    load_fact_sales,
)

from etl.quality import (
    validate_silver_products,
    validate_silver_users,
    validate_silver_cart_items,
)


def _configure_logging():
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


# Bronze
@task
def bronze_layer():
    logger = get_run_logger()
    start = time.perf_counter()

    logger.info("Starting Bronze layer ingestion...")

    p = load_products_raw()
    u = load_users_raw()
    c = load_carts_raw()

    elapsed = round(time.perf_counter() - start, 2)

    logger.info(
        f"Bronze completed | products={p} users={u} carts={c} | duration={elapsed}s"
    )

    return {"products": p, "users": u, "carts": c}


# Silver
@task
def silver_layer():
    logger = get_run_logger()
    start = time.perf_counter()

    logger.info("Starting Silver transformations...")

    p = transform_products()
    u = transform_users()
    c = transform_carts()

    logger.info("Running data quality checks (Silver layer)...")

    validate_silver_products()
    validate_silver_users()
    validate_silver_cart_items()

    logger.info("Data quality checks passed.")

    elapsed = round(time.perf_counter() - start, 2)

    logger.info(
        f"Silver completed | products={p} users={u} carts={c} | duration={elapsed}s"
    )

    return {"products": p, "users": u, "carts": c}


# Gold
@task
def gold_layer():
    logger = get_run_logger()
    start = time.perf_counter()

    logger.info("Starting Gold dimensional load...")

    du = load_dim_user()
    dp = load_dim_product()
    dd = load_dim_date()
    fs = load_fact_sales()

    elapsed = round(time.perf_counter() - start, 2)

    logger.info(
        f"Gold completed | dim_user={du} dim_product={dp} dim_date={dd} fact_sales={fs} | duration={elapsed}s"
    )

    return {
        "dim_user": du,
        "dim_product": dp,
        "dim_date": dd,
        "fact_sales": fs,
    }


# Main Flow
@flow(name="medallion-etl")
def etl_flow():
    _configure_logging()
    logger = get_run_logger()
    settings = get_settings()

    logger.info("<-------------------------------------->")
    logger.info("Starting Medallion ETL Pipeline")
    logger.info(f"Environment: {settings.environment}")
    logger.info(f"Load mode: {settings.load_mode}")
    logger.info("<-------------------------------------->")

    total_start = time.perf_counter()

    bronze = bronze_layer()
    silver = silver_layer(wait_for=[bronze])
    gold = gold_layer(wait_for=[silver])

    total_elapsed = round(time.perf_counter() - total_start, 2)

    logger.info("<-------------------------------------->")
    logger.info("Pipeline completed successfully")
    logger.info(f"Total execution time: {total_elapsed}s")
    logger.info("<-------------------------------------->")

    return {
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
        "duration_seconds": total_elapsed,
    }
