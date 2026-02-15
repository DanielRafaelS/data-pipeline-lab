import json
from typing import List, Dict

from etl.api import FakeStoreClient
from etl.db import execute_many
from etl.config import get_settings

# Internal helpers

def _prepare_raw_records(
    records: List[Dict],
    id_field: str
) -> List[tuple]:
    """
    Convert API records into tuples for insertion.

    Each tuple:
        (natural_id, json_payload)
    """
    prepared = []
    for record in records:
        natural_id = record[id_field]
        payload = json.dumps(record)
        prepared.append((natural_id, payload))

    return prepared


def _insert_raw(table: str, data: List[tuple]) -> None:
    """
    Insert raw data with upsert to avoid duplicates.
    """
    query = f"""
        INSERT INTO raw.{table} ( {table[:-1]}_id, payload )
        VALUES (%s, %s)
        ON CONFLICT ({table[:-1]}_id)
        DO UPDATE SET
            payload = EXCLUDED.payload,
            ingested_at = NOW();
    """

    execute_many(query, data)

# Public functions

def load_products_raw() -> int:
    client = FakeStoreClient()
    records = client.get_products()

    prepared = _prepare_raw_records(records, "id")
    _insert_raw("products", prepared)

    return len(prepared)


def load_users_raw() -> int:
    client = FakeStoreClient()
    records = client.get_users()

    prepared = _prepare_raw_records(records, "id")
    _insert_raw("users", prepared)

    return len(prepared)


def load_carts_raw() -> int:
    client = FakeStoreClient()
    records = client.get_carts()

    prepared = _prepare_raw_records(records, "id")
    _insert_raw("carts", prepared)

    return len(prepared)
