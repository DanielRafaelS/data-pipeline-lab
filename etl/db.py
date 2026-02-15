from contextlib import contextmanager
from typing import Generator, Iterable, Any

import psycopg
from psycopg.rows import dict_row

from etl.config import get_settings


def _build_dsn() -> str:
    settings = get_settings()
    return (
        f"host={settings.dw_host} "
        f"port={settings.dw_port} "
        f"dbname={settings.dw_name} "
        f"user={settings.dw_user} "
        f"password={settings.dw_password}"
    )


@contextmanager
def get_connection() -> Generator[psycopg.Connection, None, None]:
    """
    Context manager for PostgreSQL connection.

    Automatically commits if no exception occurs,
    otherwise rolls back.
    """
    conn = psycopg.connect(_build_dsn(), row_factory=dict_row)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_query(query: str, params: tuple | None = None) -> None:
    """
    Execute a single query without returning results.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)


def fetch_all(query: str, params: tuple | None = None) -> list[dict]:
    """
    Execute a query and return all rows as list of dicts.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()


def execute_many(query: str, data: Iterable[tuple[Any, ...]]) -> None:
    """
    Execute batch insert/update operations.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, data)
