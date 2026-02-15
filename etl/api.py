from typing import Any, List
import requests
from requests import Response
from requests.exceptions import RequestException

from etl.config import get_settings


class FakeStoreClient:
    """
    Simple client for FakeStore API.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self.base_url = settings.api_base_url.rstrip("/")
        self.timeout = settings.api_timeout_seconds

    def _get(self, endpoint: str) -> Any:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            response: Response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            raise RuntimeError(f"API request failed for {url}: {e}") from e

    # Endpoints

    def get_products(self) -> List[dict]:
        return self._get("products")

    def get_users(self) -> List[dict]:
        return self._get("users")

    def get_carts(self) -> List[dict]:
        return self._get("carts")
