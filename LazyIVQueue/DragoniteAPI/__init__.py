"""Dragonite API package."""
from LazyIVQueue.DragoniteAPI.utils.http_api import APIClient
import LazyIVQueue.config as AppConfig
from LazyIVQueue.utils.logger import logger


def get_dragonite_client() -> APIClient:
    """
    Builds an APIClient using DRAGONITE_API_BASE_URL.
    """
    base = AppConfig.DRAGONITE_API_BASE_URL or ""
    return APIClient(base)


__all__ = ["get_dragonite_client", "APIClient"]
