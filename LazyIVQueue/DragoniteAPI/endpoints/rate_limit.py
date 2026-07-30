"""Dragonite Global Rate Limit endpoints."""
from typing import Any, Dict

from LazyIVQueue.DragoniteAPI.utils.http_api import APIClient
from LazyIVQueue.utils.logger import logger


async def get_global_rate_limit(client: APIClient) -> Dict[str, Any]:
    """
    GET /global-rate-limit - Get Dragonite's current global rate limit state.

    Args:
        client: APIClient instance

    Returns:
        Rate limit state (allowed_per_second, waiters, ratio_from_base_rate, etc.)
    """
    logger.debug("[rate_limit] GET /global-rate-limit")
    return await client.get("/global-rate-limit")


async def reset_global_rate_limit(client: APIClient) -> Dict[str, Any]:
    """
    POST /global-rate-limit/reset - Reset Dragonite's global rate limit.

    Args:
        client: APIClient instance

    Returns:
        Rate limit state after reset
    """
    logger.debug("[rate_limit] POST /global-rate-limit/reset")
    return await client.post("/global-rate-limit/reset")
