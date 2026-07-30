"""Dragonite Global Rate Limit Monitor - polls Dragonite's own /global-rate-limit
state on a fixed interval and resets it if too many requests pile up waiting.

Independent of LazyIVQueue's own self-tuning circuit breaker: this watches
Dragonite's rate limiter itself and kicks it if it gets stuck backed up, rather
than reacting to our own scout queue depth.
"""
from __future__ import annotations

import asyncio
from typing import Optional

from LazyIVQueue.utils.logger import logger
from LazyIVQueue.queue.iv_queue import IVQueueManager
from LazyIVQueue.DragoniteAPI import get_dragonite_client
from LazyIVQueue.DragoniteAPI.utils.http_api import APIClient
from LazyIVQueue.DragoniteAPI.endpoints.rate_limit import get_global_rate_limit, reset_global_rate_limit
import LazyIVQueue.config as AppConfig


class DragoniteRateLimitMonitor:
    """Singleton background poller for Dragonite's /global-rate-limit state."""

    _instance: Optional[DragoniteRateLimitMonitor] = None

    def __init__(self) -> None:
        self._running: bool = False
        self._task: Optional[asyncio.Task] = None
        self._client: Optional[APIClient] = None

    @classmethod
    async def get_instance(cls) -> DragoniteRateLimitMonitor:
        """Get or create singleton instance."""
        if cls._instance is None:
            cls._instance = DragoniteRateLimitMonitor()
        return cls._instance

    async def start(self) -> None:
        """Start the polling loop."""
        if self._running:
            logger.warning("DragoniteRateLimitMonitor already running")
            return

        self._running = True
        self._client = get_dragonite_client()
        await self._client.__aenter__()
        self._task = asyncio.create_task(self._run_loop())

        logger.info(
            f"DragoniteRateLimitMonitor started (poll interval: {AppConfig.dragonite_rate_limit_poll_interval_seconds}s, "
            f"waiters threshold: {AppConfig.dragonite_rate_limit_waiters_threshold}, "
            f"reset cooldown: {AppConfig.dragonite_rate_limit_reset_cooldown_seconds}s)"
        )

    async def _run_loop(self) -> None:
        queue = await IVQueueManager.get_instance()

        while self._running:
            sleep_seconds = AppConfig.dragonite_rate_limit_poll_interval_seconds

            try:
                state = await get_global_rate_limit(self._client)
                waiters = state.get("waiters") if isinstance(state, dict) else None
                queue.set_dragonite_rate_limit_waiters(waiters if isinstance(waiters, (int, float)) else None)

                if isinstance(waiters, (int, float)) and waiters > AppConfig.dragonite_rate_limit_waiters_threshold:
                    logger.opt(colors=True).warning(
                        f"<yellow>[RateLimitMonitor]</yellow> Dragonite global rate limit waiters={waiters} "
                        f"exceeds threshold {AppConfig.dragonite_rate_limit_waiters_threshold} - resetting"
                    )
                    try:
                        reset_state = await reset_global_rate_limit(self._client)
                        logger.info(f"[RateLimitMonitor] Reset issued, Dragonite reports: {reset_state}")
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.error(f"[RateLimitMonitor] Reset request failed: {e}")

                    sleep_seconds = AppConfig.dragonite_rate_limit_reset_cooldown_seconds
                    logger.info(f"[RateLimitMonitor] Pausing {sleep_seconds}s before resuming checks")
                else:
                    logger.trace(f"[RateLimitMonitor] waiters={waiters}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                queue.set_dragonite_rate_limit_waiters(None)
                logger.debug(f"DragoniteRateLimitMonitor poll failed: {e}")

            try:
                await asyncio.sleep(sleep_seconds)
            except asyncio.CancelledError:
                break

    async def stop(self) -> None:
        """Stop the poller gracefully."""
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        if self._client:
            await self._client.__aexit__(None, None, None)

        logger.info("DragoniteRateLimitMonitor stopped")
