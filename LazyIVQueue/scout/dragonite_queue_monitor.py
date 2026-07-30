"""Dragonite Queue Monitor - polls Dragonite's own /scout/queue depth on a fixed interval.

Decoupled from the tuner's call frequency on purpose: _evaluate_self_tuning_locked()
runs from get_next_for_scout(), which the scout coordinator calls in a tight loop with
no sleep while work is available - far too often to make a live HTTP call from there.
This poller runs on its own clock and just pushes the latest reading into
IVQueueManager for the tuner to read synchronously.
"""
from __future__ import annotations

import asyncio
from typing import Optional

from LazyIVQueue.utils.logger import logger
from LazyIVQueue.queue.iv_queue import IVQueueManager
from LazyIVQueue.DragoniteAPI import get_dragonite_client
from LazyIVQueue.DragoniteAPI.utils.http_api import APIClient
from LazyIVQueue.DragoniteAPI.endpoints.scout import get_scout_queue
import LazyIVQueue.config as AppConfig


class DragoniteQueueMonitor:
    """Singleton background poller for Dragonite's /scout/queue depth."""

    _instance: Optional[DragoniteQueueMonitor] = None

    def __init__(self) -> None:
        self._running: bool = False
        self._task: Optional[asyncio.Task] = None
        self._client: Optional[APIClient] = None

    @classmethod
    async def get_instance(cls) -> DragoniteQueueMonitor:
        """Get or create singleton instance."""
        if cls._instance is None:
            cls._instance = DragoniteQueueMonitor()
        return cls._instance

    async def start(self) -> None:
        """Start the polling loop."""
        if self._running:
            logger.warning("DragoniteQueueMonitor already running")
            return

        self._running = True
        self._client = get_dragonite_client()
        await self._client.__aenter__()
        self._task = asyncio.create_task(self._run_loop())

        logger.info(
            f"DragoniteQueueMonitor started (poll interval: {AppConfig.dragonite_queue_poll_interval_seconds}s, "
            f"threshold: {AppConfig.dragonite_queue_threshold})"
        )

    async def _run_loop(self) -> None:
        queue = await IVQueueManager.get_instance()

        while self._running:
            try:
                response = await get_scout_queue(self._client)
                value = int(response.get("queue")) if isinstance(response, dict) else None
                queue.set_dragonite_queue_value(value)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug(f"DragoniteQueueMonitor poll failed: {e}")
                queue.set_dragonite_queue_value(None)

            try:
                await asyncio.sleep(AppConfig.dragonite_queue_poll_interval_seconds)
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

        logger.info("DragoniteQueueMonitor stopped")
