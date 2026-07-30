"""
Throttling logging utility for LazyIVQueue.
This module provides functions to log circuit breaker and throttling events
to a separate file with timestamps and key statistics.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from typing import Any, Dict, Optional, Tuple

from LazyIVQueue.utils.logger import logger

# Use an absolute path next to this module so the log is always found
_LOG_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_PATH = os.path.join(_LOG_DIR, "throttling.log")

# The whole array is rewritten on every event, so an uncapped file turns a long
# session into O(n^2) work. Keep the most recent window instead.
_MAX_ENTRIES = 2000


def _read_entries() -> list:
    """Load existing entries, tolerating a missing, empty or corrupt file."""
    try:
        with open(_LOG_PATH, "r") as f:
            entries = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []
    # Guard against a hand-edited file holding something that is not an array
    return entries if isinstance(entries, list) else []


def _write_entries(entries: list) -> None:
    """Replace the log file atomically so a crash mid-write cannot corrupt it."""
    fd, tmp_path = tempfile.mkstemp(dir=_LOG_DIR, prefix=".throttling.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(entries, f, indent=2)
        os.replace(tmp_path, _LOG_PATH)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _queue_counts(manager) -> Tuple[Optional[int], Optional[int]]:
    """(pending, awaiting_iv) read from the manager, or (None, None) if unavailable."""
    try:
        pending, awaiting = manager._get_pending_and_awaiting_counts()
        return int(pending), int(awaiting)
    except Exception:
        return None, None


def _baseline_percent(manager) -> Optional[float]:
    try:
        return manager._baseline_scout_percent()
    except Exception:
        return None


def log_throttling_event(event_type: str, status: str, manager, **kwargs) -> None:
    """
    Log throttling events to a separate file with timestamp and key stats.

    Args:
        event_type (str): Type of event (e.g., "CIRCUIT_BREAKER_PAUSED", "RECOVERING")
        status (str): Current tuning status
        manager: IVQueueManager instance for accessing stats
        **kwargs: Additional event-specific data to log. Callers already holding
            fresh counts should pass pending_count/awaiting_iv_count to skip the
            recount below.
    """
    try:
        log_entries = _read_entries()

        now = time.time()
        entry: Dict[str, Any] = {
            "timestamp": now,
            "time_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now)),
            "event": event_type,
            "status": status,
        }

        # Counting is an O(n) scan of the queue, so only do it when the caller
        # did not already hand us the numbers.
        pending = kwargs.pop("pending_count", None)
        awaiting = kwargs.pop("awaiting_iv_count", None)
        if pending is None or awaiting is None:
            counted_pending, counted_awaiting = _queue_counts(manager)
            pending = counted_pending if pending is None else pending
            awaiting = counted_awaiting if awaiting is None else awaiting
        entry["pending_count"] = pending
        entry["awaiting_iv_count"] = awaiting

        # getattr defaults keep one renamed attribute from discarding the whole entry
        entry.update({
            "scout_percent": getattr(manager, "_current_scout_percent", None),
            "throttled_step": getattr(manager, "_throttled_step", None),
            "baseline_scout_percent": _baseline_percent(manager),
            "concurrency": getattr(manager, "_current_concurrency", None),
            "manual_pause": getattr(manager, "_manual_pause", None),
            "pause_reason": getattr(manager, "_pause_reason", ""),
            "total_pauses_triggered": getattr(manager, "_total_pauses_triggered", None),
            # Live status, which can differ from `status` when a call site logs
            # before it mutates _tuning_status.
            "tuning_status": getattr(manager, "_tuning_status", None),
        })

        # Add any additional key metrics from kwargs
        entry.update(kwargs)

        log_entries.append(entry)
        if len(log_entries) > _MAX_ENTRIES:
            del log_entries[:-_MAX_ENTRIES]

        _write_entries(log_entries)
    except Exception as e:
        # Surface the error so we can diagnose logging failures
        logger.warning(f"[throttling] Failed to write log entry ({event_type}): {e}")


# Initialize throttling log file
def init_throttling_log() -> None:
    """Initialize (truncate) the throttling log file for a fresh session."""
    try:
        _write_entries([])
    except Exception as e:
        logger.error(f"Failed to initialize throttling log: {e}")
