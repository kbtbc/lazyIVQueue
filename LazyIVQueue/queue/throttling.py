"""
Throttling / self-tuning event log for LazyIVQueue.

Append-only plain text written to ``throttling.log`` next to this module - one
line per record, readable with ``tail``/``grep``/``less`` without any special
tooling. Each line is ``<utc timestamp> <EVENT> key=value key=value ...``
(values with spaces are quoted). The log is never truncated: a restart appends
a SESSION_START record carrying the tuning config in effect, so a session's
behaviour can always be read back against the settings that produced it.

Two kinds of record:

* **transitions** - written by the tuner whenever state actually changes
  (throttle steps, circuit breaker trips/releases, resets). Each one carries
  the "from" value and the timer that tripped it, so a step is readable on its
  own line without diffing neighbours.
* **samples** - periodic snapshots (``log_sample``) that fill in what happens
  *between* transitions. Rate limited and suppressed while nothing meaningful
  moves, so a quiet stretch costs one line every few minutes rather than one
  line per tuner pass.

Reviewing:  ``python -m LazyIVQueue.queue.throttling``
"""

from __future__ import annotations

import calendar
import os
import shlex
import time
from typing import Any, Dict, Optional, Tuple

from LazyIVQueue.utils.logger import logger

# Absolute path next to this module so the log is always found
_LOG_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_PATH = os.path.join(_LOG_DIR, "throttling.log")

# Appends are O(1), so size is the only bound needed. One previous file is kept.
_MAX_BYTES = 8 * 1024 * 1024

# Steady-state sampling: never write samples closer together than this...
_SAMPLE_MIN_INTERVAL = 30.0
# ...and always write one at least this often, so a flat stretch reads as a flat
# stretch instead of a gap that could equally mean "the process died".
_SAMPLE_MAX_INTERVAL = 300.0
# Between those bounds, a sample is only worth a line if utilization moved at
# least this many points (status/step/percent changes always qualify).
_SAMPLE_UTIL_DELTA = 5.0

# Last sample written, for the "has anything moved?" comparison above.
_last_sample: Dict[str, Any] = {"time": 0.0, "snapshot": None}


# --------------------------------------------------------------------------- #
# Line format: "<utc> <EVENT> key=value ..." - a minimal logfmt variant
# --------------------------------------------------------------------------- #

def _encode_line(utc: str, event: str, fields: Dict[str, Any]) -> str:
    parts = [utc, event]
    for key, value in fields.items():
        if value is None or value is False:
            continue  # a line of "...=False" per record is what makes a log tedious
        if value is True:
            value = "true"
        elif isinstance(value, (list, tuple)):
            value = ",".join(str(v) for v in value)
        text = str(value)
        if text == "" or any(c in text for c in " \t\"'"):
            text = '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
        parts.append(f"{key}={text}")
    return " ".join(parts)


def _coerce(text: str) -> Any:
    if text == "true":
        return True
    if text == "false":
        return False
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return text


def _decode_line(line: str) -> Optional[Dict[str, Any]]:
    try:
        tokens = shlex.split(line)
    except ValueError:
        return None  # unbalanced quotes: a partial write, skip it
    if len(tokens) < 2:
        return None
    record: Dict[str, Any] = {"utc": tokens[0], "event": tokens[1]}
    for token in tokens[2:]:
        key, sep, value = token.partition("=")
        if sep:
            record[key] = _coerce(value)
    return record


# --------------------------------------------------------------------------- #
# File I/O
# --------------------------------------------------------------------------- #

def _rotate_if_needed() -> None:
    """Roll the log over to ``.1`` once it gets large, keeping one generation."""
    try:
        if os.path.getsize(_LOG_PATH) < _MAX_BYTES:
            return
    except OSError:
        return  # missing file: nothing to rotate
    os.replace(_LOG_PATH, _LOG_PATH + ".1")


def _append(line: str) -> None:
    _rotate_if_needed()
    with open(_LOG_PATH, "a") as f:
        f.write(line + "\n")


# --------------------------------------------------------------------------- #
# Snapshot
# --------------------------------------------------------------------------- #

def _round(value: Any, digits: int = 4) -> Any:
    """Round floats for readable lines; leave anything else untouched."""
    if isinstance(value, float):
        return round(value, digits)
    return value


def _counts(manager, pending: Optional[int], awaiting: Optional[int]) -> Tuple[Optional[int], Optional[int]]:
    """Fill in queue counts, only scanning when the caller did not supply them."""
    if pending is not None and awaiting is not None:
        return pending, awaiting
    try:
        counted_pending, counted_awaiting = manager._get_pending_and_awaiting_counts()
    except Exception:
        return pending, awaiting
    return (
        counted_pending if pending is None else pending,
        counted_awaiting if awaiting is None else awaiting,
    )


# Elapsed-time signals: the tuner triggers on how long a condition has held, so
# the log records the same durations rather than raw start timestamps.
_TIMERS = (
    ("backlog_s", "_pending_backlog_start_time"),
    ("pause_s", "_pause_start_time"),
    ("high_util_s", "_high_util_start_time"),
    ("low_util_s", "_low_util_start_time"),
    ("since_step_s", "_last_concurrency_adjustment_time"),
)


def _snapshot(manager, pending: Optional[int] = None, awaiting: Optional[int] = None,
              now: Optional[float] = None) -> Dict[str, Any]:
    """
    Current tuning state as flat fields.

    Every read is defensive: a renamed attribute should cost one field, not the
    whole record. Zero-valued timers are omitted to keep lines short.
    """
    now = time.time() if now is None else now
    pending, awaiting = _counts(manager, pending, awaiting)

    try:
        baseline = manager._baseline_scout_percent()
    except Exception:
        baseline = None

    workers = getattr(manager, "_current_concurrency", None) or 0
    snap: Dict[str, Any] = {
        "status": getattr(manager, "_tuning_status", None),
        "step": getattr(manager, "_throttled_step", None),
        "pct": _round(getattr(manager, "_current_scout_percent", None)),
        "baseline_pct": _round(baseline),
        "pending": pending,
        "awaiting": awaiting,
        "workers": workers,
    }
    # Recomputed rather than read from _last_utilization_pct so events raised
    # outside the tuner (manual pause, resets) still get a truthful number.
    if awaiting is not None:
        snap["util"] = round((awaiting / max(1, workers)) * 100.0, 1)

    for key, attr in _TIMERS:
        started = getattr(manager, attr, None)
        if started:
            elapsed = round(now - started, 1)
            if elapsed > 0:
                snap[key] = elapsed

    outcomes = getattr(manager, "_recent_scout_outcomes", None) or []
    if outcomes:
        snap["scout_fail_pct"] = round((outcomes.count(False) / len(outcomes)) * 100.0, 1)

    pauses = getattr(manager, "_total_pauses_triggered", None)
    if pauses:
        snap["pauses"] = pauses
    reason = getattr(manager, "_pause_reason", "")
    if reason:
        snap["pause_reason"] = reason
    if getattr(manager, "_manual_pause", False):
        snap["manual_pause"] = True

    return snap


def config_snapshot() -> Dict[str, Any]:
    """
    The tuning knobs in effect, for SESSION_START and CONFIG_SYNC records.

    Recorded so a pattern in the log can be attributed to the settings that
    produced it after those settings have since been changed. Keys are flat
    (``cfg_`` prefixed) rather than nested, so they read the same as any other
    field on the line.
    """
    import LazyIVQueue.config as AppConfig

    keys = (
        "self_tuning_enabled",
        "iv_baseline_percent",
        "cell_baseline_percent",
        "max_scout_percent",
        "tuning_step_factor",
        "tuning_interval_seconds",
        "too_many_workers_percent",
        "too_few_workers_percent",
        "throttle_backlog_seconds",
        "hard_pause_backlog_seconds",
        "min_hard_pause_duration",
        "worker_recovery_percent",
        "concurrency_scout",
        "auto_rarity_enabled",
    )
    return {f"cfg_{key}": _round(getattr(AppConfig, key, None)) for key in keys}


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

def _emit(event: str, snapshot: Dict[str, Any], fields: Dict[str, Any], now: float) -> None:
    utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))
    merged: Dict[str, Any] = dict(snapshot)
    merged.update(fields)
    _append(_encode_line(utc, event, merged))
    # A transition line already carries a full snapshot, so it counts as one: this
    # is what stops every transition from being trailed by a near-identical SAMPLE.
    _last_sample["time"] = now
    _last_sample["snapshot"] = snapshot


def log_event(event: str, manager, pending: Optional[int] = None,
              awaiting: Optional[int] = None, **fields: Any) -> None:
    """
    Record a state transition.

    Status, percent and counters are read off the manager, so call this *after*
    the mutation the event describes - the record then shows the state the tuner
    moved *to*, and ``from_pct``-style extras show where it came from.

    Args:
        event: transition name, e.g. "STAGE_1_THROTTLED", "HIGH_UTILIZATION".
        manager: the IVQueueManager.
        pending / awaiting: queue counts the caller already computed, to skip
            the O(n) rescan. Omit and they are counted.
        **fields: event-specific context (from_pct, purged, trigger, ...).
    """
    try:
        now = time.time()
        _emit(event, _snapshot(manager, pending, awaiting, now), fields, now)
    except Exception as e:
        logger.warning(f"[throttling] Failed to write log entry ({event}): {e}")


def log_sample(manager, pending: Optional[int] = None,
               awaiting: Optional[int] = None, **fields: Any) -> None:
    """
    Record a periodic snapshot of the tuner between transitions.

    Safe to call on every tuner pass: writes at most one line per
    ``_SAMPLE_MIN_INTERVAL``, and only then if something moved or
    ``_SAMPLE_MAX_INTERVAL`` has elapsed.
    """
    try:
        now = time.time()
        elapsed = now - _last_sample["time"]
        if elapsed < _SAMPLE_MIN_INTERVAL:
            return

        snapshot = _snapshot(manager, pending, awaiting, now)
        previous = _last_sample["snapshot"]
        if previous is not None and elapsed < _SAMPLE_MAX_INTERVAL and not _sample_moved(previous, snapshot):
            return

        _emit("SAMPLE", snapshot, fields, now)
    except Exception as e:
        logger.warning(f"[throttling] Failed to write sample: {e}")


def _sample_moved(previous: Dict[str, Any], current: Dict[str, Any]) -> bool:
    """True if this sample says something the previous one did not."""
    for key in ("status", "step", "pct"):
        if previous.get(key) != current.get(key):
            return True
    # Backlog appearing or clearing is a state change even at equal utilization
    if bool(previous.get("pending")) != bool(current.get("pending")):
        return True
    return abs((current.get("util") or 0.0) - (previous.get("util") or 0.0)) >= _SAMPLE_UTIL_DELTA


def log_session_start() -> None:
    """
    Append a SESSION_START record with the tuning config in effect.

    Deliberately does not truncate: history across restarts is the point, and a
    restart is itself a data point when reviewing a bad stretch.
    """
    try:
        now = time.time()
        fields: Dict[str, Any] = {"pid": os.getpid()}
        fields.update(config_snapshot())
        _emit("SESSION_START", {}, fields, now)
        _last_sample["time"] = 0.0
        _last_sample["snapshot"] = None
    except Exception as e:
        logger.error(f"[throttling] Failed to open throttling log: {e}")


# --------------------------------------------------------------------------- #
# Review helper:  python -m LazyIVQueue.queue.throttling
# --------------------------------------------------------------------------- #

def _read_records(path: str) -> list:
    records = []
    try:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = _decode_line(line)
                if record is not None:
                    records.append(record)
    except FileNotFoundError:
        pass
    return records


def _epoch(record: Dict[str, Any]) -> float:
    """Parse the record's ``utc`` column back to a unix timestamp for duration math."""
    try:
        return calendar.timegm(time.strptime(record["utc"], "%Y-%m-%dT%H:%M:%SZ"))
    except Exception:
        return 0.0


def _summarize(records: list, include_samples: bool = False) -> None:
    """Print a per-session digest: time in each status, percent range, and timeline."""
    if not records:
        print(f"No throttling records in {_LOG_PATH}")
        return

    # Split into sessions on SESSION_START
    sessions: list = []
    for record in records:
        if record.get("event") == "SESSION_START" or not sessions:
            sessions.append([])
        sessions[-1].append(record)

    for index, session in enumerate(sessions, 1):
        head = session[0]
        print(f"\n=== Session {index}/{len(sessions)}  started {head.get('utc', '?')} "
              f"({len(session)} records) ===")
        config = {k[4:]: v for k, v in head.items() if k.startswith("cfg_")}
        if config:
            print("  config: " + ", ".join(f"{k}={v}" for k, v in config.items() if v is not None))

        # Time spent in each status, measured between consecutive records
        time_in_status: Dict[str, float] = {}
        events: Dict[str, int] = {}
        percents = []
        for current, following in zip(session, session[1:] + [None]):
            events[current.get("event", "?")] = events.get(current.get("event", "?"), 0) + 1
            status = current.get("status")
            if status and following:
                time_in_status[status] = time_in_status.get(status, 0.0) + (
                    _epoch(following) - _epoch(current)
                )
            if current.get("pct") is not None:
                percents.append(current["pct"])

        total = sum(time_in_status.values()) or 1.0
        print("  time in status: " + ", ".join(
            f"{status} {seconds / 60:.1f}m ({seconds / total * 100:.0f}%)"
            for status, seconds in sorted(time_in_status.items(), key=lambda kv: -kv[1])
        ))
        if percents:
            print(f"  scout percent: min={min(percents):.4f} max={max(percents):.4f} "
                  f"last={percents[-1]:.4f}")
        print("  events: " + ", ".join(
            f"{event}x{count}" for event, count in sorted(events.items(), key=lambda kv: -kv[1])
        ))

        skip = {"SESSION_START"} if include_samples else {"SESSION_START", "SAMPLE"}
        timeline = [r for r in session if r.get("event") not in skip]
        if timeline:
            print("  timeline:")
            for record in timeline:
                print("    " + _format_record(record))


# Fields rendered in the fixed columns below, so they are not repeated as extras
_COLUMN_KEYS = {
    "utc", "event", "status", "step", "pct", "from_pct", "baseline_pct",
    "pending", "awaiting", "workers", "util", "pid",
}


def _format_record(record: Dict[str, Any]) -> str:
    """One log record as a fixed-column line plus its event-specific fields."""
    clock = (record.get("utc") or "?")[11:19]
    pct = record.get("pct")
    from_pct = record.get("from_pct")
    if pct is None:
        move = ""
    elif from_pct is None:
        move = f"{pct:.4f}"
    else:
        move = f"{from_pct:.4f}->{pct:.4f}"

    state = record.get("status") or ""
    if record.get("step"):
        state += f"/s{record['step']}"

    counts = (f"pend={record.get('pending', '?')} await={record.get('awaiting', '?')}"
              f" util={record.get('util', '?')}%")
    extras = " ".join(f"{k}={v}" for k, v in record.items() if k not in _COLUMN_KEYS)
    return f"{clock}  {record.get('event', '?'):<24} {state:<18} {move:<17} {counts:<34} {extras}".rstrip()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Summarize the LazyIVQueue throttling log.")
    parser.add_argument("path", nargs="?", default=_LOG_PATH, help="path to throttling.log")
    parser.add_argument("--last", type=int, default=0,
                        help="only summarize the last N records")
    parser.add_argument("--samples", action="store_true",
                        help="include periodic SAMPLE records in the timeline")
    args = parser.parse_args()

    records = _read_records(args.path)
    _summarize(records[-args.last:] if args.last else records, include_samples=args.samples)


if __name__ == "__main__":
    main()
