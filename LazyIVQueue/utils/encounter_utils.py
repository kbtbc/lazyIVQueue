"""Encounter ID utilities for normalizing 64-bit uint64/int64 encounter IDs."""

from typing import Any, Optional


def normalize_encounter_id(eid: Any) -> Optional[str]:
    """
    Normalize encounter ID to standard string representation of 64-bit unsigned integer.
    Handles integers, strings, floats, and negative signed 64-bit ints (two's complement).
    Returns None if empty, zero, or invalid.
    """
    if eid is None or eid == "" or eid == 0 or eid == "0":
        return None
    try:
        val = int(eid)
        if val == 0:
            return None
        if val < 0:
            val += (1 << 64)
        return str(val)
    except (ValueError, TypeError):
        s = str(eid).strip()
        return s if s and s != "0" else None