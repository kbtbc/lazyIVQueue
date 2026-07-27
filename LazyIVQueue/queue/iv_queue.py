"""IV Queue Manager - In-memory priority queue for Pokemon needing IV data."""
from __future__ import annotations

import asyncio
import heapq
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from LazyIVQueue.utils.logger import logger
from LazyIVQueue.utils.geo_utils import is_within_distance, COORDINATE_MATCH_THRESHOLD_METERS
from LazyIVQueue.utils.encounter_utils import normalize_encounter_id
import LazyIVQueue.config as AppConfig


@dataclass(order=True)
class QueueEntry:
    """
    Entry in the IV queue.

    Ordering is by (priority, timestamp) for heapq.
    Lower priority number = higher priority (processed first).
    """

    # Comparison fields (used for heap ordering)
    priority: int = field(compare=True)
    timestamp: float = field(compare=True, default_factory=time.time)

    # Non-comparison fields
    pokemon_id: int = field(compare=False, default=0)
    form: Optional[int] = field(compare=False, default=None)
    area: str = field(compare=False, default="")
    lat: float = field(compare=False, default=0.0)
    lon: float = field(compare=False, default=0.0)
    spawnpoint_id: Optional[str] = field(compare=False, default=None)
    encounter_id: Optional[str] = field(compare=False, default=None)
    disappear_time: Optional[int] = field(compare=False, default=None)

    # Seen type for scouting strategy
    seen_type: str = field(compare=False, default="wild")  # "wild", "nearby_stop", or "nearby_cell"
    s2_cell_id: Optional[str] = field(compare=False, default=None)  # S2 level-15 cell ID (for nearby_cell)

    # Source list for tracking
    # "ivlist", "celllist", or a detailed auto_rarity string such as
    # "auto_rarity(rank=5)" / "auto_rarity(poracle-rare, pct=0.31)" / "auto_rarity(unknown)"
    list_type: str = field(compare=False, default="unknown")

    # Tracking fields
    is_removed: bool = field(compare=False, default=False)
    is_scouting: bool = field(compare=False, default=False)
    was_scouted: bool = field(compare=False, default=False)  # True after scout sent, waiting for IV
    scout_started_at: Optional[float] = field(compare=False, default=None)
    eligible_at: float = field(compare=False, default=0.0)  # unix timestamp; 0.0 = immediately eligible
    
    def __post_init__(self):
        if self.encounter_id:
            self.encounter_id = normalize_encounter_id(self.encounter_id)

    @property
    def unique_key(self) -> str:
        """Unique identifier for deduplication."""
        norm_eid = normalize_encounter_id(self.encounter_id)
        if norm_eid:
            return norm_eid
        if self.spawnpoint_id:
            return f"{self.spawnpoint_id}:{self.pokemon_id}"
        return f"{self.lat:.6f}:{self.lon:.6f}:{self.pokemon_id}"

    @property
    def pokemon_display(self) -> str:
        """Human-readable pokemon identifier."""
        from LazyIVQueue.utils.pokemon import get_pokemon_name
        name = get_pokemon_name(self.pokemon_id, self.form)
        if self.form is not None:
            return f"{name} ({self.pokemon_id}:{self.form})"
        return f"{name} ({self.pokemon_id})"


class IVQueueManager:
    """
    Priority queue manager for Pokemon needing IV data.

    Features:
    - Heap-based priority queue (lower priority number = higher priority)
    - Deduplication by encounter_id/spawnpoint_id
    - Concurrent scout tracking with semaphore
    - Proximity-based matching for removal
    """

    _instance: Optional[IVQueueManager] = None
    _lock: asyncio.Lock = asyncio.Lock()

    def __init__(self) -> None:
        self._heap: List[QueueEntry] = []
        self._entries: Dict[str, QueueEntry] = {}  # key -> entry for O(1) lookup
        self._scout_semaphore: Optional[asyncio.Semaphore] = None
        self._current_concurrency: int = 0
        self._active_scouts: int = 0
        self._queue_lock: asyncio.Lock = asyncio.Lock()
        self._initialized: bool = False

        # Stats counters by seen_type
        self._seen_types = ["wild", "nearby_stop", "nearby_cell"]
        self._queued_by_type: Dict[str, int] = {t: 0 for t in self._seen_types}
        self._matches_by_type: Dict[str, int] = {t: 0 for t in self._seen_types}
        self._early_iv_by_type: Dict[str, int] = {t: 0 for t in self._seen_types}
        self._wild_early_by_type: Dict[str, int] = {t: 0 for t in self._seen_types}
        self._timeouts_by_type: Dict[str, int] = {t: 0 for t in self._seen_types}

        # Per-Pokemon queued counts by list group:
        # "vip" = ivlist/celllist entries, "rarity" = auto_rarity entries
        self._queued_by_group: Dict[str, Dict[str, int]] = {"vip": {}, "rarity": {}}

        # Per-Pokemon breakdown by seen_type (key: seen_type -> pokemon_display -> count)
        self._queued_by_pokemon: Dict[str, Dict[str, int]] = {t: {} for t in self._seen_types}
        self._matches_by_pokemon: Dict[str, Dict[str, int]] = {t: {} for t in self._seen_types}
        self._early_iv_by_pokemon: Dict[str, Dict[str, int]] = {t: {} for t in self._seen_types}
        self._wild_early_by_pokemon: Dict[str, Dict[str, int]] = {t: {} for t in self._seen_types}
        self._timeouts_by_pokemon: Dict[str, Dict[str, int]] = {t: {} for t in self._seen_types}

        # Completed encounters cache (encounter_id -> timestamp)
        self._completed_encounters: Dict[str, float] = {}

        # Session start time for IV/hour rate calculation
        self._session_start: float = time.time()
        

    @classmethod
    async def get_instance(cls) -> IVQueueManager:
        """Get or create singleton instance."""
        async with cls._lock:
            if cls._instance is None:
                cls._instance = IVQueueManager()
                await cls._instance.initialize()
            return cls._instance

    async def initialize(self) -> None:
        """Initialize the queue manager."""
        if self._initialized:
            return

        self._scout_semaphore = asyncio.Semaphore(AppConfig.concurrency_scout)
        self._current_concurrency = AppConfig.concurrency_scout
        self._initialized = True
        logger.info(f"IVQueue initialized with concurrency limit: {AppConfig.concurrency_scout}")

    async def update_concurrency(self, new_concurrency: int) -> None:
        """
        Update scout concurrency limit by recreating the semaphore.

        Note: This is a best-effort operation. Active scouts will continue
        until they complete. New concurrency takes effect for new scouts.

        Args:
            new_concurrency: New concurrency limit
        """
        async with self._queue_lock:
            old_concurrency = self._current_concurrency
            # Create new semaphore with new limit
            self._scout_semaphore = asyncio.Semaphore(new_concurrency)
            self._current_concurrency = new_concurrency
            logger.info(
                f"Scout concurrency updated: {old_concurrency} -> {new_concurrency}"
            )
    
    def record_completed_encounter(self, encounter_id: Optional[str]) -> None:
        """Mark an encounter ID as completed (IV received or scouted)."""
        norm_eid = normalize_encounter_id(encounter_id)
        if norm_eid:
            self._completed_encounters[norm_eid] = time.time()

    def is_encounter_completed(self, encounter_id: Optional[str]) -> bool:
        """Check if an encounter ID was already completed/processed."""
        norm_eid = normalize_encounter_id(encounter_id)
        if not norm_eid:
            return False
        ts = self._completed_encounters.get(norm_eid)
        if ts is None:
            return False
        if time.time() - ts > 900:  # 15 minutes TTL
            del self._completed_encounters[norm_eid]
            return False
        return True
        
    async def add(self, entry: QueueEntry) -> bool:
        """
        Add entry to queue.

        Args:
            entry: QueueEntry to add

        Returns:
            True if added, False if duplicate
        """
        async with self._queue_lock:
            # Check if encounter has already been completed or received IV
            if self.is_encounter_completed(entry.encounter_id):
                logger.debug(f"Skipping re-queue for already completed encounter: {entry.encounter_id}")
                return False

            key = entry.unique_key
            # Check for duplicate
            if key in self._entries:
                return False

            # Add to heap and lookup dict
            heapq.heappush(self._heap, entry)
            self._entries[key] = entry

            # Update stats by seen_type (skip unknown types)
            seen_type = entry.seen_type
            if seen_type in self._seen_types:
                self._queued_by_type[seen_type] = self._queued_by_type.get(seen_type, 0) + 1
                self._queued_by_pokemon[seen_type][entry.pokemon_display] = (
                    self._queued_by_pokemon[seen_type].get(entry.pokemon_display, 0) + 1
                )

            # Update per-Pokemon queued counts by list group (vip vs rarity)
            group = "rarity" if (entry.list_type or "").startswith("auto_rarity") else "vip"
            self._queued_by_group[group][entry.pokemon_display] = (
                self._queued_by_group[group].get(entry.pokemon_display, 0) + 1
            )

            logger.debug(
                f"Added to queue: {entry.pokemon_display} in {entry.area} "
                f"[{entry.seen_type}] (priority {entry.priority}, queue size: {len(self._entries)})"
            )
            return True

    async def remove_by_match(
        self,
        encounter_id: Optional[str],
        lat: float,
        lon: float,
        pokemon_id: Optional[int] = None,
        form: Optional[int] = None,
        spawnpoint_id: Optional[str] = None,
    ) -> Optional[QueueEntry]:
        removed = None
        target_eid = normalize_encounter_id(encounter_id)

        async with self._queue_lock:
            # Step 1: Normalized encounter_id match (handles signed vs unsigned 64-bit int64)
            if target_eid:
                for key, entry in list(self._entries.items()):
                    if entry.is_removed:
                        continue
                    entry_eid = normalize_encounter_id(entry.encounter_id)
                    if entry_eid and entry_eid == target_eid:
                        removed = self._remove_entry(key)
                        break

            # Step 1.5: Spawnpoint ID match
            if not removed and spawnpoint_id:
                for key, entry in list(self._entries.items()):
                    if entry.is_removed:
                        continue
                    if entry.spawnpoint_id and entry.spawnpoint_id == spawnpoint_id:
                        if pokemon_id is None or entry.pokemon_id == pokemon_id:
                            removed = self._remove_entry(key)
                            break

            # Step 2: Proximity / Pokemon ID fallback
            if not removed and pokemon_id is not None:
                for key, entry in list(self._entries.items()):
                    if entry.is_removed or entry.pokemon_id != pokemon_id:
                        continue
                    
                    # Treat form 0 and None as default form
                    e_form = 0 if entry.form is None else entry.form
                    p_form = 0 if form is None else form
                    if e_form != p_form:
                        continue

                    threshold = 150.0
                    if entry.seen_type == "nearby_stop":
                        threshold = 300.0
                    elif entry.seen_type == "nearby_cell":
                        threshold = 350.0

                    if is_within_distance(entry.lat, entry.lon, lat, lon, threshold):
                        removed = self._remove_entry(key)
                        break

        if removed:
            self.record_completed_encounter(removed.encounter_id)
            if encounter_id:
                self.record_completed_encounter(encounter_id)

        # Release semaphore outside the lock if entry was scouting
        if removed and removed.is_scouting and self._scout_semaphore:
            self._scout_semaphore.release()

        return removed
                
    async def remove_by_cell_match(
        self, pokemon_id: int, form: Optional[int], s2_cell_id: str
    ) -> Optional[QueueEntry]:
        """
        Remove ONE entry matching pokemon and S2 cell (for nearby_cell scouting).
        """
        removed = None
        was_scouting = False
        async with self._queue_lock:
            for key, entry in list(self._entries.items()):
                if entry.is_removed:
                    continue
                # Must be a nearby_cell entry with matching s2_cell_id
                if entry.seen_type != "nearby_cell" or entry.s2_cell_id != s2_cell_id:
                    continue
                # Must match pokemon_id
                if entry.pokemon_id != pokemon_id:
                    continue
                # Form matching (0 == None for default form)
                e_form = 0 if entry.form is None else entry.form
                p_form = 0 if form is None else form
                if e_form != p_form:
                    continue

                # Found match - remove
                removed = self._remove_entry(key)
                if removed:
                    was_scouting = removed.is_scouting
                break

        # Release semaphore outside the lock if entry was scouting
        if was_scouting:
            self._scout_semaphore.release()

        return removed

    def _remove_entry(self, key: str) -> Optional[QueueEntry]:
        """
        Remove entry by key (internal, must hold lock).

        If the entry was scouting (is_scouting=True), the caller MUST release
        the semaphore after releasing the queue lock.

        Returns:
            The removed entry (check entry.is_scouting to know if semaphore needs release)
        """
        if key not in self._entries:
            return None

        entry = self._entries.pop(key)
        # Mark as removed for lazy deletion from heap
        entry.is_removed = True

        # Decrement active scouts if this entry was holding a semaphore slot
        if entry.is_scouting:
            self._active_scouts = max(0, self._active_scouts - 1)

        logger.debug(
            f"Removed from queue: {entry.pokemon_display} "
            f"(queue size: {len(self._entries)})"
        )
        return entry

    def record_match(self, pokemon_display: str, seen_type: str) -> None:
        """Record a successful IV match (after scouting)."""
        if seen_type not in self._seen_types:
            return
        self._matches_by_type[seen_type] = self._matches_by_type.get(seen_type, 0) + 1
        self._matches_by_pokemon[seen_type][pokemon_display] = (
            self._matches_by_pokemon[seen_type].get(pokemon_display, 0) + 1
        )

    def record_early_iv(self, pokemon_display: str, seen_type: str) -> None:
        """Record an early IV (received before scouting, no hold was configured)."""
        if seen_type not in self._seen_types:
            return
        self._early_iv_by_type[seen_type] = self._early_iv_by_type.get(seen_type, 0) + 1
        self._early_iv_by_pokemon[seen_type][pokemon_display] = (
            self._early_iv_by_pokemon[seen_type].get(pokemon_display, 0) + 1
        )

    def record_wild_early_iv(self, pokemon_display: str, seen_type: str) -> None:
        """Record a wild early IV (received during deliberate hold window, no scout dispatched)."""
        if seen_type not in self._seen_types:
            return
        self._wild_early_by_type[seen_type] = self._wild_early_by_type.get(seen_type, 0) + 1
        self._wild_early_by_pokemon[seen_type][pokemon_display] = (
            self._wild_early_by_pokemon[seen_type].get(pokemon_display, 0) + 1
        )

    def record_timeout(self, pokemon_display: str, seen_type: str) -> None:
        """Record a scout timeout."""
        if seen_type not in self._seen_types:
            return
        self._timeouts_by_type[seen_type] = self._timeouts_by_type.get(seen_type, 0) + 1
        self._timeouts_by_pokemon[seen_type][pokemon_display] = (
            self._timeouts_by_pokemon[seen_type].get(pokemon_display, 0) + 1
        )

    async def get_next_for_scout(self) -> Optional[QueueEntry]:
        """
        Get next highest priority entry not currently being scouted.

        Uses semaphore to limit concurrent scouts.
        Returns None if no entries available or at concurrency limit.
        """
        # Try to acquire semaphore slot without blocking
        acquired = self._scout_semaphore.locked()
        if acquired:
            # Semaphore is fully locked, check if we can get a slot
            try:
                # Non-blocking acquire attempt
                got_slot = self._scout_semaphore._value > 0
                if not got_slot:
                    return None
            except Exception:
                return None

        # Acquire the semaphore slot
        await self._scout_semaphore.acquire()

        async with self._queue_lock:
            # Clean up heap (lazy deletion) and find valid entry
            while self._heap:
                entry = self._heap[0]
                key = entry.unique_key

                # Skip if already removed, being scouted, or already scouted (waiting for IV)
                if key not in self._entries or entry.is_removed or entry.is_scouting or entry.was_scouted:
                    heapq.heappop(self._heap)
                    continue

                # Hold check: entry not yet eligible for scouting
                if entry.eligible_at > time.time():
                    # Top entry is held — find best eligible entry from active entries
                    # rather than blocking all lower-priority scouts during the hold window
                    now = time.time()
                    eligible = min(
                        (e for e in self._entries.values()
                         if not e.is_removed and not e.is_scouting and not e.was_scouted
                         and e.eligible_at <= now),
                        key=lambda e: (e.priority, e.timestamp),
                        default=None,
                    )
                    if eligible is None:
                        self._scout_semaphore.release()
                        return None
                    eligible.is_scouting = True
                    eligible.scout_started_at = now
                    self._active_scouts += 1
                    logger.debug(
                        f"Dispatching for scout (held bypass): {eligible.pokemon_display} in {eligible.area} "
                        f"(active scouts: {self._active_scouts})"
                    )
                    return eligible

                # Found valid entry
                heapq.heappop(self._heap)
                entry.is_scouting = True
                entry.scout_started_at = time.time()
                self._active_scouts += 1

                logger.debug(
                    f"Dispatching for scout: {entry.pokemon_display} in {entry.area} "
                    f"(active scouts: {self._active_scouts})"
                )
                return entry

            # No entries available, release semaphore
            self._scout_semaphore.release()
            return None

    async def mark_scout_sent(self, entry: QueueEntry, success: bool) -> None:
        """
        Mark a scout request as sent (API call completed).

        Note: This does NOT release the semaphore. The semaphore stays held
        until the entry is removed (match found, early IV, or timeout).
        This ensures we limit the number of "in-flight" scouts.

        Args:
            entry: The queue entry that was scouted
            success: Whether the scout API call was successful
        """
        async with self._queue_lock:
            # Mark entry as scouted (API call sent), waiting for IV match
            # Keep is_scouting=True to indicate semaphore is still held
            entry.was_scouted = True
            # Note: is_scouting stays True, semaphore stays held

        status = "sent" if success else "failed"
        logger.debug(
            f"Scout {status} for {entry.pokemon_display}, "
            f"active scouts: {self._active_scouts}, waiting for IV match"
        )

    def get_active_scouts_count(self) -> int:
        """Return count of currently active scouts."""
        return self._active_scouts

    def get_queue_size(self) -> int:
        """Return current queue size (excluding entries being scouted)."""
        return len(self._entries)

    def get_available_slots(self) -> int:
        """Return number of available scout slots."""
        return AppConfig.concurrency_scout - self._active_scouts

    def _get_total_from_type_dict(self, type_dict: Dict[str, int]) -> int:
        """Sum all values in a seen_type dict."""
        return sum(type_dict.values())

    def _build_type_stats(self, type_dict: Dict[str, int]) -> Dict[str, Any]:
        """Build stats dict with total and per-type breakdown."""
        return {
            "total": self._get_total_from_type_dict(type_dict),
            "wild": type_dict.get("wild", 0),
            "nearby_stop": type_dict.get("nearby_stop", 0),
            "nearby_cell": type_dict.get("nearby_cell", 0),
        }

    async def get_stats(self) -> Dict[str, Any]:
        """Return queue statistics."""
        # Count entries waiting for IV match
        now = time.time()
        waiting_for_iv = sum(1 for e in self._entries.values() if e.was_scouted and not e.is_removed)
        held = sum(1 for e in self._entries.values() if not e.is_scouting and not e.is_removed and e.eligible_at > now)
        pending = len(self._entries) - waiting_for_iv - held

        return {
            "queue_size": len(self._entries),
            "pending": pending,
            "held": held,
            "awaiting_iv": waiting_for_iv,
            "active_scouts": self._active_scouts,
            "max_concurrency": AppConfig.concurrency_scout,
            "available_slots": self.get_available_slots(),
            "iv_per_hour": self._compute_iv_per_hour(),
            "session": {
                "total_queued": self._build_type_stats(self._queued_by_type),
                "total_matches": self._build_type_stats(self._matches_by_type),
                "total_early_iv": self._build_type_stats(self._early_iv_by_type),
                "total_wild_early": self._build_type_stats(self._wild_early_by_type),
                "total_timeouts": self._build_type_stats(self._timeouts_by_type),
                "by_pokemon_group": {
                    "vip": self._queued_by_group.get("vip", {}),
                    "rarity": self._queued_by_group.get("rarity", {}),
                },
                "by_pokemon": {
                    "wild": {
                        "queued": self._queued_by_pokemon.get("wild", {}),
                        "matches": self._matches_by_pokemon.get("wild", {}),
                        "early_iv": self._early_iv_by_pokemon.get("wild", {}),
                        "wild_early": self._wild_early_by_pokemon.get("wild", {}),
                        "timeouts": self._timeouts_by_pokemon.get("wild", {}),
                    },
                    "nearby_stop": {
                        "queued": self._queued_by_pokemon.get("nearby_stop", {}),
                        "matches": self._matches_by_pokemon.get("nearby_stop", {}),
                        "early_iv": self._early_iv_by_pokemon.get("nearby_stop", {}),
                        "wild_early": self._wild_early_by_pokemon.get("nearby_stop", {}),
                        "timeouts": self._timeouts_by_pokemon.get("nearby_stop", {}),
                    },
                    "nearby_cell": {
                        "queued": self._queued_by_pokemon.get("nearby_cell", {}),
                        "matches": self._matches_by_pokemon.get("nearby_cell", {}),
                        "early_iv": self._early_iv_by_pokemon.get("nearby_cell", {}),
                        "wild_early": self._wild_early_by_pokemon.get("nearby_cell", {}),
                        "timeouts": self._timeouts_by_pokemon.get("nearby_cell", {}),
                    },
                },
            },
        }

    def get_next_entries_preview(self, count: int = 10) -> List[Dict[str, Any]]:
        """
        Get preview of the next N entries that will be processed.

        Args:
            count: Number of entries to preview (default: 10)

        Returns:
            List of entry info dicts in priority order
        """
        # Build a sorted list of valid entries (not yet scouted)
        valid_entries = []
        for entry in self._entries.values():
            if not entry.is_removed and not entry.is_scouting and not entry.was_scouted:
                valid_entries.append(entry)

        # Sort by priority, then timestamp
        valid_entries.sort(key=lambda e: (e.priority, e.timestamp))

        # Return preview of top N
        preview = []
        for entry in valid_entries[:count]:
            preview.append({
                "pokemon": entry.pokemon_display,
                "area": entry.area,
                "priority": entry.priority,
                "list_type": entry.list_type,
                "seen_type": entry.seen_type,
                "lat": round(entry.lat, 6),
                "lon": round(entry.lon, 6),
                "encounter_id": entry.encounter_id,
            })

        return preview

    def _compute_iv_per_hour(self) -> Dict[str, Any]:
        """Compute IV/hour rates since session start (matches only)."""
        elapsed_hours = max((time.time() - self._session_start) / 3600, 1 / 3600)  # floor at 1s

        def rate(seen_types: list) -> float:
            match_count = sum(self._matches_by_type.get(t, 0) for t in seen_types)
            return round(match_count / elapsed_hours, 2)

        return {
            "nearby_cell": rate(["nearby_cell"]),
            "normal": rate(["wild", "nearby_stop"]),
            "combined": rate(["wild", "nearby_stop", "nearby_cell"]),
            "elapsed_hours": round(elapsed_hours, 4),
        }

    def log_iv_per_hour(self) -> None:
        """Log current IV/hour rates as a standalone entry."""
        iv_hr = self._compute_iv_per_hour()
        logger.opt(colors=True).info(
            f"<magenta>[IV/hr]</magenta> "
            f"combined=<blue>{iv_hr['combined']}</blue> | "
            f"normal=<cyan>{iv_hr['normal']}</cyan> | "
            f"cell=<red>{iv_hr['nearby_cell']}</red>"
        )

    def log_queue_status(self) -> None:
        """Log current queue status with next 10 entries preview."""
        queue_size = len(self._entries)
        heap_size = len(self._heap)

        # Count entries by state:
        # - awaiting_iv: is_scouting=True (scout sent, holding semaphore, waiting for IV)
        # - held: is_scouting=False, eligible_at > now (in wild_scout_delay window)
        # - pending: is_scouting=False, eligible_at <= now (ready, waiting for semaphore slot)
        now = time.time()
        awaiting_iv = sum(1 for e in self._entries.values() if e.is_scouting and not e.is_removed)
        held = sum(1 for e in self._entries.values() if not e.is_scouting and not e.is_removed and e.eligible_at > now)
        pending = queue_size - awaiting_iv - held

        # Calculate totals
        total_queued = self._get_total_from_type_dict(self._queued_by_type)
        total_matches = self._get_total_from_type_dict(self._matches_by_type)
        total_early = self._get_total_from_type_dict(self._early_iv_by_type)
        total_wild_early = self._get_total_from_type_dict(self._wild_early_by_type)
        total_timeouts = self._get_total_from_type_dict(self._timeouts_by_type)

        logger.opt(colors=True).info(
            f"<magenta>IVQueue Status:</magenta> <yellow>{pending} pending</yellow> | "
            f"<white>{held} held</white> | "
            f"<blue>{awaiting_iv} awaiting IV</blue> | heap={heap_size} | "
            f"<cyan>Session: {total_queued} queued</cyan> / <green>{total_matches} matches</green> / <magenta>{total_early} early</magenta> / <cyan>{total_wild_early} wild_early</cyan> / <red>{total_timeouts} timeouts</red>"
        )

        if queue_size > 0:
            preview = self.get_next_entries_preview(10)
            if preview:
                logger.debug("Next entries in queue:")
                for i, entry in enumerate(preview, 1):
                    logger.debug(
                        f"  {i}. {entry['pokemon']} in {entry['area']} "
                        f"(priority {entry['priority']})"
                    )

    async def cleanup_expired(self) -> int:
        """
        Remove entries that have expired (disappear_time has passed).

        Returns:
            Number of entries removed
        """
        current_time = int(time.time())
        removed_count = 0
        semaphores_to_release = 0

        async with self._queue_lock:
            for key, entry in list(self._entries.items()):
                if entry.disappear_time and entry.disappear_time < current_time:
                    state = "awaiting IV" if entry.is_scouting else "pending"
                    logger.opt(colors=True).debug(
                        f"<red>[x]</red> Expired: {entry.pokemon_display} in {entry.area} "
                        f"[encounter_id: {entry.encounter_id}] - despawned while {state}"
                    )

                    # Track if we need to release semaphore
                    if entry.is_scouting:
                        semaphores_to_release += 1
                        self._active_scouts = max(0, self._active_scouts - 1)

                    entry.is_removed = True
                    del self._entries[key]
                    removed_count += 1

        # Release semaphores outside the lock
        for _ in range(semaphores_to_release):
            self._scout_semaphore.release()

        if removed_count > 0:
            logger.opt(colors=True).info(
                f"<red>[x]</red> Cleaned up {removed_count} expired queue entries"
            )

        return removed_count
        
    async def cleanup_timed_out_scouts(self) -> int:
        """
        Remove entries that timed out waiting for IV data.

        Any entry with scout_started_at that exceeds timeout_iv is removed.
        This covers both stuck scouts and scouts waiting for IV data.

        Uses AppConfig.timeout_iv to determine timeout threshold.

        Returns:
            Number of entries removed
        """
        current_time = time.time()
        timeout_threshold = AppConfig.timeout_iv
        removed_count = 0
        semaphores_to_release = 0
        timed_out_encounter_ids: list[str] = []

        async with self._queue_lock:
            for key, entry in list(self._entries.items()):
                # Check if scout started and exceeded timeout
                if entry.scout_started_at:
                    elapsed = current_time - entry.scout_started_at
                    if elapsed > timeout_threshold:
                        logger.opt(colors=True).debug(
                            f"<red>[x]</red> Scout timeout: {entry.pokemon_display} in {entry.area} "
                            f"[encounter_id: {entry.encounter_id}] - no IV after {int(elapsed)}s"
                        )
                        if entry.encounter_id:
                            timed_out_encounter_ids.append(str(entry.encounter_id))

                        pokemon_display = entry.pokemon_display
                        seen_type = entry.seen_type

                        # Track if we need to release semaphore
                        if entry.is_scouting:
                            semaphores_to_release += 1
                            self._active_scouts = max(0, self._active_scouts - 1)

                        entry.is_removed = True
                        del self._entries[key]
                        removed_count += 1
                        # Update timeout stats by seen_type (skip unknown types)
                        if seen_type in self._seen_types:
                            self._timeouts_by_type[seen_type] = self._timeouts_by_type.get(seen_type, 0) + 1
                            self._timeouts_by_pokemon[seen_type][pokemon_display] = (
                                self._timeouts_by_pokemon[seen_type].get(pokemon_display, 0) + 1
                            )

        # Release semaphores outside the lock
        for _ in range(semaphores_to_release):
            self._scout_semaphore.release()

        if removed_count > 0:
            ids_str = ", ".join(timed_out_encounter_ids) if timed_out_encounter_ids else "N/A"
            logger.opt(colors=True).info(
                f"<red>[x]</red> Cleaned up {removed_count} timed out scout entries (encounter_ids: [{ids_str}])"
            )

        return removed_count
        
    async def cleanup_stale_heap_entries(self) -> int:
        """
        Remove stale entries from the heap (lazy deletion cleanup).

        Entries marked is_removed or no longer in self._entries are physically
        pruned from the heap. Called periodically to prevent unbounded heap growth,
        especially when held entries (eligible_at) block lazy cleanup in get_next_for_scout().

        Returns:
            Number of stale entries removed from the heap.
        """
        async with self._queue_lock:
            before = len(self._heap)
            clean = [e for e in self._heap if not e.is_removed and e.unique_key in self._entries]
            if len(clean) < before:
                heapq.heapify(clean)
                self._heap = clean
                removed = before - len(clean)
                logger.debug(f"Heap cleanup: pruned {removed} stale entries (heap: {before} → {len(clean)})")
                return removed
        return 0
