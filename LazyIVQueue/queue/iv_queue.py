"""IV Queue Manager - In-memory priority queue for Pokemon needing IV data."""
from __future__ import annotations

import asyncio
import heapq
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from LazyIVQueue.utils.logger import logger
from LazyIVQueue.utils.geo_utils import is_within_distance, COORDINATE_MATCH_THRESHOLD_METERS
from LazyIVQueue.utils.encounter_utils import normalize_encounter_id
import LazyIVQueue.config as AppConfig

# Fraction of baseline the scout percent resumes at after a circuit breaker release.
# The percent that tripped the breaker is known-bad, so recovery starts well below it.
CIRCUIT_BREAKER_RELEASE_FACTOR = 0.25


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

        # Self-Tuning Queue State (Poracle auto-rarity percentage load tuning; scouts run at max_concurrency)
        self._tuning_status: str = "NORMAL"  # NORMAL, BACKLOG_WARNING, PAUSED, RECOVERING, THROTTLED, BOOSTED, MANUALLY_PAUSED
        # Load-shedding stage, only ever set by the backlog path:
        # 0 = no shedding, 1 = celllist shed, 2 = auto-rarity shed
        self._throttled_step: int = 0
        self._current_scout_percent: float = self._baseline_scout_percent()
        self._manual_pause: bool = False
        self._pending_backlog_start_time: Optional[float] = None
        self._pause_start_time: Optional[float] = None
        self._pause_reason: str = ""
        self._baseline_awaiting_iv: int = 0
        self._total_pauses_triggered: int = 0
        self._recent_scout_outcomes: List[bool] = []
        self._last_concurrency_adjustment_time: float = time.time()

        # Worker utilization dead-band tracking: when awaiting_iv utilization stays
        # above too_many_workers_percent (or below too_few_workers_percent) continuously
        # for tuning_interval_seconds, the scout percent steps down (or up).
        self._high_util_start_time: Optional[float] = None
        self._low_util_start_time: Optional[float] = None
        self._last_utilization_pct: float = 0.0
        self._was_calibrating: bool = False
        

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

        self._current_concurrency = AppConfig.concurrency_scout
        self._initialized = True
        logger.info(f"IVQueue initialized with concurrency limit: {AppConfig.concurrency_scout}")

    async def update_concurrency(self, new_concurrency: int) -> None:
        """
        Update scout concurrency limit.

        Args:
            new_concurrency: New concurrency limit
        """
        async with self._queue_lock:
            old_concurrency = self._current_concurrency
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

            # When PAUSED (Circuit Breaker or Manual Pause), reject ALL incoming entries from webhooks completely to allow pending queue to drain to 0
            if self._tuning_status in ("PAUSED", "MANUALLY_PAUSED"):
                logger.debug(f"Rejecting incoming queue entry during {self._tuning_status}: {entry.pokemon_display}")
                return False

            # When THROTTLED by a real backlog (Stage 1 Backlog Relief, _throttled_step >= 1), reject incoming
            # background auto-rarity entries to protect VIP queue. Utilization-driven throttling (_throttled_step 0)
            # only tightens the percentage threshold and does not shed.
            if self._tuning_status == "THROTTLED" and self._throttled_step >= 1 and AppConfig.suppress_auto_rarity_on_backlog and (entry.list_type or "").startswith("auto_rarity"):
                logger.debug(f"Shedding incoming auto-rarity entry during {self._tuning_status}: {entry.pokemon_display}")
                return False

            key = entry.unique_key
            # Check for existing entry
            if key in self._entries:
                existing = self._entries[key]
                if not existing.is_removed:
                    # Update coordinates and identifiers when refined webhook arrives (e.g., nearby_stop -> wild)
                    if entry.lat and entry.lon:
                        existing.lat = entry.lat
                        existing.lon = entry.lon
                    if entry.spawnpoint_id:
                        existing.spawnpoint_id = entry.spawnpoint_id
                    if entry.seen_type:
                        existing.seen_type = entry.seen_type
                    # Refresh scout timer when worker converts nearby_stop/cell into wild spawn
                    if existing.is_scouting:
                        existing.scout_started_at = time.time()
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

                    threshold = 70.0
                    if entry.seen_type == "nearby_stop":
                        threshold = 200.0
                    elif entry.seen_type == "nearby_cell":
                        threshold = 150.0

                    if is_within_distance(entry.lat, entry.lon, lat, lon, threshold):
                        removed = self._remove_entry(key)
                        break

        if removed:
            self.record_completed_encounter(removed.encounter_id)
            if encounter_id:
                self.record_completed_encounter(encounter_id)

        return removed
                
    async def remove_by_cell_match(
        self, pokemon_id: int, form: Optional[int], s2_cell_id: str
    ) -> Optional[QueueEntry]:
        """
        Remove ONE entry matching pokemon and S2 cell (for nearby_cell scouting).
        """
        removed = None
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
                break

        return removed

    def _remove_entry(self, key: str) -> Optional[QueueEntry]:
        """
        Remove entry by key (internal, must hold lock).

        Decrements active scouts if the entry was scouting.

        Returns:
            The removed entry
        """
        if key not in self._entries:
            return None

        entry = self._entries.pop(key)
        # Mark as removed for lazy deletion from heap
        entry.is_removed = True

        # Decrement active scouts if this entry was scouting
        if entry.is_scouting:
            self._active_scouts = max(0, self._active_scouts - 1)
            entry.is_scouting = False

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

    def record_scout_outcome(self, success: bool) -> None:
        """Record the outcome of a scout request for dynamic concurrency tuning."""
        self._recent_scout_outcomes.append(success)
        if len(self._recent_scout_outcomes) > 30:
            self._recent_scout_outcomes.pop(0)

    def _get_pending_and_awaiting_counts(self) -> Tuple[int, int]:
        """Calculate current pending queue count (unscouted, eligible) and awaiting IV count."""
        now = time.time()
        awaiting_iv = sum(1 for e in self._entries.values() if (e.is_scouting or e.was_scouted) and not e.is_removed)
        unscouted = sum(1 for e in self._entries.values() if not e.is_scouting and not e.was_scouted and not e.is_removed and e.eligible_at <= now)
        return unscouted, awaiting_iv

    def _shed_celllist_backlog(self) -> int:
        """Purge unscouted celllist (nearby_cell / 9-point grid) entries during Stage 1 Step 1 load shedding."""
        shed_count = 0
        for key, entry in list(self._entries.items()):
            if entry.is_removed or entry.is_scouting or entry.was_scouted:
                continue
            if entry.list_type == "celllist" or entry.seen_type == "nearby_cell":
                entry.is_removed = True
                del self._entries[key]
                shed_count += 1
        if shed_count > 0:
            logger.opt(colors=True).info(
                f"<yellow>[Self-Tuning]</yellow> Shed {shed_count} pending celllist (9-point grid) scouts to protect VIP ivlist and rarity queues."
            )
        return shed_count

    def _shed_auto_rarity_backlog(self) -> int:
        """Purge unscouted auto-rarity entries during Stage 1 Step 2 load shedding."""
        shed_count = 0
        for key, entry in list(self._entries.items()):
            if entry.is_removed or entry.is_scouting or entry.was_scouted:
                continue
            if (entry.list_type or "").startswith("auto_rarity"):
                entry.is_removed = True
                del self._entries[key]
                shed_count += 1
        if shed_count > 0:
            logger.opt(colors=True).info(
                f"<yellow>[Self-Tuning]</yellow> Shed {shed_count} pending auto-rarity background scouts to protect VIP ivlist queue."
            )
        return shed_count

    def _clear_unscouted_backlog(self) -> int:
        """Purge all unscouted pending and held entries during Stage 2 Circuit Breaker emergency stop."""
        cleared_count = 0
        for key, entry in list(self._entries.items()):
            if entry.is_removed or entry.is_scouting or entry.was_scouted:
                continue
            entry.is_removed = True
            del self._entries[key]
            cleared_count += 1
        self._heap.clear()
        if cleared_count > 0:
            logger.opt(colors=True).warning(
                f"<red>[Self-Tuning]</red> CIRCUIT BREAKER: Purged {cleared_count} pending/held backlog entries."
            )
        return cleared_count

    @staticmethod
    def _baseline_scout_percent() -> float:
        """
        Baseline Poracle scout percentage the tuner centres on (from auto_rarity.iv_threshold).
        In the 'lazy' rank system iv_threshold is a rank count, not a percentage, so the
        percentage tuner falls back to 0.03 there.
        """
        thresh = float(AppConfig.iv_threshold)
        return thresh if thresh <= 1.0 else 0.03

    def _reset_tuning_to_baseline(self) -> None:
        """
        Return the tuner to a clean baseline state (internal; callers hold the lock or own
        exclusive access). Used by full resets and after config edits, since a stale
        THROTTLED/BOOSTED percent must not survive an operator-initiated change.
        """
        self._tuning_status = "NORMAL"
        self._throttled_step = 0
        self._current_scout_percent = self._baseline_scout_percent()
        self._last_baseline_pct = self._current_scout_percent
        self._pending_backlog_start_time = None
        self._pause_start_time = None
        self._pause_reason = ""
        self._baseline_awaiting_iv = 0
        self._high_util_start_time = None
        self._low_util_start_time = None
        self._last_utilization_pct = 0.0
        # Give the tuner a full fresh interval at baseline before it steps either way
        self._last_concurrency_adjustment_time = time.time()

    def _pause_drain_target(self) -> int:
        """Awaiting-IV count the circuit breaker must drain to before it releases."""
        base_iv = max(1, self._baseline_awaiting_iv)
        return max(1, int(base_iv * (AppConfig.awaiting_iv_drain_percent / 100.0)))

    def _status_for_percent(self, pct: float, baseline: float) -> str:
        """Map current scout percent to a display status relative to baseline."""
        if pct > baseline + 1e-9:
            return "BOOSTED"
        if pct < baseline - 1e-9:
            return "THROTTLED"
        return "NORMAL"

    def get_effective_scout_percent(self) -> float:
        """
        Return active Poracle scout percentage threshold (e.g. 0.03 = top 3.0% rarest spawns allowed).
        Returns 0.0 if circuit breaker is PAUSED or MANUALLY_PAUSED.
        """
        if self._tuning_status in ("PAUSED", "MANUALLY_PAUSED"):
            return 0.0
        return max(0.0, self._current_scout_percent)

    async def _evaluate_self_tuning(self) -> None:
        """
        Evaluate queue backlog and auto-tune dispatching using dynamic Poracle percentage filtering.
        Scouts always operate at configured concurrency (AppConfig.concurrency_scout) to maximize throughput.
        Supports bidirectional tuning: steps down under load and steps up when capacity is idle.

        Holds _queue_lock for the whole evaluation: this reads and mutates _entries (utilization
        counts, load shedding), so it must not interleave with add()/remove_by_match().
        Callers must NOT already hold the lock.
        """
        async with self._queue_lock:
            await self._evaluate_self_tuning_locked()

    async def _evaluate_self_tuning_locked(self) -> None:
        """Self-tuning evaluation body. Caller must hold _queue_lock."""
        # Ensure scout worker count is always synced to concurrency_scout.
        # Set directly rather than via update_concurrency() - we already hold the lock.
        if self._current_concurrency != AppConfig.concurrency_scout:
            logger.info(
                f"Scout concurrency updated: {self._current_concurrency} -> {AppConfig.concurrency_scout}"
            )
            self._current_concurrency = AppConfig.concurrency_scout

        baseline_pct = self._baseline_scout_percent()
        max_scout_pct = float(AppConfig.max_scout_percent)

        # Additive / subtractive step delta per tuning adjustment
        step_delta = round(float(AppConfig.tuning_step_factor), 4)

        if self._manual_pause:
            self._tuning_status = "MANUALLY_PAUSED"
            return

        if not AppConfig.self_tuning_enabled:
            if self._current_scout_percent != baseline_pct or self._tuning_status != "NORMAL":
                self._reset_tuning_to_baseline()
            return

        # Check if auto-rarity system is in initial calibration state
        rarity_calibrating = False
        if AppConfig.auto_rarity_enabled:
            from LazyIVQueue.rarity.manager import RarityManager
            if RarityManager._instance is not None and not RarityManager._instance.is_ready():
                rarity_calibrating = True

        # If calibrating, do not allow BOOSTED state
        if rarity_calibrating and self._current_scout_percent > baseline_pct:
            self._current_scout_percent = baseline_pct
            self._tuning_status = "NORMAL"

        # On the calibration -> ready transition, restart the utilization timers and step
        # cooldown so at least one full baseline tuning interval passes before any boost.
        # (Workers sit idle during calibration, so the low-utilization timer would
        # otherwise already read as "sustained" the moment calibration completes.)
        if rarity_calibrating:
            self._was_calibrating = True
        elif self._was_calibrating:
            self._was_calibrating = False
            self._high_util_start_time = None
            self._low_util_start_time = None
            self._last_concurrency_adjustment_time = time.time()
            logger.opt(colors=True).info(
                f"<green>[Self-Tuning]</green> Calibration complete. Holding baseline scout threshold "
                f"({baseline_pct:.4f}%) for at least one tuning interval ({AppConfig.tuning_interval_seconds}s) before tuning."
            )

        # Track dynamic baseline updates
        last_base = getattr(self, "_last_baseline_pct", None)
        if last_base != baseline_pct:
            if self._tuning_status == "NORMAL" or last_base is None or abs(self._current_scout_percent - (last_base or 0.03)) < 1e-6:
                self._current_scout_percent = baseline_pct
            self._last_baseline_pct = baseline_pct

        now = time.time()
        pending_count, current_awaiting_iv = self._get_pending_and_awaiting_counts()

        # Track sustained worker utilization (awaiting_iv as percent of scout workers).
        # Timers measure how long utilization has continuously stayed outside the
        # too_few/too_many dead band over the tuning interval time horizon.
        workers = max(1, self._current_concurrency)
        utilization_pct = (current_awaiting_iv / workers) * 100.0
        self._last_utilization_pct = utilization_pct

        if utilization_pct >= AppConfig.too_many_workers_percent:
            if self._high_util_start_time is None:
                self._high_util_start_time = now
        else:
            self._high_util_start_time = None

        if utilization_pct <= AppConfig.too_few_workers_percent:
            if self._low_util_start_time is None:
                self._low_util_start_time = now
        else:
            self._low_util_start_time = None

        # STAGE 2: Circuit Breaker PAUSED State (Hard Emergency Stop)
        if self._tuning_status == "PAUSED":
            pause_elapsed = now - (self._pause_start_time or now)
            target_awaiting_iv = self._pause_drain_target()

            time_condition = pause_elapsed >= AppConfig.pending_pause_duration
            pending_condition = pending_count == 0
            awaiting_condition = current_awaiting_iv <= target_awaiting_iv

            if time_condition and pending_condition and awaiting_condition:
                # Release conservatively: the percent that tripped the breaker is known-bad,
                # so resume below baseline and let the dead band climb back on its own.
                # No shedding while recovering - the queue is already empty.
                self._tuning_status = "RECOVERING"
                self._throttled_step = 0
                self._current_scout_percent = max(0.001, round(baseline_pct * CIRCUIT_BREAKER_RELEASE_FACTOR, 4))
                self._pause_start_time = None
                self._pending_backlog_start_time = None
                self._pause_reason = ""
                self._baseline_awaiting_iv = 0
                # Restart the utilization timers so the drain tail does not immediately
                # count as "sustained idle" and boost us straight back into trouble.
                self._last_concurrency_adjustment_time = now
                self._high_util_start_time = None
                self._low_util_start_time = None

                logger.opt(colors=True).info(
                    f"<green>[Self-Tuning]</green> CIRCUIT BREAKER RELEASED: Pause duration ({pause_elapsed:.1f}s >= {AppConfig.pending_pause_duration}s), "
                    f"pending queue drained (0), and awaiting IV drained ({current_awaiting_iv} <= {target_awaiting_iv}). "
                    f"Queue entering RECOVERING state with conservative Poracle scout threshold ({self._current_scout_percent:.4f}%). All {self._current_concurrency} scouts active."
                )
            return

        # Monitor pending backlog buildup
        if pending_count > 0:
            if self._pending_backlog_start_time is None:
                self._pending_backlog_start_time = now
            backlog_elapsed = now - self._pending_backlog_start_time

            # If we were BOOSTED above baseline, return to baseline if backlog persists or pending exceeds active scouts
            if self._current_scout_percent > baseline_pct:
                if backlog_elapsed >= (AppConfig.pending_backlog_seconds * 0.5) or pending_count > max(1, self._current_concurrency):
                    old_pct = self._current_scout_percent
                    self._current_scout_percent = baseline_pct
                    self._tuning_status = "NORMAL"
                    self._throttled_step = 0
                    self._last_concurrency_adjustment_time = now
                    logger.opt(colors=True).info(
                        f"<yellow>[Self-Tuning]</yellow> BACKLOG DETECTED: Returning boosted scout threshold to baseline "
                        f"({old_pct:.4f}% -> {baseline_pct:.4f}%)."
                    )

            # STAGE 2: Hard Circuit Breaker Pause if backlog stays persistent
            if backlog_elapsed >= AppConfig.hard_pause_backlog_seconds:
                self._tuning_status = "PAUSED"
                self._throttled_step = 2
                self._current_scout_percent = 0.0
                self._pause_start_time = now
                # Drain target is measured against the in-flight work that actually has to
                # drain at trip time, not against the worker count (which would inflate the
                # baseline and let the release fire immediately).
                self._baseline_awaiting_iv = max(1, current_awaiting_iv)
                self._total_pauses_triggered += 1
                self._pause_reason = f"Pending backlog persisted for {backlog_elapsed:.1f}s (hard limit: {AppConfig.hard_pause_backlog_seconds}s)"
                self._high_util_start_time = None
                self._low_util_start_time = None

                # Clear pending/held unscouted items
                cleared = self._clear_unscouted_backlog()
                target = self._pause_drain_target()
                logger.opt(colors=True).warning(
                    f"<red>[Self-Tuning]</red> CIRCUIT BREAKER TRIGGERED Stage 2: {self._pause_reason}. "
                    f"Pausing dispatching & webhook queueing for min {AppConfig.pending_pause_duration}s until pending=0 and awaiting IV <= {target}. Purged {cleared} backlog entries."
                )

            # STAGE 1: Throttled / Dynamic Rarity Load Tuning (Stepping DOWN)
            elif backlog_elapsed >= AppConfig.pending_backlog_seconds:
                # Check _throttled_step (not status) so a utilization-driven THROTTLED
                # state (step 0) still triggers Stage 1 shedding when a real backlog forms
                if self._throttled_step < 1:
                    self._tuning_status = "THROTTLED"
                    self._throttled_step = 1
                    old_pct = self._current_scout_percent
                    self._current_scout_percent = max(0.001, round(self._current_scout_percent - step_delta, 4))
                    self._last_concurrency_adjustment_time = now
                    logger.opt(colors=True).warning(
                        f"<yellow>[Self-Tuning]</yellow> STAGE 1 BACKLOG RELIEF: Pending backlog building up ({backlog_elapsed:.1f}s >= {AppConfig.pending_backlog_seconds}s). "
                        f"Tightening Poracle scout threshold ({old_pct:.4f}% -> {self._current_scout_percent:.4f}%). All {self._current_concurrency} scouts active."
                    )
                    self._shed_celllist_backlog()

                # Step down scout percentage further if backlog persists
                elif (now - self._last_concurrency_adjustment_time) >= AppConfig.tuning_interval_seconds:
                    old_pct = self._current_scout_percent
                    new_pct = max(0.0005, round(self._current_scout_percent - step_delta, 4))
                    self._current_scout_percent = new_pct
                    self._last_concurrency_adjustment_time = now

                    if self._current_scout_percent <= 0.005:
                        self._throttled_step = 2
                        self._shed_auto_rarity_backlog()

                    logger.opt(colors=True).warning(
                        f"<yellow>[Self-Tuning]</yellow> STAGE 1 BACKLOG PERSISTING: Tightening Poracle scout threshold ({old_pct:.4f}% -> {new_pct:.4f}%). "
                        f"All {self._current_concurrency} scouts active."
                    )

            elif backlog_elapsed >= (AppConfig.pending_backlog_seconds * 0.5):
                if self._tuning_status == "NORMAL":
                    self._tuning_status = "BACKLOG_WARNING"

        else:
            # Pending count is 0: clear backlog timer. Tuning is now driven by sustained
            # worker utilization over the tuning interval time horizon (dead band):
            #   utilization >= too_many_workers_percent -> step DOWN (workers saturated, backlog imminent)
            #   utilization <= too_few_workers_percent  -> step UP (workers starved, capacity idle)
            #   in between                              -> hold steady (equilibrium found)
            self._pending_backlog_start_time = None

            # The pending queue is clear, so backlog-driven load shedding no longer applies.
            # Clearing this is what keeps a stale step from an earlier backlog out of the
            # utilization-driven path (where it would silently shed celllist/auto-rarity).
            self._throttled_step = 0

            if self._tuning_status == "BACKLOG_WARNING":
                self._tuning_status = self._status_for_percent(self._current_scout_percent, baseline_pct)

            interval = AppConfig.tuning_interval_seconds
            step_ready = (now - self._last_concurrency_adjustment_time) >= interval
            high_sustained = self._high_util_start_time is not None and (now - self._high_util_start_time) >= interval
            low_sustained = self._low_util_start_time is not None and (now - self._low_util_start_time) >= interval

            if high_sustained and step_ready:
                # Too many workers busy: tighten the threshold before a pending backlog forms
                old_pct = self._current_scout_percent
                new_pct = max(0.0005, round(old_pct - step_delta, 4))
                if new_pct < old_pct:
                    self._current_scout_percent = new_pct
                    self._last_concurrency_adjustment_time = now
                    self._tuning_status = self._status_for_percent(new_pct, baseline_pct)
                    logger.opt(colors=True).warning(
                        f"<yellow>[Self-Tuning]</yellow> HIGH WORKER LOAD: {utilization_pct:.0f}% of scouts awaiting IV "
                        f"for {interval}s (>= {AppConfig.too_many_workers_percent:.0f}%). Tightening Poracle scout threshold "
                        f"({old_pct:.4f}% -> {new_pct:.4f}%)."
                    )

            elif low_sustained and step_ready and not rarity_calibrating:
                # Too few workers busy: expand the threshold to feed idle capacity
                if self._current_scout_percent < max_scout_pct:
                    old_pct = self._current_scout_percent
                    new_pct = min(max_scout_pct, round(self._current_scout_percent + step_delta, 4))
                    if new_pct > old_pct:
                        self._current_scout_percent = new_pct
                        self._last_concurrency_adjustment_time = now
                        # Still below baseline = still climbing back (RECOVERING);
                        # at or above baseline = NORMAL / BOOSTED
                        if new_pct >= baseline_pct - 1e-9:
                            self._tuning_status = self._status_for_percent(new_pct, baseline_pct)
                        else:
                            self._tuning_status = "RECOVERING"
                        logger.opt(colors=True).info(
                            f"<green>[Self-Tuning]</green> WORKERS IDLE: {utilization_pct:.0f}% of scouts awaiting IV "
                            f"for {interval}s (<= {AppConfig.too_few_workers_percent:.0f}%). Expanding Poracle scout threshold "
                            f"({old_pct:.4f}% -> {new_pct:.4f}%)."
                        )
                        if self._tuning_status == "NORMAL" and old_pct < baseline_pct:
                            logger.opt(colors=True).info(
                                f"<green>[Self-Tuning]</green> Queue fully recovered to NORMAL state at baseline scout threshold ({baseline_pct:.4f}%)."
                            )

            elif self._tuning_status == "RECOVERING" and self._high_util_start_time is None and self._low_util_start_time is None:
                # Utilization is inside the dead band while below baseline: this is a stable
                # equilibrium, not a recovery in progress, so report it as THROTTLED.
                self._tuning_status = self._status_for_percent(self._current_scout_percent, baseline_pct)

    async def pause_queue_manual(self) -> Dict[str, Any]:
        """Manually pause scout dispatching."""
        async with self._queue_lock:
            self._manual_pause = True
            self._tuning_status = "MANUALLY_PAUSED"
            logger.info("Self-Tuning: Queue dispatching MANUALLY PAUSED.")
            return {"status": "success", "message": "Queue dispatching paused manually.", "tuning_status": self._tuning_status}

    async def resume_queue_manual(self) -> Dict[str, Any]:
        """Manually resume scout dispatching from a clean baseline."""
        async with self._queue_lock:
            self._manual_pause = False
            self._reset_tuning_to_baseline()
            logger.info(
                f"Self-Tuning: Queue dispatching MANUALLY RESUMED at baseline ({self._current_scout_percent:.4f}%)."
            )
            return {"status": "success", "message": "Queue dispatching resumed manually.", "tuning_status": self._tuning_status}

    async def reset_tuning_state(self) -> Dict[str, Any]:
        """Reset self-tuning circuit breaker state, tuning percent, and error metrics."""
        async with self._queue_lock:
            self._manual_pause = False
            self._recent_scout_outcomes.clear()
            self._reset_tuning_to_baseline()
            logger.info(
                f"Self-Tuning: State reset. Scout threshold returned to baseline ({self._current_scout_percent:.4f}%)."
            )
            return {"status": "success", "message": "Self-tuning state reset.", "tuning_status": self._tuning_status}

    async def sync_self_tuning_config(self) -> None:
        """
        Sync self-tuning config settings after hot reload.

        Always returns the tuner to baseline: an operator config edit invalidates any
        THROTTLED/BOOSTED percent the tuner had converged on (and the baseline itself
        may have changed), so the tuner must re-converge from a known state.
        """
        async with self._queue_lock:
            self._reset_tuning_to_baseline()
            baseline_pct = self._current_scout_percent

            if self._current_concurrency != AppConfig.concurrency_scout:
                await self.update_concurrency(AppConfig.concurrency_scout)
            logger.info(
                f"Self-Tuning config synchronized: baseline_scout_pct={baseline_pct:.3f}%, "
                f"enabled={AppConfig.self_tuning_enabled}, "
                f"step_factor={getattr(AppConfig, 'tuning_step_factor', 0.05)}, "
                f"max_scout_pct={getattr(AppConfig, 'max_scout_percent', 0.20)}, "
                f"backlog_sec={AppConfig.pending_backlog_seconds}s, "
                f"hard_pause_sec={AppConfig.hard_pause_backlog_seconds}s, "
                f"pause_dur={AppConfig.pending_pause_duration}s, "
                f"drain_pct={AppConfig.awaiting_iv_drain_percent}%, "
                f"tuning_interval={AppConfig.tuning_interval_seconds}s, "
                f"worker_band={AppConfig.too_few_workers_percent:.0f}-{AppConfig.too_many_workers_percent:.0f}%"
            )

    def get_self_tuning_stats(self, pending_count: int = 0, awaiting_iv_count: int = 0) -> Dict[str, Any]:
        """Return self-tuning state, metrics, and configuration telemetry."""
        now = time.time()
        backlog_elapsed = round(now - self._pending_backlog_start_time, 1) if self._pending_backlog_start_time else 0.0
        pause_elapsed = round(now - self._pause_start_time, 1) if self._pause_start_time else 0.0
        pause_remaining = max(0.0, round(AppConfig.pending_pause_duration - pause_elapsed, 1)) if self._pause_start_time else 0.0
        
        failed_scouts = self._recent_scout_outcomes.count(False)
        total_recent = max(1, len(self._recent_scout_outcomes))
        error_rate_pct = round((failed_scouts / total_recent) * 100.0, 1) if self._recent_scout_outcomes else 0.0

        return {
            "enabled": AppConfig.self_tuning_enabled,
            "status": self._tuning_status,
            "throttled_step": self._throttled_step,
            "manual_pause": self._manual_pause,
            "current_concurrency": self._current_concurrency,
            "pending_backlog_seconds_config": AppConfig.pending_backlog_seconds,
            "hard_pause_backlog_seconds_config": AppConfig.hard_pause_backlog_seconds,
            "pending_pause_duration_config": AppConfig.pending_pause_duration,
            "awaiting_iv_drain_percent_config": AppConfig.awaiting_iv_drain_percent,
            "suppress_auto_rarity_config": AppConfig.suppress_auto_rarity_on_backlog,
            "pending_backlog_elapsed_sec": backlog_elapsed,
            "pause_elapsed_sec": pause_elapsed,
            "pause_remaining_sec": pause_remaining,
            "baseline_awaiting_iv": self._baseline_awaiting_iv,
            "target_awaiting_iv": self._pause_drain_target(),
            "current_awaiting_iv": awaiting_iv_count,
            "current_scout_percent": self._current_scout_percent,
            "baseline_scout_percent": float(getattr(AppConfig, "iv_threshold", 0.03)) if float(getattr(AppConfig, "iv_threshold", 0.03)) <= 1.0 else 0.03,
            "tuning_step_factor_config": float(getattr(AppConfig, "tuning_step_factor", 0.005)),
            "max_scout_percent_config": float(getattr(AppConfig, "max_scout_percent", 1.0)),
            "tuning_interval_seconds_config": AppConfig.tuning_interval_seconds,
            "too_many_workers_percent_config": AppConfig.too_many_workers_percent,
            "too_few_workers_percent_config": AppConfig.too_few_workers_percent,
            "worker_utilization_pct": round(self._last_utilization_pct, 1),
            "high_utilization_elapsed_sec": round(now - self._high_util_start_time, 1) if self._high_util_start_time else 0.0,
            "low_utilization_elapsed_sec": round(now - self._low_util_start_time, 1) if self._low_util_start_time else 0.0,
            "pause_reason": self._pause_reason,
            "total_pauses_triggered": self._total_pauses_triggered,
            "recent_error_rate_pct": error_rate_pct,
        }

    async def get_next_for_scout(self) -> Optional[QueueEntry]:
        """
        Get next highest priority entry not currently being scouted.

        Strictly respects active scouts limit (_active_scouts < _current_concurrency).
        Respects self-tuning circuit breaker state.
        Returns None if no entries available, paused, or at concurrency limit.
        """
        # Check self-tuning circuit breaker status first
        await self._evaluate_self_tuning()
        if self._tuning_status in ("PAUSED", "MANUALLY_PAUSED"):
            return None

        now = time.time()

        async with self._queue_lock:
            # Check active scouts limit strictly against current concurrency
            if self._active_scouts >= self._current_concurrency:
                return None

            # Helper to check if entry is eligible under current tuning state
            def is_eligible(e: QueueEntry) -> bool:
                if e.is_removed or e.is_scouting or e.was_scouted:
                    return False
                if e.eligible_at > now:
                    return False
                if self._tuning_status == "THROTTLED":
                    if self._throttled_step >= 1 and (e.list_type == "celllist" or e.seen_type == "nearby_cell"):
                        return False
                    if self._throttled_step >= 2 and (e.list_type or "").startswith("auto_rarity"):
                        return False
                return True

            # Clean up heap dead entries
            while self._heap:
                top = self._heap[0]
                top_key = top.unique_key
                if top_key not in self._entries or self._entries[top_key].is_removed or self._entries[top_key].is_scouting or self._entries[top_key].was_scouted:
                    heapq.heappop(self._heap)
                else:
                    break

            # Find best eligible entry from active candidates
            eligible = min(
                (e for e in self._entries.values() if is_eligible(e)),
                key=lambda e: (e.priority, e.timestamp),
                default=None
            )

            if eligible is None:
                return None

            eligible.is_scouting = True
            eligible.scout_started_at = now
            self._active_scouts += 1

            logger.debug(
                f"Dispatching for scout: {eligible.pokemon_display} in {eligible.area} "
                f"(active scouts: {self._active_scouts}, tuning_status: {self._tuning_status})"
            )
            return eligible

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
        # Evaluate self tuning state
        await self._evaluate_self_tuning()

        # Count entries waiting for IV match
        now = time.time()
        waiting_for_iv = sum(1 for e in self._entries.values() if (e.is_scouting or e.was_scouted) and not e.is_removed)
        held = sum(1 for e in self._entries.values() if not e.is_scouting and not e.was_scouted and not e.is_removed and e.eligible_at > now)
        pending = max(0, len(self._entries) - waiting_for_iv - held)

        return {
            "queue_size": len(self._entries),
            "pending": pending,
            "held": held,
            "awaiting_iv": waiting_for_iv,
            "active_scouts": self._active_scouts,
            "max_concurrency": AppConfig.concurrency_scout,
            "current_concurrency": self._current_concurrency,
            "available_slots": self.get_available_slots(),
            "iv_per_hour": self._compute_iv_per_hour(),
            "self_tuning": self.get_self_tuning_stats(pending_count=pending, awaiting_iv_count=waiting_for_iv),
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

        status_name = self._tuning_status.lower()
        _thresh = float(getattr(AppConfig, "iv_threshold", 0.03))
        base_pct = _thresh if _thresh <= 1.0 else 0.03
        rarity_str = f" ({self._current_scout_percent:.3f}%)"
        if status_name == "normal":
            color = "green"
        elif status_name == "recovering":
            color = "blue"
        elif status_name in ("throttled", "backlog_warning"):
            color = "yellow"
        else:
            color = "red"

        logger.opt(colors=True).info(
            f"<magenta>IVQueue Status:</magenta> <yellow>{pending} pending</yellow> | "
            f"<white>{held} held</white> | "
            f"<blue>{awaiting_iv} awaiting IV</blue> | <{color}>{status_name}{rarity_str}: {self._active_scouts}/{self._current_concurrency} scouts</{color}> | "
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

        async with self._queue_lock:
            for key, entry in list(self._entries.items()):
                if entry.disappear_time and entry.disappear_time < current_time:
                    state = "awaiting IV" if entry.is_scouting else "pending"
                    logger.opt(colors=True).debug(
                        f"<red>[x]</red> Expired: {entry.pokemon_display} in {entry.area} "
                        f"[encounter_id: {entry.encounter_id}] - despawned while {state}"
                    )

                    if entry.is_scouting:
                        self._active_scouts = max(0, self._active_scouts - 1)
                        entry.is_scouting = False

                    entry.is_removed = True
                    del self._entries[key]
                    removed_count += 1

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
        removed_count = 0
        timed_out_encounter_ids: list[str] = []

        async with self._queue_lock:
            for key, entry in list(self._entries.items()):
                # Check if scout started and exceeded timeout
                if entry.scout_started_at:
                    elapsed = current_time - entry.scout_started_at
                    timeout_threshold = max(AppConfig.timeout_iv, 10)
                    if elapsed > timeout_threshold:
                        logger.opt(colors=True).debug(
                            f"<red>[x]</red> Scout timeout: {entry.pokemon_display} in {entry.area} "
                            f"[encounter_id: {entry.encounter_id}] - no IV after {int(elapsed)}s"
                        )
                        if entry.encounter_id:
                            timed_out_encounter_ids.append(str(entry.encounter_id))

                        pokemon_display = entry.pokemon_display
                        seen_type = entry.seen_type

                        if entry.is_scouting:
                            self._active_scouts = max(0, self._active_scouts - 1)
                            entry.is_scouting = False

                        entry.is_removed = True
                        del self._entries[key]
                        removed_count += 1
                        # Update timeout stats by seen_type (skip unknown types)
                        if seen_type in self._seen_types:
                            self._timeouts_by_type[seen_type] = self._timeouts_by_type.get(seen_type, 0) + 1
                            self._timeouts_by_pokemon[seen_type][pokemon_display] = (
                                self._timeouts_by_pokemon[seen_type].get(pokemon_display, 0) + 1
                            )

        if removed_count > 0:
            ids_str = ", ".join(timed_out_encounter_ids) if timed_out_encounter_ids else "N/A"
            logger.opt(colors=True).info(
                f"<red>[x]</red> Cleaned up {removed_count} timed out scout entries (encounter_ids: [{ids_str}])"
            )

        return removed_count

    async def reset_queue_and_stats(self) -> Dict[str, Any]:
        """
        Reset queue entries, active scouts, completed encounters, all statistics counters,
        and the self-tuning state (scout percent returns to baseline).
        """
        async with self._queue_lock:
            queue_count = len(self._entries)
            self._heap.clear()
            self._entries.clear()
            self._active_scouts = 0

            # A full reset empties the queue, so any throttled/boosted percent the tuner
            # converged on is no longer meaningful - start over from baseline.
            self._manual_pause = False
            self._recent_scout_outcomes.clear()
            self._reset_tuning_to_baseline()

            # Reset stats counters
            self._queued_by_type = {t: 0 for t in self._seen_types}
            self._matches_by_type = {t: 0 for t in self._seen_types}
            self._early_iv_by_type = {t: 0 for t in self._seen_types}
            self._wild_early_by_type = {t: 0 for t in self._seen_types}
            self._timeouts_by_type = {t: 0 for t in self._seen_types}

            self._queued_by_group = {"vip": {}, "rarity": {}}
            self._queued_by_pokemon = {t: {} for t in self._seen_types}
            self._matches_by_pokemon = {t: {} for t in self._seen_types}
            self._early_iv_by_pokemon = {t: {} for t in self._seen_types}
            self._wild_early_by_pokemon = {t: {} for t in self._seen_types}
            self._timeouts_by_pokemon = {t: {} for t in self._seen_types}

            self._completed_encounters.clear()
            self._session_start = time.time()

            logger.info(
                f"Queue and stats reset via API. Cleared {queue_count} pending/scouting entries. "
                f"Self-tuning returned to baseline ({self._current_scout_percent:.4f}%)."
            )
            return {
                "cleared_entries": queue_count,
                "status": "ok",
                "tuning_status": self._tuning_status,
                "scout_percent": self._current_scout_percent,
            }
        
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
