# LazyIVQueue

Pokemon scouting queue that receives webhooks from Golbat, selects useful targets inside your configured geofences, and asks Dragonite to send scout workers to them.

Use the **[`pinpoint` branch](https://github.com/kbtbc/lazyIVQueue/tree/pinpoint)** for both missing-IV scouting and pinpoint scouting. Pinpoint support was added in October 2026; it is not included in `develop` as of November 17, 2026.

## What the tool does today

LazyIVQueue has two jobs:

1. **Get missing IVs.** For a `wild` or `nearby_stop` sighting without IVs, queue a scout if the species/form is in `ivlist`, or qualifies through enabled, calibrated auto-rarity. IV-less `nearby_cell` sightings use `celllist` instead.
2. **Find the exact location of an IV-known Pokemon.** With pinpoint enabled, queue selected `nearby_stop` or `nearby_cell` sightings that already have IVs but still have approximate coordinates. Selection can be based on IV percentage, PvP rank, or an explicit `ivlist` opt-in.

Both jobs respect geofences, the denylist, and queue controls. A configurable hold gives regular workers time to supply the result first. Dragonite chooses the scout worker; LazyIVQueue submits coordinates through `/scout/v2` and waits for Golbat webhooks. It does not edit your regular routes.

| Incoming sighting | Selection | Scout destination / completion |
|---|---|---|
| `wild`, no IVs | `ivlist` or auto-rarity | Reported spawn coordinates; wait for IV data |
| `nearby_stop`, no IVs | `ivlist` or auto-rarity | Reported Pokestop coordinates; wait for an IV-bearing exact sighting |
| `nearby_stop`, IVs already known | Pinpoint rules | Reported Pokestop coordinates; wait for an IV-bearing exact sighting of the same encounter |
| `nearby_cell`, no IVs | `celllist` only | Nine-point grid in the reported S2 level-15 cell |
| `nearby_cell`, IVs already known | Pinpoint rules | Same grid; require the same encounter to resolve the pinpoint |
| Exact sighting with IVs | No new scout needed | Can complete an existing target |

The grid and Pokestop visit are search attempts, not guaranteed finds. Pinpoint success means receiving the matching exact sighting, normally `wild` or `encounter`. It does **not** require a verified despawn timer: better timing depends on what Golbat can establish from the resulting scan/spawnpoint data. Timeouts and expired entries are removed, not counted as successful pinpoints.

## Questions from mappers

### What does "derived nearby IVs started working" mean?

It refers to **Golbat's ability to derive Pokemon stats from the encounter ID and nearby data**, on a Golbat build that supports it. Golbat can therefore send a nearby sighting with attack/defense/stamina already populated before a worker visits the actual spawn to encounter it. LazyIVQueue consumes those values; it does not calculate them.

IV derivation is a deterministic reconstruction for supported cases, not a statistical guess at the IVs. That does not make every derived field universally reliable: species-specific or event behavior can require exceptions, and Golbat gives observed encounter data precedence. The half-hour uncertainty discussed below belongs to despawn derivation, not to the IV calculation.

Knowing the IVs and knowing the location are separate things. A `nearby_stop` marker can still be at the Pokestop rather than the actual spawn. A `nearby_cell` marker is approximate too. Neither the presence of IVs nor a successful pinpoint guarantees a verified expiry time.

Before pinpoint support, an IV-bearing webhook was treated as completed IV work. Once Golbat supplied IVs for most nearby sightings, the original queue had much less to do. **Missing-IV scouting was not switched off:** it remains the fallback for eligible sightings that actually lack IVs. Pinpoint added a reason to scout useful Pokemon whose IVs are already known.

### A rare Pokemon is near a Pokestop outside my route. Do I need to send a worker there?

**Not necessarily to learn its IVs; usually yes to find its exact position if regular workers will not get close enough.** The stop can appear in a nearby response without being a point on your route.

- If Golbat reports it **without IVs**, `ivlist` or auto-rarity can request an IV scout.
- If Golbat reports it **with IVs**, it needs to match a pinpoint rule to request a location scout. A hundo/nundo or configured PvP rank can qualify. For a rare species you want located at **any IV**, put it in `ivlist` and set `pinpoint.include_ivlist` to `true`.
- **Auto-rarity alone does not select IV-known Pokemon for pinpointing.** An ordinary-IV rare nearby Pokemon can otherwise be skipped.
- The reported coordinates must pass your geofence and denylist checks. Being off-route does not mean being outside the geofence; these are different checks.

LazyIVQueue can submit the scout automatically once a qualifying webhook arrives. It cannot discover a spawn that no scanner reports, or reconstruct a missing location from an encounter ID alone.

### Does it still scan rare wild spawns? How often?

**Yes, when a reported `wild` spawn lacks IVs and passes `ivlist` or auto-rarity selection.** No Pokestop association is needed for that path. If its exact location and IVs are already known, there is nothing additional for this queue to obtain.

There is no fixed rate. With working derivation, many sightings already contain IVs, so missing-IV scouts can be uncommon or zero in a particular window. Pinpoint scouts may still be busy, including for common species with exceptional IVs or PvP ranks. Those are different jobs.

To measure your own installation, inspect `GET /stats` at the start and end of a window: `queue.session.total_queued.wild` counts queued targets classified as wild, not all wild Pokemon detected and not necessarily dispatched scouts. Targets can complete during their hold. Logs distinguish `Queued:` (missing-IV work), `Queued pinpoint:`, `Scout sent:`, and `Pinpointed`. The existing IV/hour and match counters also include pinpoint completions; they do not count only newly discovered IVs. Counters reset with the session or `/reset`.

### What about nearby Pokemon without a Pokestop?

If Golbat emits a usable `nearby_cell` webhook, this branch can scout its cell. That is different from an unlocated entry in the newer nearby snapshot format: the Golbat source inspected for this deployment skips snapshot entries without a fort/Pokestop association. Enabling `nearby_cell_pokemon` alone does not supply their missing coordinates. LazyIVQueue cannot queue a webhook that never arrives, and searching a cell guessed from the worker's location would not guarantee finding that Pokemon.

### Does pinpointing learn a reliable spawnpoint timer or expand scan range?

**No guarantee of either.** These are separate outputs:

- **Derived IVs:** stats obtained from the encounter ID and sighting data, subject to Golbat's supported cases and precedence rules.
- **Derived Pokemon expiry:** an estimate based on the spawn ID's half-hour phase when a stored timer is unavailable. A 60-minute spawn seen in its first half can be assigned an expiry 30 minutes early; a later sighting can correct the Pokemon record. The estimate is unverified and is not written into `spawnpoint.despawn_sec`.
- **Learned spawnpoint timer:** the game's real time-till-hidden (TTH) from a wild GMO sighting, in the final 90 seconds, can teach Golbat `despawn_sec`. An encounter is not required. A normal wild sighting outside that window may locate the spawnpoint without learning its timer.

Stored timers take precedence over derived expiry, so stale timers need fresh observed TTH to correct them. Pinpoint sends a location scout after its hold; it does not schedule repeated visits around TTH windows or require a verified timer before completing. It therefore is **not a post-bootstrap timer-learning scheduler**.

Derivation operates on data the scanner already supplies. It adds no scan radius or fixed number of cells; coverage still depends on the scanner, accepted sightings, and scan rules. Filtering unknown-timer spawnpoints into a shorter route is different from scheduling arrival inside their TTH windows. This project does not implement that scheduling or establish whether a particular Koji/Dragonite version does.

Checked against Golbat main at `1a3b4e8` on November 17, 2026: [spawnpoint learning](https://github.com/UnownHash/Golbat/blob/1a3b4e82aaeead3fda964153432c11deed90872e/decoder/spawnpoint.go), [expiry derivation](https://github.com/UnownHash/Golbat/blob/1a3b4e82aaeead3fda964153432c11deed90872e/decoder/pokemon_decode.go), and [data precedence and limitations](https://github.com/UnownHash/Golbat/blob/1a3b4e82aaeead3fda964153432c11deed90872e/docs/pokemon-precedence.md). Your installed Golbat build may differ.

### Example: valuable IVs plus explicitly selected rare species

Merge these settings into your own config, keeping your own species lists, endpoints, geofences, and worker capacity:

```json
{
    "ivlist": ["201", "824", "859"],
    "pinpoint": {
        "enabled": true,
        "min_iv": 100,
        "max_iv": 0,
        "pvp_max_rank": 1,
        "include_ivlist": true,
        "priority": 500,
        "hold_seconds": 15
    }
}
```

This selects hundos, nundos, PvP rank #1s, and the listed species with approximate positions at any IV. The same `ivlist` also selects those species when IVs are missing. Set `auto_rarity.enabled` to `true` separately if you also want automatic selection of other IV-less wild/near-stop species. Keep wanted species out of `denylist`, which takes precedence.

These are example choices, **not the shipped defaults**: the supplied example config disables pinpoint and auto-rarity, leaves PvP selection and `include_ivlist` off, and uses a 60-second pinpoint hold. The OVH deployment checked on November 17, 2026 used a 15-second hold, hundo/nundo/PvP #1 rules, auto-rarity enabled, and **`include_ivlist: false`**. Its old global rate-limit reset monitor was disabled; missing-IV scouting was still enabled.

## Setup

Requires Python 3.12+

```bash
git clone --branch pinpoint https://github.com/kbtbc/lazyIVQueue.git
cd lazyIVQueue

# Recommended:
# Create virtual environment
py -3.12 -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Activate (Linux/Mac)
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy config files
cp LazyIVQueue/config/example.config.json LazyIVQueue/config/config.json
```

## Configuration

All configuration is done via `config.json`.

### config.json

**Server & Logging**
- `server.host` / `server.port` - Server bind address (default: `0.0.0.0:7070`)
- `server.max_body_size` - Max webhook payload size (default: 10485760)
- `logging.level` - Log level (default: `INFO`)
- `logging.file` - Log to file (default: `false`)

**Dragonite Scout API**
- `dragonite.api_base_url` - Dragonite Scout API endpoint (e.g., `http://127.0.0.1:7272`)
- `dragonite.global_rate_limit_monitor.enabled` - Opt in to polling and resetting Dragonite's global rate limiter (default: `false`; restart required). This is separate from scout-queue monitoring and self-tuning, which still work when the reset monitor is disabled.

**Security**
- `security.allowed_ips` - List of IPs allowed to POST webhooks (e.g., `["127.0.0.1", "192.168.1.100"]`)
- `security.headers` - Header auth (format: `HeaderName: Value`)

**Priority Lists**
- `ivlist` - Priority list of Pokemon to scout for `wild`/`nearby_stop` seen_types (first = highest priority)
  - `"pokemon_id"` - Match any form (e.g., `"1"` matches Bulbasaur any form)
  - `"pokemon_id:form"` - Match specific form only (e.g., `"3:0"` matches Venusaur form 0)
  - Example: `["Pokemon A", "Pokemon B:0", "Pokemon C"]` - A is top priority, then B form 0, then C
- `celllist` - Priority list for `nearby_cell` seen_type (same format as ivlist)
  - Both lists use their zero-based list position as priority; celllist has no unconditional precedence over ivlist
  - Uses a 3x3 grid (9 coordinates) within the S2 level-15 cell
- `denylist` - Block list of Pokemon to never scout (same format as ivlist/celllist)
  - Applies to all seen_types — denylisted Pokemon are silently dropped before queueing
  - When auto rarity is enabled, the denylist prevents rare-but-unwanted Pokemon from being queued

**Scout Settings**
- `scout.concurrency` - Max concurrent scout requests - Should match the number of scouts you have set in Dragonite
- `scout.timeout_iv` - Seconds to wait for IV data before removing from queue (default: 120)
- `scout.wild_scout_delay` - Seconds to hold `wild`/`nearby_stop` entries before scouting (default: `0`). Set to `15` if your scanner sends encounters immediately to prevent wasting a scout on natural IV spawns

**Pinpoint** (Pokemon with IVs but no exact position)
- On compatible Golbat builds, `nearby_stop` / `nearby_cell` Pokemon can arrive with derived IVs while still having approximate positions. Pinpoint requests a scout to obtain the exact sighting; it does not validate despawn accuracy.
- `pinpoint.enabled` - Turn pinpoint scouting on (default: `false`)
- `pinpoint.min_iv` / `pinpoint.max_iv` - Queue when IV% is at or above `min_iv` (default `100`) or at or below `max_iv` (default `0`); `null` disables a rule
- `pinpoint.pvp_max_rank` - Queue when the best PvP league rank is at or below this (default: `null`, off)
- `pinpoint.include_ivlist` - Also queue IV-known, approximately located `ivlist` species at any IV (default: `false`). Uses list priority when the ivlist rule selects the target; an earlier matching IV/PvP rule uses `pinpoint.priority`.
- `pinpoint.priority` - Queue priority (default: `500`: behind list positions below 500, ahead of auto-rarity)
- `pinpoint.hold_seconds` - Wait before scouting so a regular worker can encounter it first (default: `60`)
- A successful pinpoint requires an IV-bearing exact sighting with the same encounter ID, never a nearby Pokemon of the same species. `scout.timeout_iv` limits the wait after dispatch; a timeout or expiry is not success.

**Koji Geofences**
- `koji.filter_with_koji` - Enable geofence filtering (default: `true`). Set to `false` to skip geofence checks
- `koji.url` - Full Koji base URL (e.g., `http://koji.example.com:8080`). Alternative to IP/Port
- `koji.ip` / `koji.port` - Koji host and port (default: `127.0.0.1:8080`)
- `koji.token` - Koji bearer token for authentication
- `koji.project_name` - Koji project name containing the geofences to use

**Geofence Settings**
- `geofences.file_path` - Use a local geofence json file instead of a Koji project.  Location relative to root, ie. ./geofence.json (default: empty =Koji)
- `geofences.expire_cache_seconds` - How long to cache geofences before expiring (default: 1800)
- `geofences.refresh_cache_seconds` - How often to refresh geofences from Koji (default: 1800)

**Auto Rarity**
- `auto_rarity.enabled` - Enable dynamic rarity-based queueing (default: `false`)
- `auto_rarity.calibration_minutes` - Minutes to collect spawn data before rankings are used (default: 5)
- `auto_rarity.ranking_interval_seconds` - How often to cleanup expired spawns and recalculate rankings (default: 120)
- `auto_rarity.rarity` - Display/log classification thresholds for each species/form's percentage of the global active census:
  - `ultra_rare_percent` - Default: 0.01 (0.01%)
  - `very_rare_percent` - Default: 0.03 (0.03%)
  - `rare_percent` - Default: 0.5 (0.5%)
  - `uncommon_percent` - Default: 1.0 (1.0%)

**Self-Tuning Queue**
- `self_tuning.enabled` - Enable dynamic queue tuning (default: `true`)
- `self_tuning.iv_baseline_percent` - Baseline maximum species/form frequency for auto-rarity admission (`0.03` means **0.03%**, not 3%). Self-tuning can adjust the effective threshold.
- `self_tuning.circuit_breaker.throttle_backlog_seconds` - Seconds of pending backlog before Stage 1 load shedding (default: 45)
- `self_tuning.circuit_breaker.hard_pause_backlog_seconds` - Seconds of pending backlog before hard circuit breaker pause (default: 180)
- `self_tuning.circuit_breaker.min_hard_pause_duration` - Minimum hard pause duration in seconds (default: 60)
- `self_tuning.circuit_breaker.worker_recovery_percent` - Percentage of awaiting-IV workers to drain before circuit breaker release (default: 50)
- `self_tuning.tuning_interval_seconds` - Time horizon for tuning decisions (default: 30)
- `self_tuning.tuning_step_factor` - Step size for adjusting scout baseline percentage (default: 0.005)
- `self_tuning.max_scout_percent` - Maximum scout baseline percentage (default: 1.0)
- `self_tuning.too_many_workers_percent` - Worker utilization % to trigger throttling (default: 50.0)
- `self_tuning.too_few_workers_percent` - Worker utilization % to trigger boosting (default: 5.0)

## Auto Rarity

When `auto_rarity.enabled=true`, LazyIVQueue dynamically tracks Pokemon spawn rarity and queues rare Pokemon automatically.

### How it works

1. **Webhook**: Configure Golbat to send ALL Pokemon spawns to `/webhook`. The system automatically handles both rarity tracking and queue filtering from the single endpoint.
2. **Rarity Tracking**: The system tracks active spawns per area (or globally if Koji disabled)
3. **Calibration**: Automatic rarity selection waits for calibration. Explicit ivlist/celllist selection and enabled pinpoint rules do not require rarity calibration.
4. **Dynamic Queueing**: After calibration, IV-less `wild`/`nearby_stop` Pokemon whose global species/form frequency is at or below the effective threshold can be queued; entries not yet in the ranking cache are treated as rare. IV-less `nearby_cell` Pokemon must be in `celllist`. IV-known approximate sightings use the independent pinpoint rules, with no auto-rarity fallback.

### Rarity System

LazyIVQueue uses a percentage-based rarity system. Rarity is determined by the percentage of total active spawns globally. Rarity level can be further fine-tuned below.

Admission uses the queue's effective scout percentage, initially `self_tuning.iv_baseline_percent`. The `auto_rarity.rarity` thresholds below label rarity tiers for display/logging; changing `rare_percent` alone does not set the queue's admission threshold. The census counts incoming sighting records until expiry, so it is an operational frequency measure rather than a deduplicated count of unique encounters.

**Example Rarity Configuration:**
```json
"auto_rarity": {
    "enabled": true,
    "rarity": {
        "ultra_rare_percent": 0.01,
        "very_rare_percent": 0.03,
        "rare_percent": 0.5,
        "uncommon_percent": 1.0
    }
}
```

### Priority System (lower = higher priority)

- **Explicit lists**: zero-based position in celllist or ivlist; keep lists short for their intended priority advantage
- **Pinpoint**: `pinpoint.priority` (default 500), or list position when selected by the ivlist rule
- **Auto-rarity**: 1000 for unknown frequency, otherwise `1000 + int(frequency_percent * 10000)`

Lower numbers are dispatched first, subject to holds, worker capacity, and queue controls.

## Self-Tuning Queue

When `self_tuning.enabled=true` (default), LazyIVQueue dynamically adjusts the scout baseline percentage based on queue load and worker utilization.

### How it works

1. **Worker Utilization Monitoring**: The system tracks how many scouts are actively awaiting IV data as a percentage of total scout concurrency.
2. **Throttling**: If utilization stays above `too_many_workers_percent` for `tuning_interval_seconds`, the scout baseline is reduced (fewer Pokemon queued).
3. **Boosting**: If utilization stays below `too_few_workers_percent` for `tuning_interval_seconds`, the scout baseline is increased (more Pokemon queued).
4. **Backlog Protection**: If pending queue builds up, the system sheds lower-priority entries and may trigger a circuit breaker pause.

### Circuit Breaker States

- **NORMAL**: Operating at or above baseline
- **BACKLOG_WARNING**: Pending backlog building up
- **THROTTLED**: Scout baseline reduced due to high utilization or backlog
- **PAUSED**: Hard pause due to persistent backlog (circuit breaker tripped)
- **RECOVERING**: Resuming from paused/throttled state, climbing back to baseline
- **BOOSTED**: Scout baseline above normal due to idle capacity

### Manual Control

- `POST /tuning/pause` - Manually pause scout dispatching
- `POST /tuning/resume` - Manually resume from baseline
- `POST /tuning/reset` - Reset circuit breaker state and error metrics
- `POST /tuning/update` - Update self-tuning configuration

### Golbat Configuration

You need a webhook configuration in Golbat:

```toml
[[webhooks]]
url = "http://localhost:7070/webhook"
types = ["pokemon"]
headers = ["HeaderName: Value"]
```

> Use `types = ["pokemon"]` (not `"pokemon_no_iv"`) so Golbat also sends IV-bearing encounters — required for early IV detection and avoids wasting scout slots on Pokemon already have IV data.

## Run

### Local

```bash
python -m LazyIVQueue.lazyivqueue
```

### Docker

```bash
cp example.docker-compose.yml docker-compose.yml
docker-compose up -d --build
```

## Dashboard

LazyIVQueue includes an interactive web dashboard accessible at the root path (`http://localhost:7070/`).

**Features:**
- **Real-Time Stats**: View queue status, session match rates, scout success vs timeouts, and active spawn counts.
- **Rarity Rankings**: See the top rarest Pokemon currently tracked in your area or globally.
- **In-App Config Editor**: Click the "Edit Config" button in the dashboard to view and hot-reload `config.json` directly from the browser without restarting the service or manually calling the reload endpoint.

## Endpoints

- `GET /` - Interactive Dashboard with real-time stats and in-app config editor
- `POST /webhook` - Receives Pokemon webhooks from Golbat (ivlist/celllist filtering)
- `GET /health` - Health check
- `GET /stats` - Queue, scout, and rarity statistics — includes IV/hour rates (nearby_cell, normal, combined)
- `GET /queue` - Queue preview (next N entries, use `?count=N`)
- `GET /rarity` - Auto Rarity rankings per area (use `?area=AreaName&limit=100`)
- `GET /config` - Current configuration summary
- `POST /reload` - Hot-reload config.json values without restarting
- `GET /config/raw` - Get raw config.json content
- `POST /config/raw` - Update raw config.json content and reload
- `POST /reset` - Reset queue, clear all entries, reset stats, restart auto-rarity calibration
- `POST /tuning/pause` - Manually pause scout dispatching
- `POST /tuning/resume` - Manually resume scout dispatching from baseline
- `POST /tuning/reset` - Reset self-tuning circuit breaker state and error metrics
- `POST /tuning/update` - Update self-tuning configuration block

### Examples

```bash
# Health check
curl -s http://localhost:7070/health

# Per-type session breakdown (queued / matches / early_iv / timeouts)
curl -s http://localhost:7070/stats | jq '.queue.session | {queued: .total_queued, matches: .total_matches, early_iv: .total_early_iv, timeouts: .total_timeouts}'

# Full stats (queue + scout coordinator + rarity)
curl -s http://localhost:7070/stats | jq .

# Preview next 20 queue entries
curl -s "http://localhost:7070/queue?count=20" | jq .

# Rarity rankings for a specific area (top 50)
curl -s "http://localhost:7070/rarity?area=CityCenter&limit=50" | jq .

# Current config summary
curl -s http://localhost:7070/config | jq .

# Hot-reload config.json
curl -X POST http://localhost:7070/reload
```

> Add `-H "HeaderName: Value"` to any request if your server uses header auth.

### Hot Reload

The `/reload` endpoint allows you to update config.json values without restarting the service, can be triggered via dashboard.

**Reloadable settings:**
- `ivlist`, `celllist`, `denylist` - Priority/block lists
- `pinpoint` - Enablement, IV/PvP/list selection, priority, and hold for new targets
- `auto_rarity` settings - baselines, intervals
- `self_tuning` settings - tuning parameters
- `scout.concurrency`, `scout.timeout_iv`, `scout.wild_scout_delay` - Scout settings
- `geofences` cache settings

**Requires restart:**
- `server` settings (host/port)
- `dragonite` API settings
- `koji` credentials and URL
- `auto_rarity.enabled`, `koji.filter_with_koji`
- `logging` settings

## Log Prefixes

### Queue Operations
- `[+]` - Pokemon added to queue
- `[>]` - Scout request sent to Dragonite
- `[<]` - IV match found (scout successful) or Early IV (received before scout)
- `[x]` - Scout timeout (no IV received within timeout_iv seconds)
- `[!]` - Scout request failed

### Census/Rarity (when auto_rarity.enabled=true)
- `[*]` - Census status during calibration / New area discovered
- `[~]` - Census status after calibration / Census cleanup
