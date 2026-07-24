# LazyIVQueue

Pokemon IV scouting queue that receives webhooks from Golbat, filters by priority list and Koji geofences, and dispatches scout requests to Dragonite.

## Setup

Requires Python 3.12+

```bash
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

**Koji Geofences**
- `koji.filter_with_koji` - Enable geofence filtering (default: `true`). Set to `false` to skip geofence checks
- `koji.url` - Full Koji base URL (e.g., `http://koji.example.com:8080`). Alternative to IP/Port
- `koji.ip` / `koji.port` - Koji host and port (default: `127.0.0.1:8080`)
- `koji.token` - Koji bearer token for authentication
- `koji.project_name` - Koji project name containing the geofences to use

**Security**
- `security.allowed_ips` - List of IPs allowed to POST webhooks (e.g., `["127.0.0.1", "192.168.1.100"]`)
- `security.headers` - Header auth (format: `HeaderName: Value`)

**Priority Lists**
- `ivlist` - Priority list of Pokemon to scout for `wild`/`nearby_stop` seen_types (first = highest priority)
  - `"pokemon_id"` - Match any form (e.g., `"1"` matches Bulbasaur any form)
  - `"pokemon_id:form"` - Match specific form only (e.g., `"3:0"` matches Venusaur form 0)
  - Example: `["Pokemon A", "Pokemon B:0", "Pokemon C"]` - A is top priority, then B form 0, then C
- `celllist` - Priority list for `nearby_cell` seen_type (same format as ivlist)
  - Celllist entries are always processed before ivlist entries, so only insert really important ones here
  - Uses 9x9 pattern (9 coordinates) to cover S2 level-15 cell
- `denylist` - Block list of Pokemon to never scout (same format as ivlist/celllist)
  - Applies to all seen_types — denylisted Pokemon are silently dropped before queueing
  - When auto rarity is enabled, the denylist prevents rare-but-unwanted Pokemon from being queued

**Scout Settings**
- `scout.concurrency` - Max concurrent scout requests - Should match the number of scouts you have set in Dragonite
- `scout.timeout_iv` - Seconds to wait for IV data before removing from queue (default: 120)
- `scout.wild_scout_delay` - Seconds to hold `wild`/`nearby_stop` entries before scouting (default: `0`). Set to `15` if your scanner sends encounters immediately to prevent wasting a scout on natural IV spawns

**Geofence Settings**
- `geofences.file_path` - Use a local geofence json file instead of a Koji project.  Location relative to root, ie. ./geofence.json (default: empty =Koji)
- `geofences.expire_cache_seconds` - How long to cache geofences before expiring (default: 1800)
- `geofences.refresh_cache_seconds` - How often to refresh geofences from Koji (default: 1800)

**Auto Rarity**
- `auto_rarity.enabled` - Enable dynamic rarity-based queueing (default: `false`)
- `auto_rarity.system` - Rarity ranking system to use: `"lazy"` (rank-based) or `"poracle"` (percentage-based, default: `"lazy"`)
- `auto_rarity.calibration_minutes` - Minutes to collect spawn data before rankings are used (default: 5)
- `auto_rarity.iv_threshold` - Queue Pokemon with rarity rank below this (default: 50, lower = rarer)
- `auto_rarity.cell_threshold` - Cell scout threshold (default: 10)
- `auto_rarity.ranking_interval_seconds` - How often to recalculate rankings (default: 120)
- `auto_rarity.cleanup_interval_seconds` - How often to remove despawned Pokemon from tracking (default: 60)
- `auto_rarity.poracle` - Thresholds for the Poracle rarity system (used when `system="poracle"`). Represents top percentage of spawns:
  - `ultra_rare_percent` - Top X% (default: 0.01 = top 0.01%)
  - `very_rare_percent` - Top X% (default: 0.03)
  - `rare_percent` - Top X% (default: 0.5)
  - `uncommon_percent` - Top X% (default: 1.0)

## Auto Rarity

When `auto_rarity.enabled=true`, LazyIVQueue dynamically tracks Pokemon spawn rarity and queues rare Pokemon automatically.

### How it works

1. **Webhook**: Configure Golbat to send ALL Pokemon spawns to `/webhook`. The system automatically handles both rarity tracking and queue filtering from the single endpoint.
2. **Rarity Tracking**: The system tracks active spawns per area (or globally if Koji disabled)
3. **Calibration**: During the calibration period, only ivlist/celllist Pokemon are queued
4. **Dynamic Queueing**: After calibration, Pokemon with rarity rank below the threshold are queued

### Rarity Systems

LazyIVQueue supports two ways to calculate rarity, controlled by `auto_rarity.system` in `config.json`:

1. **Lazy (Rank-Based)**: (Default) Rarity is area-based on absolute rank (e.g. iv_threshold=50 for top 50 rarest Pokemon each area).
2. **Poracle (Percentage-Based)**: Rarity is determined by the percentage of total active spawns globally. This mimics PoracleJS categories but numbered asc from rarest (1 = Unseen, 2 = Ultra Rare, 3 = Very Rare, 4 = Rare, 5 = Uncommon).  (e.g. iv_threshold=3 for Vary Rare)   Rarity level can be further fine-tuned below.

**Example Rarity Configuration:**
```json
"auto_rarity": {
    "enabled": true,
    "system": "poracle",
    "poracle": {
        "ultra_rare_percent": 0.01,
        "very_rare_percent": 0.03,
        "rare_percent": 0.5,
        "uncommon_percent": 1.0
    }
}
```

If `system` is set to `"poracle"`, Pokemon that fall under the `rare_percent` (or rarer) will be automatically queued, and the UI dashboard will show classifications using Poracle tier groupings.

### Priority System (lower = higher priority)

- **Tier 0 (0-999)**: VIP lists (celllist + ivlist) - position in list determines sub-priority
- **Tier 1000+**: auto_rarity entries - 1000 for unknown, 1000+rank for ranked Pokemon

This ensures ivlist/celllist entries ALWAYS take priority over auto_rarity entries.

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
- **Rarity Rankings**: See the top rarest Pokemon currently tracked in your area or globally (uses Poracle categories if `system="poracle"` is configured).
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
- `auto_rarity` settings - thresholds, intervals
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
