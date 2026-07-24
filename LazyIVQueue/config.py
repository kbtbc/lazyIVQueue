import os
import json
from LazyIVQueue.utils.logger import logger
from typing import List, Optional, Dict

# load config.json
CONFIG_PATH = os.path.join(os.getcwd(), "LazyIVQueue", "config", "config.json")
CONFIG_EXAMPLE_PATH = os.path.join(os.getcwd(), "LazyIVQueue", "config", "example.config.json")

def load_config() -> Dict[str, any]:
    try:
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)
        logger.info(f"✅ Loaded config from {CONFIG_PATH}")
        return config
    except FileNotFoundError:
        logger.error(f"❌ Config file not found at {CONFIG_PATH}. Using default values from example.config.json if available.")
        try:
            with open(CONFIG_EXAMPLE_PATH, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

config = load_config()

# Logging settings
log_config = config.get("logging", {})
log_level = log_config.get("level", "INFO").upper()
log_file = log_config.get("file", False)

# Auto Rarity settings
auto_rarity_config = config.get("auto_rarity", {})
auto_rarity_enabled: bool = auto_rarity_config.get("enabled", False)
auto_rarity_system: str = auto_rarity_config.get("system", "lazy")

calibration_minutes: int = auto_rarity_config.get("calibration_minutes", 5)
iv_threshold: int = auto_rarity_config.get("iv_threshold", 50)
cell_threshold: int = auto_rarity_config.get("cell_threshold", 20)
ranking_interval_seconds: int = auto_rarity_config.get("ranking_interval_seconds", 300)
cleanup_interval_seconds: int = auto_rarity_config.get("cleanup_interval_seconds", 60)

# Poracle system settings
poracle_config = auto_rarity_config.get("poracle", {})
poracle_ultra_rare: float = poracle_config.get("ultra_rare_percent", 0.01)
poracle_very_rare: float = poracle_config.get("very_rare_percent", 0.03)
poracle_rare: float = poracle_config.get("rare_percent", 0.5)
poracle_uncommon: float = poracle_config.get("uncommon_percent", 1.0)

# Koji
koji_config = config.get("koji", {})
koji_bearer_token = koji_config.get("token", None)
koji_ip = koji_config.get("ip", "127.0.0.1")
koji_port = koji_config.get("port", 8080)
koji_project_name = koji_config.get("project_name", "")
koji_url_base = koji_config.get("url", None)

if koji_url_base:
    koji_url = f"{koji_url_base}/api/v1/geofence/feature-collection/{koji_project_name}"
    koji_geofence_api_url = koji_url
else:
    koji_url = None
    koji_geofence_api_url = f"http://{koji_ip}:{koji_port}/api/v1/geofence/feature-collection/{koji_project_name}"

filter_with_koji: bool = koji_config.get("filter_with_koji", True)

geofence_config = config.get("geofences", {})
geofence_file_path = geofence_config.get("file_path", None)

# Extract geofence settings
geofence_expire_cache_seconds = geofence_config.get("expire_cache_seconds", 3600)
geofence_refresh_cache_seconds = geofence_config.get("refresh_cache_seconds", 3500)

# IVQueue - Priority list of Pokemon to scout
# Format: ["1", "3:0", "10:0"] where "1" = pokemon_id 1 any form, "3:0" = pokemon_id 3 form 0
# Lower index = higher priority (first item is highest priority)
ivlist: List[str] = [str(x) for x in config.get("ivlist", [])]
celllist: List[str] = [str(x) for x in config.get("celllist", [])]
denylist: List[str] = [str(x) for x in config.get("denylist", [])]

scout_config = config.get("scout", {})
timeout_iv: int = scout_config.get("timeout_iv", 180)
wild_scout_delay: int = scout_config.get("wild_scout_delay", 0)

def parse_ivlist(raw_list: List[str]) -> Dict[str, int]:
    """
    Parses ivlist into {pokemon_key: priority} mapping.
    Returns dict where key is "pokemon_id" or "pokemon_id:form"
    and value is priority (0 = highest).
    """
    result = {}
    for idx, entry in enumerate(raw_list):
        result[str(entry).strip()] = idx
    return result

ivlist_parsed: Dict[str, int] = parse_ivlist(ivlist)
celllist_parsed: Dict[str, int] = parse_ivlist(celllist)
denylist_parsed: Dict[str, int] = parse_ivlist(denylist)

def reload_config() -> Dict[str, any]:
    """
    Hot-reload config.json values without restarting the application.
    """
    global config, ivlist, celllist, ivlist_parsed, celllist_parsed, denylist, denylist_parsed
    global auto_rarity_config, auto_rarity_enabled, calibration_minutes, iv_threshold, cell_threshold
    global ranking_interval_seconds, cleanup_interval_seconds
    global poracle_config, poracle_ultra_rare, poracle_very_rare, poracle_rare, poracle_uncommon
    global concurrency_scout, timeout_iv, wild_scout_delay
    global geofence_expire_cache_seconds, geofence_refresh_cache_seconds

    changes = {}

    # Reload config.json
    new_config = load_config()

    # Track ivlist changes
    new_ivlist = [str(x) for x in new_config.get("ivlist", [])]
    if new_ivlist != ivlist:
        changes["ivlist"] = {"old": ivlist, "new": new_ivlist}
        ivlist = new_ivlist
        ivlist_parsed = parse_ivlist(ivlist)

    # Track celllist changes
    new_celllist = [str(x) for x in new_config.get("celllist", [])]
    if new_celllist != celllist:
        changes["celllist"] = {"old": celllist, "new": new_celllist}
        celllist = new_celllist
        celllist_parsed = parse_ivlist(celllist)

    # Track denylist changes
    new_denylist = [str(x) for x in new_config.get("denylist", [])]
    if new_denylist != denylist:
        changes["denylist"] = {"old": denylist, "new": new_denylist}
        denylist = new_denylist
        denylist_parsed = parse_ivlist(denylist)

    # Track auto_rarity changes
    new_auto_rarity = new_config.get("auto_rarity", {})
    new_auto_rarity_enabled = new_auto_rarity.get("enabled", False)
    if new_auto_rarity_enabled != auto_rarity_enabled:
        changes["auto_rarity_enabled"] = {"old": auto_rarity_enabled, "new": new_auto_rarity_enabled}
        auto_rarity_enabled = new_auto_rarity_enabled

    
    global auto_rarity_system
    new_auto_rarity_system = new_auto_rarity.get("system", "lazy")
    if new_auto_rarity_system != auto_rarity_system:
        changes["auto_rarity_system"] = {"old": auto_rarity_system, "new": new_auto_rarity_system}
        auto_rarity_system = new_auto_rarity_system

    new_calibration = new_auto_rarity.get("calibration_minutes", 5)
    if new_calibration != calibration_minutes:
        changes["calibration_minutes"] = {"old": calibration_minutes, "new": new_calibration}
        calibration_minutes = new_calibration

    new_iv_threshold = new_auto_rarity.get("iv_threshold", 50)
    if new_iv_threshold != iv_threshold:
        changes["iv_threshold"] = {"old": iv_threshold, "new": new_iv_threshold}
        iv_threshold = new_iv_threshold

    new_cell_threshold = new_auto_rarity.get("cell_threshold", 20)
    if new_cell_threshold != cell_threshold:
        changes["cell_threshold"] = {"old": cell_threshold, "new": new_cell_threshold}
        cell_threshold = new_cell_threshold

    new_ranking_interval = new_auto_rarity.get("ranking_interval_seconds", 300)
    if new_ranking_interval != ranking_interval_seconds:
        changes["ranking_interval_seconds"] = {"old": ranking_interval_seconds, "new": new_ranking_interval}
        ranking_interval_seconds = new_ranking_interval

    new_cleanup_interval = new_auto_rarity.get("cleanup_interval_seconds", 60)
    if new_cleanup_interval != cleanup_interval_seconds:
        changes["cleanup_interval_seconds"] = {"old": cleanup_interval_seconds, "new": new_cleanup_interval}
        cleanup_interval_seconds = new_cleanup_interval
    new_poracle = new_auto_rarity.get("poracle", {})
    if new_poracle != poracle_config:
        changes["poracle_config"] = {"old": poracle_config, "new": new_poracle}
        poracle_config = new_poracle
        poracle_ultra_rare = poracle_config.get("ultra_rare_percent", 0.01)
        poracle_very_rare = poracle_config.get("very_rare_percent", 0.03)
        poracle_rare = poracle_config.get("rare_percent", 0.5)
        poracle_uncommon = poracle_config.get("uncommon_percent", 1.0)


    # Track scout settings changes
    new_concurrency = new_config.get("scout", {}).get("concurrency", 5)
    if new_concurrency != concurrency_scout:
        changes["concurrency_scout"] = {"old": concurrency_scout, "new": new_concurrency}
        concurrency_scout = new_concurrency

    new_timeout = new_config.get("scout", {}).get("timeout_iv", 180)
    if new_timeout != timeout_iv:
        changes["timeout_iv"] = {"old": timeout_iv, "new": new_timeout}
        timeout_iv = new_timeout

    new_wild_scout_delay = new_config.get("scout", {}).get("wild_scout_delay", 0)
    if new_wild_scout_delay != wild_scout_delay:
        changes["wild_scout_delay"] = {"old": wild_scout_delay, "new": new_wild_scout_delay}
        wild_scout_delay = new_wild_scout_delay

    # Track geofence cache settings
    new_geofence = new_config.get("geofences", {})
    new_expire = new_geofence.get("expire_cache_seconds", 3600)
    if new_expire != geofence_expire_cache_seconds:
        changes["geofence_expire_cache_seconds"] = {"old": geofence_expire_cache_seconds, "new": new_expire}
        geofence_expire_cache_seconds = new_expire

    new_refresh = new_geofence.get("refresh_cache_seconds", 3500)
    if new_refresh != geofence_refresh_cache_seconds:
        changes["geofence_refresh_cache_seconds"] = {"old": geofence_refresh_cache_seconds, "new": new_refresh}
        geofence_refresh_cache_seconds = new_refresh

    # Update the global config dict
    config = new_config
    auto_rarity_config = new_auto_rarity

    if changes:
        logger.info(f"Config reloaded with {len(changes)} change(s): {list(changes.keys())}")
    else:
        logger.info("Config reloaded - no changes detected")

    return changes

def get_pokemon_priority(pokemon_id: int, form: Optional[int]) -> Optional[int]:
    """
    Get priority for a pokemon based on ivlist.
    Returns None if not in ivlist.
    """
    # First check exact match (pokemon_id:form)
    if form is not None:
        key = f"{pokemon_id}:{form}"
        if key in ivlist_parsed:
            return ivlist_parsed[key]

    # Then check any-form match (just pokemon_id)
    key = str(pokemon_id)
    if key in ivlist_parsed:
        return ivlist_parsed[key]

    return None

def is_pokemon_in_ivlist(pokemon_id: int, form: Optional[int]) -> bool:
    """Check if pokemon matches ivlist."""
    return get_pokemon_priority(pokemon_id, form) is not None

def is_pokemon_in_denylist(pokemon_id: int, form: Optional[int]) -> bool:
    """Check if pokemon matches denylist."""
    if form is not None:
        key = f"{pokemon_id}:{form}"
        if key in denylist_parsed:
            return True

    key = str(pokemon_id)
    if key in denylist_parsed:
        return True

    return False

# Scout concurrency
concurrency_scout: int = scout_config.get("concurrency", 5)

# Dragonite
dragonite_config = config.get("dragonite", {})
DRAGONITE_API_BASE_URL = dragonite_config.get("api_base_url", None)
DRAGONITE_API_USERNAME = dragonite_config.get("api_username", None)
DRAGONITE_API_PASSWORD = dragonite_config.get("api_password", None)
DRAGONITE_API_KEY = dragonite_config.get("api_key", None)
DRAGONITE_BEARER_KEY = dragonite_config.get("bearer_key", None)

# LazyIVQueue Admin API
server_config = config.get("server", {})
lazyivqueue_host = server_config.get("host", "0.0.0.0")
lazyivqueue_port = server_config.get("port", 7070)
lazyivqueue_max_body_size = server_config.get("max_body_size", 10 * 1024 * 1024)

# Security
security_config = config.get("security", {})
allowed_ips = security_config.get("allowed_ips", [])
headers = security_config.get("headers", None)
if not headers:
    headers = None
