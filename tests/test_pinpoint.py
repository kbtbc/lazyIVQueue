"""Pinpoint queue tests. Run from the repo root: python -m tests.test_pinpoint

Drives process_webhook_message against the real IVQueueManager (no Dragonite,
no Koji): webhooks in, queue state out.
"""
import asyncio
import time

import LazyIVQueue.config as AppConfig
from LazyIVQueue.queue.iv_queue import IVQueueManager
from LazyIVQueue.webhook.filter import process_webhook_message

LAT, LON = 33.7490, -84.3880


def webhook(eid, seen_type, iv=None, pokemon_id=1, lat=LAT, lon=LON, pvp=None, spawnpoint_id=None):
    raw = {
        "encounter_id": str(eid), "pokemon_id": pokemon_id, "form": 0, "latitude": lat, "longitude": lon,
        "seen_type": seen_type, "disappear_time": int(time.time()) + 900, "spawnpoint_id": spawnpoint_id,
    }
    if iv is not None:
        raw.update(individual_attack=iv[0], individual_defense=iv[1], individual_stamina=iv[2])
    if pvp is not None:
        raw["pvp"] = pvp
    return raw


def entries(queue):
    return {e.encounter_id: e for e in queue._entries.values() if not e.is_removed}


async def reset():
    IVQueueManager._instance = None
    AppConfig.filter_with_koji = False
    AppConfig.auto_rarity_enabled = False
    AppConfig.ivlist, AppConfig.ivlist_parsed = ["1"], {"1": 0}
    AppConfig.celllist, AppConfig.celllist_parsed = [], {}
    AppConfig.denylist, AppConfig.denylist_parsed = [], {}
    AppConfig.wild_scout_delay = 0
    AppConfig.pinpoint_enabled = True
    AppConfig.pinpoint_min_iv, AppConfig.pinpoint_max_iv = 100, 0
    AppConfig.pinpoint_pvp_max_rank = None
    AppConfig.pinpoint_include_ivlist = False
    AppConfig.pinpoint_priority, AppConfig.pinpoint_hold_seconds = 500, 60
    return await IVQueueManager.get_instance()


async def test_hundo_nearby_stop_is_queued_once_and_held():
    q = await reset()
    await process_webhook_message(webhook(1001, "nearby_stop", (15, 15, 15)))
    await process_webhook_message(webhook(1001, "nearby_stop", (15, 15, 15)))  # Golbat repeat
    e = entries(q)
    assert list(e) == ["1001"], e
    assert e["1001"].list_type == "pinpoint(iv=100)" and e["1001"].priority == 500
    assert e["1001"].eligible_at > time.time() + 50, "hold_seconds not applied"


async def test_ordinary_iv_nearby_stop_is_ignored():
    q = await reset()
    await process_webhook_message(webhook(1002, "nearby_stop", (10, 12, 9)))
    assert not entries(q)


async def test_nundo_and_pvp_rules():
    q = await reset()
    AppConfig.pinpoint_pvp_max_rank = 1
    await process_webhook_message(webhook(1003, "nearby_stop", (0, 0, 0)))
    await process_webhook_message(webhook(1004, "nearby_stop", (0, 14, 15), pvp={"great": [{"rank": 1}], "ultra": [{"rank": 40}]}))
    await process_webhook_message(webhook(1005, "nearby_stop", (0, 14, 15), pvp={"great": [{"rank": 7}]}))
    e = entries(q)
    assert set(e) == {"1003", "1004"}, e
    assert e["1004"].list_type == "pinpoint(pvp_rank=1)"


async def test_only_the_same_encounter_resolves_a_pinpoint():
    q = await reset()
    await process_webhook_message(webhook(1006, "nearby_stop", (15, 15, 15)))
    # Same species, exact sighting, 30 m away, different encounter: must not resolve it
    await process_webhook_message(webhook(9999, "encounter", (3, 4, 5), lat=LAT + 0.0003, spawnpoint_id="abc"))
    assert "1006" in entries(q)
    await process_webhook_message(webhook(1006, "encounter", (15, 15, 15), lat=LAT + 0.0004, spawnpoint_id="def"))
    assert "1006" not in entries(q)
    # A later approximate webhook for it must not re-queue it
    await process_webhook_message(webhook(1006, "nearby_stop", (15, 15, 15)))
    assert "1006" not in entries(q)


async def test_nearby_cell_pinpoint_uses_cell_grid():
    q = await reset()
    await process_webhook_message(webhook(1007, "nearby_cell", (15, 15, 15)))
    e = entries(q)
    assert e["1007"].s2_cell_id, "nearby_cell pinpoint needs its S2 cell for the 9-point grid"


async def test_disabled_means_no_pinpoints():
    q = await reset()
    AppConfig.pinpoint_enabled = False
    await process_webhook_message(webhook(1008, "nearby_stop", (15, 15, 15)))
    assert not entries(q)


async def test_iv_less_flow_unchanged():
    q = await reset()
    await process_webhook_message(webhook(1009, "wild"))  # no IV, species in ivlist
    e = entries(q)
    assert e["1009"].list_type == "ivlist"
    await process_webhook_message(webhook(1009, "encounter", (1, 2, 3)))
    assert "1009" not in entries(q)


async def main():
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for t in tests:
        await t()
        print("ok  ", t.__name__)
    print(f"{len(tests)} passed")


if __name__ == "__main__":
    asyncio.run(main())
