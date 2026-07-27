def normalize_encounter_id(eid: Any) -> Optional[str]:
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