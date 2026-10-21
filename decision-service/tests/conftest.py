import orjson

MIN, HOUR = 60_000, 3_600_000
NOW = 1_790_000_000_000 // MIN * MIN + 30_000  # 30 s into a minute


def activity(minutes=(), hours=(), peers=(), as_of=NOW - 1_000) -> bytes:
    """An entity value exactly as OnlineFeatures.java writes it."""
    return orjson.dumps({"v": 1, "as_of": as_of, "m": [list(m) for m in minutes], "h": [list(h) for h in hours],
                         "p": [list(p) for p in peers]})


def minute(offset: int) -> int:
    return NOW // MIN * MIN + offset * MIN
