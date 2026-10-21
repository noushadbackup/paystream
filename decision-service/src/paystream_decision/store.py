"""Reads everything a decision needs from Redis in a single round trip."""

from __future__ import annotations

import orjson

from paystream_decision.features import Activity


class OnlineStore:
    def __init__(self, redis):
        self.redis = redis  # redis.asyncio.Redis

    async def fetch(self, req: dict) -> tuple[Activity | None, Activity | None, Activity | None, bool | None,
                                               dict[str, dict[str, dict]]]:
        s, r, dev = req["sender_id"], req["receiver_id"], req.get("device_id")
        pipe = self.redis.pipeline(transaction=False)
        pipe.mget(f"f:s:{s}", f"f:r:{r}", f"f:d:{dev}" if dev else "f:d:-")
        pipe.scard(f"devs:{s}")
        pipe.sismember(f"devs:{s}", dev or "-")
        pipe.hgetall(f"flags:{s}")
        pipe.hgetall(f"flags:{r}")
        pipe.hgetall(f"flags:{dev}" if dev else "flags:-")
        (fs, fr, fd), n_devices, known, flag_s, flag_r, flag_d = await pipe.execute()
        known_device = bool(known) if n_devices else None  # no history: unknown, not "new"
        flags = {name: {_s(k): orjson.loads(v) for k, v in raw.items()}
                 for name, raw in (("sender", flag_s), ("receiver", flag_r), ("device", flag_d))}
        return Activity.parse(fs), Activity.parse(fr), Activity.parse(fd), known_device, flags


def _s(k) -> str:
    return k.decode() if isinstance(k, bytes) else k
