import asyncio
import time
from contextlib import asynccontextmanager
from uuid import uuid4

from redis.asyncio import Redis


class LockNotAcquired(Exception):
    pass


# Release only if we still own the lock (compare token, then delete) - atomic in Lua.
_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


@asynccontextmanager
async def redis_lock(
    redis: Redis,
    name: str,
    ttl_seconds: int = 120,
    wait_seconds: float = 0.0,
    retry_interval: float = 0.1,
):
    """Distributed lock: SET NX EX with an owner token.

    - ttl_seconds protects against a crashed holder (the lock expires on its own).
    - wait_seconds is how long to keep trying before giving up. It is separate from the TTL.
    - Release compares the token, so a holder whose lock already expired
      can never delete the lock of the next owner.
    """
    key = f"lock:{name}"
    token = uuid4().hex
    deadline = time.monotonic() + wait_seconds
    while not await redis.set(key, token, nx=True, ex=ttl_seconds):
        if time.monotonic() >= deadline:
            raise LockNotAcquired(key)
        await asyncio.sleep(retry_interval)
    try:
        yield
    finally:
        await redis.eval(_RELEASE_SCRIPT, 1, key, token)


def tenant_lock(redis: Redis, tenant_id: str, ttl_seconds: int = 120):
    """Serialises provision / deprovision / freeze for one tenant across workers."""
    return redis_lock(redis, f"tenant:{tenant_id}", ttl_seconds)
