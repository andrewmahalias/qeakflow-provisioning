from typing import Protocol

from arq.connections import ArqRedis


class JobQueue(Protocol):
    async def enqueue(self, function: str, *args: str, job_id: str) -> None: ...


class ArqQueue:
    """Thin wrapper around arq. A fixed job id makes enqueueing idempotent:
    arq ignores a second job with the same id while the first one is queued or its result is kept."""

    def __init__(self, redis: ArqRedis):
        self._redis = redis

    async def enqueue(self, function: str, *args: str, job_id: str) -> None:
        await self._redis.enqueue_job(function, *args, _job_id=job_id)
