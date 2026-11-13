"""Provisioning use-cases. Plain async functions with injected dependencies,
so they can be tested with a fake provisioner and fake Redis."""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Awaitable, Callable

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db import Tenant, TenantStatus, utcnow
from app.locks import LockNotAcquired, redis_lock, tenant_lock
from app.provisioner import Provisioner
from app.queue import JobQueue

logger = logging.getLogger(__name__)

PORT_RANGE = range(20000, 21000)


class TenantNotFound(Exception):
    """The job ran before the tenant row was visible. The job is retried."""


class NoFreePort(Exception):
    pass


@dataclass
class Deps:
    session_factory: async_sessionmaker[AsyncSession]
    provisioner: Provisioner
    redis: Redis
    lock_ttl: int = 120


async def _set_status(deps: Deps, tenant_id: str, status: TenantStatus, **fields) -> None:
    async with deps.session_factory() as session:
        tenant = await session.get(Tenant, tenant_id)
        tenant.status = status
        for key, value in fields.items():
            setattr(tenant, key, value)
        await session.commit()


async def allocate_port(deps: Deps, tenant_id: str) -> int:
    """Reserve a host port for the tenant.

    The port is written to the database *before* the global lock is released.
    If it were recorded later (after the container starts), two concurrent jobs could
    read the same "used ports" list and pick the same free port. The UNIQUE column
    is a second line of defence.
    """
    async with redis_lock(deps.redis, "global:port-allocation", ttl_seconds=30, wait_seconds=30):
        async with deps.session_factory() as session:
            tenant = await session.get(Tenant, tenant_id)
            if tenant.port is not None:
                return tenant.port  # idempotent: a retry or an unfreeze keeps its port
            used = set((await session.execute(select(Tenant.port).where(Tenant.port.is_not(None)))).scalars())
            port = next((p for p in PORT_RANGE if p not in used), None)
            if port is None:
                raise NoFreePort("port range exhausted")
            tenant.port = port
            await session.commit()
            return port


async def release_port(deps: Deps, tenant_id: str) -> None:
    async with deps.session_factory() as session:
        tenant = await session.get(Tenant, tenant_id)
        if tenant is not None:
            tenant.port = None
            await session.commit()


async def _rollback(compensations: list[Callable[[], Awaitable[None]]]) -> None:
    """Undo completed steps in reverse order. One failing step must not stop the others."""
    for undo in reversed(compensations):
        try:
            await undo()
        except Exception:
            logger.exception("compensation step failed")


async def provision(tenant_id: str, deps: Deps) -> str:
    async with deps.session_factory() as session:
        tenant = await session.get(Tenant, tenant_id)
        if tenant is None:
            raise TenantNotFound(tenant_id)
        if tenant.status == TenantStatus.ACTIVE:
            return "already_active"
        if tenant.status in (TenantStatus.DEPROVISIONING, TenantStatus.DELETED):
            return "skipped"  # cancellation won the race
        # Only undo what *this* run creates. A frozen tenant keeps its container and port.
        had_port = tenant.port is not None
        had_container = tenant.container_name is not None
        tenant.status = TenantStatus.PROVISIONING
        tenant.last_error = None
        await session.commit()

    # Compensation stack is a local variable, never shared between concurrent jobs.
    compensations: list[Callable[[], Awaitable[None]]] = []
    try:
        if not had_port:
            compensations.append(lambda: release_port(deps, tenant_id))
        port = await allocate_port(deps, tenant_id)

        if not had_container:
            compensations.append(lambda: deps.provisioner.remove(tenant_id))  # registered before the action:
        name = await deps.provisioner.ensure_running(tenant_id, port)          # remove() is idempotent, so a
                                                                              # half-created container is cleaned up too
        await deps.provisioner.wait_healthy(tenant_id)
    except Exception as exc:
        await _rollback(compensations)
        await _set_status(deps, tenant_id, TenantStatus.FAILED, last_error=str(exc)[:500])
        raise  # the job runner retries with backoff

    await _set_status(deps, tenant_id, TenantStatus.ACTIVE, container_name=name)
    return "provisioned"


async def deprovision(tenant_id: str, deps: Deps) -> str:
    async with deps.session_factory() as session:
        tenant = await session.get(Tenant, tenant_id)
        if tenant is None:
            raise TenantNotFound(tenant_id)
        if tenant.status == TenantStatus.DELETED:
            return "already_deleted"
        tenant.status = TenantStatus.DEPROVISIONING
        await session.commit()

    # If removal fails this raises: the tenant stays DEPROVISIONING and the job is retried,
    # instead of being marked deleted while resources are still running.
    await deps.provisioner.remove(tenant_id)
    await release_port(deps, tenant_id)
    await _set_status(deps, tenant_id, TenantStatus.DELETED, container_name=None)
    return "deleted"


async def run_lifecycle(
    deps: Deps,
    queue: JobQueue,
    freeze_after: timedelta,
    purge_after: timedelta,
    now: datetime | None = None,
) -> dict:
    """Scheduled billing lifecycle: past due -> frozen (container stopped) -> purged much later."""
    now = now or utcnow()
    async with deps.session_factory() as session:
        to_freeze = list((await session.execute(
            select(Tenant.id).where(
                Tenant.status == TenantStatus.ACTIVE.value,
                Tenant.past_due_since.is_not(None),
                Tenant.past_due_since <= now - freeze_after,
            )
        )).scalars())
        to_purge = list((await session.execute(
            select(Tenant.id).where(
                Tenant.status == TenantStatus.FROZEN.value,
                Tenant.past_due_since.is_not(None),
                Tenant.past_due_since <= now - purge_after,
            )
        )).scalars())

    frozen = 0
    for tenant_id in to_freeze:
        try:
            async with tenant_lock(deps.redis, tenant_id, deps.lock_ttl):
                await deps.provisioner.stop(tenant_id)
                await _set_status(deps, tenant_id, TenantStatus.FROZEN)
            frozen += 1
        except LockNotAcquired:
            continue  # busy right now: picked up on the next run

    for tenant_id in to_purge:
        await queue.enqueue("deprovision_tenant", tenant_id, job_id=f"deprovision:{tenant_id}")

    return {"frozen": frozen, "purge_queued": len(to_purge)}
