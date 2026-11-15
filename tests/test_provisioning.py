import asyncio
from datetime import timedelta

import pytest
from arq import Retry

from app.db import Tenant, TenantStatus, utcnow
from app.provisioning.locks import LockNotAcquired, redis_lock, tenant_lock
from app.provisioning.provisioner import FakeProvisioner
from app.provisioning.orchestrator import PORT_RANGE, Deps, TenantNotFound, deprovision, provision, run_lifecycle
from app.provisioning.worker import provision_tenant
from tests.conftest import FakeQueue


async def make_tenant(session_factory, status=TenantStatus.PENDING, customer="cus_123", **fields) -> str:
    async with session_factory() as s:
        tenant = Tenant(stripe_customer_id=customer, status=status, **fields)
        s.add(tenant)
        await s.commit()
        return tenant.id


async def get_tenant(session_factory, tenant_id) -> Tenant:
    async with session_factory() as s:
        return await s.get(Tenant, tenant_id)


# ---- provisioning -----------------------------------------------------------------

async def test_provision_marks_tenant_active_and_reserves_port(session_factory, deps, provisioner):
    tenant_id = await make_tenant(session_factory)

    assert await provision(tenant_id, deps) == "provisioned"

    tenant = await get_tenant(session_factory, tenant_id)
    assert tenant.status == TenantStatus.ACTIVE
    assert tenant.container_name == f"tenant-{tenant_id}"
    assert tenant.port in PORT_RANGE
    assert provisioner.containers[tenant_id] == tenant.port


async def test_provision_is_idempotent(session_factory, deps, provisioner):
    tenant_id = await make_tenant(session_factory)

    await provision(tenant_id, deps)
    assert await provision(tenant_id, deps) == "already_active"
    assert provisioner.create_calls == 1


async def test_concurrent_provisioning_never_hands_out_the_same_port(session_factory, deps):
    ids = [await make_tenant(session_factory, customer=f"cus_{i}") for i in range(6)]

    await asyncio.gather(*(provision(tenant_id, deps) for tenant_id in ids))

    ports = [(await get_tenant(session_factory, tenant_id)).port for tenant_id in ids]
    assert len(set(ports)) == 6


async def test_failure_rolls_back_and_retry_succeeds(session_factory, redis):
    provisioner = FakeProvisioner(fail_times=1)
    deps = Deps(session_factory, provisioner, redis, 30)
    tenant_id = await make_tenant(session_factory)

    with pytest.raises(RuntimeError):
        await provision(tenant_id, deps)
    failed = await get_tenant(session_factory, tenant_id)
    assert failed.status == TenantStatus.FAILED
    assert "simulated" in failed.last_error
    assert failed.port is None  # reserved port was released by the rollback

    await provision(tenant_id, deps)  # the retry starts from a clean slate
    recovered = await get_tenant(session_factory, tenant_id)
    assert recovered.status == TenantStatus.ACTIVE
    assert recovered.last_error is None
    assert recovered.port is not None


async def test_unhealthy_instance_is_rolled_back_in_reverse_order(session_factory, redis):
    provisioner = FakeProvisioner(healthy=False)
    deps = Deps(session_factory, provisioner, redis, 30)
    tenant_id = await make_tenant(session_factory)

    with pytest.raises(TimeoutError):
        await provision(tenant_id, deps)

    tenant = await get_tenant(session_factory, tenant_id)
    assert tenant.status == TenantStatus.FAILED
    assert provisioner.containers == {}  # container removed
    assert tenant.port is None  # port released


async def test_provision_after_cancellation_is_skipped(session_factory, deps, provisioner):
    tenant_id = await make_tenant(session_factory, status=TenantStatus.DEPROVISIONING)

    assert await provision(tenant_id, deps) == "skipped"
    assert provisioner.containers == {}


async def test_missing_tenant_raises_for_retry(deps):
    with pytest.raises(TenantNotFound):
        await provision("does-not-exist", deps)


# ---- deprovisioning ---------------------------------------------------------------

async def test_deprovision_removes_resources_and_is_idempotent(session_factory, deps, provisioner):
    tenant_id = await make_tenant(session_factory)
    await provision(tenant_id, deps)

    assert await deprovision(tenant_id, deps) == "deleted"
    assert provisioner.containers == {}
    tenant = await get_tenant(session_factory, tenant_id)
    assert tenant.status == TenantStatus.DELETED
    assert tenant.port is None
    assert await deprovision(tenant_id, deps) == "already_deleted"


# ---- billing lifecycle ------------------------------------------------------------

async def test_unpaid_tenant_is_frozen_after_grace_period_and_resumes_on_provision(session_factory, deps, provisioner):
    overdue = await make_tenant(session_factory, customer="cus_a")
    recent = await make_tenant(session_factory, customer="cus_b")
    for tenant_id in (overdue, recent):
        await provision(tenant_id, deps)
    async with session_factory() as s:
        (await s.get(Tenant, overdue)).past_due_since = utcnow() - timedelta(days=10)
        (await s.get(Tenant, recent)).past_due_since = utcnow() - timedelta(days=2)
        await s.commit()

    result = await run_lifecycle(deps, FakeQueue(), timedelta(days=7), timedelta(days=365))

    assert result == {"frozen": 1, "purge_queued": 0}
    assert (await get_tenant(session_factory, overdue)).status == TenantStatus.FROZEN
    assert (await get_tenant(session_factory, recent)).status == TenantStatus.ACTIVE
    assert provisioner.stopped == {overdue}  # stopped, not removed: data is kept

    # Payment arrives: provisioning a frozen tenant restarts the SAME container and port.
    before = (await get_tenant(session_factory, overdue)).port
    assert await provision(overdue, deps) == "provisioned"
    assert provisioner.create_calls == 2  # one per tenant, none for the restart
    assert (await get_tenant(session_factory, overdue)).port == before
    assert provisioner.stopped == set()


async def test_long_frozen_tenant_is_scheduled_for_purge(session_factory, deps):
    tenant_id = await make_tenant(
        session_factory, status=TenantStatus.FROZEN, past_due_since=utcnow() - timedelta(days=400)
    )
    queue = FakeQueue()

    result = await run_lifecycle(deps, queue, timedelta(days=7), timedelta(days=365))

    assert result == {"frozen": 0, "purge_queued": 1}
    assert queue.jobs == [("deprovision_tenant", (tenant_id,), f"deprovision:{tenant_id}")]


# ---- locks and worker -------------------------------------------------------------

async def test_lock_is_exclusive_and_released(redis):
    async with tenant_lock(redis, "t1"):
        with pytest.raises(LockNotAcquired):
            async with tenant_lock(redis, "t1"):
                pass
    async with tenant_lock(redis, "t1"):  # free again after the first block exits
        pass


async def test_lock_waits_for_a_busy_lock_to_be_released(redis):
    async def hold_briefly():
        async with redis_lock(redis, "x", ttl_seconds=5):
            await asyncio.sleep(0.3)

    holder = asyncio.create_task(hold_briefly())
    await asyncio.sleep(0.05)
    async with redis_lock(redis, "x", ttl_seconds=5, wait_seconds=2):  # waits, then acquires
        pass
    await holder


async def test_lock_release_does_not_delete_foreign_lock(redis):
    async with tenant_lock(redis, "t1"):
        await redis.set("lock:tenant:t1", "someone-else")  # simulate TTL expiry + new owner
    assert await redis.get("lock:tenant:t1") == "someone-else"


async def test_worker_retries_when_lock_is_held(redis, session_factory):
    tenant_id = await make_tenant(session_factory)
    ctx = {
        "redis": redis,
        "session_factory": session_factory,
        "provisioner": FakeProvisioner(),
        "settings": type("S", (), {"lock_ttl_seconds": 30})(),
        "job_try": 1,
    }
    async with tenant_lock(redis, tenant_id):
        with pytest.raises(Retry):
            await provision_tenant(ctx, tenant_id)

    assert await provision_tenant(ctx, tenant_id) == "provisioned"  # lock free -> succeeds
