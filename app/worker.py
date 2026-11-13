from datetime import timedelta

from arq import Retry, cron
from arq.connections import RedisSettings

from app.config import get_settings
from app.db import make_engine, make_session_factory
from app.locks import LockNotAcquired, tenant_lock
from app.provisioner import DockerProvisioner, FakeProvisioner
from app.provisioning import Deps, TenantNotFound, deprovision, provision, run_lifecycle
from app.queue import ArqQueue


async def startup(ctx: dict) -> None:
    settings = get_settings()
    engine = make_engine(settings.database_url)
    ctx["engine"] = engine
    ctx["session_factory"] = make_session_factory(engine)
    ctx["settings"] = settings
    if settings.provisioner == "fake":
        ctx["provisioner"] = FakeProvisioner()
    else:
        ctx["provisioner"] = DockerProvisioner(
            settings.tenant_image,
            settings.tenant_network,
            settings.tenant_memory_limit,
            settings.tenant_cpu_limit,
            settings.health_timeout_seconds,
        )


async def shutdown(ctx: dict) -> None:
    await ctx["engine"].dispose()


def _deps(ctx: dict) -> Deps:
    return Deps(ctx["session_factory"], ctx["provisioner"], ctx["redis"], ctx["settings"].lock_ttl_seconds)


async def _run_locked(ctx: dict, tenant_id: str, use_case) -> str:
    """Run a use-case under the per-tenant lock.

    If another worker holds the lock, or the tenant row is not visible yet,
    ask arq to retry shortly instead of failing the job."""
    deps = _deps(ctx)
    try:
        async with tenant_lock(deps.redis, tenant_id, deps.lock_ttl):
            return await use_case(tenant_id, deps)
    except (LockNotAcquired, TenantNotFound):
        raise Retry(defer=ctx["job_try"] * 5)


async def provision_tenant(ctx: dict, tenant_id: str) -> str:
    return await _run_locked(ctx, tenant_id, provision)


async def deprovision_tenant(ctx: dict, tenant_id: str) -> str:
    return await _run_locked(ctx, tenant_id, deprovision)


async def lifecycle_task(ctx: dict, freeze_days: int | None = None, purge_days: int | None = None) -> dict:
    """Runs nightly via cron. The optional arguments let you trigger it by hand with other limits."""
    settings = ctx["settings"]
    return await run_lifecycle(
        _deps(ctx),
        ArqQueue(ctx["redis"]),
        freeze_after=timedelta(days=settings.freeze_after_days if freeze_days is None else freeze_days),
        purge_after=timedelta(days=settings.purge_after_days if purge_days is None else purge_days),
    )


class WorkerSettings:
    functions = [provision_tenant, deprovision_tenant, lifecycle_task]
    cron_jobs = [cron(lifecycle_task, hour=3, minute=0)]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings.from_dsn(get_settings().redis_url)
    max_tries = 5
    job_timeout = 180
