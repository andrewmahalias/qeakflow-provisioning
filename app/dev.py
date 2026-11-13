"""Helper endpoints for local testing (Postman, curl). Only mounted when DEV_MODE=true."""
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import Tenant
from app.queue import JobQueue
from app.webhook import get_queue, get_session

router = APIRouter(prefix="/dev", tags=["dev"])


def _view(tenant: Tenant) -> dict:
    return {
        "id": tenant.id,
        "stripe_customer_id": tenant.stripe_customer_id,
        "status": tenant.status,
        "port": tenant.port,
        "container_name": tenant.container_name,
        "past_due_since": tenant.past_due_since,
        "last_error": tenant.last_error,
    }


@router.get("/tenants")
async def list_tenants(session: AsyncSession = Depends(get_session)):
    tenants = (await session.execute(select(Tenant).order_by(Tenant.created_at))).scalars().all()
    return [_view(t) for t in tenants]


@router.get("/tenants/{tenant_id}")
async def get_tenant(tenant_id: str, session: AsyncSession = Depends(get_session)):
    tenant = await session.get(Tenant, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="tenant not found")
    return _view(tenant)


@router.post("/lifecycle")
async def run_lifecycle_now(freeze_days: int | None = None, purge_days: int | None = None,
                            queue: JobQueue = Depends(get_queue)):
    """Queue the nightly billing job right now, optionally with custom limits.

    freeze_days=0 freezes every tenant that has a recorded failed payment.
    purge_days=0 queues a purge for every frozen tenant."""
    await queue.enqueue("lifecycle_task", freeze_days, purge_days, job_id=f"lifecycle:{uuid4().hex}")
    return {"queued": True, "freeze_days": freeze_days, "purge_days": purge_days}
