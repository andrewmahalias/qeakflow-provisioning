import json
import logging

from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import ProcessedEvent, Tenant, TenantStatus, utcnow
from app.provisioning.queue import JobQueue

logger = logging.getLogger(__name__)


async def get_session(request: Request):
    async with request.app.state.session_factory() as session:
        yield session


def get_queue(request: Request) -> JobQueue:
    return request.app.state.queue


def _parse_unsigned(payload: bytes) -> dict:
    try:
        event = json.loads(payload)
        event["id"], event["type"], event["data"]["object"]["customer"]
    except (ValueError, KeyError, TypeError):
        raise HTTPException(
            status_code=400,
            detail='expected {"id": ..., "type": ..., "data": {"object": {"customer": ...}}}',
        )
    return event


async def handle_event(session: AsyncSession, queue: JobQueue, event) -> str:
    # 1. Idempotency: the event id is a primary key, so a replay fails here.
    session.add(ProcessedEvent(event_id=event["id"], event_type=event["type"]))
    try:
        await session.flush()
    except IntegrityError:
        await session.rollback()
        logger.info("duplicate event %s ignored", event["id"])
        return "duplicate"

    # 2. Dispatch. Unknown event types are recorded and acknowledged, not retried by Stripe.
    outcome = "ignored"
    obj = event["data"]["object"]
    if event["type"] == "checkout.session.completed":
        outcome = await _on_checkout_completed(session, queue, obj["customer"])
    elif event["type"] == "customer.subscription.deleted":
        outcome = await _on_subscription_deleted(session, queue, obj["customer"])
    elif event["type"] == "invoice.payment_failed":
        outcome = await _on_payment_failed(session, obj["customer"])
    elif event["type"] == "invoice.paid":
        outcome = await _on_invoice_paid(session, queue, event["id"], obj["customer"])

    # 3. Commit last. If enqueueing raised, nothing is stored and Stripe retries the event.
    await session.commit()
    return outcome


async def _on_checkout_completed(session: AsyncSession, queue: JobQueue, customer_id: str) -> str:
    tenant = (
        await session.execute(select(Tenant).where(Tenant.stripe_customer_id == customer_id))).scalar_one_or_none()
    if tenant is None:
        tenant = Tenant(stripe_customer_id=customer_id, status=TenantStatus.PENDING)
        session.add(tenant)
        await session.flush()
    await queue.enqueue("provision_tenant", tenant.id, job_id=f"provision:{tenant.id}")
    return "provisioning_queued"


async def _on_subscription_deleted(session: AsyncSession, queue: JobQueue, customer_id: str) -> str:
    tenant = (
        await session.execute(select(Tenant).where(Tenant.stripe_customer_id == customer_id))).scalar_one_or_none()
    if tenant is None:
        return "ignored"  # cancellation for a customer we never provisioned (e.g. out-of-order event)
    tenant.status = TenantStatus.DEPROVISIONING
    await queue.enqueue("deprovision_tenant", tenant.id, job_id=f"deprovision:{tenant.id}")
    return "deprovisioning_queued"


async def _find_tenant(session: AsyncSession, customer_id: str) -> Tenant | None:
    return (await session.execute(select(Tenant).where(Tenant.stripe_customer_id == customer_id))).scalar_one_or_none()


async def _on_payment_failed(session: AsyncSession, customer_id: str) -> str:
    tenant = await _find_tenant(session, customer_id)
    if tenant is None:
        return "ignored"
    if tenant.past_due_since is None:  # keep the FIRST failure: Stripe retries the charge several times
        tenant.past_due_since = utcnow()
    return "past_due_recorded"


async def _on_invoice_paid(session: AsyncSession, queue: JobQueue, event_id: str, customer_id: str) -> str:
    tenant = await _find_tenant(session, customer_id)
    if tenant is None:
        return "ignored"
    tenant.past_due_since = None
    if tenant.status == TenantStatus.FROZEN:
        # Unique job id: a "provision:<tenant>" id from the original signup may still be remembered by arq.
        await queue.enqueue("provision_tenant", tenant.id, job_id=f"unfreeze:{tenant.id}:{event_id}")
        return "unfreeze_queued"
    return "past_due_cleared"
