from sqlalchemy import func, select

from app.db import ProcessedEvent, Tenant, TenantStatus
from tests.conftest import make_event, post_event, client_dev_false


async def count(session_factory, model):
    async with session_factory() as s:
        return (await s.execute(select(func.count()).select_from(model))).scalar_one()


async def test_invalid_signature_is_rejected(client_dev_false, queue, session_factory):
    payload = make_event("evt_1", "checkout.session.completed")
    resp = await post_event(client_dev_false, payload, signature="t=1,v1=deadbeef")
    assert resp.status_code == 400
    assert queue.jobs == []
    assert await count(session_factory, ProcessedEvent) == 0


async def test_checkout_completed_creates_tenant_and_queues_job(client, queue, session_factory):
    resp = await post_event(client, make_event("evt_1", "checkout.session.completed"))
    assert resp.status_code == 200
    assert resp.json() == {"status": "provisioning_queued"}

    assert await count(session_factory, Tenant) == 1
    assert len(queue.jobs) == 1
    function, args, job_id = queue.jobs[0]
    assert function == "provision_tenant"
    assert job_id == f"provision:{args[0]}"


async def test_replayed_event_is_processed_once(client, queue, session_factory):
    payload = make_event("evt_1", "checkout.session.completed")
    first = await post_event(client, payload)
    second = await post_event(client, payload)

    assert first.json()["status"] == "provisioning_queued"
    assert second.json()["status"] == "duplicate"
    assert len(queue.jobs) == 1
    assert await count(session_factory, Tenant) == 1


async def test_second_event_for_same_customer_reuses_tenant(client, queue, session_factory):
    await post_event(client, make_event("evt_1", "checkout.session.completed"))
    await post_event(client, make_event("evt_2", "checkout.session.completed"))

    assert await count(session_factory, Tenant) == 1
    # Same job id both times: the queue deduplicates, so only one provisioning runs.
    assert {job_id for _, _, job_id in queue.jobs} == {queue.jobs[0][2]}


async def test_enqueue_failure_returns_500_and_stores_nothing(client, queue, session_factory):
    queue.fail = True
    payload = make_event("evt_1", "checkout.session.completed")
    try:
        resp = await post_event(client, payload)
        assert resp.status_code == 500
    except ConnectionError:
        pass  # ASGI test transport re-raises the app exception; production returns 500

    # Event not recorded -> Stripe's retry will be processed normally.
    assert await count(session_factory, ProcessedEvent) == 0
    queue.fail = False
    retry = await post_event(client, payload)
    assert retry.json()["status"] == "provisioning_queued"


async def test_subscription_deleted_queues_deprovisioning(client, queue, session_factory):
    await post_event(client, make_event("evt_1", "checkout.session.completed"))
    resp = await post_event(client, make_event("evt_2", "customer.subscription.deleted"))

    assert resp.json()["status"] == "deprovisioning_queued"
    assert queue.jobs[-1][0] == "deprovision_tenant"
    async with session_factory() as s:
        tenant = (await s.execute(select(Tenant))).scalar_one()
        assert tenant.status == TenantStatus.DEPROVISIONING


async def test_cancellation_for_unknown_customer_is_ignored(client, queue):
    resp = await post_event(client, make_event("evt_9", "customer.subscription.deleted", customer="cus_unknown"))
    assert resp.status_code == 200
    assert resp.json()["status"] == "ignored"
    assert queue.jobs == []


async def test_unhandled_event_type_is_acknowledged(client, queue):
    resp = await post_event(client, make_event("evt_5", "invoice.paid"))
    assert resp.status_code == 200
    assert resp.json()["status"] == "ignored"


async def test_payment_failure_records_first_failure_only(client, session_factory):
    await post_event(client, make_event("evt_1", "checkout.session.completed"))
    first = await post_event(client, make_event("evt_2", "invoice.payment_failed"))
    async with session_factory() as s:
        recorded = (await s.execute(select(Tenant))).scalar_one().past_due_since
    second = await post_event(client, make_event("evt_3", "invoice.payment_failed"))  # Stripe retries the charge

    assert first.json()["status"] == "past_due_recorded"
    assert second.json()["status"] == "past_due_recorded"
    async with session_factory() as s:
        assert (await s.execute(select(Tenant))).scalar_one().past_due_since == recorded
    assert recorded is not None


async def test_paying_an_invoice_clears_past_due_and_unfreezes(client, queue, session_factory):
    await post_event(client, make_event("evt_1", "checkout.session.completed"))
    await post_event(client, make_event("evt_2", "invoice.payment_failed"))
    async with session_factory() as s:
        tenant = (await s.execute(select(Tenant))).scalar_one()
        tenant.status = TenantStatus.FROZEN
        await s.commit()
        tenant_id = tenant.id

    resp = await post_event(client, make_event("evt_3", "invoice.paid"))

    assert resp.json()["status"] == "unfreeze_queued"
    assert queue.jobs[-1] == ("provision_tenant", (tenant_id,), f"unfreeze:{tenant_id}:evt_3")
    async with session_factory() as s:
        assert (await s.execute(select(Tenant))).scalar_one().past_due_since is None


async def test_paying_an_invoice_for_an_active_tenant_only_clears_the_flag(client, queue):
    await post_event(client, make_event("evt_1", "checkout.session.completed"))
    await post_event(client, make_event("evt_2", "invoice.payment_failed"))
    jobs_before = len(queue.jobs)

    resp = await post_event(client, make_event("evt_3", "invoice.paid"))

    assert resp.json()["status"] == "past_due_cleared"
    assert len(queue.jobs) == jobs_before
