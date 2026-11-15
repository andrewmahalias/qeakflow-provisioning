import json
from urllib import response

from tests.conftest import client_dev_false


async def test_unsigned_payload_is_rejected_when_dev_mode_is_off(client_dev_false):
    resp = await client_dev_false.post("/webhooks/stripe", content=json.dumps(
        {"id": "evt_1", "type": "checkout.session.completed", "data": {"object": {"customer": "cus_1"}}}))
    assert resp.status_code == 400


async def test_dev_endpoints_are_not_mounted_when_dev_mode_is_off(client_dev_false):
    assert (await client_dev_false.get("/dev/tenants")).status_code == 404


async def test_unsigned_payload_is_accepted_in_dev_mode(dev_client, queue):
    body = {"id": "evt_1", "type": "checkout.session.completed", "data": {"object": {"customer": "cus_1"}}}
    resp = await dev_client.post("/webhooks/stripe", json=body)

    assert resp.status_code == 200
    assert resp.json() == {"status": "provisioning_queued"}
    assert queue.jobs[0][0] == "provision_tenant"


async def test_malformed_dev_payload_returns_a_helpful_400(client_dev_true):
    resp = await client_dev_true.post(
        "/webhooks/stripe",
        json={
            "id": "evt_1",
            "type": "checkout.session.completed",
            "data": {
                "object": {}
            },
        },
    )

    assert resp.status_code == 400
    assert "customer" in resp.json()["detail"]


async def test_tenant_status_endpoints(dev_client):
    response = await dev_client.post("/webhooks/stripe", json={
        "id": "evt_1",
        "object": "event",
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "id": "cs_1",
                "customer": "cus_1"
            }
        }
    })
    print("WEBHOOK RESPONSE:", response.status_code, response.text)
    assert response.status_code == 200

    response = await dev_client.get("/dev/tenants")

    print("STATUS:", response.status_code)
    print("BODY:", response.text)

    tenants = response.json()
    assert len(tenants) == 1 and tenants[0]["status"] == "pending"
    assert (await dev_client.get(f"/dev/tenants/{tenants[0]['id']}")).json()["stripe_customer_id"] == "cus_1"
    assert (await dev_client.get("/dev/tenants/nope")).status_code == 404


async def test_lifecycle_can_be_triggered_by_hand(dev_client, queue):
    resp = await dev_client.post("/dev/lifecycle?freeze_days=0&purge_days=0")

    assert resp.json() == {"queued": True, "freeze_days": 0, "purge_days": 0}
    function, args, _ = queue.jobs[0]
    assert (function, args) == ("lifecycle_task", (0, 0))
