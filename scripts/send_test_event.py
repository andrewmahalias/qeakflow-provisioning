"""Send a correctly signed fake Stripe event to the local API.

Usage:
    python scripts/send_test_event.py checkout.session.completed cus_123
    python scripts/send_test_event.py customer.subscription.deleted cus_123
    python scripts/send_test_event.py checkout.session.completed cus_123 evt_fixed   # replay test
"""
import hashlib
import hmac
import json
import os
import sys
import time
import uuid

import httpx

SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "whsec_test_secret")
URL = os.environ.get("WEBHOOK_URL", "http://localhost:8000/webhooks/stripe")


def main() -> None:
    event_type = sys.argv[1] if len(sys.argv) > 1 else "checkout.session.completed"
    customer = sys.argv[2] if len(sys.argv) > 2 else "cus_demo"
    event_id = sys.argv[3] if len(sys.argv) > 3 else f"evt_{uuid.uuid4().hex[:16]}"

    payload = json.dumps(
        {"id": event_id, "object": "event", "type": event_type, "data": {"object": {"customer": customer}}}
    ).encode()
    ts = int(time.time())
    signature = hmac.new(SECRET.encode(), f"{ts}.".encode() + payload, hashlib.sha256).hexdigest()

    resp = httpx.post(
        URL,
        content=payload,
        headers={"Stripe-Signature": f"t={ts},v1={signature}", "Content-Type": "application/json"},
    )
    print(resp.status_code, resp.json())


if __name__ == "__main__":
    main()
