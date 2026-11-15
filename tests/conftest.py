import hashlib
import hmac
import json
import time

import fakeredis
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import get_settings
from app.db import Base, make_session_factory
from app.provisioning.provisioner import FakeProvisioner
from app.provisioning.orchestrator import Deps
from app.main import create_app

SECRET = get_settings().stripe_webhook_secret


class FakeQueue:
    def __init__(self):
        self.jobs: list[tuple[str, tuple, str]] = []
        self.fail = False

    async def enqueue(self, function, *args, job_id):
        if self.fail:
            raise ConnectionError("redis down")
        self.jobs.append((function, args, job_id))


def sign(payload: bytes, secret: str = SECRET) -> str:
    """Build a valid Stripe-Signature header (same scheme Stripe uses)."""
    ts = int(time.time())
    digest = hmac.new(secret.encode(), f"{ts}.".encode() + payload, hashlib.sha256).hexdigest()
    return f"t={ts},v1={digest}"


def make_event(event_id: str, event_type: str, customer: str = "cus_123") -> bytes:
    return json.dumps(
        {
            "id": event_id,
            "object": "event",
            "type": event_type,
            "data": {"object": {"customer": customer}},
        }
    ).encode()


@pytest_asyncio.fixture
async def session_factory(tmp_path):
    # A file database (not :memory:) so that concurrent sessions get their own connections.
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'test.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield make_session_factory(engine)
    await engine.dispose()


@pytest.fixture
def queue():
    return FakeQueue()


@pytest_asyncio.fixture
async def client(session_factory, queue):
    app = create_app(session_factory=session_factory, queue=queue)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c

@pytest_asyncio.fixture
async def client_dev_false(session_factory, queue):
    app = create_app(
        session_factory=session_factory,
        queue=queue,
        dev_mode=False,
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c

@pytest_asyncio.fixture
async def client_dev_true(session_factory, queue):
    app = create_app(
        session_factory=session_factory,
        queue=queue,
        dev_mode=True,
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c


@pytest_asyncio.fixture
async def redis():
    server = fakeredis.FakeServer()
    r = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    yield r
    await r.close()


async def post_event(client, payload: bytes, signature: str | None = None):
    return await client.post(
        "/webhooks/stripe",
        content=payload,
        headers={"Stripe-Signature": signature or sign(payload), "Content-Type": "application/json"},
    )


@pytest.fixture
def provisioner():
    return FakeProvisioner()


@pytest.fixture
def deps(session_factory, provisioner, redis):
    return Deps(session_factory=session_factory, provisioner=provisioner, redis=redis, lock_ttl=30)


@pytest_asyncio.fixture
async def dev_client(session_factory, queue):
    app = create_app(session_factory=session_factory, queue=queue, dev_mode=True)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
