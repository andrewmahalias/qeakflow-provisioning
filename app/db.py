import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class TenantStatus(str, enum.Enum):
    PENDING = "pending"
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    FROZEN = "frozen"
    FAILED = "failed"
    DEPROVISIONING = "deprovisioning"
    DELETED = "deleted"


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=lambda: uuid.uuid4().hex[:12])

    stripe_customer_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    status: Mapped[TenantStatus] = mapped_column(String(20), default=TenantStatus.PENDING)
    container_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    port: Mapped[int | None] = mapped_column(Integer, unique=True, nullable=True)
    past_due_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class ProcessedEvent(Base):
    """Every Stripe event id is stored once. The primary key is the idempotency guard."""

    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


def make_engine(url: str) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True)


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def create_tables(engine: AsyncEngine) -> None:
    """Demo-only. A real deployment would use migrations (Alembic)."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
