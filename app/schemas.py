from typing import Literal

from pydantic import BaseModel, ConfigDict


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str = "0.1.0"


class LifecycleResponse(BaseModel):
    queued: bool
    freeze_days: int | None = None
    purge_days: int | None = None
    model_config = ConfigDict(from_attributes=True)


class TenantDetailResponse(BaseModel):
    id: str
    stripe_customer_id: str | None = None
    status: str
    port: int | None = None
    container_name: str | None = None
    past_due_since: str | None = None
    last_error: str | None = None
    model_config = ConfigDict(from_attributes=True)


class StripeCustomerDetails(BaseModel):
    email: str | None = None
    name: str | None = None


class StripeCheckoutSession(BaseModel):
    id: str | None = None
    customer: str | None = None
    customer_details: StripeCustomerDetails | None = None
    payment_status: Literal["paid", "unpaid", "no_payment_required"] | None = "paid"
    status: Literal["open", "complete", "expired"] | None = "complete"


class StripeEventData(BaseModel):
    object: StripeCheckoutSession


class StripeWebhookPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    type: str
    data: StripeEventData
