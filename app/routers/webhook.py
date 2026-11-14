from fastapi import APIRouter, Request, Depends, HTTPException, Header, Body
from app.config import Settings, get_settings
import stripe

from app.schemas import StripeWebhookPayload
from app.webhook import handle_event, _parse_unsigned, get_session, get_queue
from sqlalchemy.ext.asyncio import AsyncSession
from app.provisioning.queue import JobQueue

router = APIRouter()


@router.post("/webhooks/stripe")
async def stripe_webhook(
    request: Request,
    webhook_data: StripeWebhookPayload = Body(...),
    stripe_signature: str | None = Header(
        default=None,
        alias="Stripe-Signature",
    ),
    settings: Settings = Depends(get_settings),
    session: AsyncSession = Depends(get_session),
    queue: JobQueue = Depends(get_queue),
):
    payload = await request.body()

    if request.app.state.dev_mode:
        event = webhook_data.model_dump()
    else:
        try:
            verified_event = stripe.Webhook.construct_event(
                payload,
                stripe_signature or "",
                settings.stripe_webhook_secret,
            )
            event = dict(verified_event)
        except (ValueError, stripe.SignatureVerificationError):
            raise HTTPException(
                status_code=400,
                detail="Invalid payload or signature",
            )

    outcome = await handle_event(session, queue, event)

    return {"status": outcome}