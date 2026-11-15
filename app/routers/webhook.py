import json

import stripe
from fastapi import APIRouter, Request, Depends, HTTPException, Header
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.provisioning.queue import JobQueue
from app.webhook import handle_event, get_session, get_queue, validate_event

router = APIRouter()


@router.post("/webhooks/stripe")
async def stripe_webhook(
        request: Request,
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
        try:
            event = json.loads(payload)
            validate_event(event)
        except (ValueError, TypeError) as exc:
            raise HTTPException(
                status_code=400,
                detail=str(exc),
            )
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
