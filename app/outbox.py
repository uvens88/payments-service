import asyncio
import logging

from sqlalchemy import func, select, update

from app.broker import broker, payments_exchange
from app.config import settings
from app.db import Session
from app.models import OutboxEvent

logger = logging.getLogger(__name__)


async def publish_pending() -> int:
    """Publish one batch of unpublished events; returns how many were published.

    At-least-once: a crash after publish but before commit republishes the event,
    which the idempotent consumer tolerates. SKIP LOCKED lets several relays run.
    """
    async with Session() as session, session.begin():
        events = (
            await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.id)
                .limit(settings.outbox_batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()

        published_ids = []
        for event in events:
            try:
                # Returns only after the broker confirms; raises if the message is unroutable.
                await broker.publish(
                    event.payload,
                    exchange=payments_exchange,
                    routing_key=event.routing_key,
                    persist=True,
                    message_id=str(event.id),
                )
            except Exception:
                logger.exception("Outbox event %s was not published, will retry", event.id)
                break
            published_ids.append(event.id)

        if published_ids:
            await session.execute(
                update(OutboxEvent).where(OutboxEvent.id.in_(published_ids)).values(published_at=func.now())
            )
        return len(published_ids)


async def run_relay() -> None:
    while True:
        try:
            published = await publish_pending()
        except Exception:
            logger.exception("Outbox relay iteration failed")
            published = 0
        if published < settings.outbox_batch_size:
            await asyncio.sleep(settings.outbox_poll_interval)
