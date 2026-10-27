import asyncio
import logging
import random
from contextlib import suppress
from uuid import UUID

import httpx
from faststream import FastStream
from faststream.exceptions import RejectMessage
from faststream.rabbit import RabbitMessage
from sqlalchemy import func, select, update

from app.broker import PAYMENTS_RETRY, broker, declare_topology, payments_exchange, payments_queue
from app.config import settings
from app.db import Session
from app.models import Payment, PaymentStatus
from app.outbox import run_relay
from app.schemas import PaymentEvent, PaymentOut

logger = logging.getLogger(__name__)
ATTEMPT_HEADER = "x-attempt"

http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)
app = FastStream(broker)
_relay_task: asyncio.Task[None] | None = None


async def emulate_gateway() -> PaymentStatus:
    await asyncio.sleep(random.uniform(settings.gateway_min_delay, settings.gateway_max_delay))
    return PaymentStatus.SUCCEEDED if random.random() < settings.gateway_success_rate else PaymentStatus.FAILED


async def process_payment(payment_id: UUID) -> Payment:
    """Charge the payment once; redeliveries and concurrent duplicates get the stored result."""
    async with Session() as session:
        payment = await session.get(Payment, payment_id)
    if payment is None:
        raise LookupError(f"Payment {payment_id} not found")
    if payment.status != PaymentStatus.PENDING:
        return payment

    result = await emulate_gateway()  # no DB session is held during the slow call

    async with Session() as session, session.begin():
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=result, processed_at=func.now())
        )
        # If a concurrent duplicate won the update above, this reads its result, not ours.
        return await session.scalar(select(Payment).where(Payment.id == payment_id))


async def send_webhook(payment: Payment) -> None:
    response = await http_client.post(
        payment.webhook_url, json=PaymentOut.model_validate(payment).model_dump(mode="json")
    )
    response.raise_for_status()


@broker.subscriber(payments_queue, payments_exchange)
async def handle_payment(event: PaymentEvent, msg: RabbitMessage) -> None:
    attempt = int(msg.headers.get(ATTEMPT_HEADER, 1))
    try:
        payment = await process_payment(event.payment_id)
        await send_webhook(payment)
    except Exception as exc:
        # Processing and webhook errors are retried here (the default ack policy would dead-letter at once);
        # an undecodable message never gets here and is dead-lettered immediately.
        if attempt >= settings.max_attempts:
            logger.error("Payment %s failed after %s attempts: %r", event.payment_id, attempt, exc)
            raise RejectMessage from exc  # dead-lettered to payments.new.dlq via the queue's DLX

        delay = settings.retry_base_delay * 2 ** (attempt - 1)
        logger.warning("Payment %s attempt %s failed: %r; retry in %ss", event.payment_id, attempt, exc, delay)
        await broker.publish(
            event,
            exchange=payments_exchange,
            routing_key=PAYMENTS_RETRY,
            headers={ATTEMPT_HEADER: attempt + 1},
            expiration=delay,  # seconds
            persist=True,
        )
        return
    logger.info("Payment %s is %s, webhook delivered", payment.id, payment.status)


@app.on_startup
async def setup() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    # Declare retry queue and DLQ before subscribers start, so no early message is dead-lettered into nowhere.
    await broker.connect()
    await declare_topology()


@app.after_startup
async def start_relay() -> None:
    global _relay_task
    _relay_task = asyncio.create_task(run_relay())


@app.on_shutdown
async def stop_relay() -> None:
    if _relay_task is not None:
        _relay_task.cancel()
        with suppress(asyncio.CancelledError):
            await _relay_task


@app.after_shutdown
async def close_http_client() -> None:
    # After broker.stop(): in-flight handlers still need the client to send webhooks.
    await http_client.aclose()
