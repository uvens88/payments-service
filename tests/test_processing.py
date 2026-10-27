"""Outbox relay and consumer against a real RabbitMQ (isolated 'test' vhost) and Postgres."""

import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable
from uuid import UUID

import httpx
import pytest
from sqlalchemy import insert, select

from app import consumer
from app.config import settings
from app.db import Session
from app.models import PAYMENTS_NEW, OutboxEvent, Payment, PaymentStatus
from app.outbox import publish_pending
from tests.conftest import management_client


class Gateway:
    """Deterministic stand-in for emulate_gateway that counts charges."""

    def __init__(self, *results: PaymentStatus) -> None:
        self.results = list(results)
        self.calls = 0

    async def __call__(self) -> PaymentStatus:
        self.calls += 1
        await asyncio.sleep(0.05)
        return self.results.pop(0)


class WebhookSink:
    """Records webhook requests and answers with `status_code`."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.status_code = 200

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.status_code)

    def payloads(self) -> list[dict]:
        return [json.loads(r.content) for r in self.requests]


@pytest.fixture
def webhooks(monkeypatch: pytest.MonkeyPatch) -> WebhookSink:
    sink = WebhookSink()
    monkeypatch.setattr(consumer, "http_client", httpx.AsyncClient(transport=httpx.MockTransport(sink)))
    monkeypatch.setattr(settings, "retry_base_delay", 0.2)
    return sink


async def create_payment(key: str = "key-1") -> Payment:
    async with Session() as session, session.begin():
        payment = await session.scalar(
            insert(Payment)
            .values(amount="10.00", currency="USD", description="d", webhook_url="http://hook/", idempotency_key=key)
            .returning(Payment)
        )
        session.add(OutboxEvent(aggregate_id=payment.id, routing_key=PAYMENTS_NEW, payload={"payment_id": str(payment.id)}))
    return payment


async def wait_for(check: Callable[[], bool | Awaitable[bool]], timeout: float = 10) -> None:
    async with asyncio.timeout(timeout):
        while True:
            result = check()
            if await result if inspect.isawaitable(result) else result:
                return
            await asyncio.sleep(0.05)


async def queue_messages(name: str) -> list[dict]:
    async with management_client() as client:
        response = await client.post(
            f"/api/queues/test/{name}/get",
            json={"count": 10, "ackmode": "ack_requeue_true", "encoding": "auto"},
        )
        response.raise_for_status()
        return response.json()


async def get_payment(payment_id: UUID) -> Payment:
    async with Session() as session:
        return await session.get(Payment, payment_id)


async def test_payment_is_processed_and_webhook_sent(
    running_broker: None, webhooks: WebhookSink, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(consumer, "emulate_gateway", Gateway(PaymentStatus.SUCCEEDED))
    payment = await create_payment()

    assert await publish_pending() == 1

    await wait_for(lambda: len(webhooks.requests) == 1)
    stored = await get_payment(payment.id)
    assert stored.status == PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    [payload] = webhooks.payloads()
    assert payload["payment_id"] == str(payment.id)
    assert payload["status"] == "succeeded"
    async with Session() as session:
        assert (await session.scalar(select(OutboxEvent))).published_at is not None


async def test_failed_charge_is_a_result_not_an_error(
    running_broker: None, webhooks: WebhookSink, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(consumer, "emulate_gateway", Gateway(PaymentStatus.FAILED))
    payment = await create_payment()

    await publish_pending()

    await wait_for(lambda: len(webhooks.requests) == 1)
    assert (await get_payment(payment.id)).status == PaymentStatus.FAILED
    assert webhooks.payloads()[0]["status"] == "failed"
    assert await queue_messages("payments.new.dlq") == []


async def test_webhook_failures_are_retried_then_dead_lettered(
    running_broker: None, webhooks: WebhookSink, monkeypatch: pytest.MonkeyPatch
) -> None:
    gateway = Gateway(PaymentStatus.SUCCEEDED)
    monkeypatch.setattr(consumer, "emulate_gateway", gateway)
    webhooks.status_code = 500
    payment = await create_payment()

    await publish_pending()

    async def dead_lettered() -> bool:
        return len(await queue_messages("payments.new.dlq")) == 1

    await wait_for(dead_lettered)
    assert len(webhooks.requests) == settings.max_attempts
    assert gateway.calls == 1  # retries only re-send the webhook, the payment is charged once
    assert (await get_payment(payment.id)).status == PaymentStatus.SUCCEEDED
    [dead] = await queue_messages("payments.new.dlq")
    assert dead["properties"]["headers"]["x-attempt"] == settings.max_attempts
    assert dead["properties"]["headers"]["x-death"][0]["reason"] == "rejected"


async def test_webhook_recovers_on_retry(
    running_broker: None, webhooks: WebhookSink, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(consumer, "emulate_gateway", Gateway(PaymentStatus.SUCCEEDED))
    webhooks.status_code = 503
    await create_payment()

    await publish_pending()
    await wait_for(lambda: len(webhooks.requests) == 1)
    webhooks.status_code = 200

    await wait_for(lambda: len(webhooks.requests) == 2)
    await asyncio.sleep(settings.retry_base_delay * 3)  # would-be third attempt window
    assert len(webhooks.requests) == 2
    assert await queue_messages("payments.new.dlq") == []


async def test_unroutable_event_stays_unpublished(running_broker: None) -> None:
    async with Session() as session, session.begin():
        session.add(OutboxEvent(aggregate_id=(await create_payment()).id, routing_key="nowhere", payload={}))
    async with Session() as session, session.begin():
        await session.execute(OutboxEvent.__table__.delete().where(OutboxEvent.routing_key == PAYMENTS_NEW))

    assert await publish_pending() == 0

    async with Session() as session:
        assert (await session.scalar(select(OutboxEvent))).published_at is None


async def test_concurrent_duplicates_store_one_result_and_agree(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(consumer, "emulate_gateway", Gateway(PaymentStatus.SUCCEEDED, PaymentStatus.FAILED))
    payment = await create_payment()

    first, second = await asyncio.gather(consumer.process_payment(payment.id), consumer.process_payment(payment.id))

    stored = await get_payment(payment.id)
    assert first.status == second.status == stored.status


async def test_redelivery_of_processed_payment_does_not_charge_again(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = Gateway(PaymentStatus.SUCCEEDED)
    monkeypatch.setattr(consumer, "emulate_gateway", gateway)
    payment = await create_payment()

    await consumer.process_payment(payment.id)
    again = await consumer.process_payment(payment.id)

    assert gateway.calls == 1
    assert again.status == PaymentStatus.SUCCEEDED
