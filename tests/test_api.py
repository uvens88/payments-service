from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select

from app.api import app
from app.config import settings
from app.db import Session
from app.models import PAYMENTS_NEW, OutboxEvent

BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #1",
    "metadata": {"order_id": 1},
    "webhook_url": "http://webhook-echo:8080/hook",
}


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test", headers={"X-API-Key": settings.api_key}) as c:
        yield c


async def outbox_count() -> int:
    async with Session() as session:
        return await session.scalar(select(func.count()).select_from(OutboxEvent))


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}])
async def test_requires_api_key(client: httpx.AsyncClient, headers: dict[str, str]) -> None:
    client.headers.pop("X-API-Key")
    for response in (
        await client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "k", **headers}),
        await client.get(f"/api/v1/payments/{uuid4()}", headers=headers),
    ):
        assert response.status_code == 401


async def test_create_payment_writes_payment_and_outbox_event(client: httpx.AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "key-1"})

    assert response.status_code == 202
    created = response.json()
    assert created.keys() == {"payment_id", "status", "created_at"}
    assert created["status"] == "pending"
    async with Session() as session:
        event = await session.scalar(select(OutboxEvent))
    assert event.routing_key == PAYMENTS_NEW
    assert event.payload == {"payment_id": created["payment_id"]}
    assert event.published_at is None


async def test_get_payment_returns_details(client: httpx.AsyncClient) -> None:
    created = (await client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "key-1"})).json()

    response = await client.get(f"/api/v1/payments/{created['payment_id']}")

    assert response.status_code == 200
    assert response.json() == {
        **created,
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #1",
        "metadata": {"order_id": 1},
        "idempotency_key": "key-1",
        "webhook_url": "http://webhook-echo:8080/hook",
        "processed_at": None,
    }


async def test_get_unknown_payment_is_404(client: httpx.AsyncClient) -> None:
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404


async def test_repeated_key_returns_same_payment_without_new_event(client: httpx.AsyncClient) -> None:
    first = await client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "key-1"})
    # Same request, amount written differently: still the same payment.
    second = await client.post("/api/v1/payments", json={**BODY, "amount": "100.5"}, headers={"Idempotency-Key": "key-1"})

    assert second.status_code == 202
    assert second.json() == first.json()
    assert await outbox_count() == 1


async def test_repeated_key_with_different_body_is_409(client: httpx.AsyncClient) -> None:
    await client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "key-1"})

    response = await client.post("/api/v1/payments", json={**BODY, "amount": "1"}, headers={"Idempotency-Key": "key-1"})

    assert response.status_code == 409
    assert await outbox_count() == 1


async def test_literal_backslash_u0000_text_is_accepted(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments", json={**BODY, "description": "\\u0000"}, headers={"Idempotency-Key": "key-1"}
    )

    assert response.status_code == 202


async def test_missing_idempotency_key_is_422(client: httpx.AsyncClient) -> None:
    assert (await client.post("/api/v1/payments", json=BODY)).status_code == 422


@pytest.mark.parametrize(
    "override",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"currency": "GBP"},
        {"webhook_url": "not-a-url"},
        {"unknown": 1},
        {"description": "a\u0000b"},
        {"metadata": {"k": ["\u0000"]}},
    ],
)
async def test_invalid_body_is_422(client: httpx.AsyncClient, override: dict[str, Any]) -> None:
    response = await client.post("/api/v1/payments", json={**BODY, **override}, headers={"Idempotency-Key": "key-1"})

    assert response.status_code == 422
    assert await outbox_count() == 0
