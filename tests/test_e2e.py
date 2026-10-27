"""Smoke test of the running docker compose stack: `docker compose --profile test run --rm tests pytest -m e2e`."""

import asyncio
import os
from uuid import uuid4

import httpx
import pytest

pytestmark = pytest.mark.e2e

FINAL = {"succeeded", "failed"}


@pytest.fixture
async def api() -> httpx.AsyncClient:
    async with httpx.AsyncClient(
        base_url=os.environ["API_URL"], headers={"X-API-Key": os.environ["STACK_API_KEY"]}, timeout=10
    ) as client:
        yield client


async def create(api: httpx.AsyncClient, webhook_url: str) -> str:
    response = await api.post(
        "/api/v1/payments",
        json={"amount": "42.00", "currency": "EUR", "description": "e2e", "webhook_url": webhook_url},
        headers={"Idempotency-Key": str(uuid4())},
    )
    assert response.status_code == 202, response.text
    return response.json()["payment_id"]


async def dlq_payment_ids() -> list[str]:
    url = httpx.URL(os.environ["RABBITMQ_MANAGEMENT_URL"])
    async with httpx.AsyncClient(
        base_url=url.copy_with(username=None, password=None), auth=(url.username, url.password)
    ) as client:
        response = await client.post(
            "/api/queues/%2F/payments.new.dlq/get",
            json={"count": 1000, "ackmode": "ack_requeue_true", "encoding": "auto"},
        )
        response.raise_for_status()
        return [m["payload"] for m in response.json()]


async def test_payment_reaches_final_status(api: httpx.AsyncClient) -> None:
    payment_id = await create(api, "http://webhook-echo:8080/e2e")

    async with asyncio.timeout(30):
        while (payment := (await api.get(f"/api/v1/payments/{payment_id}")).json())["status"] not in FINAL:
            await asyncio.sleep(0.5)
    assert payment["processed_at"] is not None


async def test_undeliverable_webhook_ends_in_dlq(api: httpx.AsyncClient) -> None:
    payment_id = await create(api, "http://unreachable.invalid/hook")

    async with asyncio.timeout(90):  # 2-5 s gateway + retry delays (5 s + 10 s by default)
        while not any(payment_id in payload for payload in await dlq_payment_ids()):
            await asyncio.sleep(1)
    assert (await api.get(f"/api/v1/payments/{payment_id}")).json()["status"] in FINAL
