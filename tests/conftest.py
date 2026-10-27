import os
from collections.abc import AsyncIterator

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from app.broker import broker, declare_topology
from app.config import settings
from app.db import Session

MANAGEMENT_URL = httpx.URL(os.environ.get("RABBITMQ_MANAGEMENT_URL", "http://payments:payments@localhost:15672"))


def management_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=MANAGEMENT_URL.copy_with(username=None, password=None),
        auth=(MANAGEMENT_URL.username, MANAGEMENT_URL.password),
    )


@pytest.fixture(scope="session", autouse=True)
def migrated_database() -> None:
    assert settings.database_url.endswith("_test"), "tests must run against the test database"
    command.upgrade(Config("alembic.ini"), "head")


@pytest.fixture(autouse=True)
async def clean_tables() -> None:
    async with Session() as session, session.begin():
        await session.execute(text("TRUNCATE payments, outbox"))


@pytest.fixture(scope="session")
async def rabbit_vhost() -> str:
    vhost = settings.rabbitmq_url.rstrip("/").rsplit("/", 1)[-1]
    assert vhost == "test", "broker tests must run in the isolated 'test' vhost"
    async with management_client() as client:
        (await client.put(f"/api/vhosts/{vhost}")).raise_for_status()
        (
            await client.put(
                f"/api/permissions/{vhost}/{MANAGEMENT_URL.username}",
                json={"configure": ".*", "write": ".*", "read": ".*"},
            )
        ).raise_for_status()
    return vhost


@pytest.fixture(scope="session")
async def started_broker(rabbit_vhost: str) -> AsyncIterator[None]:
    await broker.connect()
    await declare_topology()
    await broker.start()
    yield
    await broker.stop()


@pytest.fixture
async def running_broker(started_broker: None, rabbit_vhost: str) -> None:
    async with management_client() as client:
        for queue in ("payments.new", "payments.new.retry", "payments.new.dlq"):
            (await client.delete(f"/api/queues/{rabbit_vhost}/{queue}/contents")).raise_for_status()
