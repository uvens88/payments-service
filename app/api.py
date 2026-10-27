import secrets
from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, status
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import Session
from app.models import PAYMENTS_NEW, OutboxEvent, Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentOut


async def require_api_key(x_api_key: Annotated[str | None, Header()] = None) -> None:
    if x_api_key is None or not secrets.compare_digest(x_api_key.encode(), settings.api_key.encode()):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or missing API key")


async def get_session() -> AsyncIterator[AsyncSession]:
    async with Session() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])


def _same_request(payment: Payment, body: PaymentCreate) -> bool:
    return (
        payment.amount == body.amount
        and payment.currency == body.currency
        and payment.description == body.description
        and payment.metadata_ == body.metadata
        and payment.webhook_url == str(body.webhook_url)
    )


@router.post("/payments", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentAccepted)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    session: SessionDep,
) -> Payment:
    async with session.begin():
        # The payment and its event commit together or not at all: that is the outbox guarantee.
        payment_id = await session.scalar(
            insert(Payment)
            .values(
                amount=body.amount,
                currency=body.currency,
                description=body.description,
                metadata_=body.metadata,
                webhook_url=str(body.webhook_url),
                idempotency_key=idempotency_key,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment.id)
        )
        if payment_id is not None:
            session.add(
                OutboxEvent(
                    aggregate_id=payment_id,
                    routing_key=PAYMENTS_NEW,
                    payload={"payment_id": str(payment_id)},
                )
            )

    payment = await session.scalar(select(Payment).where(Payment.idempotency_key == idempotency_key))
    if payment_id is None and not _same_request(payment, body):
        raise HTTPException(status.HTTP_409_CONFLICT, "Idempotency-Key was already used with a different request")
    return payment


@router.get("/payments/{payment_id}", response_model=PaymentOut)
async def get_payment(payment_id: UUID, session: SessionDep) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return payment


app = FastAPI(title="Payments service")
app.include_router(router)
