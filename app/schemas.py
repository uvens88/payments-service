from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.models import Currency, PaymentStatus


def _has_nul(value: Any) -> bool:
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(_has_nul(k) or _has_nul(v) for k, v in value.items())
    if isinstance(value, list):
        return any(_has_nul(v) for v in value)
    return False


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = Field(max_length=1000)
    metadata: dict[str, Any] | None = None
    webhook_url: HttpUrl

    @field_validator("description", "metadata")
    @classmethod
    def reject_nul(cls, value: Any) -> Any:
        # PostgreSQL text and jsonb cannot store U+0000.
        if _has_nul(value):
            raise ValueError("NUL character is not allowed")
        return value


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias=AliasChoices("id", "payment_id"))
    status: PaymentStatus
    created_at: datetime


class PaymentOut(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] | None = Field(validation_alias=AliasChoices("metadata_", "metadata"))
    idempotency_key: str
    webhook_url: str
    processed_at: datetime | None


class PaymentEvent(BaseModel):
    payment_id: UUID
