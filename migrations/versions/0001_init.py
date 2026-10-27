"""init

Revision ID: 0001
Revises: 
Create Date: 2026-10-27 15:53:28.481747
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '0001'
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table('outbox',
    sa.Column('id', sa.BigInteger(), sa.Identity(always=False), nullable=False),
    sa.Column('aggregate_id', sa.Uuid(), nullable=False),
    sa.Column('routing_key', sa.String(length=255), nullable=False),
    sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('published_at', sa.DateTime(timezone=True), nullable=True),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_outbox'))
    )
    op.create_index('ix_outbox_unpublished', 'outbox', ['id'], unique=False, postgresql_where=sa.text('published_at IS NULL'))
    op.create_table('payments',
    sa.Column('id', sa.Uuid(), server_default=sa.text('gen_random_uuid()'), nullable=False),
    sa.Column('amount', sa.Numeric(precision=18, scale=2), nullable=False),
    sa.Column('currency', sa.Enum('RUB', 'USD', 'EUR', name='currency'), nullable=False),
    sa.Column('description', sa.Text(), nullable=False),
    sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('status', sa.Enum('pending', 'succeeded', 'failed', name='payment_status'), server_default='pending', nullable=False),
    sa.Column('idempotency_key', sa.String(length=255), nullable=False),
    sa.Column('webhook_url', sa.Text(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('processed_at', sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint('amount > 0', name=op.f('ck_payments_amount_positive')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_payments')),
    sa.UniqueConstraint('idempotency_key', name=op.f('uq_payments_idempotency_key'))
    )


def downgrade() -> None:
    op.drop_table('payments')
    op.drop_index('ix_outbox_unpublished', table_name='outbox', postgresql_where=sa.text('published_at IS NULL'))
    op.drop_table('outbox')
    # drop_table leaves the enum types behind; without this a re-upgrade fails.
    sa.Enum(name='payment_status').drop(op.get_bind())
    sa.Enum(name='currency').drop(op.get_bind())
