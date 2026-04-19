"""add secondary_account_key to dh_fact_cash

Revision ID: 20260418_0008
Revises: 20260418_0007
Create Date: 2026-04-18
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260418_0008"
down_revision = "20260418_0007"
branch_labels = None
depends_on = None


TABLE_NAME = "dh_fact_cash"
COLUMN_NAME = "secondary_account_key"
INDEX_NAME = "ix_dh_fact_cash_secondary_account_key"


def _column_names() -> set[str]:
    bind = op.get_bind()
    inspector = inspect(bind)
    return {c["name"] for c in inspector.get_columns(TABLE_NAME)}


def _index_names() -> set[str]:
    bind = op.get_bind()
    inspector = inspect(bind)
    return {i["name"] for i in inspector.get_indexes(TABLE_NAME)}


def upgrade() -> None:
    if COLUMN_NAME not in _column_names():
        with op.batch_alter_table(TABLE_NAME) as batch_op:
            batch_op.add_column(sa.Column(COLUMN_NAME, sa.String(length=100), nullable=True))

    if INDEX_NAME not in _index_names():
        with op.batch_alter_table(TABLE_NAME) as batch_op:
            batch_op.create_index(INDEX_NAME, [COLUMN_NAME], unique=False)


def downgrade() -> None:
    if INDEX_NAME in _index_names():
        with op.batch_alter_table(TABLE_NAME) as batch_op:
            batch_op.drop_index(INDEX_NAME)

    if COLUMN_NAME in _column_names():
        with op.batch_alter_table(TABLE_NAME) as batch_op:
            batch_op.drop_column(COLUMN_NAME)
