"""add branch_key to dh_fact_cash

Revision ID: 20260420_0011
Revises: 20260420_0010
Create Date: 2026-04-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260420_0011"
down_revision = "20260420_0010"
branch_labels = None
depends_on = None


TABLE_NAME = "dh_fact_cash"
COLUMN_NAME = "branch_key"
INDEX_NAME = "ix_dh_fact_cash_branch_key"


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if TABLE_NAME not in inspector.get_table_names():
        return

    columns = {col["name"] for col in inspector.get_columns(TABLE_NAME)}
    if COLUMN_NAME not in columns:
        op.add_column(TABLE_NAME, sa.Column(COLUMN_NAME, sa.String(length=100), nullable=True))

    indexes = {idx["name"] for idx in inspector.get_indexes(TABLE_NAME)}
    if INDEX_NAME not in indexes:
        op.create_index(INDEX_NAME, TABLE_NAME, [COLUMN_NAME], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if TABLE_NAME not in inspector.get_table_names():
        return

    indexes = {idx["name"] for idx in inspector.get_indexes(TABLE_NAME)}
    if INDEX_NAME in indexes:
        op.drop_index(INDEX_NAME, table_name=TABLE_NAME)

    columns = {col["name"] for col in inspector.get_columns(TABLE_NAME)}
    if COLUMN_NAME in columns:
        op.drop_column(TABLE_NAME, COLUMN_NAME)

