"""rename dh_fact_cash country_code to country_code_2

Revision ID: 20260419_0009
Revises: 20260418_0008
Create Date: 2026-04-19
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260419_0009"
down_revision = "20260418_0008"
branch_labels = None
depends_on = None


TABLE_NAME = "dh_fact_cash"
OLD_COLUMN = "country_code"
NEW_COLUMN = "country_code_2"
OLD_INDEX = "ix_dh_fact_cash_country_code"
NEW_INDEX = "ix_dh_fact_cash_country_code_2"


def _column_names() -> set[str]:
    bind = op.get_bind()
    inspector = inspect(bind)
    return {c["name"] for c in inspector.get_columns(TABLE_NAME)}


def _index_names() -> set[str]:
    bind = op.get_bind()
    inspector = inspect(bind)
    return {i["name"] for i in inspector.get_indexes(TABLE_NAME)}


def upgrade() -> None:
    columns = _column_names()
    if OLD_COLUMN in columns and NEW_COLUMN not in columns:
        with op.batch_alter_table(TABLE_NAME) as batch_op:
            batch_op.alter_column(OLD_COLUMN, new_column_name=NEW_COLUMN, existing_type=sa.String(length=20))

    indexes = _index_names()
    if OLD_INDEX in indexes:
        with op.batch_alter_table(TABLE_NAME) as batch_op:
            batch_op.drop_index(OLD_INDEX)

    indexes = _index_names()
    if NEW_INDEX not in indexes:
        with op.batch_alter_table(TABLE_NAME) as batch_op:
            batch_op.create_index(NEW_INDEX, [NEW_COLUMN], unique=False)


def downgrade() -> None:
    columns = _column_names()
    if NEW_COLUMN in columns and OLD_COLUMN not in columns:
        with op.batch_alter_table(TABLE_NAME) as batch_op:
            batch_op.alter_column(NEW_COLUMN, new_column_name=OLD_COLUMN, existing_type=sa.String(length=20))

    indexes = _index_names()
    if NEW_INDEX in indexes:
        with op.batch_alter_table(TABLE_NAME) as batch_op:
            batch_op.drop_index(NEW_INDEX)

    indexes = _index_names()
    if OLD_INDEX not in indexes:
        with op.batch_alter_table(TABLE_NAME) as batch_op:
            batch_op.create_index(OLD_INDEX, [OLD_COLUMN], unique=False)
