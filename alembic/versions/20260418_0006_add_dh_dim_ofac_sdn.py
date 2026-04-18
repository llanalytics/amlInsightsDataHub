"""add dh_dim_ofac_sdn reference dimension

Revision ID: 20260418_0006
Revises: 20260418_0005
Create Date: 2026-04-18
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260418_0006"
down_revision = "20260418_0005"
branch_labels = None
depends_on = None


TABLE_NAME = "dh_dim_ofac_sdn"


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if TABLE_NAME in inspector.get_table_names():
        return

    op.create_table(
        TABLE_NAME,
        sa.Column("sdn_uid", sa.String(length=40), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("attr_json", sa.Text(), nullable=False),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("sdn_uid", "valid_from"),
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if TABLE_NAME in inspector.get_table_names():
        op.drop_table(TABLE_NAME)
