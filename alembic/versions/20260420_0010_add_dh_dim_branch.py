"""add dh_dim_branch reference dimension

Revision ID: 20260420_0010
Revises: 20260419_0009
Create Date: 2026-04-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260420_0010"
down_revision = "20260419_0009"
branch_labels = None
depends_on = None


TABLE_NAME = "dh_dim_branch"


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if TABLE_NAME in inspector.get_table_names():
        return

    op.create_table(
        TABLE_NAME,
        sa.Column("branch_key", sa.String(length=100), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("attr_json", sa.Text(), nullable=False),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("branch_key", "valid_from"),
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if TABLE_NAME in inspector.get_table_names():
        op.drop_table(TABLE_NAME)
