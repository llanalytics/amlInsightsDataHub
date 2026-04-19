"""add panama papers reference tables

Revision ID: 20260418_0007
Revises: 20260418_0006
Create Date: 2026-04-18
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260418_0007"
down_revision = "20260418_0006"
branch_labels = None
depends_on = None


PANAMA_NODE_TABLE = "dh_dim_panama_node"
PANAMA_REL_TABLE = "dh_bridge_panama_relationship"


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    tables = set(inspector.get_table_names())

    if PANAMA_NODE_TABLE not in tables:
        op.create_table(
            PANAMA_NODE_TABLE,
            sa.Column("node_id", sa.String(length=40), nullable=False),
            sa.Column("node_type", sa.String(length=20), nullable=False),
            sa.Column("valid_from", sa.DateTime(), nullable=False),
            sa.Column("valid_to", sa.DateTime(), nullable=True),
            sa.Column("is_current", sa.Boolean(), nullable=False),
            sa.Column("attr_json", sa.Text(), nullable=False),
            sa.Column("source_file", sa.String(length=255), nullable=True),
            sa.PrimaryKeyConstraint("node_id", "node_type", "valid_from"),
        )

    if PANAMA_REL_TABLE not in tables:
        op.create_table(
            PANAMA_REL_TABLE,
            sa.Column("start_node_id", sa.String(length=40), nullable=False),
            sa.Column("end_node_id", sa.String(length=40), nullable=False),
            sa.Column("rel_type", sa.String(length=60), nullable=False),
            sa.Column("valid_from", sa.DateTime(), nullable=False),
            sa.Column("valid_to", sa.DateTime(), nullable=True),
            sa.Column("is_current", sa.Boolean(), nullable=False),
            sa.Column("link", sa.String(length=120), nullable=True),
            sa.Column("status", sa.String(length=60), nullable=True),
            sa.Column("start_date", sa.String(length=40), nullable=True),
            sa.Column("end_date", sa.String(length=40), nullable=True),
            sa.Column("source_id", sa.String(length=60), nullable=True),
            sa.PrimaryKeyConstraint("start_node_id", "end_node_id", "rel_type", "valid_from"),
        )
        op.create_index("ix_dh_bridge_panama_relationship_source_id", PANAMA_REL_TABLE, ["source_id"])


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    tables = set(inspector.get_table_names())

    if PANAMA_REL_TABLE in tables:
        with op.batch_alter_table(PANAMA_REL_TABLE) as batch_op:
            batch_op.drop_index("ix_dh_bridge_panama_relationship_source_id")
        op.drop_table(PANAMA_REL_TABLE)

    if PANAMA_NODE_TABLE in tables:
        op.drop_table(PANAMA_NODE_TABLE)
