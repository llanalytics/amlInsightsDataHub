"""add lov values table

Revision ID: 20260417_0003
Revises: 20260417_0002
Create Date: 2026-04-17 18:05:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260417_0003"
down_revision = "20260417_0002"
branch_labels = None
depends_on = None


def _table_exists(bind, table_name: str) -> bool:
    insp = sa.inspect(bind)
    return insp.has_table(table_name)


def _index_exists(bind, table_name: str, index_name: str) -> bool:
    insp = sa.inspect(bind)
    return index_name in {idx["name"] for idx in insp.get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, "dh_lov_values"):
        return

    op.create_table(
        "dh_lov_values",
        sa.Column("lookup_name", sa.String(length=80), nullable=False),
        sa.Column("valid_value", sa.String(length=120), nullable=False),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.PrimaryKeyConstraint("lookup_name", "valid_value"),
    )
    if not _index_exists(bind, "dh_lov_values", op.f("ix_dh_lov_values_lookup_name")):
        op.create_index(op.f("ix_dh_lov_values_lookup_name"), "dh_lov_values", ["lookup_name"], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, "dh_lov_values"):
        return

    if _index_exists(bind, "dh_lov_values", op.f("ix_dh_lov_values_lookup_name")):
        op.drop_index(op.f("ix_dh_lov_values_lookup_name"), table_name="dh_lov_values")
    op.drop_table("dh_lov_values")
