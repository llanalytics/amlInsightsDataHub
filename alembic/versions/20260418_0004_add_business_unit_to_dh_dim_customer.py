"""add business_unit to dh_dim_customer

Revision ID: 20260418_0004
Revises: 20260417_0003
Create Date: 2026-04-18 09:25:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260418_0004"
down_revision = "20260417_0003"
branch_labels = None
depends_on = None


TABLE_NAME = "dh_dim_customer"
COLUMN_NAME = "business_unit"


def _table_exists(bind, table_name: str) -> bool:
    insp = sa.inspect(bind)
    return insp.has_table(table_name)


def _column_exists(bind, table_name: str, column_name: str) -> bool:
    insp = sa.inspect(bind)
    return column_name in {col["name"] for col in insp.get_columns(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, TABLE_NAME):
        return
    if _column_exists(bind, TABLE_NAME, COLUMN_NAME):
        return

    op.add_column(TABLE_NAME, sa.Column(COLUMN_NAME, sa.String(length=120), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, TABLE_NAME):
        return
    if not _column_exists(bind, TABLE_NAME, COLUMN_NAME):
        return

    op.drop_column(TABLE_NAME, COLUMN_NAME)
