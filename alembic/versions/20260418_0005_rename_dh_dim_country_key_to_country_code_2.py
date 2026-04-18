"""rename dh_dim_country key column to country_code_2

Revision ID: 20260418_0005
Revises: 20260418_0004
Create Date: 2026-04-18 09:52:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260418_0005"
down_revision = "20260418_0004"
branch_labels = None
depends_on = None


TABLE_NAME = "dh_dim_country"
OLD_COLUMN = "country_code"
NEW_COLUMN = "country_code_2"


def _table_exists(bind, table_name: str) -> bool:
    insp = sa.inspect(bind)
    return insp.has_table(table_name)


def _column_exists(bind, table_name: str, column_name: str) -> bool:
    insp = sa.inspect(bind)
    return column_name in {col["name"] for col in insp.get_columns(table_name)}


def _is_sqlite(bind) -> bool:
    return bind.dialect.name == "sqlite"


def upgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, TABLE_NAME):
        return

    has_old = _column_exists(bind, TABLE_NAME, OLD_COLUMN)
    has_new = _column_exists(bind, TABLE_NAME, NEW_COLUMN)

    if has_new:
        return
    if not has_old:
        return

    if _is_sqlite(bind):
        # SQLite requires batch operations for column rename in migrations.
        with op.batch_alter_table(TABLE_NAME) as batch_op:
            batch_op.alter_column(OLD_COLUMN, new_column_name=NEW_COLUMN)
    else:
        op.alter_column(TABLE_NAME, OLD_COLUMN, new_column_name=NEW_COLUMN)


def downgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, TABLE_NAME):
        return

    has_old = _column_exists(bind, TABLE_NAME, OLD_COLUMN)
    has_new = _column_exists(bind, TABLE_NAME, NEW_COLUMN)

    if has_old:
        return
    if not has_new:
        return

    if _is_sqlite(bind):
        with op.batch_alter_table(TABLE_NAME) as batch_op:
            batch_op.alter_column(NEW_COLUMN, new_column_name=OLD_COLUMN)
    else:
        op.alter_column(TABLE_NAME, NEW_COLUMN, new_column_name=OLD_COLUMN)
