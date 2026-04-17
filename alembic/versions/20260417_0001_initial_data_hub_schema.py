"""initial data hub schema

Revision ID: 20260417_0001
Revises:
Create Date: 2026-04-17 16:28:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260417_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dh_job_runs",
        sa.Column("job_run_id", sa.String(length=36), nullable=False),
        sa.Column("job_name", sa.String(length=80), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("ended_at", sa.DateTime(), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("files_seen", sa.Integer(), nullable=False),
        sa.Column("files_processed", sa.Integer(), nullable=False),
        sa.Column("records_read", sa.Integer(), nullable=False),
        sa.Column("records_loaded", sa.Integer(), nullable=False),
        sa.Column("records_rejected", sa.Integer(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("job_run_id"),
    )

    op.create_table(
        "dh_job_file_stats",
        sa.Column("run_file_key", sa.String(length=128), nullable=False),
        sa.Column("job_run_id", sa.String(length=36), nullable=False),
        sa.Column("input_file_name", sa.String(length=255), nullable=False),
        sa.Column("records_read", sa.Integer(), nullable=False),
        sa.Column("records_loaded", sa.Integer(), nullable=False),
        sa.Column("records_rejected", sa.Integer(), nullable=False),
        sa.Column("processed_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("run_file_key"),
    )
    op.create_index(
        op.f("ix_dh_job_file_stats_job_run_id"),
        "dh_job_file_stats",
        ["job_run_id"],
        unique=False,
    )

    op.create_table(
        "dh_dq_rules",
        sa.Column("rule_name", sa.String(length=120), nullable=False),
        sa.Column("entity_name", sa.String(length=80), nullable=False),
        sa.Column("field_name", sa.String(length=80), nullable=True),
        sa.Column("rule_type", sa.String(length=80), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("rule_param", sa.String(length=255), nullable=True),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("rule_name"),
    )
    op.create_index(op.f("ix_dh_dq_rules_entity_name"), "dh_dq_rules", ["entity_name"], unique=False)

    op.create_table(
        "dh_dq_results",
        sa.Column("dq_result_id", sa.String(length=64), nullable=False),
        sa.Column("job_run_id", sa.String(length=36), nullable=False),
        sa.Column("input_file_name", sa.String(length=255), nullable=False),
        sa.Column("row_number", sa.Integer(), nullable=False),
        sa.Column("entity_name", sa.String(length=80), nullable=False),
        sa.Column("rule_name", sa.String(length=120), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("action_taken", sa.String(length=20), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("dq_result_id"),
    )
    op.create_index(op.f("ix_dh_dq_results_job_run_id"), "dh_dq_results", ["job_run_id"], unique=False)

    op.create_table(
        "dh_dim_household",
        sa.Column("household_key", sa.String(length=100), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("attr_json", sa.Text(), nullable=False),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("household_key", "valid_from"),
    )

    op.create_table(
        "dh_dim_customer",
        sa.Column("customer_key", sa.String(length=100), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("attr_json", sa.Text(), nullable=False),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("customer_key", "valid_from"),
    )

    op.create_table(
        "dh_dim_associated_party",
        sa.Column("associated_party_key", sa.String(length=100), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("attr_json", sa.Text(), nullable=False),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("associated_party_key", "valid_from"),
    )

    op.create_table(
        "dh_dim_account",
        sa.Column("account_key", sa.String(length=100), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("attr_json", sa.Text(), nullable=False),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("account_key", "valid_from"),
    )

    op.create_table(
        "dh_dim_sub_account",
        sa.Column("sub_account_key", sa.String(length=100), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("attr_json", sa.Text(), nullable=False),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("sub_account_key", "valid_from"),
    )

    op.create_table(
        "dh_dim_country",
        sa.Column("country_code", sa.String(length=20), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("attr_json", sa.Text(), nullable=False),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("country_code", "valid_from"),
    )

    op.create_table(
        "dh_dim_currency",
        sa.Column("currency_code", sa.String(length=20), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("attr_json", sa.Text(), nullable=False),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("currency_code", "valid_from"),
    )

    op.create_table(
        "dh_dim_counterparty_account",
        sa.Column("counterparty_account_key", sa.String(length=100), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("attr_json", sa.Text(), nullable=False),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("counterparty_account_key", "valid_from"),
    )

    op.create_table(
        "dh_dim_transaction_type",
        sa.Column("transaction_type_code", sa.String(length=50), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("attr_json", sa.Text(), nullable=False),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("transaction_type_code", "valid_from"),
    )

    op.create_table(
        "dh_bridge_household_customer",
        sa.Column("household_key", sa.String(length=100), nullable=False),
        sa.Column("customer_key", sa.String(length=100), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("household_key", "customer_key", "valid_from"),
    )

    op.create_table(
        "dh_bridge_customer_account",
        sa.Column("customer_key", sa.String(length=100), nullable=False),
        sa.Column("account_key", sa.String(length=100), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("customer_key", "account_key", "valid_from"),
    )

    op.create_table(
        "dh_bridge_customer_associated_party",
        sa.Column("customer_key", sa.String(length=100), nullable=False),
        sa.Column("associated_party_key", sa.String(length=100), nullable=False),
        sa.Column("valid_from", sa.DateTime(), nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("customer_key", "associated_party_key", "valid_from"),
    )

    op.create_table(
        "dh_fact_cash",
        sa.Column("transaction_key", sa.String(length=120), nullable=False),
        sa.Column("account_key", sa.String(length=100), nullable=False),
        sa.Column("transaction_type_code", sa.String(length=50), nullable=False),
        sa.Column("country_code", sa.String(length=20), nullable=False),
        sa.Column("currency_code", sa.String(length=20), nullable=False),
        sa.Column("counterparty_account_key", sa.String(length=100), nullable=False),
        sa.Column("sub_account_key", sa.String(length=100), nullable=True),
        sa.Column("amount", sa.Float(), nullable=False),
        sa.Column("transaction_ts", sa.DateTime(), nullable=False),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.Column("loaded_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("transaction_key"),
    )
    op.create_index(op.f("ix_dh_fact_cash_account_key"), "dh_fact_cash", ["account_key"], unique=False)
    op.create_index(op.f("ix_dh_fact_cash_transaction_type_code"), "dh_fact_cash", ["transaction_type_code"], unique=False)
    op.create_index(op.f("ix_dh_fact_cash_country_code"), "dh_fact_cash", ["country_code"], unique=False)
    op.create_index(op.f("ix_dh_fact_cash_currency_code"), "dh_fact_cash", ["currency_code"], unique=False)
    op.create_index(op.f("ix_dh_fact_cash_counterparty_account_key"), "dh_fact_cash", ["counterparty_account_key"], unique=False)
    op.create_index(op.f("ix_dh_fact_cash_sub_account_key"), "dh_fact_cash", ["sub_account_key"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_dh_fact_cash_sub_account_key"), table_name="dh_fact_cash")
    op.drop_index(op.f("ix_dh_fact_cash_counterparty_account_key"), table_name="dh_fact_cash")
    op.drop_index(op.f("ix_dh_fact_cash_currency_code"), table_name="dh_fact_cash")
    op.drop_index(op.f("ix_dh_fact_cash_country_code"), table_name="dh_fact_cash")
    op.drop_index(op.f("ix_dh_fact_cash_transaction_type_code"), table_name="dh_fact_cash")
    op.drop_index(op.f("ix_dh_fact_cash_account_key"), table_name="dh_fact_cash")
    op.drop_table("dh_fact_cash")

    op.drop_table("dh_bridge_customer_associated_party")
    op.drop_table("dh_bridge_customer_account")
    op.drop_table("dh_bridge_household_customer")

    op.drop_table("dh_dim_transaction_type")
    op.drop_table("dh_dim_counterparty_account")
    op.drop_table("dh_dim_currency")
    op.drop_table("dh_dim_country")
    op.drop_table("dh_dim_sub_account")
    op.drop_table("dh_dim_account")
    op.drop_table("dh_dim_associated_party")
    op.drop_table("dh_dim_customer")
    op.drop_table("dh_dim_household")

    op.drop_index(op.f("ix_dh_dq_results_job_run_id"), table_name="dh_dq_results")
    op.drop_table("dh_dq_results")

    op.drop_index(op.f("ix_dh_dq_rules_entity_name"), table_name="dh_dq_rules")
    op.drop_table("dh_dq_rules")

    op.drop_index(op.f("ix_dh_job_file_stats_job_run_id"), table_name="dh_job_file_stats")
    op.drop_table("dh_job_file_stats")

    op.drop_table("dh_job_runs")
