from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class DHJobRun(Base):
    __tablename__ = "dh_job_runs"

    job_run_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    job_name: Mapped[str] = mapped_column(String(80), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    files_seen: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    files_processed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_read: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_loaded: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_rejected: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text)


class DHJobFileStat(Base):
    __tablename__ = "dh_job_file_stats"

    run_file_key: Mapped[str] = mapped_column(String(128), primary_key=True)
    job_run_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    input_file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    records_read: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_loaded: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_rejected: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class DHDQRule(Base):
    __tablename__ = "dh_dq_rules"

    rule_name: Mapped[str] = mapped_column(String(120), primary_key=True)
    entity_name: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    field_name: Mapped[str | None] = mapped_column(String(80))
    rule_type: Mapped[str] = mapped_column(String(80), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)  # informative|reject
    rule_param: Mapped[str | None] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class DHDQResult(Base):
    __tablename__ = "dh_dq_results"

    dq_result_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    job_run_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    input_file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    entity_name: Mapped[str] = mapped_column(String(80), nullable=False)
    rule_name: Mapped[str] = mapped_column(String(120), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    action_taken: Mapped[str] = mapped_column(String(20), nullable=False)  # pass|reject
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class DHLovValue(Base):
    __tablename__ = "dh_lov_values"

    lookup_name: Mapped[str] = mapped_column(String(80), primary_key=True, index=True)
    valid_value: Mapped[str] = mapped_column(String(120), primary_key=True)
    description: Mapped[str | None] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class SCDBase:
    valid_from: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    valid_to: Mapped[datetime | None] = mapped_column(DateTime)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    attr_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    source_file: Mapped[str | None] = mapped_column(String(255))


class DHDimHousehold(SCDBase, Base):
    __tablename__ = "dh_dim_household"
    household_key: Mapped[str] = mapped_column(String(100), primary_key=True)


class DHDimCustomer(SCDBase, Base):
    __tablename__ = "dh_dim_customer"
    customer_key: Mapped[str] = mapped_column(String(100), primary_key=True)


class DHDimAssociatedParty(SCDBase, Base):
    __tablename__ = "dh_dim_associated_party"
    associated_party_key: Mapped[str] = mapped_column(String(100), primary_key=True)


class DHDimAccount(SCDBase, Base):
    __tablename__ = "dh_dim_account"
    account_key: Mapped[str] = mapped_column(String(100), primary_key=True)


class DHDimSubAccount(SCDBase, Base):
    __tablename__ = "dh_dim_sub_account"
    sub_account_key: Mapped[str] = mapped_column(String(100), primary_key=True)


class DHDimCountry(SCDBase, Base):
    __tablename__ = "dh_dim_country"
    country_code: Mapped[str] = mapped_column(String(20), primary_key=True)


class DHDimCurrency(SCDBase, Base):
    __tablename__ = "dh_dim_currency"
    currency_code: Mapped[str] = mapped_column(String(20), primary_key=True)


class DHDimCounterpartyAccount(SCDBase, Base):
    __tablename__ = "dh_dim_counterparty_account"
    counterparty_account_key: Mapped[str] = mapped_column(String(100), primary_key=True)


class DHDimTransactionType(SCDBase, Base):
    __tablename__ = "dh_dim_transaction_type"
    transaction_type_code: Mapped[str] = mapped_column(String(50), primary_key=True)


class DHBridgeHouseholdCustomer(Base):
    __tablename__ = "dh_bridge_household_customer"

    household_key: Mapped[str] = mapped_column(String(100), primary_key=True)
    customer_key: Mapped[str] = mapped_column(String(100), primary_key=True)
    valid_from: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    valid_to: Mapped[datetime | None] = mapped_column(DateTime)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class DHBridgeCustomerAccount(Base):
    __tablename__ = "dh_bridge_customer_account"

    customer_key: Mapped[str] = mapped_column(String(100), primary_key=True)
    account_key: Mapped[str] = mapped_column(String(100), primary_key=True)
    valid_from: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    valid_to: Mapped[datetime | None] = mapped_column(DateTime)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    relationship_type: Mapped[str | None] = mapped_column(String(50))


class DHBridgeCustomerAssociatedParty(Base):
    __tablename__ = "dh_bridge_customer_associated_party"

    customer_key: Mapped[str] = mapped_column(String(100), primary_key=True)
    associated_party_key: Mapped[str] = mapped_column(String(100), primary_key=True)
    valid_from: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    valid_to: Mapped[datetime | None] = mapped_column(DateTime)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class DHFactCash(Base):
    __tablename__ = "dh_fact_cash"

    transaction_key: Mapped[str] = mapped_column(String(120), primary_key=True)
    account_key: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    transaction_type_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    country_code: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    currency_code: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    counterparty_account_key: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    sub_account_key: Mapped[str | None] = mapped_column(String(100), index=True)
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    transaction_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    source_file: Mapped[str | None] = mapped_column(String(255))
    loaded_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
