from __future__ import annotations

import csv
import hashlib
import json
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.config import LANDING_DIR, PROCESSED_DIR, REJECTED_DIR
from app.dim_schema_validator import (
    DimSchemaError,
    collect_all_schema_lookup_names,
    validate_dim_attrs,
)
from app.dq import evaluate_rule
from app.lov_loader import lookup_names_from_csv
from app.models import (
    DHDQResult,
    DHDQRule,
    DHBridgeCustomerAccount,
    DHBridgeCustomerAssociatedParty,
    DHBridgePanamaRelationship,
    DHBridgeHouseholdCustomer,
    DHDimAccount,
    DHDimAssociatedParty,
    DHDimBranch,
    DHDimCounterpartyAccount,
    DHDimCountry,
    DHDimCurrency,
    DHDimOfacSdn,
    DHDimPanamaNode,
    DHDimCustomer,
    DHDimHousehold,
    DHDimSubAccount,
    DHDimTransactionType,
    DHFactCash,
    DHLovValue,
    DHJobFileStat,
    DHJobRun,
)


@dataclass
class FileSummary:
    file_name: str
    read: int = 0
    loaded: int = 0
    rejected: int = 0


@dataclass(frozen=True)
class DimSpec:
    table_name: str
    entity_name: str
    model: type
    natural_field: str


DIM_SPECS: list[DimSpec] = [
    DimSpec("dh_dim_country", "dh_dim_country", DHDimCountry, "country_code_2"),
    DimSpec("dh_dim_currency", "dh_dim_currency", DHDimCurrency, "currency_code"),
    DimSpec("dh_dim_transaction_type", "dh_dim_transaction_type", DHDimTransactionType, "transaction_type_code"),
    DimSpec("dh_dim_household", "dh_dim_household", DHDimHousehold, "household_key"),
    DimSpec("dh_dim_customer", "dh_dim_customer", DHDimCustomer, "customer_key"),
    DimSpec("dh_dim_associated_party", "dh_dim_associated_party", DHDimAssociatedParty, "associated_party_key"),
    DimSpec("dh_dim_account", "dh_dim_account", DHDimAccount, "account_key"),
    DimSpec("dh_dim_sub_account", "dh_dim_sub_account", DHDimSubAccount, "sub_account_key"),
    DimSpec("dh_dim_branch", "dh_dim_branch", DHDimBranch, "branch_key"),
    DimSpec("dh_dim_ofac_sdn", "dh_dim_ofac_sdn", DHDimOfacSdn, "sdn_uid"),
    DimSpec("dh_dim_panama_node", "dh_dim_panama_node", DHDimPanamaNode, "node_id"),
    DimSpec("dh_dim_counterparty_account", "dh_dim_counterparty_account", DHDimCounterpartyAccount, "counterparty_account_key"),
]

TABLE_PROCESS_ORDER: list[str] = [
    *[s.table_name for s in DIM_SPECS],
    "dh_bridge_household_customer",
    "dh_bridge_customer_account",
    "dh_bridge_customer_associated_party",
    "dh_bridge_panama_relationship",
    "dh_fact_cash",
]

PRIMARY_RELATIONSHIP_TYPE = "Primary"


TABLE_TO_ENTITY: dict[str, str] = {
    **{s.table_name: s.entity_name for s in DIM_SPECS},
    "dh_bridge_household_customer": "dh_bridge_household_customer",
    "dh_bridge_customer_account": "dh_bridge_customer_account",
    "dh_bridge_customer_associated_party": "dh_bridge_customer_associated_party",
    "dh_bridge_panama_relationship": "dh_bridge_panama_relationship",
    "dh_fact_cash": "cash",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json_hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _val(row: dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


def _scd_upsert(
    db: Session,
    model,
    natural_field: str,
    natural_key: str,
    attrs: dict,
    static_fields: dict | None,
    now: datetime,
    source_file: str,
    key_fields: list[str] | None = None,
) -> None:
    if not natural_key:
        return
    key_fields = key_fields or []

    filters = [
        getattr(model, natural_field) == natural_key,
        model.is_current.is_(True),
    ]
    for field_name in key_fields:
        if field_name not in static_fields:
            continue
        filters.append(getattr(model, field_name) == static_fields[field_name])

    current = db.execute(select(model).where(and_(*filters))).scalar_one_or_none()

    attr_json = json.dumps(attrs, sort_keys=True)
    static_fields = static_fields or {}
    if current is None:
        row = model(
            **{natural_field: natural_key},
            **static_fields,
            valid_from=now,
            valid_to=None,
            is_current=True,
            attr_json=attr_json,
            source_file=source_file,
        )
        db.add(row)
        return

    attrs_unchanged = _json_hash(json.loads(current.attr_json or "{}")) == _json_hash(attrs)
    static_fields_unchanged = all(
        (getattr(current, field_name) or None) == (field_value or None)
        for field_name, field_value in static_fields.items()
    )
    if attrs_unchanged and static_fields_unchanged:
        return

    current.is_current = False
    current.valid_to = now
    next_row = model(
        **{natural_field: natural_key},
        **static_fields,
        valid_from=now,
        valid_to=None,
        is_current=True,
        attr_json=attr_json,
        source_file=source_file,
    )
    db.add(next_row)


def _ensure_bridge(db: Session, model, key_values: dict, now: datetime) -> None:
    filters = [getattr(model, k) == v for k, v in key_values.items()]
    current = db.execute(select(model).where(and_(*filters, model.is_current.is_(True)))).scalar_one_or_none()
    if current is None:
        db.add(model(**key_values, valid_from=now, valid_to=None, is_current=True))


def _is_primary_relationship(relationship_type: str | None) -> bool:
    return (relationship_type or "").strip().casefold() == PRIMARY_RELATIONSHIP_TYPE.casefold()


def _active_account_customer_rows(db: Session, account_key: str) -> list[DHBridgeCustomerAccount]:
    return db.execute(
        select(DHBridgeCustomerAccount).where(
            and_(
                DHBridgeCustomerAccount.account_key == account_key,
                DHBridgeCustomerAccount.is_current.is_(True),
            )
        )
    ).scalars().all()


def _validate_account_primary_relationship_rule(
    db: Session,
    customer_key: str,
    account_key: str,
    relationship_type: str | None,
) -> str | None:
    active_rows = _active_account_customer_rows(db, account_key)

    # Simulate post-upsert state for the target customer/account pair.
    remaining_rows = [row for row in active_rows if row.customer_key != customer_key]
    primary_count = sum(1 for row in remaining_rows if _is_primary_relationship(row.relationship_type))
    if _is_primary_relationship(relationship_type):
        primary_count += 1

    if primary_count == 0:
        return (
            "Business rule failed: account must have exactly one Primary relationship_type in "
            "dh_bridge_customer_account; operation would leave zero Primary relationships."
        )

    if primary_count > 1:
        return (
            "Business rule failed: account must have exactly one Primary relationship_type in "
            "dh_bridge_customer_account; operation would create multiple Primary relationships."
        )

    return None


def _ensure_bridge_customer_account(
    db: Session,
    key_values: dict[str, str],
    relationship_type: str | None,
    now: datetime,
) -> None:
    filters = [getattr(DHBridgeCustomerAccount, k) == v for k, v in key_values.items()]
    current = db.execute(
        select(DHBridgeCustomerAccount).where(and_(*filters, DHBridgeCustomerAccount.is_current.is_(True)))
    ).scalar_one_or_none()

    rel = relationship_type or None
    if current is None:
        db.add(
            DHBridgeCustomerAccount(
                **key_values,
                valid_from=now,
                valid_to=None,
                is_current=True,
                relationship_type=rel,
            )
        )
        return

    if (current.relationship_type or None) == rel:
        return

    current.is_current = False
    current.valid_to = now
    db.add(
        DHBridgeCustomerAccount(
            **key_values,
            valid_from=now,
            valid_to=None,
            is_current=True,
            relationship_type=rel,
        )
    )


def _ensure_bridge_panama_relationship(
    db: Session,
    key_values: dict[str, str],
    payload: dict[str, str | None],
    now: datetime,
) -> None:
    filters = [getattr(DHBridgePanamaRelationship, k) == v for k, v in key_values.items()]
    current = db.execute(
        select(DHBridgePanamaRelationship).where(and_(*filters, DHBridgePanamaRelationship.is_current.is_(True)))
    ).scalar_one_or_none()

    normalized_payload = {k: (v or None) for k, v in payload.items()}

    if current is None:
        db.add(
            DHBridgePanamaRelationship(
                **key_values,
                valid_from=now,
                valid_to=None,
                is_current=True,
                **normalized_payload,
            )
        )
        return

    unchanged = all((getattr(current, k) or None) == v for k, v in normalized_payload.items())
    if unchanged:
        return

    current.is_current = False
    current.valid_to = now
    db.add(
        DHBridgePanamaRelationship(
            **key_values,
            valid_from=now,
            valid_to=None,
            is_current=True,
            **normalized_payload,
        )
    )


def _record_dq_issue(
    db: Session,
    run_id: str,
    file_name: str,
    row_number: int,
    entity_name: str,
    rule_name: str,
    severity: str,
    action: str,
    message: str,
) -> None:
    db.add(
        DHDQResult(
            dq_result_id=f"{run_id}:{file_name}:{row_number}:{rule_name}:{uuid.uuid4().hex[:8]}",
            job_run_id=run_id,
            input_file_name=file_name,
            row_number=row_number,
            entity_name=entity_name,
            rule_name=rule_name,
            severity=severity,
            action_taken=action,
            message=message,
            created_at=_utc_now(),
        )
    )


def _parse_dt(raw: str) -> datetime:
    raw = (raw or "").strip()
    if not raw:
        return _utc_now()
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return _utc_now()


def _lov_value_exists(db: Session, lookup_name: str, valid_value: str) -> bool:
    lookup = (lookup_name or "").strip()
    value = (valid_value or "").strip()
    if not lookup or not value:
        return False
    return (
        db.execute(
            select(DHLovValue).where(
                and_(
                    DHLovValue.lookup_name == lookup,
                    DHLovValue.valid_value == value,
                    DHLovValue.is_active.is_(True),
                )
            )
        ).scalar_one_or_none()
        is not None
    )


def _dim_exists(db: Session, model, field_name: str, value: str) -> bool:
    if not value:
        return False
    return (
        db.execute(
            select(model).where(
                and_(
                    getattr(model, field_name) == value,
                    model.is_current.is_(True),
                )
            )
        ).scalar_one_or_none()
        is not None
    )


def _dimension_lookup_exists(db: Session, table_name: str, field_name: str, value: str) -> bool:
    table = (table_name or "").strip()
    field = (field_name or "").strip()
    if not table or not field or not value:
        return False

    spec = next((s for s in DIM_SPECS if s.table_name == table), None)
    if spec is None:
        return False

    return _dim_exists(db, spec.model, field, value)


def _require_fields(row: dict[str, str], required: list[str]) -> list[str]:
    return [f for f in required if not _val(row, f)]


def _process_dim_row(db: Session, row: dict[str, str], file_name: str, now: datetime, spec: DimSpec) -> str | None:
    natural_key = _val(row, spec.natural_field)
    if not natural_key:
        return f"Missing required field '{spec.natural_field}'."

    normalized = {k: (v or "").strip() for k, v in row.items()}

    try:
        validate_dim_attrs(
            spec.table_name,
            normalized,
            lov_checker=lambda n, v: _lov_value_exists(db, n, v),
            dimension_checker=lambda table, field, value: _dimension_lookup_exists(db, table, field, value),
        )
    except DimSchemaError as exc:
        return f"Dimension schema validation failed: {exc}"

    static_fields: dict[str, str | None] = {}
    attrs = {}
    for key, value in normalized.items():
        if key == spec.natural_field:
            continue
        if spec.table_name == "dh_dim_customer" and key == "business_unit":
            static_fields["business_unit"] = value or None
            continue
        if spec.table_name == "dh_dim_panama_node" and key == "node_type":
            static_fields["node_type"] = value or None
            continue
        if value:
            attrs[key] = value

    key_fields = ["node_type"] if spec.table_name == "dh_dim_panama_node" else None
    _scd_upsert(
        db,
        spec.model,
        spec.natural_field,
        natural_key,
        attrs,
        static_fields,
        now,
        file_name,
        key_fields=key_fields,
    )
    return None


def _process_bridge_household_customer_row(db: Session, row: dict[str, str], now: datetime) -> str | None:
    missing = _require_fields(row, ["household_key", "customer_key"])
    if missing:
        return f"Missing required fields: {', '.join(missing)}"

    household_key = _val(row, "household_key")
    customer_key = _val(row, "customer_key")

    if not _dim_exists(db, DHDimHousehold, "household_key", household_key):
        return f"Referential integrity failed: household_key '{household_key}' not found in dh_dim_household."
    if not _dim_exists(db, DHDimCustomer, "customer_key", customer_key):
        return f"Referential integrity failed: customer_key '{customer_key}' not found in dh_dim_customer."

    _ensure_bridge(
        db,
        DHBridgeHouseholdCustomer,
        {"household_key": household_key, "customer_key": customer_key},
        now,
    )
    return None


def _process_bridge_customer_account_row(db: Session, row: dict[str, str], now: datetime) -> str | None:
    missing = _require_fields(row, ["customer_key", "account_key", "relationship_type"])
    if missing:
        return f"Missing required fields: {', '.join(missing)}"

    customer_key = _val(row, "customer_key")
    account_key = _val(row, "account_key")
    relationship_type = _val(row, "relationship_type")

    if not _dim_exists(db, DHDimCustomer, "customer_key", customer_key):
        return f"Referential integrity failed: customer_key '{customer_key}' not found in dh_dim_customer."
    if not _dim_exists(db, DHDimAccount, "account_key", account_key):
        return f"Referential integrity failed: account_key '{account_key}' not found in dh_dim_account."

    primary_rule_error = _validate_account_primary_relationship_rule(
        db,
        customer_key,
        account_key,
        relationship_type,
    )
    if primary_rule_error:
        return primary_rule_error

    _ensure_bridge_customer_account(
        db,
        {"customer_key": customer_key, "account_key": account_key},
        relationship_type,
        now,
    )
    return None


def _process_bridge_customer_associated_party_row(db: Session, row: dict[str, str], now: datetime) -> str | None:
    missing = _require_fields(row, ["customer_key", "associated_party_key"])
    if missing:
        return f"Missing required fields: {', '.join(missing)}"

    customer_key = _val(row, "customer_key")
    associated_party_key = _val(row, "associated_party_key")

    if not _dim_exists(db, DHDimCustomer, "customer_key", customer_key):
        return f"Referential integrity failed: customer_key '{customer_key}' not found in dh_dim_customer."
    if not _dim_exists(db, DHDimAssociatedParty, "associated_party_key", associated_party_key):
        return (
            "Referential integrity failed: "
            f"associated_party_key '{associated_party_key}' not found in dh_dim_associated_party."
        )

    _ensure_bridge(
        db,
        DHBridgeCustomerAssociatedParty,
        {"customer_key": customer_key, "associated_party_key": associated_party_key},
        now,
    )
    return None


def _process_bridge_panama_relationship_row(db: Session, row: dict[str, str], now: datetime) -> str | None:
    missing = _require_fields(row, ["start_node_id", "end_node_id", "rel_type", "source_id"])
    if missing:
        return f"Missing required fields: {', '.join(missing)}"

    start_node_id = _val(row, "start_node_id")
    end_node_id = _val(row, "end_node_id")
    rel_type = _val(row, "rel_type")
    link = _val(row, "link")
    status = _val(row, "status")
    start_date = _val(row, "start_date")
    end_date = _val(row, "end_date")
    source_id = _val(row, "source_id")

    if not _dim_exists(db, DHDimPanamaNode, "node_id", start_node_id):
        return (
            "Referential integrity failed: "
            f"start_node_id '{start_node_id}' not found in dh_dim_panama_node."
        )
    if not _dim_exists(db, DHDimPanamaNode, "node_id", end_node_id):
        return (
            "Referential integrity failed: "
            f"end_node_id '{end_node_id}' not found in dh_dim_panama_node."
        )

    _ensure_bridge_panama_relationship(
        db,
        {
            "start_node_id": start_node_id,
            "end_node_id": end_node_id,
            "rel_type": rel_type,
        },
        {
            "link": link or None,
            "status": status or None,
            "start_date": start_date or None,
            "end_date": end_date or None,
            "source_id": source_id or None,
        },
        now,
    )
    return None


def _process_fact_cash_row(db: Session, row: dict[str, str], file_name: str, now: datetime) -> str | None:
    required_fields = [
        "transaction_key",
        "account_key",
        "transaction_type_code",
        "country_code_2",
        "currency_code",
        "counterparty_account_key",
        "amount",
    ]
    missing = _require_fields(row, required_fields)
    if missing:
        return f"Missing required fields: {', '.join(missing)}"

    transaction_key = _val(row, "transaction_key")
    account_key = _val(row, "account_key")
    transaction_type_code = _val(row, "transaction_type_code")
    country_code_2 = _val(row, "country_code_2")
    currency_code = _val(row, "currency_code")
    counterparty_account_key = _val(row, "counterparty_account_key")
    branch_key = _val(row, "branch_key")
    sub_account_key = _val(row, "sub_account_key")
    secondary_account_key = _val(row, "secondary_account_key")

    existing = db.execute(select(DHFactCash).where(DHFactCash.transaction_key == transaction_key)).scalar_one_or_none()
    if existing is not None:
        return f"Duplicate transaction_key '{transaction_key}' already exists in dh_fact_cash."

    if not _dim_exists(db, DHDimAccount, "account_key", account_key):
        return f"Referential integrity failed: account_key '{account_key}' not found in dh_dim_account."
    if not _dim_exists(db, DHDimTransactionType, "transaction_type_code", transaction_type_code):
        return (
            "Referential integrity failed: "
            f"transaction_type_code '{transaction_type_code}' not found in dh_dim_transaction_type."
        )
    if not _dim_exists(db, DHDimCountry, "country_code_2", country_code_2):
        return f"Referential integrity failed: country_code_2 '{country_code_2}' not found in dh_dim_country."
    if not _dim_exists(db, DHDimCurrency, "currency_code", currency_code):
        return f"Referential integrity failed: currency_code '{currency_code}' not found in dh_dim_currency."
    if not _dim_exists(db, DHDimCounterpartyAccount, "counterparty_account_key", counterparty_account_key):
        return (
            "Referential integrity failed: "
            f"counterparty_account_key '{counterparty_account_key}' not found in dh_dim_counterparty_account."
        )
    if branch_key and not _dim_exists(db, DHDimBranch, "branch_key", branch_key):
        return f"Referential integrity failed: branch_key '{branch_key}' not found in dh_dim_branch."
    if secondary_account_key and not _dim_exists(db, DHDimAccount, "account_key", secondary_account_key):
        return (
            "Referential integrity failed: "
            f"secondary_account_key '{secondary_account_key}' not found in dh_dim_account."
        )
    if sub_account_key and not _dim_exists(db, DHDimSubAccount, "sub_account_key", sub_account_key):
        return f"Referential integrity failed: sub_account_key '{sub_account_key}' not found in dh_dim_sub_account."

    try:
        amount = float(_val(row, "amount"))
    except ValueError:
        return f"Invalid amount '{_val(row, 'amount')}'."

    db.add(
        DHFactCash(
            transaction_key=transaction_key,
            account_key=account_key,
            transaction_type_code=transaction_type_code,
            country_code_2=country_code_2,
            currency_code=currency_code,
            counterparty_account_key=counterparty_account_key,
            branch_key=branch_key or None,
            secondary_account_key=secondary_account_key or None,
            sub_account_key=sub_account_key or None,
            amount=amount,
            transaction_ts=_parse_dt(row.get("transaction_ts") or ""),
            source_file=file_name,
            loaded_at=now,
        )
    )
    return None


def _rules_for_entity(db: Session, entity_name: str) -> list[DHDQRule]:
    return db.execute(
        select(DHDQRule).where(
            and_(
                DHDQRule.entity_name == entity_name,
                DHDQRule.is_active.is_(True),
            )
        )
    ).scalars().all()


def _infer_error_rule_name(error: str) -> str:
    if error.startswith("Referential integrity failed"):
        return "referential_integrity"
    if error.startswith("Business rule failed"):
        return "business_rule_validation"
    if error.startswith("Dimension schema validation failed"):
        return "dimension_schema_validation"
    return "data_validation"


def _write_rejected_rows(rejected_file: Path, rejected_rows: list[dict[str, str]]) -> None:
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    all_fields: set[str] = set()
    for row in rejected_rows:
        all_fields.update(row.keys())

    ordered_fields = [f for f in sorted(all_fields) if f != "_error"] + (["_error"] if "_error" in all_fields else [])
    with rejected_file.open("w", encoding="utf-8", newline="") as rf:
        writer = csv.DictWriter(rf, fieldnames=ordered_fields)
        writer.writeheader()
        writer.writerows(rejected_rows)


def _run_for_file(
    db: Session,
    run_id: str,
    table_name: str,
    file_path: Path,
    process_row: Callable[[Session, dict[str, str], str, datetime], str | None],
) -> FileSummary:
    now = _utc_now()
    file_name = file_path.name
    summary = FileSummary(file_name=file_name)
    entity_name = TABLE_TO_ENTITY[table_name]
    rules = _rules_for_entity(db, entity_name)

    rejected_rows: list[dict[str, str]] = []
    with file_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=2):
            summary.read += 1
            blocking = False
            rejection_messages: list[str] = []

            for rule in rules:
                msg = evaluate_rule(row, rule.rule_type, rule.field_name, rule.rule_param)
                if msg is None:
                    continue
                action = "reject" if rule.severity == "reject" else "pass"
                _record_dq_issue(db, run_id, file_name, row_num, entity_name, rule.rule_name, rule.severity, action, msg)
                if rule.severity == "reject":
                    blocking = True
                    rejection_messages.append(msg)

            if not blocking:
                error = process_row(db, row, file_name, now)
                if error:
                    blocking = True
                    rejection_messages.append(error)
                    _record_dq_issue(
                        db,
                        run_id,
                        file_name,
                        row_num,
                        entity_name,
                        _infer_error_rule_name(error),
                        "reject",
                        "reject",
                        error,
                    )

            if blocking:
                summary.rejected += 1
                rejected_rows.append({**row, "_error": " | ".join(rejection_messages)})
                continue

            summary.loaded += 1

    if rejected_rows:
        _write_rejected_rows(REJECTED_DIR / f"{file_name}.rejected.csv", rejected_rows)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    shutil.move(str(file_path), str(PROCESSED_DIR / file_name))

    db.add(
        DHJobFileStat(
            run_file_key=f"{run_id}:{file_name}",
            job_run_id=run_id,
            input_file_name=file_name,
            records_read=summary.read,
            records_loaded=summary.loaded,
            records_rejected=summary.rejected,
            processed_at=_utc_now(),
        )
    )

    return summary


def _preflight_schema_lov_alignment() -> None:
    schema_lookup_names, lookup_errors = collect_all_schema_lookup_names()
    if lookup_errors:
        raise ValueError(
            "Schema validation failed during preflight: " + " | ".join(lookup_errors)
        )

    lov_lookup_names = lookup_names_from_csv()
    missing = sorted(schema_lookup_names - lov_lookup_names)
    if missing:
        raise ValueError(
            "Schema dq.lookup_name values missing from config/lov_values.csv: "
            + ", ".join(missing)
        )


def _match_table_name(file_name: str) -> str | None:
    lower = file_name.lower()
    for table_name in TABLE_PROCESS_ORDER:
        if lower.startswith(table_name):
            return table_name
    return None


def _process_unknown_file(db: Session, run_id: str, file_path: Path) -> FileSummary:
    file_name = file_path.name
    _record_dq_issue(
        db,
        run_id,
        file_name,
        1,
        "unknown",
        "unsupported_input_file",
        "reject",
        "reject",
        (
            "Unsupported input file name. Use table-prefixed files like "
            "dh_dim_customer*.csv, dh_bridge_customer_account*.csv, dh_fact_cash*.csv."
        ),
    )

    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    shutil.move(str(file_path), str(REJECTED_DIR / f"{file_name}.rejected.csv"))

    summary = FileSummary(file_name=file_name, read=0, loaded=0, rejected=1)
    db.add(
        DHJobFileStat(
            run_file_key=f"{run_id}:{file_name}",
            job_run_id=run_id,
            input_file_name=file_name,
            records_read=summary.read,
            records_loaded=summary.loaded,
            records_rejected=summary.rejected,
            processed_at=_utc_now(),
        )
    )
    return summary


def _dim_processors() -> dict[str, Callable[[Session, dict[str, str], str, datetime], str | None]]:
    processors = {}
    for spec in DIM_SPECS:
        processors[spec.table_name] = (
            lambda db, row, file_name, now, s=spec: _process_dim_row(db, row, file_name, now, s)
        )
    return processors


def _table_processors() -> dict[str, Callable[[Session, dict[str, str], str, datetime], str | None]]:
    processors = _dim_processors()
    processors.update(
        {
            "dh_bridge_household_customer": lambda db, row, _f, now: _process_bridge_household_customer_row(db, row, now),
            "dh_bridge_customer_account": lambda db, row, _f, now: _process_bridge_customer_account_row(db, row, now),
            "dh_bridge_customer_associated_party": lambda db, row, _f, now: _process_bridge_customer_associated_party_row(db, row, now),
            "dh_bridge_panama_relationship": lambda db, row, _f, now: _process_bridge_panama_relationship_row(db, row, now),
            "dh_fact_cash": _process_fact_cash_row,
        }
    )
    return processors


def run_cash_pipeline(db: Session, job_name: str = "cash_pipeline") -> dict:
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())
    started = _utc_now()
    run = DHJobRun(
        job_run_id=run_id,
        job_name=job_name,
        started_at=started,
        status="running",
        files_seen=0,
        files_processed=0,
        records_read=0,
        records_loaded=0,
        records_rejected=0,
    )
    db.add(run)
    db.commit()

    files = sorted([p for p in LANDING_DIR.glob("*.csv") if p.is_file()])
    run.files_seen = len(files)
    db.commit()

    files_by_table: dict[str, list[Path]] = {name: [] for name in TABLE_PROCESS_ORDER}
    unknown_files: list[Path] = []
    for f in files:
        table_name = _match_table_name(f.name)
        if table_name is None:
            unknown_files.append(f)
            continue
        files_by_table[table_name].append(f)

    processors = _table_processors()

    try:
        _preflight_schema_lov_alignment()

        print(f"[pipeline] Starting run {run_id} with {run.files_seen} file(s)", flush=True)
        print(f"[pipeline] Table processing order count: {len(TABLE_PROCESS_ORDER)}", flush=True)

        for table_idx, table_name in enumerate(TABLE_PROCESS_ORDER, start=1):
            table_files = sorted(files_by_table[table_name])
            print(
                f"[pipeline] [{table_idx}/{len(TABLE_PROCESS_ORDER)}] {table_name}: "
                f"{len(table_files)} file(s)",
                flush=True,
            )

            for file_idx, f in enumerate(table_files, start=1):
                print(
                    f"[pipeline]   ({file_idx}/{len(table_files)}) processing {f.name}",
                    flush=True,
                )
                summary = _run_for_file(db, run_id, table_name, f, processors[table_name])
                run.files_processed += 1
                run.records_read += summary.read
                run.records_loaded += summary.loaded
                run.records_rejected += summary.rejected
                db.commit()
                print(
                    f"[pipeline]   completed {f.name}: "
                    f"read={summary.read}, loaded={summary.loaded}, rejected={summary.rejected} | "
                    f"files_processed={run.files_processed}/{run.files_seen}",
                    flush=True,
                )

        if unknown_files:
            print(f"[pipeline] Processing {len(unknown_files)} unknown file(s)", flush=True)

        for idx, f in enumerate(unknown_files, start=1):
            print(f"[pipeline]   (unknown {idx}/{len(unknown_files)}) processing {f.name}", flush=True)
            summary = _process_unknown_file(db, run_id, f)
            run.files_processed += 1
            run.records_read += summary.read
            run.records_loaded += summary.loaded
            run.records_rejected += summary.rejected
            db.commit()
            print(
                f"[pipeline]   completed unknown {f.name}: "
                f"read={summary.read}, loaded={summary.loaded}, rejected={summary.rejected} | "
                f"files_processed={run.files_processed}/{run.files_seen}",
                flush=True,
            )

        run.status = "success"
        run.ended_at = _utc_now()
        db.commit()
        print(f"[pipeline] Run {run_id} finished with status=success", flush=True)
    except Exception as exc:
        db.rollback()
        run = db.execute(select(DHJobRun).where(DHJobRun.job_run_id == run_id)).scalar_one()
        run.status = "failed"
        run.ended_at = _utc_now()
        run.notes = str(exc)
        db.commit()
        print(f"[pipeline] Run {run_id} failed: {exc}", flush=True)
        raise

    return {
        "job_run_id": run_id,
        "status": run.status,
        "files_seen": run.files_seen,
        "files_processed": run.files_processed,
        "records_read": run.records_read,
        "records_loaded": run.records_loaded,
        "records_rejected": run.records_rejected,
        "started_at": run.started_at.isoformat(),
        "ended_at": run.ended_at.isoformat() if run.ended_at else None,
    }
