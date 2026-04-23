#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from sqlalchemy import and_, select

from app.database import SessionLocal
from app.models import (
    DHDimAccount,
    DHDimAssociatedParty,
    DHDimBranch,
    DHDimCounterpartyAccount,
    DHDimCountry,
    DHDimCurrency,
    DHDimCustomer,
    DHDimHousehold,
    DHDimSubAccount,
    DHDimTransactionType,
)


UNKNOWN_KEY = "NA"
SOURCE_FILE = "system:seed_unknown_dimension_keys"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


SEED_SPECS = [
    (DHDimHousehold, "household_key", {"name": "NA"}),
    (DHDimCustomer, "customer_key", {"name": "NA", "segment": "Individual", "business_unit": "commercial_banking"}),
    (DHDimAssociatedParty, "associated_party_key", {"name": "NA"}),
    (DHDimAccount, "account_key", {"account_type": "Checking", "account_name": "NA"}),
    (DHDimSubAccount, "sub_account_key", {"sub_account_type": "Branch"}),
    (DHDimBranch, "branch_key", {"branch_type": "Standard"}),
    (DHDimCountry, "country_code_2", {"country_code_3": "NA", "country_name": "NA"}),
    (DHDimCurrency, "currency_code", {"currency_name": "NA", "associated_countries": "NA"}),
    (DHDimCounterpartyAccount, "counterparty_account_key", {"counterparty_name": "NA"}),
    (DHDimTransactionType, "transaction_type_code", {"aml_classification": "Cash", "direction": "Inbound"}),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Seed unknown-member (-1) rows into core dimensions so fact/bridge rows "
            "can safely reference unknown keys."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be inserted without writing to the database.",
    )
    args = parser.parse_args()

    print("Unknown dimension key seed plan:")
    for model, key_field, _attrs in SEED_SPECS:
        print(f"  - {model.__tablename__}.{key_field} = {UNKNOWN_KEY}")

    if args.dry_run:
        print("Dry run only; no records inserted.")
        return 0

    db = SessionLocal()
    now = _utc_now()
    inserted = 0
    try:
        for model, key_field, attrs in SEED_SPECS:
            existing = db.execute(
                select(model).where(
                    and_(
                        getattr(model, key_field) == UNKNOWN_KEY,
                        model.is_current.is_(True),
                    )
                )
            ).scalar_one_or_none()
            if existing is not None:
                continue

            db.add(
                model(
                    **{key_field: UNKNOWN_KEY},
                    valid_from=now,
                    valid_to=None,
                    is_current=True,
                    attr_json=json.dumps(attrs, sort_keys=True),
                    source_file=SOURCE_FILE,
                )
            )
            inserted += 1

        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    print(f"Unknown dimension key seed complete. Inserted rows: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
