#!/usr/bin/env python3
from __future__ import annotations

import argparse

from sqlalchemy import text

from app.database import SessionLocal


# Core pipeline-loaded business tables (explicitly excluding OFAC + Panama tables)
CORE_TABLES_DELETE_ORDER = [
    # Fact first
    "dh_fact_cash",
    # Core bridges
    "dh_bridge_customer_associated_party",
    "dh_bridge_customer_account",
    "dh_bridge_household_customer",
    # Core dimensions
    "dh_dim_transaction_type",
    "dh_dim_counterparty_account",
    "dh_dim_currency",
    "dh_dim_country",
    "dh_dim_sub_account",
    "dh_dim_branch",
    "dh_dim_account",
    "dh_dim_associated_party",
    "dh_dim_customer",
    "dh_dim_household",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Delete all records from core pipeline-loaded tables, excluding OFAC/Panama reference tables."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print table delete order without deleting data.",
    )
    args = parser.parse_args()

    print("Core table reset plan (excludes dh_dim_ofac_sdn, dh_dim_panama_node, dh_bridge_panama_relationship):")
    for t in CORE_TABLES_DELETE_ORDER:
        print(f"  - {t}")

    if args.dry_run:
        print("Dry run only; no records deleted.")
        return 0

    db = SessionLocal()
    try:
        for t in CORE_TABLES_DELETE_ORDER:
            db.execute(text(f"DELETE FROM {t}"))
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    print("Core table reset complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
