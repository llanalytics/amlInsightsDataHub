#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from app.database import SessionLocal
from app.dq_rules_loader import DEFAULT_DQ_RULES_PATH, sync_dq_rules


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Data Hub DQ rules from CSV into dh_dq_rules.")
    parser.add_argument(
        "--csv",
        dest="csv_path",
        default=str(DEFAULT_DQ_RULES_PATH),
        help="Path to dq rules CSV (default: config/dq_rules.csv)",
    )
    parser.add_argument(
        "--deactivate-missing",
        action="store_true",
        help="Deactivate DB rules not present in CSV.",
    )
    args = parser.parse_args()

    db = SessionLocal()
    try:
        result = sync_dq_rules(
            db,
            csv_path=Path(args.csv_path),
            deactivate_missing=args.deactivate_missing,
        )
    finally:
        db.close()

    print("DQ rules sync complete")
    for key in ["inserted", "updated", "unchanged", "deactivated", "total_in_csv"]:
        print(f"  {key}: {result[key]}")


if __name__ == "__main__":
    main()
