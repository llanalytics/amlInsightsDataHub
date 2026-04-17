#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from app.database import SessionLocal
from app.lov_loader import DEFAULT_LOV_VALUES_PATH, sync_lov_values


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Data Hub LOV values from CSV into dh_lov_values.")
    parser.add_argument(
        "--csv",
        dest="csv_path",
        default=str(DEFAULT_LOV_VALUES_PATH),
        help="Path to LOV CSV (default: config/lov_values.csv)",
    )
    parser.add_argument(
        "--deactivate-missing",
        action="store_true",
        help="Deactivate DB LOV rows not present in CSV.",
    )
    args = parser.parse_args()

    db = SessionLocal()
    try:
        result = sync_lov_values(
            db,
            csv_path=Path(args.csv_path),
            deactivate_missing=args.deactivate_missing,
        )
    finally:
        db.close()

    print("LOV sync complete")
    for key in ["inserted", "updated", "unchanged", "deactivated", "total_in_csv"]:
        print(f"  {key}: {result[key]}")


if __name__ == "__main__":
    main()
