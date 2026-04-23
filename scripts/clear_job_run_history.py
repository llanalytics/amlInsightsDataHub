#!/usr/bin/env python3
from __future__ import annotations

import argparse

from sqlalchemy import text

from app.database import SessionLocal


# Delete child records first, then parent run records.
JOB_HISTORY_TABLES_DELETE_ORDER = [
    "dh_dq_results",
    "dh_job_file_stats",
    "dh_job_runs",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Delete all job run history/status records, including DQ result errors, "
            "while preserving DQ rule definitions."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print table delete order without deleting data.",
    )
    args = parser.parse_args()

    print("Job run history clear plan (preserves dh_dq_rules):")
    for t in JOB_HISTORY_TABLES_DELETE_ORDER:
        print(f"  - {t}")

    if args.dry_run:
        print("Dry run only; no records deleted.")
        return 0

    db = SessionLocal()
    try:
        for t in JOB_HISTORY_TABLES_DELETE_ORDER:
            db.execute(text(f"DELETE FROM {t}"))
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    print("Job run history clear complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

