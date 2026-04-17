#!/usr/bin/env python3
from app.database import SessionLocal
from app.init_db import init_db
from app.pipeline import run_cash_pipeline


def main() -> None:
    init_db()
    db = SessionLocal()
    try:
        result = run_cash_pipeline(db)
        print("Pipeline complete:")
        for k, v in result.items():
            print(f"  {k}: {v}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
