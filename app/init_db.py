from __future__ import annotations

from app.database import Base, SessionLocal, engine
from app.dq_rules_loader import sync_dq_rules
from app.lov_loader import sync_lov_values


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        # Seed and update rule definitions from config/dq_rules.csv.
        sync_dq_rules(db, deactivate_missing=False)
        # Seed and update list-of-values from config/lov_values.csv.
        sync_lov_values(db, deactivate_missing=False)
    finally:
        db.close()


if __name__ == "__main__":
    init_db()
