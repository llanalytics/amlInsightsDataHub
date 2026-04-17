from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import DHLovValue


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_LOV_VALUES_PATH = BASE_DIR / "config" / "lov_values.csv"


@dataclass(frozen=True)
class LovValueConfig:
    lookup_name: str
    valid_value: str
    description: str | None
    is_active: bool


def _clean(value: str | None) -> str:
    return (value or "").strip()


def _parse_bool(raw: str | None) -> bool:
    value = _clean(raw).lower()
    if value in {"1", "true", "t", "yes", "y"}:
        return True
    if value in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"Invalid boolean value '{raw}' in LOV CSV")


def load_lov_values_from_csv(csv_path: Path | str = DEFAULT_LOV_VALUES_PATH) -> list[LovValueConfig]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"LOV CSV not found: {path}")

    rows: list[LovValueConfig] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required_cols = {"lookup_name", "valid_value", "description", "is_active"}
        missing = required_cols - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"LOV CSV missing required columns: {', '.join(sorted(missing))}")

        for idx, row in enumerate(reader, start=2):
            lookup_name = _clean(row.get("lookup_name"))
            valid_value = _clean(row.get("valid_value"))
            if not lookup_name or not valid_value:
                raise ValueError(f"Invalid LOV row {idx}: lookup_name and valid_value are required")

            rows.append(
                LovValueConfig(
                    lookup_name=lookup_name,
                    valid_value=valid_value,
                    description=_clean(row.get("description")) or None,
                    is_active=_parse_bool(row.get("is_active")),
                )
            )

    return rows


def lookup_names_from_csv(csv_path: Path | str = DEFAULT_LOV_VALUES_PATH) -> set[str]:
    return {row.lookup_name for row in load_lov_values_from_csv(csv_path)}


def sync_lov_values(
    db: Session,
    csv_path: Path | str = DEFAULT_LOV_VALUES_PATH,
    deactivate_missing: bool = False,
) -> dict[str, int]:
    configs = load_lov_values_from_csv(csv_path)
    existing_rows = db.execute(select(DHLovValue)).scalars().all()
    existing_by_key = {(r.lookup_name, r.valid_value): r for r in existing_rows}

    inserted = 0
    updated = 0
    unchanged = 0

    keys_in_csv = {(c.lookup_name, c.valid_value) for c in configs}
    for cfg in configs:
        key = (cfg.lookup_name, cfg.valid_value)
        existing = existing_by_key.get(key)
        if existing is None:
            db.add(
                DHLovValue(
                    lookup_name=cfg.lookup_name,
                    valid_value=cfg.valid_value,
                    description=cfg.description,
                    is_active=cfg.is_active,
                )
            )
            inserted += 1
            continue

        has_change = False
        if existing.description != cfg.description:
            existing.description = cfg.description
            has_change = True
        if existing.is_active != cfg.is_active:
            existing.is_active = cfg.is_active
            has_change = True

        if has_change:
            updated += 1
        else:
            unchanged += 1

    deactivated = 0
    if deactivate_missing:
        for existing in existing_rows:
            key = (existing.lookup_name, existing.valid_value)
            if key in keys_in_csv:
                continue
            if existing.is_active:
                existing.is_active = False
                deactivated += 1

    db.commit()
    return {
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "deactivated": deactivated,
        "total_in_csv": len(configs),
    }
