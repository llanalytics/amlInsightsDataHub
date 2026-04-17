from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import DHDQRule


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DQ_RULES_PATH = BASE_DIR / "config" / "dq_rules.csv"


@dataclass(frozen=True)
class DQRuleConfig:
    rule_name: str
    entity_name: str
    field_name: str | None
    rule_type: str
    severity: str
    rule_param: str | None
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
    raise ValueError(f"Invalid boolean value '{raw}' in DQ rules CSV")


def load_dq_rules_from_csv(csv_path: Path | str = DEFAULT_DQ_RULES_PATH) -> list[DQRuleConfig]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"DQ rules CSV not found: {path}")

    rules: list[DQRuleConfig] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required_cols = {
            "rule_name",
            "entity_name",
            "field_name",
            "rule_type",
            "severity",
            "rule_param",
            "description",
            "is_active",
        }
        missing = required_cols - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"DQ rules CSV missing required columns: {', '.join(sorted(missing))}")

        for idx, row in enumerate(reader, start=2):
            rule_name = _clean(row.get("rule_name"))
            entity_name = _clean(row.get("entity_name"))
            rule_type = _clean(row.get("rule_type"))
            severity = _clean(row.get("severity"))

            if not rule_name or not entity_name or not rule_type or not severity:
                raise ValueError(
                    "Invalid DQ rule row "
                    f"{idx}: rule_name, entity_name, rule_type, severity are required"
                )

            rules.append(
                DQRuleConfig(
                    rule_name=rule_name,
                    entity_name=entity_name,
                    field_name=_clean(row.get("field_name")) or None,
                    rule_type=rule_type,
                    severity=severity,
                    rule_param=_clean(row.get("rule_param")) or None,
                    description=_clean(row.get("description")) or None,
                    is_active=_parse_bool(row.get("is_active")),
                )
            )

    return rules


def sync_dq_rules(
    db: Session,
    csv_path: Path | str = DEFAULT_DQ_RULES_PATH,
    deactivate_missing: bool = False,
) -> dict[str, int]:
    configs = load_dq_rules_from_csv(csv_path)
    existing_rows = db.execute(select(DHDQRule)).scalars().all()
    existing_by_name = {row.rule_name: row for row in existing_rows}

    inserted = 0
    updated = 0
    unchanged = 0

    config_rule_names = {cfg.rule_name for cfg in configs}
    for cfg in configs:
        existing = existing_by_name.get(cfg.rule_name)
        if existing is None:
            db.add(
                DHDQRule(
                    rule_name=cfg.rule_name,
                    entity_name=cfg.entity_name,
                    field_name=cfg.field_name,
                    rule_type=cfg.rule_type,
                    severity=cfg.severity,
                    rule_param=cfg.rule_param,
                    description=cfg.description,
                    is_active=cfg.is_active,
                )
            )
            inserted += 1
            continue

        has_change = False
        for attr_name, value in (
            ("entity_name", cfg.entity_name),
            ("field_name", cfg.field_name),
            ("rule_type", cfg.rule_type),
            ("severity", cfg.severity),
            ("rule_param", cfg.rule_param),
            ("description", cfg.description),
            ("is_active", cfg.is_active),
        ):
            if getattr(existing, attr_name) != value:
                setattr(existing, attr_name, value)
                has_change = True

        if has_change:
            updated += 1
        else:
            unchanged += 1

    deactivated = 0
    if deactivate_missing:
        for existing in existing_rows:
            if existing.rule_name in config_rule_names:
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
