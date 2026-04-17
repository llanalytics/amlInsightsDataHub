from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class DQIssue:
    rule_name: str
    severity: str
    message: str


def evaluate_rule(row: dict[str, str], rule_type: str, field_name: str | None, rule_param: str | None) -> str | None:
    value = (row.get(field_name, "") if field_name else "").strip() if field_name else ""

    if rule_type == "required_not_null":
        if not value:
            return f"{field_name} is required"
        return None

    if rule_type == "regex_match":
        if value and rule_param and not re.fullmatch(rule_param, value):
            return f"{field_name} does not match required pattern"
        return None

    if rule_type == "amount_positive":
        raw = row.get(field_name or "amount", "").strip()
        try:
            amount = float(raw)
        except Exception:
            return f"{field_name or 'amount'} is not numeric"
        if amount <= 0:
            return f"{field_name or 'amount'} must be greater than zero"
        return None

    return None
