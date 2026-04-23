#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import random
import string
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Account:
    account_key: str
    account_type: str

    @property
    def is_commercial(self) -> bool:
        return self.account_type.startswith("Commercial")


@dataclass(frozen=True)
class ExternalTxnType:
    code: str
    direction: str
    mechanism: str


def _parse_month(raw: str) -> tuple[int, int]:
    dt = datetime.strptime(raw, "%Y-%m")
    return dt.year, dt.month


def _iter_month_starts(start: tuple[int, int], end: tuple[int, int]):
    y, m = start
    end_y, end_m = end
    while (y, m) <= (end_y, end_m):
        yield datetime(y, m, 1)
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1


def _next_month(dt: datetime) -> datetime:
    if dt.month == 12:
        return datetime(dt.year + 1, 1, 1)
    return datetime(dt.year, dt.month + 1, 1)


def _random_timestamp_in_month(rng: random.Random, month_start: datetime) -> datetime:
    month_end = _next_month(month_start)
    span_seconds = int((month_end - month_start).total_seconds())
    offset = rng.randint(0, max(0, span_seconds - 1))
    return month_start + timedelta(seconds=offset)


def _monthly_transaction_count(rng: random.Random, is_commercial: bool, min_per_month: int, max_per_month: int) -> int:
    if max_per_month <= min_per_month:
        return min_per_month
    raw = rng.betavariate(3.2, 2.2) if is_commercial else rng.betavariate(1.3, 4.5)
    span = max_per_month - min_per_month
    return min_per_month + int(raw * (span + 1))


def _random_amount(rng: random.Random, mechanism: str) -> float:
    if mechanism.casefold() == "ach":
        val = rng.uniform(10, 25000)
    else:
        val = rng.uniform(100, 150000)
    return round(val, 2)


def _random_id(rng: random.Random, length: int) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(rng.choice(alphabet) for _ in range(length))


def _load_accounts(path: Path) -> list[Account]:
    rows: list[Account] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            key = (row.get("account_key") or "").strip()
            typ = (row.get("account_type") or "").strip()
            if not key or not typ or key == "-1":
                continue
            rows.append(Account(account_key=key, account_type=typ))
    if not rows:
        raise ValueError(f"No valid accounts found in {path}")
    return rows


def _load_external_types(path: Path) -> list[ExternalTxnType]:
    rows: list[ExternalTxnType] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            code = (row.get("transaction_type_code") or "").strip()
            cls = (row.get("aml_classification") or "").strip()
            direction = (row.get("direction") or "").strip()
            mechanism = (row.get("mechanism") or "").strip()
            if not code or code == "-1":
                continue
            if cls.casefold() != "external funds transfer":
                continue
            if mechanism.casefold() not in {"wire", "ach"}:
                continue
            rows.append(ExternalTxnType(code=code, direction=direction, mechanism=mechanism))
    if not rows:
        raise ValueError(f"No external transfer transaction types found in {path}")
    return rows


def _load_values(path: Path, col: str, exclude: set[str] | None = None) -> list[str]:
    exclude = exclude or set()
    vals: list[str] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            v = (row.get(col) or "").strip()
            if not v or v in exclude:
                continue
            vals.append(v)
    if not vals:
        raise ValueError(f"No values for '{col}' in {path}")
    return vals


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate an external transfer feed that outputs both counterparty-dimension and "
            "cash-fact CSVs. ACH counterparties are US; Wire counterparties are random global jurisdictions."
        )
    )
    parser.add_argument("--start", default="2025-01", help="Start month inclusive (YYYY-MM)")
    parser.add_argument("--end", default="2026-01", help="End month inclusive (YYYY-MM)")
    parser.add_argument("--min-per-month", type=int, default=0, help="Min txns per account/month")
    parser.add_argument("--max-per-month", type=int, default=8, help="Max txns per account/month")
    parser.add_argument("--seed", type=int, default=101, help="RNG seed")
    parser.add_argument(
        "--accounts-csv",
        default=str(BASE_DIR / "data" / "sample" / "dh_dim_account_sample.csv"),
        help="Accounts source CSV",
    )
    parser.add_argument(
        "--txn-types-csv",
        default=str(BASE_DIR / "data" / "sample" / "dh_dim_transaction_type_sample.csv"),
        help="Transaction types source CSV",
    )
    parser.add_argument(
        "--countries-csv",
        default=str(BASE_DIR / "data" / "sample" / "dh_dim_country_sample.csv"),
        help="Country source CSV",
    )
    parser.add_argument(
        "--currencies-csv",
        default=str(BASE_DIR / "data" / "sample" / "dh_dim_currency_sample.csv"),
        help="Currency source CSV",
    )
    parser.add_argument(
        "--branches-csv",
        default=str(BASE_DIR / "data" / "sample" / "dh_dim_branch_sample.csv"),
        help="Branch source CSV",
    )
    parser.add_argument(
        "--counterparty-output",
        default=str(BASE_DIR / "data" / "sample" / "dh_dim_counterparty_account_external_sample.csv"),
        help="Counterparty output CSV",
    )
    parser.add_argument(
        "--cash-output",
        default=str(BASE_DIR / "data" / "sample" / "dh_fact_cash_external_sample.csv"),
        help="Cash output CSV",
    )
    args = parser.parse_args()

    if args.min_per_month < 0 or args.max_per_month < 0:
        raise ValueError("min/max per month must be >= 0")
    if args.min_per_month > args.max_per_month:
        raise ValueError("--min-per-month must be <= --max-per-month")

    start = _parse_month(args.start)
    end = _parse_month(args.end)
    if start > end:
        raise ValueError("--start must be <= --end")

    rng = random.Random(args.seed)

    accounts = _load_accounts(Path(args.accounts_csv))
    txn_types = _load_external_types(Path(args.txn_types_csv))
    countries_all = _load_values(Path(args.countries_csv), "country_code_2", exclude={"-1"})
    countries_non_us = [c for c in countries_all if c != "US"]
    if not countries_non_us:
        raise ValueError("countries list has no non-US values for Wire jurisdiction randomization")
    _ = _load_values(Path(args.currencies_csv), "currency_code", exclude={"-1"})
    _ = _load_values(Path(args.branches_csv), "branch_key", exclude={"-1"})

    counterparty_out = Path(args.counterparty_output)
    cash_out = Path(args.cash_output)
    counterparty_out.parent.mkdir(parents=True, exist_ok=True)
    cash_out.parent.mkdir(parents=True, exist_ok=True)

    cp_fields = ["counterparty_account_key", "counterparty_name", "account_id", "bank_id", "jurisdiction"]
    cash_fields = [
        "transaction_key",
        "account_key",
        "secondary_account_key",
        "transaction_type_code",
        "country_code_2",
        "currency_code",
        "counterparty_account_key",
        "branch_key",
        "sub_account_key",
        "amount",
        "transaction_ts",
    ]

    cp_seq = 700000
    txn_seq = 900000001
    cp_rows: list[dict[str, str]] = []
    cash_rows: list[dict[str, str]] = []

    company_prefix = ["Global", "Summit", "Blue", "Prime", "Northern", "Horizon", "Atlas", "Oceanic"]
    company_suffix = ["Trading", "Logistics", "Holdings", "Partners", "Group", "Services", "Capital", "Ventures"]

    for month_start in _iter_month_starts(start, end):
        for account in accounts:
            txn_count = _monthly_transaction_count(rng, account.is_commercial, args.min_per_month, args.max_per_month)
            for _ in range(txn_count):
                tx = rng.choice(txn_types)
                mechanism = tx.mechanism.casefold()
                jurisdiction = "US" if mechanism == "ach" else rng.choice(countries_non_us)
                currency = "USD"
                cp_seq += 1
                cp_key = f"CP-{cp_seq}"
                cp_name = f"{rng.choice(company_prefix)} {rng.choice(company_suffix)}"
                cp_rows.append(
                    {
                        "counterparty_account_key": cp_key,
                        "counterparty_name": cp_name,
                        "account_id": _random_id(rng, 12),
                        "bank_id": _random_id(rng, 9),
                        "jurisdiction": jurisdiction,
                    }
                )

                ts = _random_timestamp_in_month(rng, month_start)
                cash_rows.append(
                    {
                        "transaction_key": f"TXN-EXT-{txn_seq:09d}",
                        "account_key": account.account_key,
                        "secondary_account_key": "NA",
                        "transaction_type_code": tx.code,
                        "country_code_2": jurisdiction,
                        "currency_code": currency,
                        "counterparty_account_key": cp_key,
                        "branch_key": "NA",
                        "sub_account_key": "",
                        "amount": f"{_random_amount(rng, tx.mechanism):.2f}",
                        "transaction_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    }
                )
                txn_seq += 1

    with counterparty_out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cp_fields)
        w.writeheader()
        w.writerows(cp_rows)

    with cash_out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cash_fields)
        w.writeheader()
        w.writerows(cash_rows)

    print("External transfer feed generation complete")
    print(f"  counterparty_output: {counterparty_out}")
    print(f"  cash_output: {cash_out}")
    print(f"  counterparties_generated: {len(cp_rows)}")
    print(f"  transactions_generated: {len(cash_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
