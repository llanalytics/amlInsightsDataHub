#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import random
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
class TransactionType:
    code: str
    aml_classification: str

    @property
    def is_internal_transfer(self) -> bool:
        return "internal" in self.aml_classification.casefold()


def _parse_month(raw: str) -> tuple[int, int]:
    try:
        dt = datetime.strptime(raw, "%Y-%m")
    except ValueError as exc:
        raise ValueError(f"Invalid month '{raw}'. Use YYYY-MM format.") from exc
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


def _load_accounts(path: Path) -> list[Account]:
    accounts: list[Account] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            key = (row.get("account_key") or "").strip()
            typ = (row.get("account_type") or "").strip()
            if not key or not typ:
                continue
            accounts.append(Account(account_key=key, account_type=typ))
    if not accounts:
        raise ValueError(f"No accounts found in {path}")
    return accounts


def _load_transaction_types(path: Path) -> list[TransactionType]:
    txns: list[TransactionType] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            code = (row.get("transaction_type_code") or "").strip()
            aml = (row.get("aml_classification") or "").strip()
            if not code:
                continue
            txns.append(TransactionType(code=code, aml_classification=aml))
    if not txns:
        raise ValueError(f"No transaction types found in {path}")
    return txns


def _load_single_column(path: Path, col: str) -> list[str]:
    values: list[str] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            val = (row.get(col) or "").strip()
            if val:
                values.append(val)
    if not values:
        raise ValueError(f"No values found for column '{col}' in {path}")
    return values


def _monthly_transaction_count(rng: random.Random, is_commercial: bool, max_per_month: int) -> int:
    # Both are bounded in [0, max_per_month]. Commercial distribution has higher expected value.
    if is_commercial:
        raw = rng.betavariate(3.0, 2.0)
    else:
        raw = rng.betavariate(1.2, 4.8)
    return min(max_per_month, max(0, int(raw * (max_per_month + 1))))


def _random_timestamp_in_month(rng: random.Random, month_start: datetime) -> datetime:
    month_end = _next_month(month_start)
    span_seconds = int((month_end - month_start).total_seconds())
    offset = rng.randint(0, max(0, span_seconds - 1))
    return month_start + timedelta(seconds=offset)


def _random_amount(rng: random.Random, txn_type: TransactionType) -> float:
    label = txn_type.aml_classification.casefold()
    if "cash" in label:
        val = rng.uniform(20, 5000)
    elif "internal" in label:
        val = rng.uniform(50, 20000)
    elif "check" in label:
        val = rng.uniform(25, 15000)
    else:
        val = rng.uniform(10, 50000)
    return round(val, 2)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate dh_fact_cash sample data by month/account with 0-1000 random transactions per month, "
            "biased higher for Commercial account types."
        )
    )
    parser.add_argument("--start", default="2025-01", help="Start month inclusive (YYYY-MM), default 2025-01")
    parser.add_argument("--end", default="2026-01", help="End month inclusive (YYYY-MM), default 2026-01")
    parser.add_argument("--max-per-month", type=int, default=1000, help="Max txns per month/account, default 1000")
    parser.add_argument("--seed", type=int, default=None, help="Optional RNG seed for reproducible output")
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
        "--counterparty-csv",
        default=str(BASE_DIR / "data" / "sample" / "dh_dim_counterparty_account_sample.csv"),
        help="Counterparty accounts source CSV",
    )
    parser.add_argument(
        "--country-csv",
        default=str(BASE_DIR / "data" / "sample" / "dh_dim_country_sample.csv"),
        help="Country source CSV",
    )
    parser.add_argument(
        "--currency-csv",
        default=str(BASE_DIR / "data" / "sample" / "dh_dim_currency_sample.csv"),
        help="Currency source CSV",
    )
    parser.add_argument(
        "--output",
        default=str(BASE_DIR / "data" / "sample" / "dh_fact_cash_sample.csv"),
        help="Output CSV path",
    )
    args = parser.parse_args()

    if args.max_per_month < 0:
        raise ValueError("--max-per-month must be >= 0")

    start = _parse_month(args.start)
    end = _parse_month(args.end)
    if start > end:
        raise ValueError("--start must be <= --end")

    rng = random.Random(args.seed)

    accounts = _load_accounts(Path(args.accounts_csv))
    txn_types = _load_transaction_types(Path(args.txn_types_csv))
    counterparties = _load_single_column(Path(args.counterparty_csv), "counterparty_account_key")
    countries = _load_single_column(Path(args.country_csv), "country_code_2")
    currencies = _load_single_column(Path(args.currency_csv), "currency_code")

    all_account_keys = [a.account_key for a in accounts]

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "transaction_key",
        "account_key",
        "secondary_account_key",
        "transaction_type_code",
        "country_code_2",
        "currency_code",
        "counterparty_account_key",
        "sub_account_key",
        "amount",
        "transaction_ts",
    ]

    txn_seq = 1
    total_rows = 0
    monthly_totals: dict[str, int] = {}

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for month_start in _iter_month_starts(start, end):
            month_key = month_start.strftime("%Y-%m")
            monthly_totals.setdefault(month_key, 0)

            for account in accounts:
                txn_count = _monthly_transaction_count(rng, account.is_commercial, args.max_per_month)
                if txn_count <= 0:
                    continue

                for _ in range(txn_count):
                    txn_type = rng.choice(txn_types)
                    secondary = ""
                    if txn_type.is_internal_transfer and len(all_account_keys) > 1:
                        secondary = rng.choice(all_account_keys)
                        if secondary == account.account_key:
                            secondary = ""

                    ts = _random_timestamp_in_month(rng, month_start)
                    amount = _random_amount(rng, txn_type)

                    writer.writerow(
                        {
                            "transaction_key": f"TXN-GEN-{txn_seq:09d}",
                            "account_key": account.account_key,
                            "secondary_account_key": secondary,
                            "transaction_type_code": txn_type.code,
                            "country_code_2": rng.choice(countries),
                            "currency_code": rng.choice(currencies),
                            "counterparty_account_key": rng.choice(counterparties),
                            "sub_account_key": "",  # user requested no sub-account for now
                            "amount": f"{amount:.2f}",
                            "transaction_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        }
                    )
                    txn_seq += 1
                    total_rows += 1
                    monthly_totals[month_key] += 1

    print("Cash fact sample generation complete")
    print(f"  output: {output_path}")
    print(f"  months: {args.start} .. {args.end}")
    print(f"  accounts: {len(accounts)}")
    print(f"  transaction_types: {len(txn_types)}")
    print(f"  total_transactions: {total_rows}")
    print("  monthly_totals:")
    for mk in sorted(monthly_totals.keys()):
        print(f"    {mk}: {monthly_totals[mk]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
