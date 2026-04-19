#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
from pathlib import Path
from urllib.request import Request, urlopen


DEFAULT_URL = "https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv"
DEFAULT_OUTPUT = Path("data/sample/dh_dim_country_sample.csv")
OUTPUT_COLUMNS = ["country_code_2", "country_code_3", "country_name"]


def _clean(value: str | None) -> str:
    return (value or "").strip()


def _fetch_text(url: str, timeout_sec: int) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "amlInsightsDataHub/1.0 (+ISO country sync)",
            "Accept": "text/csv,*/*",
        },
    )
    with urlopen(req, timeout=timeout_sec) as resp:  # noqa: S310
        raw = resp.read()

    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _rows_from_text(text: str) -> list[dict[str, str]]:
    reader = csv.DictReader(io.StringIO(text))
    rows: list[dict[str, str]] = []
    for row in reader:
        code2 = _clean(row.get("alpha-2")).upper()
        code3 = _clean(row.get("alpha-3")).upper()
        name = _clean(row.get("name"))

        if not code2 or not code3 or not name:
            continue

        rows.append(
            {
                "country_code_2": code2,
                "country_code_3": code3,
                "country_name": name,
            }
        )

    rows.sort(key=lambda r: (r["country_code_2"], r["country_code_3"], r["country_name"]))
    return rows


def _write_rows(output_path: Path, rows: list[dict[str, str]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download ISO country data and update data/sample/dh_dim_country_sample.csv",
    )
    parser.add_argument("--url", default=DEFAULT_URL, help=f"Source CSV URL (default: {DEFAULT_URL})")
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output CSV path (default: data/sample/dh_dim_country_sample.csv)",
    )
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds (default: 60)")
    parser.add_argument("--limit", type=int, default=0, help="Optional row limit for testing (0 = no limit)")
    args = parser.parse_args()

    text = _fetch_text(args.url, args.timeout)
    rows = _rows_from_text(text)

    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    output_path = Path(args.output)
    _write_rows(output_path, rows)

    print("Country sample update complete")
    print(f"  source_url: {args.url}")
    print(f"  output: {output_path}")
    print(f"  rows_written: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
