#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET


DEFAULT_URL = "https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml"
DEFAULT_OUTPUT = Path("data/sample/dh_dim_currency_sample.csv")
OUTPUT_COLUMNS = ["currency_code", "currency_name", "associated_countries"]


def _clean(value: str | None) -> str:
    return (value or "").strip()


def _fetch_bytes(url: str, timeout_sec: int) -> bytes:
    req = Request(
        url,
        headers={
            "User-Agent": "amlInsightsDataHub/1.0 (+ISO currency sync)",
            "Accept": "application/xml,text/xml,*/*",
        },
    )
    with urlopen(req, timeout=timeout_sec) as resp:  # noqa: S310
        return resp.read()


def _find_text(parent: ET.Element, tag: str) -> str:
    child = parent.find(tag)
    if child is None:
        return ""
    return _clean(child.text)


def _rows_from_xml(xml_bytes: bytes) -> list[dict[str, str]]:
    root = ET.fromstring(xml_bytes)
    country_rows = root.findall("./CcyTbl/CcyNtry")

    code_to_name: dict[str, str] = {}
    code_to_countries: defaultdict[str, set[str]] = defaultdict(set)

    for item in country_rows:
        currency_code = _find_text(item, "Ccy").upper()
        currency_name = _find_text(item, "CcyNm")
        country_name = _find_text(item, "CtryNm")

        if not currency_code or not currency_name:
            continue

        if currency_code not in code_to_name:
            code_to_name[currency_code] = currency_name

        if country_name:
            code_to_countries[currency_code].add(country_name)

    rows: list[dict[str, str]] = []
    for currency_code in sorted(code_to_name.keys()):
        countries = sorted(code_to_countries.get(currency_code, set()))
        rows.append(
            {
                "currency_code": currency_code,
                "currency_name": code_to_name[currency_code],
                "associated_countries": "; ".join(countries),
            }
        )

    return rows


def _write_rows(output_path: Path, rows: list[dict[str, str]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download ISO 4217 currency data and update data/sample/dh_dim_currency_sample.csv",
    )
    parser.add_argument("--url", default=DEFAULT_URL, help=f"Source XML URL (default: {DEFAULT_URL})")
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output CSV path (default: data/sample/dh_dim_currency_sample.csv)",
    )
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds (default: 60)")
    parser.add_argument("--limit", type=int, default=0, help="Optional row limit for testing (0 = no limit)")
    args = parser.parse_args()

    xml_bytes = _fetch_bytes(args.url, args.timeout)
    rows = _rows_from_xml(xml_bytes)

    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    output_path = Path(args.output)
    _write_rows(output_path, rows)

    print("Currency sample update complete")
    print(f"  source_url: {args.url}")
    print(f"  output: {output_path}")
    print(f"  rows_written: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
