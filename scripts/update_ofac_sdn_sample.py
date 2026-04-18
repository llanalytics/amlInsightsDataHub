#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
from pathlib import Path
from urllib.request import Request, urlopen


DEFAULT_URL = "https://www.treasury.gov/ofac/downloads/sdn.csv"
DEFAULT_OUTPUT = Path("data/sample/dh_dim_ofac_sdn_sample.csv")
OUTPUT_COLUMNS = [
    "sdn_uid",
    "name",
    "sdn_type",
    "program_list",
    "title",
    "call_sign",
    "vessel_type",
    "tonnage",
    "gross_registered_tonnage",
    "vessel_flag",
    "vessel_owner",
    "remarks",
]


def _clean(value: str | None) -> str:
    return (value or "").strip()


def _fetch_text(url: str, timeout_sec: int) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "amlInsightsDataHub/1.0 (+OFAC SDN sync)",
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


def _normalize_header_name(name: str) -> str:
    return "".join(ch for ch in name.strip().lower() if ch.isalnum() or ch == "_")


def _pick(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        if key in row:
            return _clean(row.get(key))
    return ""


def _transform_legacy_row(cols: list[str]) -> dict[str, str]:
    c = [str(x).strip() for x in cols]
    c.extend([""] * (12 - len(c)))
    return {
        "sdn_uid": c[0],
        "name": c[1],
        "sdn_type": c[2],
        "program_list": c[3],
        "title": c[4],
        "call_sign": c[5],
        "vessel_type": c[6],
        "tonnage": c[7],
        "gross_registered_tonnage": c[8],
        "vessel_flag": c[9],
        "vessel_owner": c[10],
        "remarks": c[11],
    }


def _transform_header_row(row: dict[str, str]) -> dict[str, str]:
    normalized = {_normalize_header_name(k): v for k, v in row.items()}
    return {
        "sdn_uid": _pick(normalized, "uid", "ent_num", "entityid", "entity_id", "id"),
        "name": _pick(normalized, "sdn_name", "name"),
        "sdn_type": _pick(normalized, "sdn_type", "type"),
        "program_list": _pick(normalized, "program", "programs", "program_list"),
        "title": _pick(normalized, "title"),
        "call_sign": _pick(normalized, "call_sign", "callsign"),
        "vessel_type": _pick(normalized, "vess_type", "vessel_type"),
        "tonnage": _pick(normalized, "tonnage"),
        "gross_registered_tonnage": _pick(normalized, "grt", "gross_registered_tonnage"),
        "vessel_flag": _pick(normalized, "vess_flag", "vessel_flag"),
        "vessel_owner": _pick(normalized, "vess_owner", "vessel_owner"),
        "remarks": _pick(normalized, "remarks", "comment", "comments", "notes"),
    }


def _rows_from_text(text: str) -> list[dict[str, str]]:
    raw_reader = csv.reader(io.StringIO(text))
    raw_rows = [r for r in raw_reader if any(_clean(x) for x in r)]
    if not raw_rows:
        return []

    first = [_clean(x) for x in raw_rows[0]]
    first_norm = {_normalize_header_name(x) for x in first if x}

    header_mode = (
        ("uid" in first_norm or "ent_num" in first_norm)
        and ("sdn_name" in first_norm or "name" in first_norm)
    )

    parsed: list[dict[str, str]] = []
    if header_mode:
        dict_reader = csv.DictReader(io.StringIO(text))
        for row in dict_reader:
            transformed = _transform_header_row({str(k): str(v or "") for k, v in row.items() if k is not None})
            if transformed["sdn_uid"] and transformed["name"]:
                parsed.append(transformed)
        return parsed

    for cols in raw_rows:
        transformed = _transform_legacy_row(cols)
        if transformed["sdn_uid"] and transformed["name"]:
            parsed.append(transformed)
    return parsed


def _sort_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    def key_fn(row: dict[str, str]):
        uid = row["sdn_uid"]
        try:
            return (0, int(uid))
        except ValueError:
            return (1, uid)

    return sorted(rows, key=key_fn)


def _write_rows(output_path: Path, rows: list[dict[str, str]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _clean(row.get(k)) for k in OUTPUT_COLUMNS})


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download OFAC SDN data and update data/sample/dh_dim_ofac_sdn_sample.csv",
    )
    parser.add_argument("--url", default=DEFAULT_URL, help=f"Source CSV URL (default: {DEFAULT_URL})")
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output CSV path (default: data/sample/dh_dim_ofac_sdn_sample.csv)",
    )
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds (default: 60)")
    parser.add_argument("--limit", type=int, default=0, help="Optional row limit for testing (0 = no limit)")
    args = parser.parse_args()

    text = _fetch_text(args.url, args.timeout)
    rows = _sort_rows(_rows_from_text(text))

    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    output_path = Path(args.output)
    _write_rows(output_path, rows)

    print("OFAC SDN sample update complete")
    print(f"  source_url: {args.url}")
    print(f"  output: {output_path}")
    print(f"  rows_written: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
