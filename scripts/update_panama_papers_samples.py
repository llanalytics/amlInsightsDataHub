#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
from pathlib import Path
from urllib.request import Request, urlopen
import zipfile


DEFAULT_ZIP_URL = "https://offshoreleaks-data.icij.org/offshoreleaks/csv/full-oldb.LATEST.zip"
DEFAULT_NODE_OUTPUT = Path("data/sample/dh_dim_panama_node_sample.csv")
DEFAULT_REL_OUTPUT = Path("data/sample/dh_bridge_panama_relationship_sample.csv")
PANAMA_SOURCE_ID = "Panama Papers"

NODE_COLUMNS = [
    "node_id",
    "node_type",
    "name",
    "source_id",
    "countries",
    "country_codes",
    "jurisdiction",
    "jurisdiction_description",
    "status",
    "valid_until",
    "note",
    "original_name",
    "former_name",
    "address",
    "company_type",
    "internal_id",
    "incorporation_date",
    "inactivation_date",
    "struck_off_date",
    "dorm_date",
    "service_provider",
    "ibcRUC",
]

REL_COLUMNS = [
    "start_node_id",
    "end_node_id",
    "rel_type",
    "link",
    "status",
    "start_date",
    "end_date",
    "source_id",
]

NODE_FILES = {
    "nodes-entities.csv": "entity",
    "nodes-officers.csv": "officer",
    "nodes-intermediaries.csv": "intermediary",
    "nodes-addresses.csv": "address",
    "nodes-others.csv": "other",
}


def _clean(value: str | None) -> str:
    return (value or "").strip()


def _fetch_zip_bytes(url: str, timeout_sec: int) -> bytes:
    req = Request(
        url,
        headers={
            "User-Agent": "amlInsightsDataHub/1.0 (+Panama Papers sync)",
            "Accept": "application/zip,*/*",
        },
    )
    with urlopen(req, timeout=timeout_sec) as resp:  # noqa: S310
        return resp.read()


def _node_row(raw: dict[str, str], node_type: str) -> dict[str, str]:
    return {
        "node_id": _clean(raw.get("node_id")),
        "node_type": node_type,
        "name": _clean(raw.get("name")),
        "source_id": _clean(raw.get("sourceID")),
        "countries": _clean(raw.get("countries")),
        "country_codes": _clean(raw.get("country_codes")),
        "jurisdiction": _clean(raw.get("jurisdiction")),
        "jurisdiction_description": _clean(raw.get("jurisdiction_description")),
        "status": _clean(raw.get("status")),
        "valid_until": _clean(raw.get("valid_until")),
        "note": _clean(raw.get("note")),
        "original_name": _clean(raw.get("original_name")),
        "former_name": _clean(raw.get("former_name")),
        "address": _clean(raw.get("address")),
        "company_type": _clean(raw.get("company_type")),
        "internal_id": _clean(raw.get("internal_id")),
        "incorporation_date": _clean(raw.get("incorporation_date")),
        "inactivation_date": _clean(raw.get("inactivation_date")),
        "struck_off_date": _clean(raw.get("struck_off_date")),
        "dorm_date": _clean(raw.get("dorm_date")),
        "service_provider": _clean(raw.get("service_provider")),
        "ibcRUC": _clean(raw.get("ibcRUC")),
    }


def _relationship_row(raw: dict[str, str]) -> dict[str, str]:
    return {
        "start_node_id": _clean(raw.get("node_id_start")),
        "end_node_id": _clean(raw.get("node_id_end")),
        "rel_type": _clean(raw.get("rel_type")),
        "link": _clean(raw.get("link")),
        "status": _clean(raw.get("status")),
        "start_date": _clean(raw.get("start_date")),
        "end_date": _clean(raw.get("end_date")),
        "source_id": _clean(raw.get("sourceID")),
    }


def _read_nodes(z: zipfile.ZipFile) -> list[dict[str, str]]:
    dedup: dict[tuple[str, str], dict[str, str]] = {}

    for file_name, node_type in NODE_FILES.items():
        with z.open(file_name) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            for raw in reader:
                if _clean(raw.get("sourceID")) != PANAMA_SOURCE_ID:
                    continue

                row = _node_row(raw, node_type)
                node_id = row["node_id"]
                name = row["name"]
                if not node_id or not name:
                    continue

                dedup[(node_id, node_type)] = row

    rows = list(dedup.values())

    def key_fn(r: dict[str, str]):
        try:
            return (r["node_type"], int(r["node_id"]))
        except ValueError:
            return (r["node_type"], r["node_id"])

    rows.sort(key=key_fn)
    return rows


def _read_relationships(z: zipfile.ZipFile) -> list[dict[str, str]]:
    dedup: dict[tuple[str, str, str, str, str, str, str, str], dict[str, str]] = {}

    with z.open("relationships.csv") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
        for raw in reader:
            if _clean(raw.get("sourceID")) != PANAMA_SOURCE_ID:
                continue

            row = _relationship_row(raw)
            if not row["start_node_id"] or not row["end_node_id"] or not row["rel_type"]:
                continue

            key = tuple(row[col] for col in REL_COLUMNS)
            dedup[key] = row

    rows = list(dedup.values())

    def key_fn(r: dict[str, str]):
        try:
            s = int(r["start_node_id"])
        except ValueError:
            s = r["start_node_id"]
        try:
            e = int(r["end_node_id"])
        except ValueError:
            e = r["end_node_id"]
        return (s, e, r["rel_type"], r["link"])

    rows.sort(key=key_fn)
    return rows


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download Offshore Leaks CSV bundle and update Panama Papers sample CSVs",
    )
    parser.add_argument("--url", default=DEFAULT_ZIP_URL, help=f"Source ZIP URL (default: {DEFAULT_ZIP_URL})")
    parser.add_argument("--node-output", default=str(DEFAULT_NODE_OUTPUT), help="Node sample CSV output path")
    parser.add_argument(
        "--relationship-output",
        default=str(DEFAULT_REL_OUTPUT),
        help="Relationship sample CSV output path",
    )
    parser.add_argument("--timeout", type=int, default=180, help="HTTP timeout in seconds (default: 180)")
    parser.add_argument("--limit-nodes", type=int, default=0, help="Optional node row limit for testing")
    parser.add_argument("--limit-relationships", type=int, default=0, help="Optional relationship row limit for testing")
    args = parser.parse_args()

    zip_bytes = _fetch_zip_bytes(args.url, args.timeout)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        node_rows = _read_nodes(z)
        rel_rows = _read_relationships(z)

    if args.limit_nodes > 0:
        node_rows = node_rows[: args.limit_nodes]
    if args.limit_relationships > 0:
        rel_rows = rel_rows[: args.limit_relationships]

    node_output = Path(args.node_output)
    rel_output = Path(args.relationship_output)
    _write_csv(node_output, NODE_COLUMNS, node_rows)
    _write_csv(rel_output, REL_COLUMNS, rel_rows)

    print("Panama Papers sample update complete")
    print(f"  source_url: {args.url}")
    print(f"  node_output: {node_output}")
    print(f"  relationship_output: {rel_output}")
    print(f"  nodes_written: {len(node_rows)}")
    print(f"  relationships_written: {len(rel_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
