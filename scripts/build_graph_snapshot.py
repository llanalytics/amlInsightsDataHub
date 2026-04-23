#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.database import SessionLocal
from app.graph_layer import build_graph_payload


def _ts_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build Data Hub graph snapshot and write Cytoscape-compatible JSON elements to disk."
        )
    )
    parser.add_argument(
        "--output",
        default=str(BASE_DIR / "data" / "graphs" / f"graph_snapshot_{_ts_slug()}.json"),
        help="Output JSON file path",
    )
    parser.add_argument("--include-surrogates", action="store_true", default=True, help="Include surrogate inferred nodes/edges")
    parser.add_argument("--no-include-surrogates", dest="include_surrogates", action="store_false")
    parser.add_argument("--include-ofac-matches", action="store_true", default=True, help="Include inferred OFAC match edges")
    parser.add_argument("--no-include-ofac-matches", dest="include_ofac_matches", action="store_false")
    parser.add_argument("--include-txn-flow", action="store_true", default=True, help="Include transaction flow aggregate edges")
    parser.add_argument("--no-include-txn-flow", dest="include_txn_flow", action="store_false")
    args = parser.parse_args()

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    db = SessionLocal()
    try:
        payload = build_graph_payload(
            db,
            include_surrogates=args.include_surrogates,
            include_ofac_matches=args.include_ofac_matches,
            include_txn_flow=args.include_txn_flow,
        )
    finally:
        db.close()

    with out.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print("Graph snapshot build complete")
    print(f"  output: {out}")
    print(f"  snapshot_id: {payload['snapshot_id']}")
    print(f"  nodes: {payload['node_count']}")
    print(f"  edges: {payload['edge_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
