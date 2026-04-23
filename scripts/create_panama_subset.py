#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create smaller Panama Papers node/relationship CSVs while preserving "
            "relationship -> node referential matching."
        )
    )
    parser.add_argument(
        "--nodes-input",
        default=str(BASE_DIR / "data" / "sample" / "dh_dim_panama_node_sample.csv"),
        help="Input node CSV",
    )
    parser.add_argument(
        "--relationships-input",
        default=str(BASE_DIR / "data" / "sample" / "dh_bridge_panama_relationship_sample.csv"),
        help="Input relationship CSV",
    )
    parser.add_argument(
        "--nodes-output",
        default=str(BASE_DIR / "data" / "sample" / "dh_dim_panama_node_sample_small.csv"),
        help="Output node CSV",
    )
    parser.add_argument(
        "--relationships-output",
        default=str(BASE_DIR / "data" / "sample" / "dh_bridge_panama_relationship_sample_small.csv"),
        help="Output relationship CSV",
    )
    parser.add_argument("--ratio", type=float, default=0.05, help="Keep ratio, default 0.05 (5%%)")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for reproducible subset")
    args = parser.parse_args()

    if args.ratio <= 0 or args.ratio > 1:
        raise ValueError("--ratio must be > 0 and <= 1")

    nodes_input = Path(args.nodes_input)
    rel_input = Path(args.relationships_input)
    node_rows = _read_rows(nodes_input)
    rel_rows = _read_rows(rel_input)
    if not node_rows:
        raise ValueError(f"No rows in {nodes_input}")
    if not rel_rows:
        raise ValueError(f"No rows in {rel_input}")

    node_fieldnames = list(node_rows[0].keys())
    rel_fieldnames = list(rel_rows[0].keys())

    rng = random.Random(args.seed)

    # Keep roughly ratio of nodes.
    target_nodes = max(1, int(len(node_rows) * args.ratio))
    idxs = list(range(len(node_rows)))
    rng.shuffle(idxs)
    keep_idxs = set(idxs[:target_nodes])
    kept_nodes = [node_rows[i] for i in range(len(node_rows)) if i in keep_idxs]
    kept_node_ids = {r.get("node_id", "").strip() for r in kept_nodes}

    # Keep only relationships where both endpoints exist in kept nodes.
    rel_filtered = [
        r
        for r in rel_rows
        if r.get("start_node_id", "").strip() in kept_node_ids
        and r.get("end_node_id", "").strip() in kept_node_ids
    ]

    # Cap relationship rows to roughly same ratio when enough are available.
    target_rels = max(1, int(len(rel_rows) * args.ratio))
    if len(rel_filtered) > target_rels:
        rel_idxs = list(range(len(rel_filtered)))
        rng.shuffle(rel_idxs)
        rel_keep = set(rel_idxs[:target_rels])
        kept_relationships = [rel_filtered[i] for i in range(len(rel_filtered)) if i in rel_keep]
    else:
        kept_relationships = rel_filtered

    # Ensure all relationship endpoints exist in output nodes by trimming nodes to used ids.
    rel_node_ids = {
        r.get("start_node_id", "").strip()
        for r in kept_relationships
    } | {
        r.get("end_node_id", "").strip()
        for r in kept_relationships
    }
    if rel_node_ids:
        kept_nodes = [r for r in kept_nodes if r.get("node_id", "").strip() in rel_node_ids]

    _write_rows(Path(args.nodes_output), node_fieldnames, kept_nodes)
    _write_rows(Path(args.relationships_output), rel_fieldnames, kept_relationships)

    print("Panama subset creation complete")
    print(f"  ratio: {args.ratio:.4f}")
    print(f"  nodes_in: {len(node_rows)}")
    print(f"  nodes_out: {len(kept_nodes)}")
    print(f"  relationships_in: {len(rel_rows)}")
    print(f"  relationships_out: {len(kept_relationships)}")
    print(f"  nodes_output: {Path(args.nodes_output)}")
    print(f"  relationships_output: {Path(args.relationships_output)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

