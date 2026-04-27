from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

import networkx as nx
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.models import (
    DHBridgeCustomerAccount,
    DHBridgeCustomerAssociatedParty,
    DHBridgeHouseholdCustomer,
    DHBridgePanamaRelationship,
    DHDimAccount,
    DHDimAssociatedParty,
    DHDimBranch,
    DHDimCounterpartyAccount,
    DHDimCountry,
    DHDimCurrency,
    DHDimCustomer,
    DHDimHousehold,
    DHDimOfacSdn,
    DHDimPanamaNode,
    DHDimTransactionType,
    DHFactCash,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"


def _node_id(node_type: str, business_key: str) -> str:
    return f"{node_type}:{business_key}"


def _norm_text(value: str | None) -> str:
    if not value:
        return ""
    s = value.upper().strip()
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _hash_id(prefix: str, raw: str) -> str:
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{prefix}:{h}"


def _attrs_json(row: Any) -> dict[str, Any]:
    raw = getattr(row, "attr_json", None) or "{}"
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def _add_node(
    g: nx.MultiDiGraph,
    node_type: str,
    business_key: str,
    label: str,
    source_table: str,
    is_inferred: bool,
    as_of_ts: str,
    extra: dict[str, Any] | None = None,
) -> str:
    nid = _node_id(node_type, business_key)
    payload = {
        "id": nid,
        "node_type": node_type,
        "business_key": business_key,
        "label": label,
        "source_table": source_table,
        "is_inferred": is_inferred,
        "as_of_ts": as_of_ts,
    }
    if extra:
        payload.update(extra)
    g.add_node(nid, **payload)
    return nid


def _add_edge(
    g: nx.MultiDiGraph,
    source: str,
    target: str,
    edge_type: str,
    as_of_ts: str,
    attrs: dict[str, Any] | None = None,
) -> None:
    eid = f"{source}|{edge_type}|{target}|{as_of_ts}"
    payload = {
        "id": eid,
        "source": source,
        "target": target,
        "edge_type": edge_type,
        "as_of_ts": as_of_ts,
        "is_inferred": False,
    }
    if attrs:
        payload.update(attrs)
    g.add_edge(source, target, key=eid, **payload)


def _add_surrogates_for_entity(
    g: nx.MultiDiGraph,
    entity_node_id: str,
    entity_label: str,
    address_parts: list[str],
    as_of_ts: str,
) -> None:
    norm_name = _norm_text(entity_label)
    if norm_name:
        s_name_id = _hash_id("SurrogateName", norm_name)
        g.add_node(
            s_name_id,
            id=s_name_id,
            node_type="SurrogateName",
            label=norm_name,
            source_table="inferred",
            is_inferred=True,
            as_of_ts=as_of_ts,
        )
        _add_edge(
            g,
            entity_node_id,
            s_name_id,
            "HAS_NAME_SIGNATURE",
            as_of_ts,
            {
                "is_inferred": True,
                "match_method": "normalized_exact",
                "confidence": 1.0,
                "normalization_version": "1.0",
            },
        )

    raw_address_parts = [p.strip() for p in address_parts if p and p.strip()]
    address_parts_without_country_only = [
        p
        for p in raw_address_parts
        if not re.fullmatch(r"[A-Z]{2,3}(?:\s+[A-Z]{2,3})*", _norm_text(p))
    ]
    addr_raw = " | ".join(raw_address_parts if address_parts_without_country_only else [])
    norm_addr = _norm_text(addr_raw)
    if norm_addr:
        s_addr_id = _hash_id("SurrogateAddress", norm_addr)
        g.add_node(
            s_addr_id,
            id=s_addr_id,
            node_type="SurrogateAddress",
            label=norm_addr,
            source_table="inferred",
            is_inferred=True,
            as_of_ts=as_of_ts,
        )
        _add_edge(
            g,
            entity_node_id,
            s_addr_id,
            "HAS_ADDRESS_SIGNATURE",
            as_of_ts,
            {
                "is_inferred": True,
                "match_method": "normalized_exact",
                "confidence": 1.0,
                "normalization_version": "1.0",
            },
        )

    if norm_name and norm_addr:
        composite = f"{norm_name}||{norm_addr}"
        s_comp_id = _hash_id("SurrogateNameAddress", composite)
        g.add_node(
            s_comp_id,
            id=s_comp_id,
            node_type="SurrogateNameAddress",
            label=composite,
            source_table="inferred",
            is_inferred=True,
            as_of_ts=as_of_ts,
        )
        _add_edge(
            g,
            entity_node_id,
            s_comp_id,
            "HAS_NAME_ADDRESS_SIGNATURE",
            as_of_ts,
            {
                "is_inferred": True,
                "match_method": "normalized_composite",
                "confidence": 1.0,
                "normalization_version": "1.0",
            },
        )


def _prune_unshared_surrogate_nodes(g: nx.MultiDiGraph) -> None:
    surrogate_types = {"SurrogateName", "SurrogateAddress", "SurrogateNameAddress"}
    to_remove: list[str] = []
    for nid, data in g.nodes(data=True):
        if data.get("node_type") not in surrogate_types:
            continue
        neighbors = set(g.predecessors(nid)) | set(g.successors(nid))
        if len(neighbors) < 2:
            to_remove.append(str(nid))
    if to_remove:
        g.remove_nodes_from(to_remove)


def _annotate_node_degrees(g: nx.MultiDiGraph) -> None:
    for nid in list(g.nodes()):
        neighbors = set(g.predecessors(nid)) | set(g.successors(nid))
        g.nodes[nid]["degree_total"] = len(neighbors)


def _is_current_rows(db: Session, model: Any) -> list[Any]:
    return db.execute(select(model).where(model.is_current.is_(True))).scalars().all()


def search_customer_seeds(
    db: Session,
    q: str,
    limit: int = 20,
    business_unit: str | None = None,
    customer_segment: str | None = None,
) -> list[dict[str, Any]]:
    term = _norm_text(q)
    if not term:
        return []

    term_tokens = term.split()
    bu_norm = _norm_text(business_unit) if business_unit else ""
    seg_norm = _norm_text(customer_segment) if customer_segment else ""

    hits: list[tuple[int, dict[str, Any]]] = []
    for row in _is_current_rows(db, DHDimCustomer):
        attrs = _attrs_json(row)
        name = str(attrs.get("name", ""))
        seg = str(attrs.get("customer_segment", ""))
        city = str(attrs.get("address_city", ""))
        state = str(attrs.get("address_state_province", ""))
        bu = str(attrs.get("business_unit", "") or getattr(row, "business_unit", "") or "")

        name_norm = _norm_text(name)
        key_norm = _norm_text(row.customer_key)
        seg_row_norm = _norm_text(seg)
        bu_row_norm = _norm_text(bu)

        if bu_norm and bu_row_norm != bu_norm:
            continue
        if seg_norm and seg_row_norm != seg_norm:
            continue

        score = 0
        if key_norm == term:
            score += 1000
        elif key_norm.startswith(term):
            score += 600
        elif term in key_norm:
            score += 400

        if name_norm == term:
            score += 900
        elif name_norm.startswith(term):
            score += 500
        elif term in name_norm:
            score += 350

        if term_tokens and all(t in name_norm for t in term_tokens):
            score += 250
        elif term_tokens and any(t in name_norm for t in term_tokens):
            score += 100

        if score <= 0:
            continue

        score += max(0, 50 - abs(len(name_norm) - len(term)))
        hits.append(
            (
                score,
                {
                    "customer_key": row.customer_key,
                    "name": name or row.customer_key,
                    "business_unit": bu or None,
                    "customer_segment": seg or None,
                    "address_city": city or None,
                    "address_state_province": state or None,
                    "score": score,
                },
            )
        )

    hits.sort(key=lambda x: (-x[0], x[1]["customer_key"]))
    return [item for _score, item in hits[: max(1, limit)]]


_GENERIC_ENTITY_MATCH_TOKENS = {
    "LIMITED",
    "LTD",
    "LLC",
    "INC",
    "CORP",
    "CORPORATION",
    "COMPANY",
    "CO",
    "PLC",
    "SA",
    "AG",
    "GMBH",
    "LP",
    "LLP",
}


def _score_term_match(term: str, term_tokens: list[str], candidate_norm: str) -> int:
    if not term or not candidate_norm:
        return 0
    score = 0
    if candidate_norm == term:
        score += 1000
    elif candidate_norm.startswith(term):
        score += 650
    elif term in candidate_norm:
        score += 450

    match_tokens = [t for t in term_tokens if t not in _GENERIC_ENTITY_MATCH_TOKENS] or term_tokens
    if match_tokens and all(t in candidate_norm for t in match_tokens):
        score += 220
    elif match_tokens and any(t in candidate_norm for t in match_tokens):
        score += 90

    if score <= 0:
        return 0

    score += max(0, 35 - abs(len(candidate_norm) - len(term)))
    return score


_ENTITY_INTRO_WORDS = ("ON", "FOR", "ABOUT", "REGARDING", "INVOLVING", "RELATED TO")
_ENTITY_STOP_WORDS = (
    "AND",
    "ANY",
    "WITH",
    "WHO",
    "THAT",
    "WHICH",
    "PAYMENT",
    "PAYMENTS",
    "TRANSACTION",
    "TRANSACTIONS",
    "COUNTERPARTY",
    "COUNTERPARTIES",
    "OUTSIDE",
    "INSIDE",
    "NEGATIVE",
    "NEWS",
    "ADVERSE",
    "MEDIA",
)
_ORG_SUFFIX_PATTERN = re.compile(
    r"\b(?:[A-Z0-9]+ ){0,8}(?:LIMITED|LTD|LLC|INC|CORP|CORPORATION|COMPANY|CO|PLC|SA|AG|GMBH|LP|LLP)\b"
)


def _trim_entity_phrase(value: str) -> str:
    tokens = [t for t in _norm_text(value).split() if t]
    if not tokens:
        return ""
    stop_set = set(_ENTITY_STOP_WORDS)
    kept: list[str] = []
    for token in tokens:
        if token in stop_set and kept:
            break
        if token in {"THE", "A", "AN"} and not kept:
            continue
        kept.append(token)
    while kept and kept[-1] in stop_set:
        kept.pop()
    return " ".join(kept).strip()


def _entity_search_terms_from_question(q: str) -> list[str]:
    norm = _norm_text(q)
    if not norm:
        return []

    terms: list[str] = []

    for intro in _ENTITY_INTRO_WORDS:
        marker = f" {intro} "
        idx = f" {norm} ".find(marker)
        if idx < 0:
            continue
        after = norm[idx + len(marker) - 1 :]
        candidate = _trim_entity_phrase(after)
        if candidate:
            terms.append(candidate)

    for match in _ORG_SUFFIX_PATTERN.finditer(norm):
        phrase = _norm_text(match.group(0))
        phrase_tokens = phrase.split()
        intro_positions = [
            idx
            for idx, token in enumerate(phrase_tokens)
            if token in _ENTITY_INTRO_WORDS
        ]
        if intro_positions:
            phrase = " ".join(phrase_tokens[intro_positions[-1] + 1 :])
        terms.append(_trim_entity_phrase(phrase))

    out: list[str] = []
    seen: set[str] = set()
    for term in terms:
        clean = _trim_entity_phrase(term)
        if not clean or clean in seen:
            continue
        # Avoid reducing a plain one-word exploratory search to a generic word.
        if len(clean.split()) == 1 and clean not in norm:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def search_exposure_seeds(
    db: Session,
    q: str,
    limit: int = 25,
) -> list[dict[str, Any]]:
    term = _norm_text(q)
    if not term:
        return []
    search_terms = _entity_search_terms_from_question(q) or [term]
    scored_terms = [(search_term, search_term.split()) for search_term in search_terms]
    hits: list[tuple[int, dict[str, Any]]] = []

    def add_hit(node_type: str, business_key: str, label: str, matched_fields: list[str], score: int) -> None:
        if score <= 0:
            return
        hits.append(
            (
                score,
                {
                    "node_id": _node_id(node_type, business_key),
                    "node_type": node_type,
                    "business_key": business_key,
                    "label": label or business_key,
                    "matched_fields": matched_fields,
                    "score": score,
                },
            )
        )

    for row in _is_current_rows(db, DHDimCustomer):
        attrs = _attrs_json(row)
        name = str(attrs.get("name", "") or row.customer_key)
        key_val = str(row.customer_key)
        name_score = max(_score_term_match(t, toks, _norm_text(name)) for t, toks in scored_terms)
        key_score = max(_score_term_match(t, toks, _norm_text(key_val)) for t, toks in scored_terms)
        score = max(name_score, key_score)
        matched: list[str] = []
        if name_score > 0:
            matched.append("name")
        if key_score > 0:
            matched.append("customer_key")
        add_hit("Customer", key_val, name, matched, score)

    for row in _is_current_rows(db, DHDimAccount):
        attrs = _attrs_json(row)
        name = str(attrs.get("account_name", "") or row.account_key)
        key_val = str(row.account_key)
        name_score = max(_score_term_match(t, toks, _norm_text(name)) for t, toks in scored_terms)
        key_score = max(_score_term_match(t, toks, _norm_text(key_val)) for t, toks in scored_terms)
        score = max(name_score, key_score)
        matched: list[str] = []
        if name_score > 0:
            matched.append("account_name")
        if key_score > 0:
            matched.append("account_key")
        add_hit("Account", key_val, name, matched, score)

    for row in _is_current_rows(db, DHDimCounterpartyAccount):
        attrs = _attrs_json(row)
        name = str(attrs.get("counterparty_name", "") or row.counterparty_account_key)
        key_val = str(row.counterparty_account_key)
        name_score = max(_score_term_match(t, toks, _norm_text(name)) for t, toks in scored_terms)
        key_score = max(_score_term_match(t, toks, _norm_text(key_val)) for t, toks in scored_terms)
        score = max(name_score, key_score)
        matched: list[str] = []
        if name_score > 0:
            matched.append("counterparty_name")
        if key_score > 0:
            matched.append("counterparty_account_key")
        add_hit("CounterpartyAccount", key_val, name, matched, score)

    for row in _is_current_rows(db, DHDimOfacSdn):
        attrs = _attrs_json(row)
        name = str(attrs.get("name", "") or row.sdn_uid)
        key_val = str(row.sdn_uid)
        name_score = max(_score_term_match(t, toks, _norm_text(name)) for t, toks in scored_terms)
        key_score = max(_score_term_match(t, toks, _norm_text(key_val)) for t, toks in scored_terms)
        score = max(name_score, key_score)
        matched: list[str] = []
        if name_score > 0:
            matched.append("name")
        if key_score > 0:
            matched.append("sdn_uid")
        add_hit("OfacSdn", key_val, name, matched, score)

    for row in _is_current_rows(db, DHDimPanamaNode):
        attrs = _attrs_json(row)
        name = str(attrs.get("name", "") or row.node_id)
        notes_text = ""
        if "notes" in attrs:
            notes_text = str(attrs.get("notes", "") or "")
        elif "note" in attrs:
            notes_text = str(attrs.get("note", "") or "")
        else:
            # Fall back to any textual note-like attributes.
            note_like = [str(v) for k, v in attrs.items() if "note" in str(k).lower()]
            notes_text = " ".join([v for v in note_like if v])

        key_val = str(row.node_id)
        name_score = max(_score_term_match(t, toks, _norm_text(name)) for t, toks in scored_terms)
        key_score = max(_score_term_match(t, toks, _norm_text(key_val)) for t, toks in scored_terms)
        notes_score = max(_score_term_match(t, toks, _norm_text(notes_text)) for t, toks in scored_terms)
        score = max(name_score, key_score, notes_score)
        matched: list[str] = []
        if name_score > 0:
            matched.append("name")
        if key_score > 0:
            matched.append("node_id")
        if notes_score > 0:
            matched.append("notes")
            score += 120
        add_hit("PanamaNode", key_val, name, matched, score)

    deduped: dict[str, tuple[int, dict[str, Any]]] = {}
    for score, item in hits:
        node_id = str(item.get("node_id", ""))
        if not node_id:
            continue
        existing = deduped.get(node_id)
        if not existing or score > existing[0]:
            deduped[node_id] = (score, item)

    ordered = sorted(deduped.values(), key=lambda x: (-x[0], str(x[1].get("node_type", "")), str(x[1].get("label", ""))))
    return [item for _score, item in ordered[: max(1, limit)]]


def build_graph_payload(
    db: Session,
    include_surrogates: bool = True,
    include_ofac_matches: bool = True,
    include_txn_flow: bool = True,
) -> dict[str, Any]:
    as_of_ts = _utc_now_iso()
    g = nx.MultiDiGraph()

    # Dimensions: base nodes
    households = _is_current_rows(db, DHDimHousehold)
    customers = _is_current_rows(db, DHDimCustomer)
    associated = _is_current_rows(db, DHDimAssociatedParty)
    accounts = _is_current_rows(db, DHDimAccount)
    counterparties = _is_current_rows(db, DHDimCounterpartyAccount)
    branches = _is_current_rows(db, DHDimBranch)
    countries = _is_current_rows(db, DHDimCountry)
    currencies = _is_current_rows(db, DHDimCurrency)
    txn_types = _is_current_rows(db, DHDimTransactionType)
    ofac_rows = _is_current_rows(db, DHDimOfacSdn)
    panama_nodes = _is_current_rows(db, DHDimPanamaNode)

    customer_name_map: dict[str, str] = {}
    associated_name_map: dict[str, str] = {}
    counterparty_name_map: dict[str, str] = {}
    panama_name_map: dict[str, str] = {}
    ofac_name_map: dict[str, str] = {}
    txn_dir_map: dict[str, str] = {}
    txn_class_map: dict[str, str] = {}

    for r in households:
        attrs = _attrs_json(r)
        _add_node(g, "Household", r.household_key, attrs.get("name", r.household_key), r.__tablename__, False, as_of_ts, attrs)

    for r in customers:
        attrs = _attrs_json(r)
        label = attrs.get("name", r.customer_key)
        nid = _add_node(g, "Customer", r.customer_key, label, r.__tablename__, False, as_of_ts, attrs)
        customer_name_map[nid] = label
        if include_surrogates:
            _add_surrogates_for_entity(
                g,
                nid,
                label,
                [
                    str(attrs.get("address_line_1", "")),
                    str(attrs.get("address_line_2", "")),
                    str(attrs.get("address_city", "")),
                    str(attrs.get("address_state_province", "")),
                    str(attrs.get("address_postal_code", "")),
                    str(attrs.get("address_country_code", "")),
                ],
                as_of_ts,
            )

    for r in associated:
        attrs = _attrs_json(r)
        label = attrs.get("name", r.associated_party_key)
        nid = _add_node(g, "AssociatedParty", r.associated_party_key, label, r.__tablename__, False, as_of_ts, attrs)
        associated_name_map[nid] = label
        if include_surrogates:
            _add_surrogates_for_entity(g, nid, label, [], as_of_ts)

    for r in accounts:
        attrs = _attrs_json(r)
        _add_node(g, "Account", r.account_key, attrs.get("account_name", r.account_key), r.__tablename__, False, as_of_ts, attrs)

    for r in counterparties:
        attrs = _attrs_json(r)
        label = attrs.get("counterparty_name", r.counterparty_account_key)
        nid = _add_node(
            g,
            "CounterpartyAccount",
            r.counterparty_account_key,
            label,
            r.__tablename__,
            False,
            as_of_ts,
            attrs,
        )
        counterparty_name_map[nid] = label
        if include_surrogates:
            _add_surrogates_for_entity(g, nid, label, [], as_of_ts)

    for r in branches:
        attrs = _attrs_json(r)
        _add_node(g, "Branch", r.branch_key, attrs.get("branch_type", r.branch_key), r.__tablename__, False, as_of_ts, attrs)

    for r in countries:
        attrs = _attrs_json(r)
        _add_node(g, "Country", r.country_code_2, attrs.get("country_name", r.country_code_2), r.__tablename__, False, as_of_ts, attrs)

    for r in currencies:
        attrs = _attrs_json(r)
        _add_node(g, "Currency", r.currency_code, attrs.get("currency_name", r.currency_code), r.__tablename__, False, as_of_ts, attrs)

    for r in txn_types:
        attrs = _attrs_json(r)
        label = attrs.get("aml_classification", r.transaction_type_code)
        _add_node(g, "TransactionType", r.transaction_type_code, label, r.__tablename__, False, as_of_ts, attrs)
        txn_dir_map[r.transaction_type_code] = str(attrs.get("direction", "")).strip()
        txn_class_map[r.transaction_type_code] = str(attrs.get("aml_classification", "")).strip()

    for r in ofac_rows:
        attrs = _attrs_json(r)
        label = attrs.get("name", r.sdn_uid)
        nid = _add_node(g, "OfacSdn", r.sdn_uid, label, r.__tablename__, False, as_of_ts, attrs)
        ofac_name_map[nid] = label

    for r in panama_nodes:
        attrs = _attrs_json(r)
        label = attrs.get("name", r.node_id)
        business_key = str(r.node_id)
        nid = _add_node(g, "PanamaNode", business_key, label, r.__tablename__, False, as_of_ts, attrs | {"node_type_code": r.node_type})
        panama_name_map[nid] = label
        if include_surrogates:
            _add_surrogates_for_entity(
                g,
                nid,
                label,
                [str(attrs.get("address", "")), str(attrs.get("country_codes", ""))],
                as_of_ts,
            )

    # Base relationship edges
    for r in _is_current_rows(db, DHBridgeHouseholdCustomer):
        src = _node_id("Household", r.household_key)
        dst = _node_id("Customer", r.customer_key)
        if src in g and dst in g:
            _add_edge(g, src, dst, "HOUSEHOLD_HAS_CUSTOMER", as_of_ts, {"source_table": r.__tablename__})

    for r in _is_current_rows(db, DHBridgeCustomerAccount):
        src = _node_id("Customer", r.customer_key)
        dst = _node_id("Account", r.account_key)
        if src in g and dst in g:
            _add_edge(
                g,
                src,
                dst,
                "CUSTOMER_HAS_ACCOUNT",
                as_of_ts,
                {"source_table": r.__tablename__, "relationship_type": r.relationship_type or "NA"},
            )

    for r in _is_current_rows(db, DHBridgeCustomerAssociatedParty):
        src = _node_id("Customer", r.customer_key)
        dst = _node_id("AssociatedParty", r.associated_party_key)
        if src in g and dst in g:
            _add_edge(g, src, dst, "CUSTOMER_HAS_ASSOCIATED_PARTY", as_of_ts, {"source_table": r.__tablename__})

    for r in _is_current_rows(db, DHBridgePanamaRelationship):
        src = _node_id("PanamaNode", r.start_node_id)
        dst = _node_id("PanamaNode", r.end_node_id)
        if src in g and dst in g:
            _add_edge(
                g,
                src,
                dst,
                "PANAMA_RELATIONSHIP",
                as_of_ts,
                {"source_table": r.__tablename__, "rel_type": r.rel_type, "status": r.status, "source_id": r.source_id},
            )

    # Transaction flow edges (aggregated one link per account<->counterparty)
    if include_txn_flow:
        rows = db.execute(
            select(
                DHFactCash.account_key,
                DHFactCash.counterparty_account_key,
                DHFactCash.transaction_type_code,
                func.sum(DHFactCash.amount).label("total_amount"),
                func.count().label("txn_count"),
                func.min(DHFactCash.transaction_ts).label("first_txn_ts"),
                func.max(DHFactCash.transaction_ts).label("last_txn_ts"),
            ).group_by(
                DHFactCash.account_key,
                DHFactCash.counterparty_account_key,
                DHFactCash.transaction_type_code,
            )
        ).all()

        agg_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            acct = str(row.account_key)
            cp = str(row.counterparty_account_key)
            if not acct or not cp:
                continue

            # Limit transaction-flow edges to external transfers only.
            txn_class = str(txn_class_map.get(str(row.transaction_type_code), "")).strip().casefold()
            if txn_class != "external funds transfer":
                continue

            direction = str(txn_dir_map.get(str(row.transaction_type_code), "")).strip().casefold()
            direction_label = direction if direction in {"inbound", "outbound"} else "unknown"
            aml_class_label = str(txn_class_map.get(str(row.transaction_type_code), "")).strip() or "Unknown"
            key = (acct, cp)
            bucket = agg_by_pair.setdefault(
                key,
                {
                    "account_key": acct,
                    "counterparty_account_key": cp,
                    "total_amount": 0.0,
                    "txn_count": 0,
                    "inbound_amount": 0.0,
                    "inbound_txn_count": 0,
                    "outbound_amount": 0.0,
                    "outbound_txn_count": 0,
                    "transaction_type_codes": set(),
                    "activity_breakdown": {},
                    "first_txn_ts": None,
                    "last_txn_ts": None,
                },
            )
            amount = float(row.total_amount or 0.0)
            count = int(row.txn_count or 0)
            bucket["total_amount"] += amount
            bucket["txn_count"] += count
            if direction == "inbound":
                bucket["inbound_amount"] += amount
                bucket["inbound_txn_count"] += count
            elif direction == "outbound":
                bucket["outbound_amount"] += amount
                bucket["outbound_txn_count"] += count
            bucket["transaction_type_codes"].add(str(row.transaction_type_code))
            breakdown_key = f"{aml_class_label}|{direction_label}"
            breakdown = bucket["activity_breakdown"].setdefault(
                breakdown_key,
                {
                    "aml_classification": aml_class_label,
                    "direction": direction_label,
                    "total_amount": 0.0,
                    "txn_count": 0,
                },
            )
            breakdown["total_amount"] += amount
            breakdown["txn_count"] += count

            first_ts = row.first_txn_ts.isoformat() if row.first_txn_ts else None
            last_ts = row.last_txn_ts.isoformat() if row.last_txn_ts else None
            if first_ts and (not bucket["first_txn_ts"] or first_ts < bucket["first_txn_ts"]):
                bucket["first_txn_ts"] = first_ts
            if last_ts and (not bucket["last_txn_ts"] or last_ts > bucket["last_txn_ts"]):
                bucket["last_txn_ts"] = last_ts

        for (acct, cp), bucket in agg_by_pair.items():
            acct_node = _node_id("Account", acct)
            cp_node = _node_id("CounterpartyAccount", cp)
            if acct_node not in g or cp_node not in g:
                continue

            _add_edge(
                g,
                acct_node,
                cp_node,
                "TXN_FLOW_AGG",
                as_of_ts,
                {
                    "is_inferred": False,
                    "source_table": "dh_fact_cash",
                    "aggregation_level": "account_counterparty",
                    "transaction_type_codes": sorted(bucket["transaction_type_codes"]),
                    "total_amount": float(bucket["total_amount"]),
                    "txn_count": int(bucket["txn_count"]),
                    "inbound_amount": float(bucket["inbound_amount"]),
                    "inbound_txn_count": int(bucket["inbound_txn_count"]),
                    "outbound_amount": float(bucket["outbound_amount"]),
                    "outbound_txn_count": int(bucket["outbound_txn_count"]),
                    "activity_breakdown": sorted(
                        [
                            {
                                "aml_classification": str(item.get("aml_classification") or "Unknown"),
                                "direction": str(item.get("direction") or "unknown"),
                                "total_amount": float(item.get("total_amount") or 0.0),
                                "txn_count": int(item.get("txn_count") or 0),
                            }
                            for item in bucket["activity_breakdown"].values()
                        ],
                        key=lambda x: (x["aml_classification"], x["direction"]),
                    ),
                    "first_txn_ts": bucket["first_txn_ts"],
                    "last_txn_ts": bucket["last_txn_ts"],
                },
            )

    # OFAC inferred edges (normalized exact name)
    if include_ofac_matches and ofac_name_map:
        ofac_by_norm: dict[str, list[tuple[str, str]]] = {}
        for nid, nm in ofac_name_map.items():
            n = _norm_text(nm)
            if not n:
                continue
            ofac_by_norm.setdefault(n, []).append((nid, nm))

        entity_maps = [customer_name_map, associated_name_map, counterparty_name_map, panama_name_map]
        for emap in entity_maps:
            for entity_nid, entity_name in emap.items():
                n = _norm_text(entity_name)
                if not n or n not in ofac_by_norm:
                    continue
                for ofac_nid, _ in ofac_by_norm[n]:
                    _add_edge(
                        g,
                        entity_nid,
                        ofac_nid,
                        "POTENTIAL_OFAC_MATCH",
                        as_of_ts,
                        {
                            "is_inferred": True,
                            "match_method": "normalized_name",
                            "match_score": 1.0,
                            "is_confirmed": False,
                        },
                    )

    if include_surrogates:
        _prune_unshared_surrogate_nodes(g)

    _annotate_node_degrees(g)

    # Cytoscape elements serialization
    nodes = [{"data": data} for _nid, data in g.nodes(data=True)]
    edges = [{"data": data} for _src, _dst, _k, data in g.edges(keys=True, data=True)]
    snapshot_id = f"graph_{as_of_ts}"

    return {
        "snapshot_id": snapshot_id,
        "model_version": "1.0",
        "as_of_ts": as_of_ts,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "elements": {
            "nodes": nodes,
            "edges": edges,
        },
    }


def _adjacency_from_elements(elements: dict[str, Any]) -> dict[str, set[str]]:
    adj: dict[str, set[str]] = {}
    for edge in elements.get("edges", []):
        data = edge.get("data", {})
        src = str(data.get("source", ""))
        dst = str(data.get("target", ""))
        if not src or not dst:
            continue
        adj.setdefault(src, set()).add(dst)
        adj.setdefault(dst, set()).add(src)
    return adj


def _bfs_nodes(seed: str, adj: dict[str, set[str]], hops: int, max_nodes: int) -> set[str]:
    selected: set[str] = set([seed])
    frontier = {seed}
    for _ in range(max(0, hops)):
        nxt: set[str] = set()
        for n in frontier:
            nxt.update(adj.get(n, set()))
        nxt -= selected
        if not nxt:
            break
        for n in sorted(nxt):
            if len(selected) >= max_nodes:
                return selected
            selected.add(n)
        frontier = nxt
        if len(selected) >= max_nodes:
            break
    return selected


def _add_shared_surrogates_to_selection(payload: dict[str, Any], selected_nodes: set[str], max_nodes: int) -> set[str]:
    surrogate_types = {"SurrogateName", "SurrogateAddress", "SurrogateNameAddress"}
    node_types: dict[str, str] = {}
    for n in payload.get("elements", {}).get("nodes", []):
        data = n.get("data", {}) if isinstance(n, dict) else {}
        nid = str(data.get("id", ""))
        if nid:
            node_types[nid] = str(data.get("node_type", ""))

    selected = set(selected_nodes)
    selected_real_nodes = {nid for nid in selected if node_types.get(nid) not in surrogate_types}
    surrogate_neighbors: dict[str, set[str]] = {}
    for e in payload.get("elements", {}).get("edges", []):
        data = e.get("data", {}) if isinstance(e, dict) else {}
        src = str(data.get("source", ""))
        dst = str(data.get("target", ""))
        if not src or not dst:
            continue
        src_is_surrogate = node_types.get(src) in surrogate_types
        dst_is_surrogate = node_types.get(dst) in surrogate_types
        if src_is_surrogate and dst in selected_real_nodes:
            surrogate_neighbors.setdefault(src, set()).add(dst)
        elif dst_is_surrogate and src in selected_real_nodes:
            surrogate_neighbors.setdefault(dst, set()).add(src)

    for surrogate_id, neighbors in sorted(surrogate_neighbors.items(), key=lambda item: (-len(item[1]), item[0])):
        if len(selected) >= max_nodes:
            break
        if len(neighbors) >= 2:
            selected.add(surrogate_id)
    return selected


def _subgraph_payload_from_nodes(
    payload: dict[str, Any],
    selected_nodes: set[str],
    max_edges: int,
) -> dict[str, Any]:
    nodes = [
        n for n in payload.get("elements", {}).get("nodes", [])
        if str(n.get("data", {}).get("id", "")) in selected_nodes
    ]
    edges: list[dict[str, Any]] = []
    for e in payload.get("elements", {}).get("edges", []):
        d = e.get("data", {})
        src = str(d.get("source", ""))
        dst = str(d.get("target", ""))
        if src in selected_nodes and dst in selected_nodes:
            edges.append(e)
            if len(edges) >= max_edges:
                break

    return {
        "snapshot_id": payload.get("snapshot_id"),
        "model_version": payload.get("model_version"),
        "as_of_ts": payload.get("as_of_ts"),
        "node_count": len(nodes),
        "edge_count": len(edges),
        "elements": {
            "nodes": nodes,
            "edges": edges,
        },
    }


def build_customer_graph_payload(
    db: Session,
    customer_key: str,
    hops: int = 2,
    max_nodes: int = 500,
    max_edges: int = 2000,
    include_surrogates: bool = True,
    include_ofac_matches: bool = True,
    include_txn_flow: bool = True,
) -> dict[str, Any]:
    full = build_graph_payload(
        db,
        include_surrogates=include_surrogates,
        include_ofac_matches=include_ofac_matches,
        include_txn_flow=include_txn_flow,
    )
    seed = _node_id("Customer", customer_key)
    node_ids = {str(n.get("data", {}).get("id", "")) for n in full.get("elements", {}).get("nodes", [])}
    if seed not in node_ids:
        raise KeyError(f"Customer node not found: {seed}")

    adj = _adjacency_from_elements(full.get("elements", {}))
    selected = _bfs_nodes(seed, adj, hops=hops, max_nodes=max_nodes)
    if include_surrogates:
        selected = _add_shared_surrogates_to_selection(full, selected, max_nodes=max_nodes)
    out = _subgraph_payload_from_nodes(full, selected, max_edges=max_edges)
    out["center_node"] = seed
    out["hops"] = hops
    return out


def _filter_exposure_subgraph(payload: dict[str, Any], seed_node_id: str) -> dict[str, Any]:
    elements = payload.get("elements", {}) if isinstance(payload, dict) else {}
    nodes_in = list(elements.get("nodes", []))
    edges_in = list(elements.get("edges", []))

    node_map: dict[str, dict[str, Any]] = {}
    for n in nodes_in:
        data = n.get("data", {}) if isinstance(n, dict) else {}
        nid = str(data.get("id", ""))
        if nid:
            node_map[nid] = n

    # Remove NA-key nodes from exposure view.
    keep_ids: set[str] = set()
    for nid in node_map:
        key_part = nid.split(":", 1)[1] if ":" in nid else ""
        if key_part.strip().upper() == "NA":
            continue
        keep_ids.add(nid)

    edges_step1 = []
    for e in edges_in:
        d = e.get("data", {}) if isinstance(e, dict) else {}
        src = str(d.get("source", ""))
        dst = str(d.get("target", ""))
        if src in keep_ids and dst in keep_ids:
            edges_step1.append(e)

    # Keep OFAC/Panama only when linked (via connected component) to Customer/Account/Counterparty.
    adj: dict[str, set[str]] = {nid: set() for nid in keep_ids}
    for e in edges_step1:
        d = e.get("data", {})
        src = str(d.get("source", ""))
        dst = str(d.get("target", ""))
        if src in adj and dst in adj:
            adj[src].add(dst)
            adj[dst].add(src)

    visited: set[str] = set()
    anchor_types = {"Customer", "Account", "CounterpartyAccount"}
    remove_ids: set[str] = set()
    for nid in sorted(keep_ids):
        if nid in visited:
            continue
        stack = [nid]
        comp: set[str] = set()
        while stack:
            cur = stack.pop()
            if cur in visited:
                continue
            visited.add(cur)
            comp.add(cur)
            stack.extend([nb for nb in adj.get(cur, set()) if nb not in visited])

        has_anchor = any(
            str((node_map.get(cid, {}).get("data", {}) or {}).get("node_type", "")) in anchor_types
            for cid in comp
        )
        if has_anchor:
            continue
        for cid in comp:
            ctype = str((node_map.get(cid, {}).get("data", {}) or {}).get("node_type", ""))
            if ctype in {"OfacSdn", "PanamaNode"}:
                remove_ids.add(cid)

    keep_ids -= remove_ids

    nodes_out = [node_map[nid] for nid in keep_ids if nid in node_map]
    edges_out = []
    for e in edges_step1:
        d = e.get("data", {})
        src = str(d.get("source", ""))
        dst = str(d.get("target", ""))
        if src in keep_ids and dst in keep_ids:
            edges_out.append(e)

    out = dict(payload)
    out["elements"] = {"nodes": nodes_out, "edges": edges_out}
    out["node_count"] = len(nodes_out)
    out["edge_count"] = len(edges_out)
    if seed_node_id in keep_ids:
        out["center_node"] = seed_node_id
    elif nodes_out:
        out["center_node"] = str((nodes_out[0].get("data", {}) or {}).get("id", ""))
    else:
        out["center_node"] = ""
    return out


def build_seed_graph_payload(
    db: Session,
    node_id: str,
    hops: int = 2,
    max_nodes: int = 500,
    max_edges: int = 2000,
    include_surrogates: bool = True,
    include_ofac_matches: bool = True,
    include_txn_flow: bool = True,
) -> dict[str, Any]:
    seed = str(node_id or "").strip()
    if not seed:
        raise KeyError("Seed node id is required.")

    full = build_graph_payload(
        db,
        include_surrogates=include_surrogates,
        include_ofac_matches=include_ofac_matches,
        include_txn_flow=include_txn_flow,
    )
    node_ids = {str(n.get("data", {}).get("id", "")) for n in full.get("elements", {}).get("nodes", [])}
    if seed not in node_ids:
        raise KeyError(f"Seed node not found: {seed}")

    adj = _adjacency_from_elements(full.get("elements", {}))
    selected = _bfs_nodes(seed, adj, hops=hops, max_nodes=max_nodes)
    if include_surrogates:
        selected = _add_shared_surrogates_to_selection(full, selected, max_nodes=max_nodes)
    out = _subgraph_payload_from_nodes(full, selected, max_edges=max_edges)
    out = _filter_exposure_subgraph(out, seed)
    out["hops"] = hops
    return out


def build_customer_graph_summary(
    db: Session,
    customer_key: str,
    hops: int = 2,
    include_surrogates: bool = True,
    include_ofac_matches: bool = True,
    include_txn_flow: bool = True,
) -> dict[str, Any]:
    payload = build_customer_graph_payload(
        db,
        customer_key=customer_key,
        hops=hops,
        include_surrogates=include_surrogates,
        include_ofac_matches=include_ofac_matches,
        include_txn_flow=include_txn_flow,
    )

    node_data = [n.get("data", {}) for n in payload.get("elements", {}).get("nodes", [])]
    edge_data = [e.get("data", {}) for e in payload.get("elements", {}).get("edges", [])]

    accounts = {n.get("id") for n in node_data if n.get("node_type") == "Account"}
    counterparties = {n.get("id") for n in node_data if n.get("node_type") == "CounterpartyAccount"}
    ofac_nodes = {n.get("id") for n in node_data if n.get("node_type") == "OfacSdn"}
    panama_nodes = {n.get("id") for n in node_data if n.get("node_type") == "PanamaNode"}

    ofac_edges = [e for e in edge_data if e.get("edge_type") == "POTENTIAL_OFAC_MATCH"]
    flow_edges = [e for e in edge_data if str(e.get("edge_type", "")).startswith("TXN_FLOW")]
    total_txn_flow_amount = float(sum(float(e.get("total_amount") or 0.0) for e in flow_edges))
    total_txn_flow_count = int(sum(int(e.get("txn_count") or 0) for e in flow_edges))

    return {
        "snapshot_id": payload.get("snapshot_id"),
        "as_of_ts": payload.get("as_of_ts"),
        "customer_key": customer_key,
        "hops": hops,
        "node_count": payload.get("node_count", 0),
        "edge_count": payload.get("edge_count", 0),
        "connected_account_count": len(accounts),
        "connected_counterparty_count": len(counterparties),
        "ofac_node_count": len(ofac_nodes),
        "ofac_match_edge_count": len(ofac_edges),
        "panama_node_count": len(panama_nodes),
        "txn_flow_edge_count": len(flow_edges),
        "total_txn_flow_amount": total_txn_flow_amount,
        "total_txn_flow_count": total_txn_flow_count,
    }


def build_node_neighbors_payload(
    db: Session,
    node_id: str,
    limit: int = 50,
    offset: int = 0,
    exclude_node_ids: set[str] | None = None,
    include_surrogates: bool = True,
    include_ofac_matches: bool = True,
    include_txn_flow: bool = True,
) -> dict[str, Any]:
    payload = build_graph_payload(
        db,
        include_surrogates=include_surrogates,
        include_ofac_matches=include_ofac_matches,
        include_txn_flow=include_txn_flow,
    )
    elements = payload.get("elements", {})
    node_map: dict[str, dict[str, Any]] = {}
    for n in elements.get("nodes", []):
        data = n.get("data", {})
        nid = str(data.get("id", ""))
        if nid:
            node_map[nid] = n

    if node_id not in node_map:
        raise KeyError(f"Node not found: {node_id}")

    edge_rows = elements.get("edges", [])
    neighbor_ids: set[str] = set()
    for e in edge_rows:
        d = e.get("data", {})
        src = str(d.get("source", ""))
        dst = str(d.get("target", ""))
        if src == node_id and dst:
            neighbor_ids.add(dst)
        elif dst == node_id and src:
            neighbor_ids.add(src)

    excludes = {str(v) for v in (exclude_node_ids or set()) if str(v)}
    excludes.add(node_id)
    available = sorted([nid for nid in neighbor_ids if nid not in excludes])
    total_neighbor_count = len(neighbor_ids)

    safe_offset = max(0, int(offset))
    safe_limit = max(1, int(limit))
    selected_neighbor_ids = available[safe_offset : safe_offset + safe_limit]
    remaining_neighbor_count = max(0, len(available) - (safe_offset + len(selected_neighbor_ids)))

    selected_node_ids = {node_id, *selected_neighbor_ids}
    out_nodes = [node_map[nid] for nid in selected_node_ids if nid in node_map]

    out_edges: list[dict[str, Any]] = []
    selected_neighbors_set = set(selected_neighbor_ids)
    for e in edge_rows:
        d = e.get("data", {})
        src = str(d.get("source", ""))
        dst = str(d.get("target", ""))
        if (src == node_id and dst in selected_neighbors_set) or (dst == node_id and src in selected_neighbors_set):
            out_edges.append(e)

    return {
        "snapshot_id": payload.get("snapshot_id"),
        "as_of_ts": payload.get("as_of_ts"),
        "node_id": node_id,
        "total_neighbor_count": total_neighbor_count,
        "available_neighbor_count": len(available),
        "returned_neighbor_count": len(selected_neighbor_ids),
        "remaining_neighbor_count": remaining_neighbor_count,
        "elements": {
            "nodes": out_nodes,
            "edges": out_edges,
        },
    }




def _node_ids_within_hops(payload: dict[str, Any], seed_node_id: str, hops: int) -> set[str]:
    elements = payload.get("elements") if isinstance(payload, dict) else None
    nodes = elements.get("nodes") if isinstance(elements, dict) else []
    edges = elements.get("edges") if isinstance(elements, dict) else []
    node_ids = {
        str((n.get("data", {}) or {}).get("id", ""))
        for n in (nodes if isinstance(nodes, list) else [])
        if isinstance(n, dict)
    }
    if seed_node_id not in node_ids:
        return set()

    adj: dict[str, set[str]] = {nid: set() for nid in node_ids}
    for e in (edges if isinstance(edges, list) else []):
        if not isinstance(e, dict):
            continue
        d = e.get("data")
        if not isinstance(d, dict):
            continue
        src = str(d.get("source") or "")
        dst = str(d.get("target") or "")
        if src in adj and dst in adj:
            adj[src].add(dst)
            adj[dst].add(src)

    max_hops = max(0, int(hops))
    visited: set[str] = {seed_node_id}
    frontier: set[str] = {seed_node_id}
    for _ in range(max_hops):
        nxt: set[str] = set()
        for cur in frontier:
            for nb in adj.get(cur, set()):
                if nb in visited:
                    continue
                visited.add(nb)
                nxt.add(nb)
        if not nxt:
            break
        frontier = nxt
    return visited


def build_transaction_filter_catalog(db: Session) -> dict[str, Any]:
    meta = _transaction_type_metadata(db)
    directions = sorted({str(v.get("direction") or "").strip() for v in meta.values() if str(v.get("direction") or "").strip()})
    mechanisms = sorted({str(v.get("mechanism") or "").strip() for v in meta.values() if str(v.get("mechanism") or "").strip()})
    aml_classifications = sorted({str(v.get("aml_classification") or "").strip() for v in meta.values() if str(v.get("aml_classification") or "").strip()})
    aml_sub_classifications = sorted({str(v.get("aml_sub_classification") or "").strip() for v in meta.values() if str(v.get("aml_sub_classification") or "").strip()})

    country_codes_2 = sorted(
        {
            str(r[0]).strip().upper()
            for r in db.execute(select(DHDimCountry.country_code_2).where(DHDimCountry.is_current.is_(True))).all()
            if str(r[0] or "").strip()
        }
    )
    counterparty_jurisdictions = sorted(
        {
            str(_attrs_json(row).get("jurisdiction") or "").strip().upper()
            for row in _is_current_rows(db, DHDimCounterpartyAccount)
            if str(_attrs_json(row).get("jurisdiction") or "").strip()
        }
    )
    customer_country_codes = sorted(
        {
            str(_attrs_json(row).get("address_country_code") or "").strip().upper()
            for row in _is_current_rows(db, DHDimCustomer)
            if str(_attrs_json(row).get("address_country_code") or "").strip()
        }
    )
    branch_country_codes = sorted(
        {
            str(_attrs_json(row).get("address_country_code") or "").strip().upper()
            for row in _is_current_rows(db, DHDimBranch)
            if str(_attrs_json(row).get("address_country_code") or "").strip()
        }
    )

    return {
        "directions": directions,
        "mechanisms": mechanisms,
        "aml_classifications": aml_classifications,
        "aml_sub_classifications": aml_sub_classifications,
        "country_codes_2": country_codes_2,
        "counterparty_jurisdictions": counterparty_jurisdictions,
        "customer_country_codes": customer_country_codes,
        "branch_country_codes": branch_country_codes,
        "transaction_type_count": len(meta),
    }


def _transaction_type_metadata(db: Session) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in _is_current_rows(db, DHDimTransactionType):
        attrs = _attrs_json(row)
        out[str(row.transaction_type_code)] = {
            "aml_classification": str(attrs.get("aml_classification") or "").strip(),
            "aml_sub_classification": str(attrs.get("aml_sub_classification") or "").strip(),
            "direction": str(attrs.get("direction") or "").strip(),
            "mechanism": str(attrs.get("mechanism") or "").strip(),
        }
    return out


def build_exposure_cash_transactions(
    db: Session,
    node_id: str,
    hops: int = 2,
    limit: int = 500,
    outside_country_code_2: str | None = None,
    outside_counterparty_jurisdiction: str | None = None,
    counterparty_jurisdiction: str | None = None,
    outside_customer_country_code: str | None = None,
    customer_country_code: str | None = None,
    outside_branch_country_code: str | None = None,
    branch_country_code: str | None = None,
    account_type_contains: str | None = None,
    account_name_contains: str | None = None,
    customer_segment_contains: str | None = None,
    customer_business_unit: str | None = None,
    branch_type_contains: str | None = None,
    direction: str | None = None,
    aml_classification_contains: str | None = None,
    mechanism_contains: str | None = None,
    include_surrogates: bool = True,
    include_ofac_matches: bool = True,
    include_txn_flow: bool = True,
) -> dict[str, Any]:
    seed = str(node_id or "").strip()
    if not seed:
        raise KeyError("Seed node id is required.")

    graph_payload = build_seed_graph_payload(
        db,
        node_id=seed,
        hops=hops,
        max_nodes=2000,
        max_edges=8000,
        include_surrogates=include_surrogates,
        include_ofac_matches=include_ofac_matches,
        include_txn_flow=include_txn_flow,
    )

    elements = graph_payload.get("elements") if isinstance(graph_payload, dict) else None
    nodes = elements.get("nodes") if isinstance(elements, dict) else []
    nearby_node_ids = _node_ids_within_hops(graph_payload, seed, hops=max(0, int(hops)))

    account_keys: set[str] = set()
    counterparty_keys: set[str] = set()
    for node in (nodes if isinstance(nodes, list) else []):
        if not isinstance(node, dict):
            continue
        data = node.get("data")
        if not isinstance(data, dict):
            continue
        nid = str(data.get("id") or "")
        if nid not in nearby_node_ids:
            continue
        node_type = str(data.get("node_type") or "")
        business_key = str(data.get("business_key") or "")
        if node_type == "Account" and business_key:
            account_keys.add(business_key)
        if node_type == "CounterpartyAccount" and business_key:
            counterparty_keys.add(business_key)

    if not account_keys and not counterparty_keys:
        return {
            "seed_node_id": seed,
            "hops": int(hops),
            "row_count": 0,
            "summary": {
                "total_amount": 0.0,
                "distinct_account_count": 0,
                "distinct_counterparty_count": 0,
                "outside_country_code_2": (outside_country_code_2 or "").strip().upper() or None,
                "outside_counterparty_jurisdiction": (outside_counterparty_jurisdiction or "").strip().upper() or None,
                "counterparty_jurisdiction": (counterparty_jurisdiction or "").strip().upper() or None,
                "outside_customer_country_code": (outside_customer_country_code or "").strip().upper() or None,
                "customer_country_code": (customer_country_code or "").strip().upper() or None,
                "outside_branch_country_code": (outside_branch_country_code or "").strip().upper() or None,
                "branch_country_code": (branch_country_code or "").strip().upper() or None,
                "direction_filter": (direction or "").strip().lower() or None,
                "aml_classification_contains": (aml_classification_contains or "").strip() or None,
                "mechanism_contains": (mechanism_contains or "").strip() or None,
                "account_type_contains": (account_type_contains or "").strip() or None,
                "account_name_contains": (account_name_contains or "").strip() or None,
                "customer_segment_contains": (customer_segment_contains or "").strip() or None,
                "customer_business_unit": (customer_business_unit or "").strip() or None,
                "branch_type_contains": (branch_type_contains or "").strip() or None,
            },
            "rows": [],
        }

    q = select(
        DHFactCash.transaction_key,
        DHFactCash.transaction_ts,
        DHFactCash.account_key,
        DHFactCash.transaction_type_code,
        DHFactCash.amount,
        DHFactCash.country_code_2,
        DHFactCash.currency_code,
        DHFactCash.counterparty_account_key,
        DHFactCash.branch_key,
    )

    conditions = []
    if account_keys and counterparty_keys:
        conditions.append(
            (DHFactCash.account_key.in_(sorted(account_keys)))
            | (DHFactCash.counterparty_account_key.in_(sorted(counterparty_keys)))
        )
    elif account_keys:
        conditions.append(DHFactCash.account_key.in_(sorted(account_keys)))
    else:
        conditions.append(DHFactCash.counterparty_account_key.in_(sorted(counterparty_keys)))

    country_ex = (outside_country_code_2 or "").strip().upper()
    if country_ex:
        conditions.append(func.upper(func.coalesce(DHFactCash.country_code_2, "")) != country_ex)

    for cond in conditions:
        q = q.where(cond)

    safe_limit = max(1, min(int(limit), 10000))
    fact_rows = db.execute(
        q.order_by(DHFactCash.transaction_ts.desc(), DHFactCash.transaction_key.desc()).limit(safe_limit)
    ).all()

    txn_type_meta = _transaction_type_metadata(db)
    fact_account_keys = {str(row.account_key or "") for row in fact_rows if str(row.account_key or "")}
    fact_counterparty_keys = {str(row.counterparty_account_key or "") for row in fact_rows if str(row.counterparty_account_key or "")}
    fact_branch_keys = {str(row.branch_key or "") for row in fact_rows if str(row.branch_key or "")}

    account_attr_map: dict[str, dict[str, Any]] = {}
    if fact_account_keys:
        for row in db.execute(
            select(DHDimAccount).where(
                DHDimAccount.account_key.in_(sorted(fact_account_keys)),
                DHDimAccount.is_current.is_(True),
            )
        ).scalars().all():
            account_attr_map[str(row.account_key)] = _attrs_json(row)

    counterparty_attr_map: dict[str, dict[str, Any]] = {}
    if fact_counterparty_keys:
        for row in db.execute(
            select(DHDimCounterpartyAccount).where(
                DHDimCounterpartyAccount.counterparty_account_key.in_(sorted(fact_counterparty_keys)),
                DHDimCounterpartyAccount.is_current.is_(True),
            )
        ).scalars().all():
            counterparty_attr_map[str(row.counterparty_account_key)] = _attrs_json(row)

    branch_attr_map: dict[str, dict[str, Any]] = {}
    if fact_branch_keys:
        for row in db.execute(
            select(DHDimBranch).where(
                DHDimBranch.branch_key.in_(sorted(fact_branch_keys)),
                DHDimBranch.is_current.is_(True),
            )
        ).scalars().all():
            branch_attr_map[str(row.branch_key)] = _attrs_json(row)

    account_customer_keys: dict[str, set[str]] = {}
    if fact_account_keys:
        bridge_rows = db.execute(
            select(DHBridgeCustomerAccount).where(
                DHBridgeCustomerAccount.account_key.in_(sorted(fact_account_keys)),
                DHBridgeCustomerAccount.is_current.is_(True),
            )
        ).scalars().all()
        customer_keys_for_accounts = {str(row.customer_key) for row in bridge_rows if str(row.customer_key)}
        for row in bridge_rows:
            account_customer_keys.setdefault(str(row.account_key), set()).add(str(row.customer_key))
    else:
        customer_keys_for_accounts = set()

    customer_attr_map: dict[str, dict[str, Any]] = {}
    customer_business_unit_map: dict[str, str] = {}
    if customer_keys_for_accounts:
        for row in db.execute(
            select(DHDimCustomer).where(
                DHDimCustomer.customer_key.in_(sorted(customer_keys_for_accounts)),
                DHDimCustomer.is_current.is_(True),
            )
        ).scalars().all():
            customer_attr_map[str(row.customer_key)] = _attrs_json(row)
            customer_business_unit_map[str(row.customer_key)] = str(row.business_unit or "")

    direction_filter = (direction or "").strip().lower()
    aml_contains = (aml_classification_contains or "").strip().lower()
    mechanism_filter = (mechanism_contains or "").strip().lower()
    cp_jurisdiction_ex = (outside_counterparty_jurisdiction or "").strip().upper()
    cp_jurisdiction_eq = (counterparty_jurisdiction or "").strip().upper()
    customer_country_ex = (outside_customer_country_code or "").strip().upper()
    customer_country_eq = (customer_country_code or "").strip().upper()
    branch_country_ex = (outside_branch_country_code or "").strip().upper()
    branch_country_eq = (branch_country_code or "").strip().upper()
    account_type_filter = (account_type_contains or "").strip().lower()
    account_name_filter = (account_name_contains or "").strip().lower()
    customer_segment_filter = (customer_segment_contains or "").strip().lower()
    customer_bu_filter = (customer_business_unit or "").strip().lower()
    branch_type_filter = (branch_type_contains or "").strip().lower()

    filtered_rows = []
    for row in fact_rows:
        ttc = str(row.transaction_type_code or "")
        meta = txn_type_meta.get(ttc, {})
        acct_key = str(row.account_key or "")
        cp_key = str(row.counterparty_account_key or "")
        branch_key = str(row.branch_key or "")
        acct_attrs = account_attr_map.get(acct_key, {})
        cp_attrs = counterparty_attr_map.get(cp_key, {})
        branch_attrs = branch_attr_map.get(branch_key, {})
        customer_keys = account_customer_keys.get(acct_key, set())
        customer_attrs = [customer_attr_map.get(k, {}) for k in customer_keys]
        dir_val = str(meta.get("direction") or "").strip().lower()
        aml_val = str(meta.get("aml_classification") or "").strip()
        mechanism_val = str(meta.get("mechanism") or "").strip()
        cp_jurisdiction = str(cp_attrs.get("jurisdiction") or "").strip().upper()
        customer_countries = {
            str(attrs.get("address_country_code") or "").strip().upper()
            for attrs in customer_attrs
            if str(attrs.get("address_country_code") or "").strip()
        }
        customer_segments = {
            str(attrs.get("segment") or attrs.get("customer_segment") or "").strip().lower()
            for attrs in customer_attrs
            if str(attrs.get("segment") or attrs.get("customer_segment") or "").strip()
        }
        customer_bus = {
            customer_business_unit_map.get(k, "").strip().lower()
            for k in customer_keys
            if customer_business_unit_map.get(k, "").strip()
        }
        branch_country = str(branch_attrs.get("address_country_code") or "").strip().upper()
        if direction_filter and dir_val != direction_filter:
            continue
        if aml_contains and aml_contains not in aml_val.lower():
            continue
        if mechanism_filter and mechanism_filter not in mechanism_val.lower():
            continue
        if cp_jurisdiction_ex and (not cp_jurisdiction or cp_jurisdiction == cp_jurisdiction_ex):
            continue
        if cp_jurisdiction_eq and cp_jurisdiction != cp_jurisdiction_eq:
            continue
        if customer_country_ex and (not customer_countries or customer_country_ex in customer_countries):
            continue
        if customer_country_eq and customer_country_eq not in customer_countries:
            continue
        if branch_country_ex and (not branch_country or branch_country == branch_country_ex):
            continue
        if branch_country_eq and branch_country != branch_country_eq:
            continue
        if account_type_filter and account_type_filter not in str(acct_attrs.get("account_type") or "").lower():
            continue
        if account_name_filter and account_name_filter not in str(acct_attrs.get("account_name") or "").lower():
            continue
        if customer_segment_filter and not any(customer_segment_filter in v for v in customer_segments):
            continue
        if customer_bu_filter and customer_bu_filter not in customer_bus:
            continue
        if branch_type_filter and branch_type_filter not in str(branch_attrs.get("branch_type") or "").lower():
            continue
        filtered_rows.append((row, meta))

    cp_name_map: dict[str, str] = {}
    for cp_key, attrs in counterparty_attr_map.items():
        cp_name_map[cp_key] = str(attrs.get("counterparty_name") or cp_key)

    out_rows: list[dict[str, Any]] = []
    total_amount = 0.0
    out_account_keys: set[str] = set()
    out_counterparty_keys: set[str] = set()
    countries: dict[str, int] = {}
    counterparty_jurisdictions: dict[str, int] = {}
    customer_countries_out: dict[str, int] = {}
    branch_countries: dict[str, int] = {}
    for row, meta in filtered_rows:
        cp_key = str(row.counterparty_account_key or "")
        acct_key = str(row.account_key or "")
        branch_key = str(row.branch_key or "")
        acct_attrs = account_attr_map.get(acct_key, {})
        cp_attrs = counterparty_attr_map.get(cp_key, {})
        branch_attrs = branch_attr_map.get(branch_key, {})
        customer_keys = sorted(account_customer_keys.get(acct_key, set()))
        customer_attrs_for_row = [customer_attr_map.get(k, {}) for k in customer_keys]
        row_customer_countries = sorted(
            {
                str(attrs.get("address_country_code") or "").strip().upper()
                for attrs in customer_attrs_for_row
                if str(attrs.get("address_country_code") or "").strip()
            }
        )
        row_customer_segments = sorted(
            {
                str(attrs.get("segment") or attrs.get("customer_segment") or "").strip()
                for attrs in customer_attrs_for_row
                if str(attrs.get("segment") or attrs.get("customer_segment") or "").strip()
            }
        )
        ccy = str(row.currency_code or "")
        ctry = str(row.country_code_2 or "")
        cp_jurisdiction = str(cp_attrs.get("jurisdiction") or "").strip().upper()
        branch_country = str(branch_attrs.get("address_country_code") or "").strip().upper()
        amt = float(row.amount or 0.0)
        total_amount += amt
        if acct_key:
            out_account_keys.add(acct_key)
        if cp_key:
            out_counterparty_keys.add(cp_key)
        if ctry:
            countries[ctry] = countries.get(ctry, 0) + 1
        if cp_jurisdiction:
            counterparty_jurisdictions[cp_jurisdiction] = counterparty_jurisdictions.get(cp_jurisdiction, 0) + 1
        for customer_country in row_customer_countries:
            customer_countries_out[customer_country] = customer_countries_out.get(customer_country, 0) + 1
        if branch_country:
            branch_countries[branch_country] = branch_countries.get(branch_country, 0) + 1
        out_rows.append(
            {
                "transaction_key": str(row.transaction_key or ""),
                "transaction_date": row.transaction_ts.date().isoformat() if row.transaction_ts else None,
                "transaction_ts": row.transaction_ts.isoformat() if row.transaction_ts else None,
                "account_key": acct_key,
                "account_name": str(acct_attrs.get("account_name") or ""),
                "account_type": str(acct_attrs.get("account_type") or ""),
                "customer_keys": customer_keys,
                "customer_country_codes": row_customer_countries,
                "customer_segments": row_customer_segments,
                "customer_business_units": sorted(
                    {customer_business_unit_map.get(k, "") for k in customer_keys if customer_business_unit_map.get(k, "")}
                ),
                "branch_key": branch_key,
                "branch_type": str(branch_attrs.get("branch_type") or ""),
                "branch_country_code": branch_country,
                "counterparty_account_key": cp_key,
                "counterparty_name": cp_name_map.get(cp_key, cp_key),
                "counterparty_jurisdiction": cp_jurisdiction,
                "transaction_type_code": str(row.transaction_type_code or ""),
                "aml_classification": str(meta.get("aml_classification") or "Unknown"),
                "aml_sub_classification": str(meta.get("aml_sub_classification") or ""),
                "direction": str(meta.get("direction") or "unknown"),
                "mechanism": str(meta.get("mechanism") or ""),
                "amount": amt,
                "currency_code": ccy,
                "country_code_2": ctry,
                "matched_by": (
                    "account"
                    if acct_key in account_keys and cp_key not in counterparty_keys
                    else "counterparty"
                    if cp_key in counterparty_keys and acct_key not in account_keys
                    else "account_or_counterparty"
                ),
            }
        )

    top_countries = sorted(countries.items(), key=lambda item: item[1], reverse=True)[:10]
    top_counterparty_jurisdictions = sorted(counterparty_jurisdictions.items(), key=lambda item: item[1], reverse=True)[:10]
    top_customer_countries = sorted(customer_countries_out.items(), key=lambda item: item[1], reverse=True)[:10]
    top_branch_countries = sorted(branch_countries.items(), key=lambda item: item[1], reverse=True)[:10]

    return {
        "seed_node_id": seed,
        "hops": int(hops),
        "row_count": len(out_rows),
        "summary": {
            "total_amount": total_amount,
            "distinct_account_count": len(out_account_keys),
            "distinct_counterparty_count": len(out_counterparty_keys),
            "top_countries": [{"country_code_2": c, "txn_count": n} for c, n in top_countries],
            "top_counterparty_jurisdictions": [
                {"jurisdiction": c, "txn_count": n} for c, n in top_counterparty_jurisdictions
            ],
            "top_customer_countries": [{"country_code": c, "txn_count": n} for c, n in top_customer_countries],
            "top_branch_countries": [{"country_code": c, "txn_count": n} for c, n in top_branch_countries],
            "outside_country_code_2": country_ex or None,
            "outside_counterparty_jurisdiction": cp_jurisdiction_ex or None,
            "counterparty_jurisdiction": cp_jurisdiction_eq or None,
            "outside_customer_country_code": customer_country_ex or None,
            "customer_country_code": customer_country_eq or None,
            "outside_branch_country_code": branch_country_ex or None,
            "branch_country_code": branch_country_eq or None,
            "direction_filter": direction_filter or None,
            "aml_classification_contains": aml_contains or None,
            "mechanism_contains": mechanism_filter or None,
            "account_type_contains": account_type_filter or None,
            "account_name_contains": account_name_filter or None,
            "customer_segment_contains": customer_segment_filter or None,
            "customer_business_unit": customer_bu_filter or None,
            "branch_type_contains": branch_type_filter or None,
        },
        "rows": out_rows,
    }


def build_global_cash_transactions(
    db: Session,
    limit: int = 10000,
    counterparty_jurisdiction: str | None = None,
    outside_counterparty_jurisdiction: str | None = None,
    outside_country_code_2: str | None = None,
    direction: str | None = None,
    aml_classification_contains: str | None = None,
    mechanism_contains: str | None = None,
) -> dict[str, Any]:
    safe_limit = max(1, min(int(limit), 10000))
    fact_rows = db.execute(
        select(
            DHFactCash.transaction_key,
            DHFactCash.transaction_ts,
            DHFactCash.account_key,
            DHFactCash.transaction_type_code,
            DHFactCash.amount,
            DHFactCash.country_code_2,
            DHFactCash.currency_code,
            DHFactCash.counterparty_account_key,
            DHFactCash.branch_key,
        )
        .order_by(DHFactCash.transaction_ts.desc(), DHFactCash.transaction_key.desc())
        .limit(safe_limit)
    ).all()

    txn_type_meta = _transaction_type_metadata(db)
    fact_counterparty_keys = {str(row.counterparty_account_key or "") for row in fact_rows if str(row.counterparty_account_key or "")}
    counterparty_attr_map: dict[str, dict[str, Any]] = {}
    if fact_counterparty_keys:
        for row in db.execute(
            select(DHDimCounterpartyAccount).where(
                DHDimCounterpartyAccount.counterparty_account_key.in_(sorted(fact_counterparty_keys)),
                DHDimCounterpartyAccount.is_current.is_(True),
            )
        ).scalars().all():
            counterparty_attr_map[str(row.counterparty_account_key)] = _attrs_json(row)

    direction_filter = (direction or "").strip().lower()
    aml_contains = (aml_classification_contains or "").strip().lower()
    mechanism_filter = (mechanism_contains or "").strip().lower()
    cp_jurisdiction_eq = (counterparty_jurisdiction or "").strip().upper()
    cp_jurisdiction_ex = (outside_counterparty_jurisdiction or "").strip().upper()
    country_ex = (outside_country_code_2 or "").strip().upper()

    out_rows: list[dict[str, Any]] = []
    total_amount = 0.0
    countries: dict[str, int] = {}
    counterparty_jurisdictions: dict[str, int] = {}
    for row in fact_rows:
        meta = txn_type_meta.get(str(row.transaction_type_code or ""), {})
        dir_val = str(meta.get("direction") or "").strip().lower()
        aml_val = str(meta.get("aml_classification") or "").strip()
        mechanism_val = str(meta.get("mechanism") or "").strip()
        cp_key = str(row.counterparty_account_key or "")
        cp_attrs = counterparty_attr_map.get(cp_key, {})
        cp_jurisdiction = str(cp_attrs.get("jurisdiction") or "").strip().upper()
        ctry = str(row.country_code_2 or "").strip().upper()

        if direction_filter and dir_val != direction_filter:
            continue
        if aml_contains and aml_contains not in aml_val.lower():
            continue
        if mechanism_filter and mechanism_filter not in mechanism_val.lower():
            continue
        if country_ex and ctry == country_ex:
            continue
        if cp_jurisdiction_eq and cp_jurisdiction != cp_jurisdiction_eq:
            continue
        if cp_jurisdiction_ex and (not cp_jurisdiction or cp_jurisdiction == cp_jurisdiction_ex):
            continue

        amt = float(row.amount or 0.0)
        total_amount += amt
        if ctry:
            countries[ctry] = countries.get(ctry, 0) + 1
        if cp_jurisdiction:
            counterparty_jurisdictions[cp_jurisdiction] = counterparty_jurisdictions.get(cp_jurisdiction, 0) + 1
        out_rows.append(
            {
                "transaction_key": str(row.transaction_key or ""),
                "transaction_date": row.transaction_ts.date().isoformat() if row.transaction_ts else None,
                "transaction_ts": row.transaction_ts.isoformat() if row.transaction_ts else None,
                "account_key": str(row.account_key or ""),
                "counterparty_account_key": cp_key,
                "counterparty_name": str(cp_attrs.get("counterparty_name") or cp_key),
                "counterparty_jurisdiction": cp_jurisdiction,
                "transaction_type_code": str(row.transaction_type_code or ""),
                "aml_classification": str(meta.get("aml_classification") or "Unknown"),
                "aml_sub_classification": str(meta.get("aml_sub_classification") or ""),
                "direction": str(meta.get("direction") or "unknown"),
                "mechanism": str(meta.get("mechanism") or ""),
                "amount": amt,
                "currency_code": str(row.currency_code or ""),
                "country_code_2": ctry,
                "matched_by": "global_filter",
            }
        )

    top_countries = sorted(countries.items(), key=lambda item: item[1], reverse=True)[:10]
    top_counterparty_jurisdictions = sorted(counterparty_jurisdictions.items(), key=lambda item: item[1], reverse=True)[:10]
    return {
        "seed_node_id": None,
        "hops": None,
        "row_count": len(out_rows),
        "summary": {
            "total_amount": total_amount,
            "distinct_account_count": len({row["account_key"] for row in out_rows if row.get("account_key")}),
            "distinct_counterparty_count": len(
                {row["counterparty_account_key"] for row in out_rows if row.get("counterparty_account_key")}
            ),
            "top_countries": [{"country_code_2": c, "txn_count": n} for c, n in top_countries],
            "top_counterparty_jurisdictions": [
                {"jurisdiction": c, "txn_count": n} for c, n in top_counterparty_jurisdictions
            ],
            "counterparty_jurisdiction": cp_jurisdiction_eq or None,
            "outside_counterparty_jurisdiction": cp_jurisdiction_ex or None,
            "outside_country_code_2": country_ex or None,
            "direction_filter": direction_filter or None,
            "aml_classification_contains": aml_contains or None,
            "mechanism_contains": mechanism_filter or None,
        },
        "rows": out_rows,
    }
def build_customer_primary_account_transactions(
    db: Session,
    customer_key: str,
    limit: int = 5000,
) -> dict[str, Any]:
    customer_key_norm = str(customer_key or "").strip()
    if not customer_key_norm:
        raise KeyError("Customer key is required.")

    customer_exists = (
        db.execute(
            select(DHDimCustomer.customer_key).where(
                DHDimCustomer.customer_key == customer_key_norm,
                DHDimCustomer.is_current.is_(True),
            )
        ).first()
        is not None
    )
    if not customer_exists:
        raise KeyError(f"Customer not found: {customer_key_norm}")

    primary_accounts = {
        str(r[0])
        for r in db.execute(
            select(DHBridgeCustomerAccount.account_key).where(
                DHBridgeCustomerAccount.customer_key == customer_key_norm,
                DHBridgeCustomerAccount.is_current.is_(True),
                func.lower(func.coalesce(DHBridgeCustomerAccount.relationship_type, "")) == "primary",
            )
        ).all()
        if str(r[0] or "").strip()
    }
    if not primary_accounts:
        return {
            "customer_key": customer_key_norm,
            "relationship_type_filter": "primary",
            "row_count": 0,
            "rows": [],
        }

    safe_limit = max(1, min(int(limit), 50000))
    fact_rows = db.execute(
        select(
            DHFactCash.transaction_key,
            DHFactCash.transaction_ts,
            DHFactCash.account_key,
            DHFactCash.transaction_type_code,
            DHFactCash.amount,
            DHFactCash.country_code_2,
            DHFactCash.currency_code,
            DHFactCash.counterparty_account_key,
        )
        .where(DHFactCash.account_key.in_(sorted(primary_accounts)))
        .order_by(DHFactCash.transaction_ts.desc(), DHFactCash.transaction_key.desc())
        .limit(safe_limit)
    ).all()
    if not fact_rows:
        return {
            "customer_key": customer_key_norm,
            "relationship_type_filter": "primary",
            "row_count": 0,
            "rows": [],
        }

    txn_type_codes = {str(r.transaction_type_code) for r in fact_rows if r.transaction_type_code}
    counterparty_keys = {str(r.counterparty_account_key) for r in fact_rows if r.counterparty_account_key}

    txn_type_map: dict[str, dict[str, Any]] = {}
    if txn_type_codes:
        for row in db.execute(
            select(DHDimTransactionType).where(
                DHDimTransactionType.transaction_type_code.in_(sorted(txn_type_codes)),
                DHDimTransactionType.is_current.is_(True),
            )
        ).scalars().all():
            attrs = _attrs_json(row)
            txn_type_map[str(row.transaction_type_code)] = {
                "aml_classification": str(attrs.get("aml_classification") or "Unknown"),
                "direction": str(attrs.get("direction") or "unknown"),
            }

    counterparty_name_map: dict[str, str] = {}
    if counterparty_keys:
        for row in db.execute(
            select(DHDimCounterpartyAccount).where(
                DHDimCounterpartyAccount.counterparty_account_key.in_(sorted(counterparty_keys)),
                DHDimCounterpartyAccount.is_current.is_(True),
            )
        ).scalars().all():
            attrs = _attrs_json(row)
            counterparty_name_map[str(row.counterparty_account_key)] = str(
                attrs.get("counterparty_name") or row.counterparty_account_key
            )

    out_rows: list[dict[str, Any]] = []
    for row in fact_rows:
        ttc = str(row.transaction_type_code or "")
        cp_key = str(row.counterparty_account_key or "")
        txn_meta = txn_type_map.get(ttc, {})
        ts_val = row.transaction_ts.isoformat() if row.transaction_ts else None
        out_rows.append(
            {
                "transaction_key": str(row.transaction_key or ""),
                "transaction_date": row.transaction_ts.date().isoformat() if row.transaction_ts else None,
                "transaction_ts": ts_val,
                "account_key": str(row.account_key or ""),
                "aml_classification": str(txn_meta.get("aml_classification") or "Unknown"),
                "direction": str(txn_meta.get("direction") or "unknown"),
                "amount": float(row.amount or 0.0),
                "country_code_2": str(row.country_code_2 or ""),
                "currency_code": str(row.currency_code or ""),
                "counterparty_name": counterparty_name_map.get(cp_key, cp_key),
            }
        )

    return {
        "customer_key": customer_key_norm,
        "relationship_type_filter": "primary",
        "row_count": len(out_rows),
        "rows": out_rows,
    }
