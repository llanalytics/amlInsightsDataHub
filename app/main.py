from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.config import JOB_PAGE_SIZE
from app.database import get_db
from app.graph_layer import (
    build_customer_primary_account_transactions,
    build_customer_graph_payload,
    build_customer_graph_summary,
    build_graph_payload,
    build_node_neighbors_payload,
    build_seed_graph_payload,
    search_exposure_seeds,
    search_customer_seeds,
)
from app.init_db import init_db
from app.models import DHDQResult, DHDQRule, DHJobFileStat, DHJobRun
from app.pipeline import run_cash_pipeline


app = FastAPI(title="AML Data Hub")
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
def _startup() -> None:
    init_db()


@app.get("/health")
def health() -> dict:
    return {"ok": True, "service": "data-hub"}


@app.get("/api/jobs/runs")
def list_runs(limit: int = JOB_PAGE_SIZE, db: Session = Depends(get_db)) -> list[dict]:
    rows = db.execute(select(DHJobRun).order_by(desc(DHJobRun.started_at)).limit(limit)).scalars().all()
    return [
        {
            "job_run_id": r.job_run_id,
            "job_name": r.job_name,
            "status": r.status,
            "started_at": r.started_at.isoformat(),
            "ended_at": r.ended_at.isoformat() if r.ended_at else None,
            "files_seen": r.files_seen,
            "files_processed": r.files_processed,
            "records_read": r.records_read,
            "records_loaded": r.records_loaded,
            "records_rejected": r.records_rejected,
            "notes": r.notes,
        }
        for r in rows
    ]


@app.get("/api/jobs/batch-results")
def list_batch_results(limit: int = JOB_PAGE_SIZE, db: Session = Depends(get_db)) -> list[dict]:
    runs = db.execute(select(DHJobRun).order_by(desc(DHJobRun.started_at)).limit(limit)).scalars().all()

    run_ids = [r.job_run_id for r in runs]
    file_stats_by_run: dict[str, list[dict]] = {rid: [] for rid in run_ids}
    dq_counts_by_run: dict[str, int] = {rid: 0 for rid in run_ids}

    if run_ids:
        file_rows = db.execute(
            select(DHJobFileStat).where(DHJobFileStat.job_run_id.in_(run_ids)).order_by(desc(DHJobFileStat.processed_at))
        ).scalars().all()
        for f in file_rows:
            file_stats_by_run[f.job_run_id].append(
                {
                    "run_file_key": f.run_file_key,
                    "input_file_name": f.input_file_name,
                    "records_read": f.records_read,
                    "records_loaded": f.records_loaded,
                    "records_rejected": f.records_rejected,
                    "processed_at": f.processed_at.isoformat(),
                }
            )

        dq_count_rows = db.execute(
            select(DHDQResult.job_run_id, func.count().label("violations"))
            .where(DHDQResult.job_run_id.in_(run_ids))
            .group_by(DHDQResult.job_run_id)
        ).all()
        for row in dq_count_rows:
            dq_counts_by_run[row.job_run_id] = int(row.violations)

    return [
        {
            "job_run_id": r.job_run_id,
            "job_name": r.job_name,
            "status": r.status,
            "started_at": r.started_at.isoformat(),
            "ended_at": r.ended_at.isoformat() if r.ended_at else None,
            "files_seen": r.files_seen,
            "files_processed": r.files_processed,
            "records_read": r.records_read,
            "records_loaded": r.records_loaded,
            "records_rejected": r.records_rejected,
            "notes": r.notes,
            "dq_violations": dq_counts_by_run.get(r.job_run_id, 0),
            "file_stats": file_stats_by_run.get(r.job_run_id, []),
        }
        for r in runs
    ]


@app.get("/api/dq/rules")
def list_dq_rules(
    limit: int = 500,
    active_only: bool = True,
    entity_name: str | None = None,
    db: Session = Depends(get_db),
) -> list[dict]:
    q = select(DHDQRule)
    if active_only:
        q = q.where(DHDQRule.is_active.is_(True))
    if entity_name:
        q = q.where(DHDQRule.entity_name == entity_name)

    rows = db.execute(q.order_by(DHDQRule.entity_name, DHDQRule.rule_name).limit(limit)).scalars().all()
    return [
        {
            "rule_name": r.rule_name,
            "entity_name": r.entity_name,
            "field_name": r.field_name,
            "rule_type": r.rule_type,
            "severity": r.severity,
            "rule_param": r.rule_param,
            "description": r.description,
            "is_active": r.is_active,
        }
        for r in rows
    ]


@app.get("/api/dq/results")
def list_dq_results(limit: int = JOB_PAGE_SIZE, db: Session = Depends(get_db)) -> list[dict]:
    rows = db.execute(select(DHDQResult).order_by(desc(DHDQResult.created_at)).limit(limit)).scalars().all()
    return [
        {
            "dq_result_id": r.dq_result_id,
            "job_run_id": r.job_run_id,
            "file": r.input_file_name,
            "row_number": r.row_number,
            "entity_name": r.entity_name,
            "rule_name": r.rule_name,
            "severity": r.severity,
            "action_taken": r.action_taken,
            "message": r.message,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]


@app.get("/api/dq/violations")
def list_dq_violations(limit: int = JOB_PAGE_SIZE, db: Session = Depends(get_db)) -> list[dict]:
    return list_dq_results(limit=limit, db=db)


@app.post("/api/jobs/run")
def run_job(db: Session = Depends(get_db)) -> dict:
    return run_cash_pipeline(db)


@app.get("/api/graph/summary")
def graph_summary(
    include_surrogates: bool = True,
    include_ofac_matches: bool = True,
    include_txn_flow: bool = True,
    db: Session = Depends(get_db),
) -> dict:
    payload = build_graph_payload(
        db,
        include_surrogates=include_surrogates,
        include_ofac_matches=include_ofac_matches,
        include_txn_flow=include_txn_flow,
    )
    return {
        "snapshot_id": payload["snapshot_id"],
        "model_version": payload["model_version"],
        "as_of_ts": payload["as_of_ts"],
        "node_count": payload["node_count"],
        "edge_count": payload["edge_count"],
    }


@app.get("/api/graph/elements")
def graph_elements(
    include_surrogates: bool = True,
    include_ofac_matches: bool = True,
    include_txn_flow: bool = True,
    db: Session = Depends(get_db),
) -> dict:
    return build_graph_payload(
        db,
        include_surrogates=include_surrogates,
        include_ofac_matches=include_ofac_matches,
        include_txn_flow=include_txn_flow,
    )


@app.get("/api/graph/customer/{customer_key}")
def graph_customer(
    customer_key: str,
    hops: int = 2,
    max_nodes: int = 500,
    max_edges: int = 2000,
    include_surrogates: bool = True,
    include_ofac_matches: bool = True,
    include_txn_flow: bool = True,
    db: Session = Depends(get_db),
) -> dict:
    try:
        return build_customer_graph_payload(
            db,
            customer_key=customer_key,
            hops=hops,
            max_nodes=max_nodes,
            max_edges=max_edges,
            include_surrogates=include_surrogates,
            include_ofac_matches=include_ofac_matches,
            include_txn_flow=include_txn_flow,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/graph/customer-seed-search")
def graph_customer_seed_search(
    q: str,
    limit: int = 20,
    business_unit: str | None = None,
    customer_segment: str | None = None,
    db: Session = Depends(get_db),
) -> dict:
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required.")
    if limit < 1:
        raise HTTPException(status_code=400, detail="Query parameter 'limit' must be >= 1.")
    if limit > 100:
        limit = 100

    results = search_customer_seeds(
        db,
        q=q,
        limit=limit,
        business_unit=business_unit,
        customer_segment=customer_segment,
    )
    return {
        "query": q,
        "limit": limit,
        "business_unit": business_unit,
        "customer_segment": customer_segment,
        "result_count": len(results),
        "results": results,
    }


@app.get("/api/graph/exposure-seed-search")
def graph_exposure_seed_search(
    q: str,
    limit: int = 25,
    db: Session = Depends(get_db),
) -> dict:
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required.")
    if limit < 1:
        raise HTTPException(status_code=400, detail="Query parameter 'limit' must be >= 1.")
    if limit > 200:
        limit = 200

    results = search_exposure_seeds(
        db,
        q=q,
        limit=limit,
    )
    return {
        "query": q,
        "limit": limit,
        "result_count": len(results),
        "results": results,
    }


@app.get("/api/graph/customer/{customer_key}/summary")
def graph_customer_summary(
    customer_key: str,
    hops: int = 2,
    include_surrogates: bool = True,
    include_ofac_matches: bool = True,
    include_txn_flow: bool = True,
    db: Session = Depends(get_db),
) -> dict:
    try:
        return build_customer_graph_summary(
            db,
            customer_key=customer_key,
            hops=hops,
            include_surrogates=include_surrogates,
            include_ofac_matches=include_ofac_matches,
            include_txn_flow=include_txn_flow,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/graph/exposure")
def graph_exposure(
    node_id: str,
    hops: int = 2,
    max_nodes: int = 500,
    max_edges: int = 2000,
    include_surrogates: bool = True,
    include_ofac_matches: bool = True,
    include_txn_flow: bool = True,
    db: Session = Depends(get_db),
) -> dict:
    try:
        return build_seed_graph_payload(
            db,
            node_id=node_id,
            hops=hops,
            max_nodes=max_nodes,
            max_edges=max_edges,
            include_surrogates=include_surrogates,
            include_ofac_matches=include_ofac_matches,
            include_txn_flow=include_txn_flow,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/graph/customer/{customer_key}/transactions")
def graph_customer_transactions(
    customer_key: str,
    limit: int = 5000,
    db: Session = Depends(get_db),
) -> dict:
    try:
        return build_customer_primary_account_transactions(
            db,
            customer_key=customer_key,
            limit=limit,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/graph/node-neighbors")
def graph_node_neighbors(
    node_id: str,
    limit: int = 50,
    offset: int = 0,
    exclude_node_ids: str | None = None,
    include_surrogates: bool = True,
    include_ofac_matches: bool = True,
    include_txn_flow: bool = True,
    db: Session = Depends(get_db),
) -> dict:
    parsed_excludes = {
        v.strip()
        for v in (exclude_node_ids or "").split(",")
        if v and v.strip()
    }
    try:
        return build_node_neighbors_payload(
            db,
            node_id=node_id,
            limit=limit,
            offset=offset,
            exclude_node_ids=parsed_excludes,
            include_surrogates=include_surrogates,
            include_ofac_matches=include_ofac_matches,
            include_txn_flow=include_txn_flow,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api-browser", response_class=HTMLResponse)
def api_browser(request: Request):
    return templates.TemplateResponse("api_browser.html", {"request": request})


@app.get("/", response_class=HTMLResponse)
def home(request: Request, db: Session = Depends(get_db)):
    latest_runs = db.execute(select(DHJobRun).order_by(desc(DHJobRun.started_at)).limit(10)).scalars().all()
    latest_dq = db.execute(select(DHDQResult).order_by(desc(DHDQResult.created_at)).limit(25)).scalars().all()
    latest_files = db.execute(select(DHJobFileStat).order_by(desc(DHJobFileStat.processed_at)).limit(20)).scalars().all()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "latest_runs": latest_runs,
            "latest_dq": latest_dq,
            "latest_files": latest_files,
        },
    )
