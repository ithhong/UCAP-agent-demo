import sys
from pathlib import Path
from typing import Any, Dict, List
from time import perf_counter

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from orchestrator import nl_query, query_across_systems
from api.schemas import NLQueryRequest, QueryRequest, QueryResponse


app = FastAPI(title="UCAP API", version="1.0.0")


def _to_dict(item: Any) -> Any:
    try:
        return item.model_dump()
    except Exception:
        try:
            return item.__dict__
        except Exception:
            return str(item)


def _serialize_list(items: List[Any]) -> List[Dict[str, Any]]:
    return [ _to_dict(x) for x in items ]


@app.exception_handler(RequestValidationError)
def validation_exception_handler(request: Request, exc: RequestValidationError):
    payload = {
        "organizations": [],
        "persons": [],
        "customers": [],
        "transactions": [],
        "warnings": [],
        "errors": ["Invalid request", str(exc.errors())],
        "metrics": {"api": {"status": "invalid_request"}},
    }
    return JSONResponse(status_code=422, content=payload)


@app.exception_handler(Exception)
def general_exception_handler(request: Request, exc: Exception):
    payload = {
        "organizations": [],
        "persons": [],
        "customers": [],
        "transactions": [],
        "warnings": [],
        "errors": [str(exc)],
        "metrics": {"api": {"status": "error"}},
    }
    return JSONResponse(status_code=400, content=payload)


@app.post("/nl-query", response_model=QueryResponse)
def nl_query_endpoint(req: NLQueryRequest) -> QueryResponse:
    start = perf_counter()
    fp = req.default_filters.model_dump() if req.default_filters else None
    result = nl_query(
        text=req.text,
        default_filters=fp,
        systems=req.systems,
        timeout_ms=req.timeout_ms or 5000,
    )
    dur_ms = (perf_counter() - start) * 1000.0
    metrics = result.get("metrics", {})
    metrics["api"] = {
        "duration_ms": dur_ms,
        "timeout_ms": req.timeout_ms or 5000,
        "systems": req.systems,
        "entity_type": fp.get("entity_type") if isinstance(fp, dict) else None,
        "limit": fp.get("limit") if isinstance(fp, dict) else None,
    }
    orgs = _serialize_list(result.get("organizations", []))
    persons = _serialize_list(result.get("persons", []))
    customers = _serialize_list(result.get("customers", []))
    txs = _serialize_list(result.get("transactions", []))
    return QueryResponse(
        organizations=orgs,
        persons=persons,
        customers=customers,
        transactions=txs,
        warnings=result.get("warnings", []),
        errors=result.get("errors", []),
        metrics=metrics,
    )


@app.post("/query", response_model=QueryResponse)
def query_endpoint(req: QueryRequest) -> QueryResponse:
    start = perf_counter()
    fp = req.filter_params.model_dump() if req.filter_params else None
    result = query_across_systems(
        filter_params=fp,
        systems=req.systems,
        timeout_ms=req.timeout_ms or 5000,
    )
    dur_ms = (perf_counter() - start) * 1000.0
    metrics = result.get("metrics", {})
    metrics["api"] = {
        "duration_ms": dur_ms,
        "timeout_ms": req.timeout_ms or 5000,
        "systems": req.systems,
        "entity_type": fp.get("entity_type") if isinstance(fp, dict) else None,
        "limit": fp.get("limit") if isinstance(fp, dict) else None,
    }
    orgs = _serialize_list(result.get("organizations", []))
    persons = _serialize_list(result.get("persons", []))
    customers = _serialize_list(result.get("customers", []))
    txs = _serialize_list(result.get("transactions", []))
    return QueryResponse(
        organizations=orgs,
        persons=persons,
        customers=customers,
        transactions=txs,
        warnings=result.get("warnings", []),
        errors=result.get("errors", []),
        metrics=metrics,
    )