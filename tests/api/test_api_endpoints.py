from fastapi.testclient import TestClient
from api.main import app


client = TestClient(app)


def _keys_ok(resp):
    assert "code" in resp and "success" in resp and "data" in resp
    obj = resp["data"]
    for k in [
        "organizations",
        "persons",
        "customers",
        "transactions",
        "warnings",
        "errors",
        "metrics",
    ]:
        assert k in obj


def test_nl_query_structure_ok():
    payload = {
        "text": "查询近三个月的财务流水",
        "default_filters": {"limit": 10},
        "systems": ["fin"],
        "timeout_ms": 5000,
    }
    r = client.post("/api/nl-query", json=payload)
    assert r.status_code == 200
    resp = r.json()
    _keys_ok(resp)
    metrics = resp["data"]["metrics"]
    assert "api" in metrics
    assert isinstance(metrics["api"].get("duration_ms"), (int, float))


def test_query_structure_ok():
    payload = {
        "filter_params": {"limit": 5},
        "systems": ["erp", "hr", "fin"],
        "timeout_ms": 4000,
    }
    r = client.post("/api/query", json=payload)
    assert r.status_code == 200
    resp = r.json()
    _keys_ok(resp)
    metrics = resp["data"]["metrics"]
    assert "api" in metrics


def test_query_invalid_entity_type_warning():
    payload = {
        "filter_params": {"entity_type": "foo"},
        "systems": ["erp", "hr", "fin"],
    }
    r = client.post("/api/query", json=payload)
    assert r.status_code == 200
    resp = r.json()
    _keys_ok(resp)
    warnings = resp["data"]["warnings"]
    assert any(("非法 entity_type" in w) or ("entity_type" in w) for w in warnings)


def test_query_unknown_system_warning():
    payload = {
        "filter_params": {"limit": 5},
        "systems": ["erp", "unknown", "fin"],
    }
    r = client.post("/api/query", json=payload)
    assert r.status_code == 200
    resp = r.json()
    _keys_ok(resp)
    warnings = resp["data"]["warnings"]
    assert any(("未知系统值" in w) or ("未知" in w) for w in warnings)


def test_nl_query_missing_text_422():
    payload = {"default_filters": {"limit": 10}}
    r = client.post("/api/nl-query", json=payload)
    assert r.status_code == 422
    resp = r.json()
    _keys_ok(resp)
    metrics = resp["data"]["metrics"]
    assert metrics["api"]["status"] == "invalid_request"
    assert "Invalid request" in resp["data"]["errors"][0]


def test_query_timeout_ms_too_small_422():
    payload = {"filter_params": {"limit": 1}, "timeout_ms": 10}
    r = client.post("/api/query", json=payload)
    assert r.status_code == 422