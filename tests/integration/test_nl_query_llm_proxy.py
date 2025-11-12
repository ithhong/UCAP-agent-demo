"""
LLM 代理与 nl_query 集成测试

作者: Tom
创建时间: 2025-11-11T18:07:08+08:00 (Asia/Shanghai)

说明：
- 不依赖真实外网与 API Key，通过 monkeypatch LLMProxy._call_llm 控制解析路径
- 覆盖降级路径、正常解析、显式 systems 覆盖与非法 entity_type 校正
"""

import pytest
from typing import Any, Dict, Tuple

from orchestrator import nl_query
from orchestrator.llm_proxy import LLMProxy


def _mock_llm_return(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    return payload, "{mock_json}"


def test_nl_query_degraded_no_key(monkeypatch):
    """当 LLM 返回 None（模拟无 key / 解析失败）时，走降级路径并记录指标。"""

    def _degraded(_self, prompt: str):
        return None, None

    monkeypatch.setattr(LLMProxy, "_call_llm", _degraded, raising=True)

    result = nl_query(
        text="查询财务流水 近三个月 限制10条",
        default_filters={"limit": 10},
        systems=None,
        timeout_ms=1500,
    )

    assert isinstance(result, dict)
    assert "metrics" in result and "warnings" in result
    assert "llm" in result["metrics"]
    assert result["metrics"]["llm"]["llm_status"] == "degraded"
    # 有降级告警
    assert any("LLM 降级" in w for w in result["warnings"])
    # 执行器指标存在
    assert isinstance(result["metrics"].get("per_agent_duration_ms"), dict)


def test_nl_query_llm_parse_normal(monkeypatch):
    """正常解析路径：LLM 返回 JSON，路由到指定 systems 并统计指标。"""

    payload = {
        "filter_params": {"entity_type": "transactions", "limit": 7},
        "systems": ["fin"],
        "timeout_ms": 1200,
    }

    monkeypatch.setattr(LLMProxy, "_call_llm", lambda self, prompt: _mock_llm_return(payload), raising=True)

    result = nl_query(text="请查询最近一周的财务交易，限制7条")

    assert isinstance(result, dict)
    assert result["metrics"]["llm"]["llm_status"] == "ok"
    # 仅执行 FIN
    per_keys = set(result["metrics"]["per_agent_duration_ms"].keys())
    assert per_keys == {"fin"}


def test_nl_query_explicit_systems_override(monkeypatch):
    """显式 systems 覆盖 LLM 推断值。"""

    payload = {
        "filter_params": {"entity_type": "transactions", "limit": 5},
        "systems": ["fin"],
    }

    monkeypatch.setattr(LLMProxy, "_call_llm", lambda self, prompt: _mock_llm_return(payload), raising=True)

    # 显式指定为 ERP，仅 ERP 执行
    result = nl_query(text="查交易", systems=["erp"], timeout_ms=1000)
    assert result["metrics"]["llm"]["llm_status"] == "ok"
    per_keys = set(result["metrics"]["per_agent_duration_ms"].keys())
    assert per_keys == {"erp"}


def test_nl_query_schema_correction_invalid_entity(monkeypatch):
    """LLM 返回非法 entity_type 时应被路由层移除并产生告警。"""

    payload = {
        "filter_params": {"entity_type": "unknown", "limit": 5},
        "systems": ["erp", "fin"],
    }

    monkeypatch.setattr(LLMProxy, "_call_llm", lambda self, prompt: _mock_llm_return(payload), raising=True)

    result = nl_query(text="随便查一下")
    assert isinstance(result, dict)
    assert result["metrics"]["llm"]["llm_status"] == "ok"
    # 路由层发出非法 entity_type 告警
    assert any("非法 entity_type" in w for w in result["warnings"])