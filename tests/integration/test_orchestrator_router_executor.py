"""
编排层集成测试：路由与执行器

作者: Tom
创建时间: 2025-11-10T16:39:14+08:00 (Asia/Shanghai)

说明：
- 覆盖路由子集选择、并发执行部分失败、超时触发、过滤参数透传与同系统内去重。
"""

import pytest
import time
from datetime import datetime, timedelta

from orchestrator import query_across_systems
from orchestrator.router import Router
from orchestrator.executor import Executor
from canonical.models import SystemType


def test_high_level_entry_returns_shape():
    """基本形态校验：高层入口返回统一结构，包含实体列表、错误、告警与指标。"""
    result = query_across_systems({"entity_type": "transactions", "limit": 5})

    # 结果键存在
    expected_keys = {"organizations", "persons", "customers", "transactions", "errors", "warnings", "metrics"}
    assert expected_keys.issubset(set(result.keys()))

    # 类型校验（列表/字典）
    assert isinstance(result["organizations"], list)
    assert isinstance(result["persons"], list)
    assert isinstance(result["customers"], list)
    assert isinstance(result["transactions"], list)
    assert isinstance(result["errors"], list)
    assert isinstance(result["warnings"], list)
    assert isinstance(result["metrics"], dict)


def test_routing_subset_systems():
    """验证 systems=["erp","fin"] 时仅路由到两系统；默认路由覆盖三系统。"""
    router = Router()
    routed_all = router.route(filter_params=None, systems=None)
    routed_subset = router.route(filter_params=None, systems=["erp", "fin"]) 

    # 默认路由：包含全部三系统
    set_all = {a.system_type.value for a in routed_all["agents"]}
    assert set_all == {"erp", "hr", "fin"}
    assert len(routed_all["agents"]) == 3

    # 子集路由：仅包含指定两系统
    set_subset = {a.system_type.value for a in routed_subset["agents"]}
    assert set_subset == {"erp", "fin"}
    assert len(routed_subset["agents"]) == 2
    # 无额外告警
    assert routed_subset.get("warnings", []) == []


def test_concurrent_execution_partial_failure():
    """模拟单系统数据源失败，验证部分成功与错误收集与指标统计。"""
    executor = Executor()
    router = Router()
    routed = router.route(filter_params={"entity_type": "organizations"}, systems=None)
    # 将 FINAgent 的数据源路径改为不存在，触发 DataSourceError
    fin_agent = next(a for a in routed["agents"] if a.system_type.value == "fin")
    original_path = fin_agent.db_path
    try:
        fin_agent.db_path = "d:/Vsproject/UCAP-agent-demo/data/this_file_should_not_exist.db"
        result = executor.execute(routed["agents"], filter_params=routed["filter_params"], timeout_ms=3000)
        # 断言错误被收集，且至少一个系统成功
        assert isinstance(result, dict)
        assert "errors" in result and "metrics" in result
        assert any("查询失败" in e for e in result["errors"])  # 包含失败信息
        assert result["metrics"]["fail_count"] >= 1
        assert result["metrics"]["success_count"] >= 1
        # FIN 的计数应为零，其他系统组织数应大于等于1
        per_counts = result["metrics"]["per_agent_result_counts"]
        assert "fin" in per_counts and per_counts["fin"]["organizations"] == 0
        # 聚合组织应有数据（来自其他成功系统）
        assert len(result["organizations"]) >= 1
    finally:
        fin_agent.db_path = original_path


def test_timeout_enforcement(monkeypatch):
    """设置较小的 timeout_ms + 注入慢查询，验证超时错误与时长统计。"""
    router = Router()
    routed = router.route(filter_params={"entity_type": "customers"}, systems=None)
    executor = Executor()
    # 选取一个系统注入慢查询（休眠 0.5s），设置超时 100ms
    slow_agent = next(a for a in routed["agents"] if a.system_type.value == "erp")
    original_query = slow_agent.query_canonical

    def slow_query(fp=None):
        time.sleep(0.5)
        return original_query(fp)

    monkeypatch.setattr(slow_agent, "query_canonical", slow_query, raising=True)

    timeout_ms = 100
    result = executor.execute(routed["agents"], filter_params=routed["filter_params"], timeout_ms=timeout_ms)
    assert isinstance(result, dict)
    assert "errors" in result
    assert any("查询超时" in e and f">{timeout_ms}ms" in e for e in result["errors"])  # 包含超时错误
    # 校验该系统的时长记录被标记为超时值
    system_key = slow_agent.system_type.value
    assert result["metrics"]["per_agent_duration_ms"][system_key] == float(timeout_ms)
    # 其他系统应有客户数据返回（聚合结果至少有数据）
    assert len(result["customers"]) >= 1


def test_filter_params_passthrough_effect():
    """验证 entity_type/limit/date_from 透传并在编排层生效。"""
    # 场景1：仅返回人员集合，其他实体为空；各系统人员数不超过1
    result1 = query_across_systems({"entity_type": "persons", "limit": 1})
    assert isinstance(result1, dict)
    assert result1["organizations"] == []
    assert result1["customers"] == []
    assert result1["transactions"] == []
    per_counts1 = result1["metrics"]["per_agent_result_counts"]
    for sys_key, counts in per_counts1.items():
        assert counts["persons"] <= 1
        assert counts["organizations"] == 0
        assert counts["customers"] == 0
        assert counts["transactions"] == 0

    # 场景2：未来时间过滤，四类实体均为空
    future_from = "2031-01-01T00:00:00"
    result2 = query_across_systems({"date_from": future_from})
    assert result2["organizations"] == []
    assert result2["persons"] == []
    assert result2["customers"] == []
    assert result2["transactions"] == []
    for sys_key, counts in result2["metrics"]["per_agent_result_counts"].items():
        assert counts == {"organizations": 0, "persons": 0, "customers": 0, "transactions": 0}


def test_deduplication_within_system(monkeypatch):
    """在同系统内制造重复主键，验证编排层按主键去重。"""
    router = Router()
    routed = router.route(filter_params={"entity_type": "organizations"}, systems=["erp"])  # 仅测试ERP
    agent = routed["agents"][0]

    # 保存原方法并计算原始计数
    original_query = agent.query_canonical
    original_result = original_query(routed["filter_params"])  # 仅organizations有效
    original_count = len(original_result["organizations"])

    # 注入重复数据：将首个组织再追加一次，形成重复ID
    def dup_query(fp=None):
        data = original_query(fp)
        if data["organizations"]:
            data = {
                **data,
                "organizations": data["organizations"] + [data["organizations"][0]],
            }
        return data

    monkeypatch.setattr(agent, "query_canonical", dup_query, raising=True)

    executor = Executor()
    result = executor.execute(routed["agents"], filter_params=routed["filter_params"], timeout_ms=2000)
    assert isinstance(result, dict)
    # 编排层去重后，organizations 计数应回到原始计数
    counts = result["metrics"]["per_agent_result_counts"]["erp"]["organizations"]
    assert counts == original_count
    # 聚合后列表长度也应等于原始计数
    assert len(result["organizations"]) == original_count