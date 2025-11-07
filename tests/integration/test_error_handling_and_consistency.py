"""
错误处理与健壮性 / 系统内关联一致性 集成测试

作者: Tom
创建时间: 2025-11-07T22:13:35+08:00

说明:
- 验证数据源异常(DataSourceError)、查询包装异常(AgentError)与映射异常(DataMappingError)
- 校验各系统ID命名空间一致性（组织/人员/客户/交易），确保跨系统统一前缀
- 健康检查在异常路径下返回unhealthy且不抛出异常
"""

import pytest
from typing import Type

from agents.base import AgentError, DataSourceError, DataMappingError
from agents.erp import ERPAgent
from agents.hr import HRAgent
from agents.fin import FINAgent


# 供参数化使用的Agent列表与ID前缀
AGENT_CASES = [
    (ERPAgent, "erp_"),
    (HRAgent, "hr_"),
    (FINAgent, "fin_"),
]


@pytest.mark.parametrize("AgentClass,prefix", AGENT_CASES)
def test_health_check_normal(AgentClass: Type, prefix: str):
    agent = AgentClass()
    info = agent.health_check()
    assert isinstance(info, dict)
    assert info.get("status") == "healthy"
    assert info.get("data_count", 0) >= 0


@pytest.mark.parametrize("AgentClass,prefix", AGENT_CASES)
def test_pull_raw_datasource_error(AgentClass: Type, prefix: str):
    agent = AgentClass()
    # 记录并篡改数据库路径，模拟数据源异常
    original_path = agent.db_path
    try:
        agent.db_path = "d:/Vsproject/UCAP-agent-demo/data/this_file_should_not_exist.db"
        with pytest.raises(DataSourceError):
            _ = agent.pull_raw()
    finally:
        # 恢复配置，避免影响后续测试
        agent.db_path = original_path


@pytest.mark.parametrize("AgentClass,prefix", AGENT_CASES)
def test_query_canonical_error_wrapped(AgentClass: Type, prefix: str):
    agent = AgentClass()
    original_path = agent.db_path
    try:
        agent.clear_cache()
        agent.db_path = "d:/Vsproject/UCAP-agent-demo/data/this_file_should_not_exist.db"
        with pytest.raises(AgentError):
            _ = agent.query_canonical()
    finally:
        agent.db_path = original_path


@pytest.mark.parametrize("AgentClass,prefix", AGENT_CASES)
def test_health_check_unhealthy_on_bad_source(AgentClass: Type, prefix: str):
    agent = AgentClass()
    original_path = agent.db_path
    try:
        agent.db_path = "d:/Vsproject/UCAP-agent-demo/data/this_file_should_not_exist.db"
        info = agent.health_check()
        assert info.get("status") == "unhealthy"
        assert "error" in info
    finally:
        agent.db_path = original_path


def _broken_org_payload(system: str):
    if system == "erp":
        return [{"_entity": "organization", "erp_org_id": None, "org_name": "BrokenOrg"}]
    if system == "hr":
        return [{"_entity": "organization", "hr_org_id": "", "org_name": "BrokenOrg"}]
    if system == "fin":
        return [{"_entity": "organization", "fin_org_id": None, "org_name": "BrokenOrg"}]
    return []


@pytest.mark.parametrize("AgentClass,system_key", [
    (ERPAgent, "erp"),
    (HRAgent, "hr"),
    (FINAgent, "fin"),
])
def test_map_to_canonical_mapping_error(AgentClass: Type, system_key: str):
    agent = AgentClass()
    raw = _broken_org_payload(system_key)
    with pytest.raises(DataMappingError):
        _ = agent.map_to_canonical(raw)


@pytest.mark.parametrize("AgentClass,prefix", AGENT_CASES)
def test_id_namespace_consistency(AgentClass: Type, prefix: str):
    agent = AgentClass()
    agent.clear_cache()
    data = agent.query_canonical({"limit": 50})

    # 组织
    for org in data.get("organizations", []):
        assert isinstance(org.org_id, str)
        assert org.org_id.startswith(prefix)

    # 人员
    for person in data.get("persons", []):
        assert isinstance(person.person_id, str)
        assert person.person_id.startswith(prefix)
        assert isinstance(person.org_id, str)
        # FIN可能回退到"fin_unknown_org"；其他系统为标准前缀
        if prefix == "fin_":
            assert person.org_id.startswith("fin_")
        else:
            assert person.org_id.startswith(prefix)

    # 客户
    for cust in data.get("customers", []):
        assert isinstance(cust.customer_id, str)
        assert cust.customer_id.startswith(prefix)

    # 交易
    for tx in data.get("transactions", []):
        assert isinstance(tx.tx_id, str)
        assert tx.tx_id.startswith(prefix)
        if tx.person_id is not None:
            assert isinstance(tx.person_id, str)
            assert tx.person_id.startswith(prefix)
        if tx.customer_id is not None:
            assert isinstance(tx.customer_id, str)
            assert tx.customer_id.startswith(prefix)