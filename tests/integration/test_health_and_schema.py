"""
Agent集成测试：健康检查与模式验证

作者: Tom
创建时间: 2025-11-07T16:47:00+08:00
"""

# 说明：本文件仅覆盖健康检查与模式验证两个维度，
# 复用现有 BaseAgent 接口，不做重复实现。

from agents.erp import ERPAgent
from agents.hr import HRAgent
from agents.fin import FINAgent


def _get_agents():
    """构造待测的三类 Agent"""
    return (
        ERPAgent(),
        HRAgent(),
        FINAgent(),
    )


def test_agents_health_check():
    """
    健康检查：
    - status=healthy
    - data_count > 0
    - system_type 为 erp/hr/fin
    """
    for agent in _get_agents():
        result = agent.health_check()

        assert isinstance(result, dict)
        assert result.get("status") == "healthy"
        assert isinstance(result.get("data_count"), int) and result.get("data_count") > 0
        assert result.get("system_type") in ("erp", "hr", "fin")


def test_agents_schema_supported_entities():
    """
    模式验证：
    - 返回描述中包含 supported_entities
    - 包含 organizations/persons/customers/transactions 四类
    - 每个实体的 schema 为字典结构
    """
    expected = {"organizations", "persons", "customers", "transactions"}

    for agent in _get_agents():
        schema = agent.get_schema()

        assert isinstance(schema, dict)
        assert schema.get("system_type") in ("erp", "hr", "fin")

        supported = schema.get("supported_entities")
        assert isinstance(supported, dict)

        assert expected.issubset(set(supported.keys()))

        for key in expected:
            entity_schema = supported.get(key)
            assert isinstance(entity_schema, dict)
            # Pydantic JSON Schema 通常包含 title/properties 等，做存在性抽样校验
            assert any(k in entity_schema for k in ("title", "properties", "$defs"))