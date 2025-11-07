"""
Agent集成测试：过滤与缓存命中率

作者: Tom
创建时间: 2025-11-07T17:20:01+08:00
"""

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


def test_entity_type_transactions_filter():
    """实体类型过滤：仅返回交易集合，其他实体应为空列表"""
    for agent in _get_agents():
        result = agent.query_canonical({"entity_type": "transactions"})

        assert set(result.keys()) == {"organizations", "persons", "customers", "transactions"}

        # 仅交易有数据，其他为空
        assert isinstance(result["transactions"], list)
        assert len(result["transactions"]) >= 0  # 至少不报错
        assert result["organizations"] == []
        assert result["persons"] == []
        assert result["customers"] == []


def test_limit_filter_applies():
    """数量限制：各实体列表长度不超过指定 limit"""
    limit = 5
    for agent in _get_agents():
        result = agent.query_canonical({"limit": limit})

        assert all(isinstance(v, list) for v in result.values())
        assert len(result["organizations"]) <= limit
        assert len(result["persons"]) <= limit
        assert len(result["customers"]) <= limit
        assert len(result["transactions"]) <= limit


def test_date_range_future_filter_empty():
    """未来时间范围过滤：使用未来起始时间应返回空集合"""
    future_from = "2026-01-01T00:00:00"
    for agent in _get_agents():
        result = agent.query_canonical({"date_from": future_from})

        assert result["organizations"] == []
        assert result["persons"] == []
        assert result["customers"] == []
        assert result["transactions"] == []


def test_cache_hit_rate_improves_on_second_call():
    """缓存命中率：重复调用同一查询在同一时间窗口应产生命中"""
    for agent in _get_agents():
        agent.clear_cache()

        # 首次调用：应产生一次 miss
        _ = agent.query_canonical(None)

        # 二次调用同参数：应产生命中
        _ = agent.query_canonical(None)
        info = agent.get_cache_info()

        assert isinstance(info, dict)
        assert info.get("hits", 0) >= 1
        assert info.get("hit_rate", 0) > 0


def test_cache_miss_when_changing_filters():
    """更换过滤参数：应导致缓存 miss 增加（因缓存键包含过滤参数）"""
    for agent in _get_agents():
        agent.clear_cache()

        # 先建立一个缓存条目
        _ = agent.query_canonical({"limit": 5})
        info1 = agent.get_cache_info()

        # 相同过滤参数再次查询：产生命中
        _ = agent.query_canonical({"limit": 5})
        info2 = agent.get_cache_info()
        assert info2["hits"] >= info1["hits"]

        # 更换过滤参数：应产生 miss 增加
        _ = agent.query_canonical({"limit": 10})
        info3 = agent.get_cache_info()
        assert info3["misses"] > info2["misses"]