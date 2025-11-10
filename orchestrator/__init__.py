"""
编排层模块
包含意图路由、并行执行器和LLM代理

作者: Tom
创建时间: 2025-11-10T16:17:02+08:00 (Asia/Shanghai)
"""

from typing import Any, Dict, List, Optional

from loguru import logger

from .router import Router
from .executor import Executor

__version__ = "1.0.0"


def query_across_systems(
    filter_params: Optional[Dict[str, Any]] = None,
    systems: Optional[List[str]] = None,
    timeout_ms: int = 5000,
) -> Dict[str, Any]:
    """
    高层入口：跨系统查询。

    - 路由：根据 systems 选择目标 Agent，并对 filter_params 做轻量校验与规范化。
    - 执行：并发调用各 Agent 的 query_canonical，并聚合四类实体结果。
    - 降级：部分失败或超时时返回错误列表与指标信息。

    Args:
        filter_params: 过滤参数，支持 entity_type/date_from/date_to/limit
        systems: 系统子集（如 ["erp", "hr", "fin"]），未提供则默认全部系统
        timeout_ms: 并发查询超时毫秒数

    Returns:
        统一结果字典：
        {
            "organizations": List[Organization],
            "persons": List[Person],
            "customers": List[Customer],
            "transactions": List[Transaction],
            "errors": List[str],
            "warnings": List[str],
            "metrics": Dict[str, Any]
        }
    """
    logger.info("Orchestrator: 跨系统查询入口调用")

    router = Router()
    routed = router.route(filter_params=filter_params, systems=systems)
    agents = routed["agents"]
    normalized_filters = routed["filter_params"]
    warnings = routed.get("warnings", [])

    executor = Executor()
    result = executor.execute(agents=agents, filter_params=normalized_filters, timeout_ms=timeout_ms)

    # 附加路由产生的告警信息
    result["warnings"] = warnings
    return result


__all__ = ["query_across_systems", "__version__"]