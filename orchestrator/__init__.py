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
from .llm_proxy import LLMProxy

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


def nl_query(
    text: str,
    default_filters: Optional[Dict[str, Any]] = None,
    systems: Optional[List[str]] = None,
    timeout_ms: int = 5000,
) -> Dict[str, Any]:
    """
    高层入口：自然语言查询。

    - 使用 LLMProxy 将自然语言解析为 `filter_params` 与可选的 `systems`/`timeout_ms`。
    - 通过 `query_across_systems` 进行桥接，不影响既有导出与行为。
    - 支持显式覆盖：若调用方传入 `systems`/`timeout_ms` 参数，则优先使用显式值。

    Args:
        text: 自然语言查询文本
        default_filters: 在 LLM 不可用或降级时作为基本过滤参数的兜底
        systems: 显式指定系统子集（覆盖 LLM 推断值）
        timeout_ms: 并发执行的超时毫秒数（可被 LLM 推断值覆盖，但范围裁剪到 50..60000）

    Returns:
        同 `query_across_systems`，但 metrics 中会附加 `llm` 字段记录 LLM 相关指标。
    """
    logger.info("Orchestrator: 自然语言查询入口调用")

    proxy = LLMProxy(timeout_ms=2000)
    inferred = proxy.infer(text=text, default_filters=default_filters)

    inferred_fp = inferred.get("filter_params", {})
    inferred_sys = inferred.get("systems")
    inferred_timeout = inferred.get("timeout_ms")
    llm_warnings = inferred.get("warnings", [])
    llm_metrics = inferred.get("metrics", {})

    # 选择最终 systems（显式参数优先）
    final_systems = systems if systems else inferred_sys

    # 选择最终 timeout，并做范围裁剪
    final_timeout = timeout_ms
    if isinstance(inferred_timeout, int):
        final_timeout = max(50, min(60000, inferred_timeout))

    # 桥接到既有入口
    result = query_across_systems(
        filter_params=inferred_fp,
        systems=final_systems,
        timeout_ms=final_timeout,
    )

    # 合并告警与指标（保持返回结构不变）
    warnings = result.get("warnings", [])
    result["warnings"] = [*warnings, *llm_warnings]
    metrics = result.get("metrics", {})
    metrics["llm"] = llm_metrics
    result["metrics"] = metrics
    return result


__all__ = ["query_across_systems", "nl_query", "__version__"]