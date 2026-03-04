"""
编排层模块
包含意图路由、并行执行器和LLM代理

作者: Tom
创建时间: 2025-11-10T16:17:02+08:00 (Asia/Shanghai)
"""

from typing import Any, Dict, List, Optional, Set, Tuple
import json
import os

from loguru import logger

from .router import Router, discover_capabilities
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

    # 打印最终参数，便于核对不同自然语言查询的差异
    logger.info(
        f"Orchestrator: 最终参数确认 filter_params={inferred_fp}, systems={final_systems}, timeout_ms={final_timeout}"
    )

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


def _normalize_type(value: Any) -> Optional[Tuple[str, ...]]:
    if value is None:
        return None
    if isinstance(value, list):
        return tuple(sorted(str(v) for v in value))
    return (str(value),)


def _schema_props(schema: Any) -> Tuple[Dict[str, Any], Set[str]]:
    if not isinstance(schema, dict):
        return {}, set()
    if schema.get("type") != "object":
        return {}, set()
    props = schema.get("properties") or {}
    required = set(schema.get("required") or [])
    return props, required


def _compare_enum(path: str, base_schema: Dict[str, Any], curr_schema: Dict[str, Any], breaking: List[str], compatible: List[str]) -> None:
    base_enum = base_schema.get("enum")
    curr_enum = curr_schema.get("enum")
    if isinstance(curr_enum, list) and not isinstance(base_enum, list):
        breaking.append(f"{path}:enum_added")
        return
    if isinstance(base_enum, list) and not isinstance(curr_enum, list):
        compatible.append(f"{path}:enum_removed")
        return
    if isinstance(base_enum, list) and isinstance(curr_enum, list):
        base_set = set(base_enum)
        curr_set = set(curr_enum)
        if not base_set.issubset(curr_set):
            breaking.append(f"{path}:enum_restricted")
        elif curr_set != base_set:
            compatible.append(f"{path}:enum_extended")


def _compare_schema(path: str, base_schema: Any, curr_schema: Any, breaking: List[str], compatible: List[str]) -> None:
    if not isinstance(base_schema, dict) or not isinstance(curr_schema, dict):
        return
    base_type = _normalize_type(base_schema.get("type"))
    curr_type = _normalize_type(curr_schema.get("type"))
    if base_type and curr_type and base_type != curr_type:
        breaking.append(f"{path}:type_changed")
        return
    _compare_enum(path, base_schema, curr_schema, breaking, compatible)
    if base_schema.get("type") == "array":
        _compare_schema(f"{path}[]", base_schema.get("items", {}), curr_schema.get("items", {}), breaking, compatible)
        return
    base_props, base_req = _schema_props(base_schema)
    curr_props, curr_req = _schema_props(curr_schema)
    for key in base_props:
        if key not in curr_props:
            breaking.append(f"{path}.{key}:removed")
        else:
            _compare_schema(f"{path}.{key}", base_props[key], curr_props[key], breaking, compatible)
    for key in curr_props:
        if key not in base_props:
            if key in curr_req:
                breaking.append(f"{path}.{key}:required_added")
            else:
                compatible.append(f"{path}.{key}:added")
    for key in curr_req - base_req:
        if key in curr_props:
            breaking.append(f"{path}.{key}:required_added")
    for key in base_req - curr_req:
        if key in base_props:
            compatible.append(f"{path}.{key}:required_removed")


def _normalize_capabilities(capabilities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for cap in capabilities:
        cap = dict(cap or {})
        skills = cap.get("skills") or []
        if isinstance(skills, list):
            cap["skills"] = sorted(skills, key=lambda s: str((s or {}).get("skill_id") or ""))
        normalized.append(cap)
    return sorted(normalized, key=lambda c: str(c.get("system_type") or c.get("system_name") or ""))


def export_capability_contracts(systems: Optional[List[str]] = None) -> Dict[str, Any]:
    res = discover_capabilities(systems)
    capabilities = _normalize_capabilities(res.get("capabilities", []))
    return {"capabilities": capabilities, "warnings": res.get("warnings", [])}


def save_contract_baseline(path: str = "data/contract_baseline.json", systems: Optional[List[str]] = None) -> str:
    payload = export_capability_contracts(systems)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


def load_contract_baseline(path: str = "data/contract_baseline.json") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def diff_contracts(baseline: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    breaking: List[str] = []
    compatible: List[str] = []
    base_caps = {str(c.get("system_type") or c.get("system_name")): c for c in baseline.get("capabilities", [])}
    curr_caps = {str(c.get("system_type") or c.get("system_name")): c for c in current.get("capabilities", [])}
    for key in base_caps:
        if key not in curr_caps:
            breaking.append(f"capability.{key}:removed")
    for key in curr_caps:
        if key not in base_caps:
            compatible.append(f"capability.{key}:added")
    for key, base_cap in base_caps.items():
        curr_cap = curr_caps.get(key)
        if not curr_cap:
            continue
        base_skills = {str(s.get("skill_id")): s for s in base_cap.get("skills", [])}
        curr_skills = {str(s.get("skill_id")): s for s in curr_cap.get("skills", [])}
        for sid in base_skills:
            if sid not in curr_skills:
                breaking.append(f"skill.{key}.{sid}:removed")
        for sid in curr_skills:
            if sid not in base_skills:
                compatible.append(f"skill.{key}.{sid}:added")
        for sid, base_skill in base_skills.items():
            curr_skill = curr_skills.get(sid)
            if not curr_skill:
                continue
            _compare_schema(f"skill.{key}.{sid}.input", base_skill.get("input_schema", {}), curr_skill.get("input_schema", {}), breaking, compatible)
            _compare_schema(f"skill.{key}.{sid}.output", base_skill.get("output_schema", {}), curr_skill.get("output_schema", {}), breaking, compatible)
    return {
        "breaking_changes": breaking,
        "compatible_changes": compatible,
        "summary": {
            "breaking_count": len(breaking),
            "compatible_count": len(compatible),
        }
    }


__all__ = [
    "query_across_systems",
    "nl_query",
    "discover_capabilities",
    "export_capability_contracts",
    "save_contract_baseline",
    "load_contract_baseline",
    "diff_contracts",
    "__version__",
]
