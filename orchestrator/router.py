"""
编排层路由器（Router）

职责：
- 根据调用方提供的 `systems`（如 ["erp", "hr", "fin"]）选择目标 Agent 集合。
- 对查询过滤参数进行轻量校验与规范化，不改变语义，仅清洗非法值。
- 该模块不执行查询，仅负责路由 与 参数校验，执行由 executor 模块承担。

作者：Tom
创建时间（Asia/Shanghai）：2025-11-07T23:56:50+08:00
"""

from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from agents.base import BaseAgent
from agents.erp import ERPAgent
from agents.hr import HRAgent
from agents.fin import FINAgent


# 支持的系统到 Agent 类的映射
SUPPORTED_SYSTEMS: Dict[str, type] = {
    "erp": ERPAgent,
    "hr": HRAgent,
    "fin": FINAgent,
}

# 支持的实体类型（与 BaseAgent.filter 语义对齐）
SUPPORTED_ENTITY_TYPES: List[str] = [
    "organizations",
    "persons",
    "customers",
    "transactions",
]


class Router:
    """负责编排层的系统选择与参数校验。

    用法示例：
        router = Router()
        result = router.route({"entity_type": "transactions", "limit": 50}, systems=["erp", "fin"])
        agents = result["agents"]           # List[BaseAgent]
        filter_params = result["filter_params"]  # 规范化后的过滤参数
        warnings = result["warnings"]       # 参数/系统校验产生的告警
    """

    def validate_systems(self, systems: Optional[List[str]]) -> Tuple[List[str], List[str]]:
        """校验并规范化系统列表。

        - None 或空列表：默认路由到全部支持系统。
        - 未知系统值：忽略并返回告警。
        """
        warnings: List[str] = []
        if not systems:
            logger.debug("Router: systems 未提供，默认选择全部系统")
            return list(SUPPORTED_SYSTEMS.keys()), warnings

        normalized: List[str] = []
        for s in systems:
            key = (s or "").strip().lower()
            if key in SUPPORTED_SYSTEMS:
                normalized.append(key)
            else:
                msg = f"Router: 未知系统值 '{s}'，已忽略"
                logger.warning(msg)
                warnings.append(msg)

        # 去重保持顺序
        seen = set()
        normalized_unique = []
        for s in normalized:
            if s not in seen:
                normalized_unique.append(s)
                seen.add(s)

        # 若全部非法，降级为全部系统
        if not normalized_unique:
            msg = "Router: 有效系统为空，降级为全部系统"
            logger.warning(msg)
            warnings.append(msg)
            return list(SUPPORTED_SYSTEMS.keys()), warnings

        return normalized_unique, warnings

    def validate_filter_params(self, filter_params: Optional[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[str]]:
        """轻量校验与规范化过滤参数。

        保持与 BaseAgent._apply_filters 的语义一致：
        - 支持键：entity_type/date_from/date_to/limit
        - entity_type 必须在 SUPPORTED_ENTITY_TYPES 中，否则移除并告警
        - limit 转换为 int，非正数告警并移除
        - 其它未知键保留原样，不在路由层做裁剪
        """
        warnings: List[str] = []
        normalized: Dict[str, Any] = dict(filter_params or {})

        # entity_type 校验
        et = normalized.get("entity_type")
        if et is not None:
            et_str = str(et).strip().lower()
            if et_str in SUPPORTED_ENTITY_TYPES:
                normalized["entity_type"] = et_str
            else:
                msg = f"Router: 非法 entity_type '{et}', 已移除"
                logger.warning(msg)
                warnings.append(msg)
                normalized.pop("entity_type", None)

        # limit 规范化
        if "limit" in normalized:
            lim = normalized.get("limit")
            try:
                lim_int = int(lim)
                if lim_int <= 0:
                    raise ValueError("limit must be positive")
                normalized["limit"] = lim_int
            except Exception:
                msg = f"Router: 非法 limit 值 '{lim}', 已移除"
                logger.warning(msg)
                warnings.append(msg)
                normalized.pop("limit", None)

        # date_from/date_to 保留为原样字符串，具体解析交由 Agent 层处理
        # 在此仅做类型提示性的告警，不强制修改
        for key in ("date_from", "date_to"):
            if key in normalized and normalized[key] is not None:
                if not isinstance(normalized[key], str):
                    msg = f"Router: {key} 建议为字符串（ISO8601），当前类型为 {type(normalized[key]).__name__}"
                    logger.debug(msg)
                    warnings.append(msg)

        return normalized, warnings

    def select_agents(self, systems: Optional[List[str]]) -> Tuple[List[BaseAgent], List[str]]:
        """根据系统列表实例化目标 Agents，并返回告警。"""
        system_keys, warnings = self.validate_systems(systems)
        agents: List[BaseAgent] = [SUPPORTED_SYSTEMS[s]() for s in system_keys]
        logger.debug(f"Router: 选择的系统为 {system_keys}，实例化 {len(agents)} 个 Agent")
        return agents, warnings

    def route(
        self,
        filter_params: Optional[Dict[str, Any]] = None,
        systems: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """路由入口：返回目标 Agents 与规范化过滤参数。

        返回结构：
        {
            "agents": List[BaseAgent],
            "filter_params": Dict[str, Any],
            "warnings": List[str],
        }
        """
        agents, sys_warnings = self.select_agents(systems)
        normalized_filters, fp_warnings = self.validate_filter_params(filter_params)
        return {
            "agents": agents,
            "filter_params": normalized_filters,
            "warnings": [*sys_warnings, *fp_warnings],
        }


__all__ = ["Router", "SUPPORTED_SYSTEMS", "SUPPORTED_ENTITY_TYPES"]