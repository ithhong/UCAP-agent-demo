"""
编排层执行器（Executor）

职责：
- 并发调用多个 Agent 的 `query_canonical(filter_params)`。
- 聚合四类实体结果（organizations/persons/customers/transactions）。
- 进行错误降级（部分失败不影响整体返回），并收集指标。
- 在同一系统内对实体按主键去重，跨系统仅拼接不合并。

作者: Tom
创建时间: 2025-11-08T15:08:21+08:00 (Asia/Shanghai)
"""

from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, wait
from time import perf_counter

from loguru import logger

from agents.base import BaseAgent, AgentError, DataSourceError, DataMappingError


class Executor:
    """负责编排层的并发执行与结果聚合。"""

    def __init__(self) -> None:
        logger.debug("Executor: 初始化完成")

    def _system_key(self, agent: BaseAgent) -> str:
        """从 Agent 的系统类型枚举提取统一的系统键（小写）。"""
        try:
            return str(agent.system_type.value).lower()
        except Exception:
            # 回退为类名前缀的小写
            name = agent.__class__.__name__.replace("Agent", "").lower()
            return name or "unknown"

    def _dedup_list_by_id(self, items: List[Any], id_attr: str) -> List[Any]:
        """在同一系统内按主键字段去重。"""
        seen = set()
        result: List[Any] = []
        for item in items:
            key = getattr(item, id_attr, None)
            if not key:
                result.append(item)
                continue
            if key not in seen:
                result.append(item)
                seen.add(key)
        return result

    def _safe_query(
        self, agent: BaseAgent, filter_params: Optional[Dict[str, Any]]
    ) -> Tuple[str, Optional[Dict[str, List[Any]]], Optional[str], float, Dict[str, int]]:
        """
        包装每个 Agent 的查询，返回 (system_key, result, error_msg, duration_ms, counts)。
        counts 为每个实体列表的长度统计。
        """
        system = self._system_key(agent)
        start = perf_counter()
        try:
            logger.debug(f"Executor: 开始查询系统 {system}")
            data = agent.query_canonical(filter_params)

            # 在同系统内进行去重
            orgs = self._dedup_list_by_id(data.get("organizations", []), "org_id")
            persons = self._dedup_list_by_id(data.get("persons", []), "person_id")
            customers = self._dedup_list_by_id(data.get("customers", []), "customer_id")
            txns = self._dedup_list_by_id(data.get("transactions", []), "tx_id")

            deduped = {
                "organizations": orgs,
                "persons": persons,
                "customers": customers,
                "transactions": txns,
            }
            duration_ms = (perf_counter() - start) * 1000.0
            counts = {k: len(v) for k, v in deduped.items()}
            logger.info(
                f"Executor: 系统 {system} 查询成功，用时 {duration_ms:.2f}ms，"
                f"数量 org={counts['organizations']} person={counts['persons']} cust={counts['customers']} tx={counts['transactions']}"
            )
            return system, deduped, None, duration_ms, counts

        except (DataSourceError, DataMappingError, AgentError) as e:
            duration_ms = (perf_counter() - start) * 1000.0
            msg = f"{agent.system_name} 查询失败: {str(e)}"
            logger.error(msg)
            return system, None, msg, duration_ms, {"organizations": 0, "persons": 0, "customers": 0, "transactions": 0}
        except Exception as e:
            duration_ms = (perf_counter() - start) * 1000.0
            msg = f"{agent.system_name} 未预期异常: {str(e)}"
            logger.exception(msg)
            return system, None, msg, duration_ms, {"organizations": 0, "persons": 0, "customers": 0, "transactions": 0}

    def execute(
        self,
        agents: List[BaseAgent],
        filter_params: Optional[Dict[str, Any]] = None,
        timeout_ms: int = 5000,
    ) -> Dict[str, Any]:
        """
        并发执行多个 Agent 查询并聚合结果。

        返回结构：
        {
            "organizations": List[Organization],
            "persons": List[Person],
            "customers": List[Customer],
            "transactions": List[Transaction],
            "errors": List[str],
            "metrics": {
                "per_agent_duration_ms": Dict[str, float],
                "per_agent_result_counts": Dict[str, Dict[str, int]],
                "success_count": int,
                "fail_count": int,
                "total_duration_ms": float
            }
        }
        """
        overall_start = perf_counter()
        logger.info(
            f"Executor: 并发执行开始（agents={len(agents)}, timeout={timeout_ms}ms）"
        )

        aggregated = {
            "organizations": [],
            "persons": [],
            "customers": [],
            "transactions": [],
        }
        errors: List[str] = []
        per_agent_duration_ms: Dict[str, float] = {}
        per_agent_result_counts: Dict[str, Dict[str, int]] = {}
        success_count = 0
        fail_count = 0

        timeout_sec = max(0.001, timeout_ms / 1000.0)

        with ThreadPoolExecutor(max_workers=max(1, len(agents))) as pool:
            future_map = {
                pool.submit(self._safe_query, agent, filter_params): agent for agent in agents
            }
            done, pending = wait(future_map.keys(), timeout=timeout_sec)

            # 处理已完成任务
            for fut in done:
                agent = future_map[fut]
                system, data, err, dur_ms, counts = fut.result()
                per_agent_duration_ms[system] = dur_ms
                per_agent_result_counts[system] = counts
                if err is None and data is not None:
                    # 跨系统只做拼接
                    aggregated["organizations"].extend(data.get("organizations", []))
                    aggregated["persons"].extend(data.get("persons", []))
                    aggregated["customers"].extend(data.get("customers", []))
                    aggregated["transactions"].extend(data.get("transactions", []))
                    success_count += 1
                else:
                    errors.append(err or f"{agent.system_name} 未知错误")
                    fail_count += 1

            # 处理超时任务
            for fut in pending:
                agent = future_map[fut]
                system = self._system_key(agent)
                # 尝试取消未完成任务
                fut.cancel()
                msg = f"{agent.system_name} 查询超时（>{timeout_ms}ms）"
                logger.warning(msg)
                errors.append(msg)
                per_agent_duration_ms[system] = float(timeout_ms)
                per_agent_result_counts[system] = {
                    "organizations": 0,
                    "persons": 0,
                    "customers": 0,
                    "transactions": 0,
                }
                fail_count += 1

        total_duration_ms = (perf_counter() - overall_start) * 1000.0
        logger.info(
            f"Executor: 并发执行完成，总耗时 {total_duration_ms:.2f}ms，"
            f"成功 {success_count}，失败 {fail_count}"
        )

        return {
            "organizations": aggregated["organizations"],
            "persons": aggregated["persons"],
            "customers": aggregated["customers"],
            "transactions": aggregated["transactions"],
            "errors": errors,
            "metrics": {
                "per_agent_duration_ms": per_agent_duration_ms,
                "per_agent_result_counts": per_agent_result_counts,
                "success_count": success_count,
                "fail_count": fail_count,
                "total_duration_ms": total_duration_ms,
            },
        }


__all__ = ["Executor"]