"""
LLM 代理模块（Qwen/DashScope）

职责：
- 固化 LLM 工具调用所需的 `filter_params` JSON Schema（与 Router 校验规则对齐）。
- 构造请求并调用模型；在失败或超时情况下进行错误降级与日志埋点。
- 输出推断的 `filter_params` 与 `systems`，供上层 `nl_query` 桥接到 `query_across_systems`。

作者: Tom
创建时间: 2025-11-11T15:11:07+08:00 (Asia/Shanghai)
"""

from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import math
from dateutil.relativedelta import relativedelta

from loguru import logger

from config.settings import settings, validate_api_key, get_model_config
from .router import Router, SUPPORTED_SYSTEMS, SUPPORTED_ENTITY_TYPES


# 固化的 JSON Schema：仅定义 filter_params 的结构，保持与 Router.validate_filter_params 语义一致
FILTER_PARAMS_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "entity_type": {
            "type": "string",
            "enum": SUPPORTED_ENTITY_TYPES,
            "description": "目标实体类型，受支持的集合与 BaseAgent.filter 对齐",
        },
        "date_from": {
            "type": "string",
            "format": "date-time",
            "description": "起始时间（ISO8601 字符串），示例：2024-01-01T00:00:00Z",
        },
        "date_to": {
            "type": "string",
            "format": "date-time",
            "description": "结束时间（ISO8601 字符串）",
        },
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": 1000,
            "description": "返回记录上限（正整数，默认 50，最大 1000）",
        },
    },
    "additionalProperties": True,
}


def validate_filter_params_for_llm(filter_params: Optional[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[str]]:
    """封装对 LLM 产生的 filter_params 的轻量校验与规范化。

    - 直接复用 Router.validate_filter_params，以保持与编排层一致的语义。
    - 返回规范化后的参数与告警。
    """
    router = Router()
    normalized, warnings = router.validate_filter_params(filter_params)
    return normalized, warnings


class LLMProxy:
    """Qwen/DashScope 代理。

    注意：
    - 依赖 `config.settings` 中的 `dashscope_api_key` 与 `default_model`。
    - 优先指导模型输出包含 JSON（含 `filter_params` 与可选的 `systems`/`timeout_ms`）。
    - 若响应无法解析或请求失败/超时，降级为基于关键词的简单规则。
    """

    def __init__(self, timeout_ms: int = 2000) -> None:
        self.timeout_ms = timeout_ms

    def _compose_prompt(self, text: str) -> str:
        """构造提示词，要求按 JSON Schema 输出工具参数。"""
        schema_str = json.dumps(FILTER_PARAMS_JSON_SCHEMA, ensure_ascii=False)
        supported_systems = ", ".join(SUPPORTED_SYSTEMS.keys())
        return (
            "你是一名企业数据协作助手，需要为一个跨系统查询工具生成 JSON 参数。"\
            "\n只输出一个 JSON 对象，不要包含多余文字。"\
            "\n支持的系统: " + supported_systems +
            "\nfilter_params 的 JSON Schema 如下（其它未知键允许保留）：\n" + schema_str +
            "\n可选地，你也可以在同一 JSON 中提供 'systems' 字段（数组，元素取自支持的系统）和 'timeout_ms'（整数，50..60000）。"\
            "\n用户需求：" + text
        )

    def _call_llm(self, prompt: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """调用 DashScope 文本生成接口。

        返回 (parsed_json, raw_text)。若解析失败，parsed_json 为 None，但可能返回原始文本用于后续正则提取。"""
        if not validate_api_key():
            logger.warning("LLMProxy: 未配置 API Key，跳过 LLM 调用，走降级")
            return None, None

        try:
            # 这里使用 requests 直接调用 DashScope 文本生成 REST API，避免强耦合。
            import requests

            url = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
            headers = {
                "Authorization": f"Bearer {settings.dashscope_api_key}",
                "Content-Type": "application/json",
            }
            # DashScope 接口要求 input 为 JSON 对象，改为 messages 结构以避免 400 错误
            payload = {
                "model": get_model_config()["model"],
                "input": {
                    "messages": [
                        {"role": "user", "content": prompt}
                    ]
                },
                "parameters": {
                    "max_tokens": get_model_config()["max_tokens"],
                    "temperature": get_model_config()["temperature"],
                },
            }

            start = time.time()
            resp = requests.post(url, headers=headers, json=payload, timeout=self.timeout_ms / 1000.0)
            latency_ms = int((time.time() - start) * 1000)
            logger.info(f"LLMProxy: LLM 调用完成，耗时 {latency_ms}ms，status={resp.status_code}")

            if resp.status_code != 200:
                logger.warning(f"LLMProxy: 非 200 响应，body={resp.text[:200]}")
                return None, None

            data = resp.json()
            # 尝试常见字段获取文本
            # 兼容不同返回结构：优先 output.text，其次 choices[0].text，再次 output.choices[0].message.content
            raw_text = (
                data.get("output", {}).get("text", None)
                or data.get("choices", [{}])[0].get("text")
                or (
                    data.get("output", {})
                    .get("choices", [{}])[0]
                    .get("message", {})
                    .get("content")
                )
            )
            if not raw_text:
                # 某些版本返回 content 字段
                raw_text = data.get("content")

            if isinstance(raw_text, str):
                # 尝试直接解析为 JSON
                try:
                    parsed = json.loads(raw_text)
                    if isinstance(parsed, dict):
                        return parsed, raw_text
                except Exception:
                    # 尝试正则提取第一个 JSON 对象
                    match = re.search(r"\{[\s\S]*\}", raw_text)
                    if match:
                        try:
                            parsed = json.loads(match.group(0))
                            if isinstance(parsed, dict):
                                return parsed, raw_text
                        except Exception:
                            pass
            # 若解析失败，返回原始文本供回退分析
            return None, raw_text if isinstance(raw_text, str) else None
        except Exception as e:
            logger.error(f"LLMProxy: 调用失败/超时：{e}")
            return None, None

    def _fallback_parse(self, text: str, default_filters: Optional[Dict[str, Any]]) -> Tuple[Dict[str, Any], Optional[List[str]], List[str]]:
        """基于关键词的简单降级解析。仅用于 LLM 不可用或响应不可解析时。"""
        warnings: List[str] = ["LLM 降级：使用关键词规则推断参数"]
        s: Optional[List[str]] = None
        fp: Dict[str, Any] = dict(default_filters or {})

        t = text.lower()
        if any(k in t for k in ["财务", "流水", "发票", "交易", "fin"]):
            s = ["fin"]
            fp.setdefault("entity_type", "transactions")
            fp.setdefault("limit", 50)
        elif any(k in t for k in ["员工", "人力", "hr", "薪资", "人员"]):
            s = ["hr"]
            fp.setdefault("entity_type", "persons")
            fp.setdefault("limit", 50)
        elif any(k in t for k in ["客户", "订单", "erp", "供应商", "组织"]):
            s = ["erp"]
            fp.setdefault("entity_type", "customers")
            fp.setdefault("limit", 50)

        # 解析中文相对时间范围，如：近两年/最近3月/过去一周/近十天
        # 支持阿拉伯数字与常见中文数字（零一二三四五六七八九十百）
        try:
            now = datetime.now()
            num = None
            unit = None

            # 优先匹配格式：近|最近|过去 + (中文/阿拉伯数字) + (年|月|天|日|周|星期)
            import re
            m = re.search(r"(近|最近|过去)([0-9]+|[零一二两三四五六七八九十百]+)(年|月|天|日|周|星期)", text)
            if m:
                raw_num = m.group(2)
                unit = m.group(3)

                def chinese_to_int(s: str) -> int:
                    mapping = {
                        "零": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4,
                        "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10,
                        "百": 100
                    }
                    # 简易中文数字到整数，覆盖常见表达：一至十、两、十X、X十、百
                    if raw_num.isdigit():
                        return int(raw_num)
                    # 处理“十X”或“X十”
                    if raw_num.startswith("十") and len(raw_num) > 1:
                        return 10 + mapping.get(raw_num[1], 0)
                    if raw_num.endswith("十") and len(raw_num) > 1:
                        return mapping.get(raw_num[0], 0) * 10
                    # 单字或“十”“百”
                    if raw_num in mapping:
                        return mapping[raw_num]
                    # 逐字累加（如“二三”→23 的情况不常见，保守做逐字相加）
                    total = 0
                    for ch in raw_num:
                        total += mapping.get(ch, 0)
                    return max(1, total)

                num = chinese_to_int(raw_num)

            # 次要匹配："近3个月" 等英文形式
            if not num or not unit:
                m2 = re.search(r"(近|最近|过去)\s*(\d+)\s*(个)?\s*(年|月|天|日|周|星期)", text)
                if m2:
                    num = int(m2.group(2))
                    unit = m2.group(3)

            if num and unit:
                # 统一 unit
                if unit in ("年",):
                    date_from = now - relativedelta(years=num)
                elif unit in ("月",):
                    date_from = now - relativedelta(months=num)
                elif unit in ("天", "日"):
                    date_from = now - relativedelta(days=num)
                elif unit in ("周", "星期"):
                    date_from = now - relativedelta(weeks=num)
                else:
                    date_from = None

                if date_from:
                    fp["date_from"] = date_from.isoformat()
                    fp["date_to"] = now.isoformat()
                    warnings.append(f"降级解析时间范围：{num}{unit}")
        except Exception:
            # 忽略时间解析错误，不影响主流程
            pass

        return fp, s, warnings

    def infer(self, text: str, default_filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """核心入口：根据自然语言文本推断 filter_params 与 systems。

        返回：
        {
            "filter_params": Dict[str, Any],
            "systems": Optional[List[str]],
            "warnings": List[str],
            "metrics": {"llm_used": bool, "llm_status": str, "llm_latency_ms": int, "llm_model": str}
        }
        """
        prompt = self._compose_prompt(text)

        start = time.time()
        parsed, raw_text = self._call_llm(prompt)
        latency_ms = int((time.time() - start) * 1000)

        warnings: List[str] = []
        llm_used = parsed is not None
        llm_status = "ok" if parsed is not None else "degraded"

        if not parsed:
            # 降级
            fp, s, w = self._fallback_parse(text, default_filters)
            warnings.extend(w)
            router = Router()
            normalized_fp, w_fp = router.validate_filter_params(fp)
            normalized_sys, w_sys = router.validate_systems(s)
            warnings.extend(w_fp)
            warnings.extend(w_sys)
            return {
                "filter_params": normalized_fp,
                "systems": normalized_sys,
                "warnings": warnings,
                "metrics": {
                    "llm_used": llm_used,
                    "llm_status": llm_status,
                    "llm_latency_ms": latency_ms,
                    "llm_model": get_model_config()["model"],
                },
            }

        # 正常路径：解析 JSON 中的字段
        try:
            fp_raw = parsed.get("filter_params", {}) if isinstance(parsed, dict) else {}
            systems_raw = parsed.get("systems") if isinstance(parsed, dict) else None
            timeout_raw = parsed.get("timeout_ms") if isinstance(parsed, dict) else None

            router = Router()
            normalized_fp, w_fp = router.validate_filter_params(fp_raw)
            normalized_sys, w_sys = router.validate_systems(systems_raw)
            warnings.extend(w_fp)
            warnings.extend(w_sys)

            # 若 LLM 正常解析但缺少关键字段，则按需使用降级解析进行“补齐”，不覆盖已有值
            try:
                fp_fb, sys_fb, w_fb = self._fallback_parse(text, default_filters)
                # 时间范围补齐（仅在缺失时）
                for key in ("date_from", "date_to"):
                    if key not in normalized_fp and isinstance(fp_fb, dict) and fp_fb.get(key):
                        normalized_fp[key] = fp_fb[key]
                # 实体类型补齐（仅在缺失时）
                if "entity_type" not in normalized_fp and isinstance(fp_fb, dict) and fp_fb.get("entity_type"):
                    normalized_fp["entity_type"] = fp_fb["entity_type"]
                # 系统集合补齐（仅在缺失时）
                if not normalized_sys and sys_fb:
                    normalized_sys = sys_fb
                if w_fb:
                    warnings.extend(w_fb)
            except Exception:
                # 补齐失败不影响主路径
                pass

            result: Dict[str, Any] = {
                "filter_params": normalized_fp,
                "systems": normalized_sys,
                "warnings": warnings,
                "metrics": {
                    "llm_used": llm_used,
                    "llm_status": llm_status,
                    "llm_latency_ms": latency_ms,
                    "llm_model": get_model_config()["model"],
                },
            }

            # 透传可选的 timeout_ms（在上层入口做范围裁剪）
            if isinstance(timeout_raw, int):
                result["timeout_ms"] = timeout_raw
            return result
        except Exception as e:
            logger.error(f"LLMProxy: 解析 LLM JSON 失败，降级。错误：{e}")
            fp, s, w = self._fallback_parse(text, default_filters)
            warnings.extend(w)
            router = Router()
            normalized_fp, w_fp = router.validate_filter_params(fp)
            normalized_sys, w_sys = router.validate_systems(s)
            warnings.extend(w_fp)
            warnings.extend(w_sys)
            return {
                "filter_params": normalized_fp,
                "systems": normalized_sys,
                "warnings": warnings,
                "metrics": {
                    "llm_used": llm_used,
                    "llm_status": "degraded",
                    "llm_latency_ms": latency_ms,
                    "llm_model": get_model_config()["model"],
                },
            }


__all__ = [
    "LLMProxy",
    "FILTER_PARAMS_JSON_SCHEMA",
    "validate_filter_params_for_llm",
]