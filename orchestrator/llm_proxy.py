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
        """构造提示词，要求按 JSON Schema 输出工具参数，并强化时间字段输出约束。"""
        schema_str = json.dumps(FILTER_PARAMS_JSON_SCHEMA, ensure_ascii=False)
        supported_systems = ", ".join(SUPPORTED_SYSTEMS.keys())
        return (
            "你是一名企业数据协作助手，需要为一个跨系统查询工具生成 JSON 参数。"\
            "\n只输出一个 JSON 对象，不要包含多余文字。禁止输出解释性文本或代码块标记。"\
            "\n支持的系统: " + supported_systems +
            "\nfilter_params 的 JSON Schema 如下（其它未知键允许保留）：\n" + schema_str +
            "\n请严格遵守以下约束："\
            "\n1) 当用户文本包含任何时间含义时，必须在 'filter_params' 中给出 'date_from' 与 'date_to'，并使用 ISO8601 字符串（如 '2024-01-01T00:00:00Z' 或 '2024-01-01'）。"\
            "\n2) 对中文相对时间表达进行归一：例如 '近N年/近N月/近N周/近N天/最近N…/过去N…/近两年/近两月/近3个月/近1个星期'，转换为区间：'date_to=今天'，'date_from=今天减去对应跨度'。"\
            "\n3) 若无法确定时间，请不要输出 'date_from' 或 'date_to' 这两个键，以便后续组件自动补齐；禁止输出 null。"\
            "\n4) 'systems'（如有）只能取自支持的系统集合；'timeout_ms'（如有）为 50..60000 的整数。"\
            "\n只返回一个 JSON，如：{\"filter_params\":{...},\"systems\":[...],\"timeout_ms\":1234}。"\
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

    def _compose_prompt_time_narrow(self, text: str) -> str:
        """构造窄域时间抽取提示词，仅输出时间区间。"""
        now_iso = datetime.now().isoformat()
        return (
            "请只输出一个 JSON 对象，格式为 {\"date_from\":\"...\",\"date_to\":\"...\"}。"\
            f"\n当前时间（ISO8601，作为相对时间的锚点）：{now_iso}"\
            "\n规则：\n- 周起始为周一；\n- 相对时间如近N年/月/周/天/季度，以当前时间为终点并向前回溯；\n- 今天/昨天/前天为单日区间；\n- 本周/本月/本季度起点为各自然周期起点；上周/上月/上季度为完整周期。"\
            "\n若无法确定时间，请不要输出任何键。"\
            "\n用户输入：" + text
        )

    def _extract_time_narrow(self, text: str) -> Tuple[Optional[Dict[str, str]], int]:
        """窄域时间抽取：调用 LLM 仅提取时间区间。

        返回 (time_fp, latency_ms)。time_fp 为包含 date_from/date_to 的字典，若无法解析则为 None。
        """
        prompt = self._compose_prompt_time_narrow(text)
        start = time.time()
        parsed, raw_text = self._call_llm(prompt)
        latency_ms = int((time.time() - start) * 1000)

        def pick_dates(obj: Dict[str, Any]) -> Optional[Dict[str, str]]:
            if not isinstance(obj, dict):
                return None
            # 兼容直接输出或嵌套在 filter_params 内的两种形态
            if obj.get("date_from") and obj.get("date_to"):
                return {"date_from": str(obj.get("date_from")), "date_to": str(obj.get("date_to"))}
            fp = obj.get("filter_params") if isinstance(obj.get("filter_params"), dict) else None
            if fp and fp.get("date_from") and fp.get("date_to"):
                return {"date_from": str(fp.get("date_from")), "date_to": str(fp.get("date_to"))}
            return None

        if parsed:
            picked = pick_dates(parsed)
            if picked:
                return picked, latency_ms

        # 尝试从原始文本中提取 JSON
        if isinstance(raw_text, str):
            try:
                m = re.search(r"\{[\s\S]*\}", raw_text)
                if m:
                    obj = json.loads(m.group(0))
                    picked = pick_dates(obj)
                    if picked:
                        return picked, latency_ms
            except Exception:
                pass

        return None, latency_ms

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

        # 解析中文固定与相对时间范围
        # 先匹配固定表达：本周/上周、本月/上月、今年/去年/前年、今天/昨天/前天、本季度/上季度、近半年/近1个季度
        # 再匹配相对表达：近N年/月/周/天
        # 注意：仅在缺失时补齐 date_from/date_to，避免覆盖主路径结果
        try:
            now = datetime.now()
            # 固定表达归一
            try:
                # 周起始按“周一”为起点
                weekday = now.weekday()  # 0=周一
                start_of_week = now - relativedelta(days=weekday)

                def set_range(df: datetime, dt: datetime, label: str) -> None:
                    if "date_from" not in fp:
                        fp["date_from"] = df.isoformat()
                    if "date_to" not in fp:
                        fp["date_to"] = dt.isoformat()
                    warnings.append(f"降级解析固定时间：{label}")

                # 本周：从本周周一到今天
                if re.search(r"(本周|这周|这一周)", text):
                    set_range(start_of_week, now, "本周")

                # 上周：从上周周一到上周周日
                elif re.search(r"(上周|上一周|上星期|上个星期)", text):
                    start_of_last_week = start_of_week - relativedelta(days=7)
                    end_of_last_week = start_of_week - relativedelta(days=1)
                    set_range(start_of_last_week, end_of_last_week, "上周")

                # 本月：从本月1日到今天
                elif re.search(r"(本月|这个月|当月)", text):
                    start_of_month = datetime(now.year, now.month, 1)
                    set_range(start_of_month, now, "本月")

                # 上月：从上月1日到上月最后一天
                elif re.search(r"(上月|上个月)", text):
                    start_of_month = datetime(now.year, now.month, 1)
                    start_of_last_month = start_of_month - relativedelta(months=1)
                    end_of_last_month = start_of_month - relativedelta(days=1)
                    set_range(start_of_last_month, end_of_last_month, "上月")

                # 今年：从当年1月1日至今天
                elif re.search(r"(今年|本年|当年)", text):
                    start_of_year = datetime(now.year, 1, 1)
                    set_range(start_of_year, now, "今年")

                # 去年：跨整年
                elif re.search(r"(去年)", text):
                    start_last_year = datetime(now.year - 1, 1, 1)
                    end_last_year = datetime(now.year - 1, 12, 31)
                    set_range(start_last_year, end_last_year, "去年")

                # 前年：跨整年
                elif re.search(r"(前年)", text):
                    start_prev_year = datetime(now.year - 2, 1, 1)
                    end_prev_year = datetime(now.year - 2, 12, 31)
                    set_range(start_prev_year, end_prev_year, "前年")

                # 本季度：从当季首日至今天
                elif re.search(r"(本季度|这个季度|当季)", text):
                    q_start_month = ((now.month - 1) // 3) * 3 + 1
                    start_of_quarter = datetime(now.year, q_start_month, 1)
                    set_range(start_of_quarter, now, "本季度")

                # 上季度：上季完整区间
                elif re.search(r"(上季度|上一季度|上季)", text):
                    q_start_month = ((now.month - 1) // 3) * 3 + 1
                    start_of_quarter = datetime(now.year, q_start_month, 1)
                    start_of_last_quarter = start_of_quarter - relativedelta(months=3)
                    end_of_last_quarter = start_of_quarter - relativedelta(days=1)
                    set_range(start_of_last_quarter, end_of_last_quarter, "上季度")

                # 今天/昨天/前天：单日区间（起止同日）
                elif re.search(r"(今天|今日|当天)", text):
                    day = datetime(now.year, now.month, now.day)
                    set_range(day, day, "今天")
                elif re.search(r"(昨天|昨日)", text):
                    day = datetime(now.year, now.month, now.day) - relativedelta(days=1)
                    set_range(day, day, "昨天")
                elif re.search(r"(前天)", text):
                    day = datetime(now.year, now.month, now.day) - relativedelta(days=2)
                    set_range(day, day, "前天")

                # 近半年：今天减去 6 个月
                elif re.search(r"(近半年|最近半年|过去半年)", text):
                    start = now - relativedelta(months=6)
                    set_range(start, now, "近半年")

                # 近1个季度/近一个季度/近一季：今天减去 3 个月
                elif re.search(r"(近1个季度|近一个季度|近一季|最近1个季度|过去1个季度)", text):
                    start = now - relativedelta(months=3)
                    set_range(start, now, "近一个季度")
            except Exception:
                # 固定表达识别失败不影响后续相对解析
                pass

            # 相对表达：近N年/月/天/周
            num = None
            unit = None

            # 优先匹配格式：近|最近|过去 + (中文/阿拉伯数字) + (年|月|天|日|周|星期)
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
                    if s.isdigit():
                        return int(s)
                    # 处理“十X”或“X十”
                    if s.startswith("十") and len(s) > 1:
                        return 10 + mapping.get(s[1], 0)
                    if s.endswith("十") and len(s) > 1:
                        return mapping.get(s[0], 0) * 10
                    # 单字或“十”“百”
                    if s in mapping:
                        return mapping[s]
                    # 逐字累加（如“二三”→23 的情况不常见，保守做逐字相加）
                    total = 0
                    for ch in s:
                        total += mapping.get(ch, 0)
                    return max(1, total)

                num = chinese_to_int(raw_num)

            # 次要匹配："近3个月" 等英文形式
            if not num or not unit:
                m2 = re.search(r"(近|最近|过去)\s*(\d+)\s*(个)?\s*(年|月|天|日|周|星期)", text)
                if m2:
                    num = int(m2.group(2))
                    unit = m2.group(4)

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
                    if "date_from" not in fp:
                        fp["date_from"] = date_from.isoformat()
                    if "date_to" not in fp:
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
        time_narrow_used: bool = False
        time_narrow_latency_ms: int = 0
        time_anchor_override_used: bool = False
        time_anchor_latency_ms: int = 0

        if not parsed:
            # 降级
            fp, s, w = self._fallback_parse(text, default_filters)
            warnings.extend(w)
            router = Router()
            normalized_fp, w_fp = router.validate_filter_params(fp)
            normalized_sys, w_sys = router.validate_systems(s)
            warnings.extend(w_fp)
            warnings.extend(w_sys)
            # 若依然缺失时间且开启窄域提取，则调用窄域 LLM 进行补齐
            if settings.enable_time_enhancements and settings.enable_narrow_time_llm:
                missing_from = not normalized_fp.get("date_from")
                missing_to = not normalized_fp.get("date_to")
                if missing_from or missing_to:
                    picked, latency_ms2 = self._extract_time_narrow(text)
                    time_narrow_latency_ms = latency_ms2
                    if picked and picked.get("date_from") and picked.get("date_to"):
                        normalized_fp["date_from"] = picked["date_from"]
                        normalized_fp["date_to"] = picked["date_to"]
                        time_narrow_used = True
                        warnings.append("Narrow 时间抽取触发：主/降级路径均缺失时间，采用窄域 LLM 提取")
                        logger.info(
                            f"LLMProxy: 窄域时间提取触发（降级路径），区间 {picked['date_from']}..{picked['date_to']}，耗时 {latency_ms2}ms"
                        )
            return {
                "filter_params": normalized_fp,
                "systems": normalized_sys,
                "warnings": warnings,
                "metrics": {
                    "llm_used": llm_used,
                    "llm_status": llm_status,
                    "llm_latency_ms": latency_ms,
                    "llm_model": get_model_config()["model"],
                    "time_narrow_used": time_narrow_used,
                    "time_narrow_latency_ms": time_narrow_latency_ms,
                    "time_anchor_override_used": time_anchor_override_used,
                    "time_anchor_latency_ms": time_anchor_latency_ms,
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

            # 若 LLM 正常解析但缺少关键字段，则按需使用降级解析进行补齐；
            # 同时引入“服务端时间锚定纠偏”，当识别到固定/相对时间表达时覆盖 LLM 的时间区间。
            try:
                _fb_start = time.time()
                fp_fb, sys_fb, w_fb = self._fallback_parse(text, default_filters)
                time_anchor_latency_ms = int((time.time() - _fb_start) * 1000)
                # 服务端时间锚定纠偏：若识别到固定/相对时间表达
                fallback_has_time = (
                    isinstance(fp_fb, dict)
                    and bool(fp_fb.get("date_from"))
                    and bool(fp_fb.get("date_to"))
                )
                if fallback_has_time:
                    if settings.enable_time_enhancements:
                        # 总开关开启：覆盖 LLM 的 date_from/date_to
                        normalized_fp["date_from"] = fp_fb["date_from"]
                        normalized_fp["date_to"] = fp_fb["date_to"]
                        time_anchor_override_used = True
                        warnings.append("LLM 时间感知纠偏：覆盖 LLM 时间区间为服务端计算值")
                        logger.warning(
                            f"LLMProxy: 时间锚定纠偏触发，覆盖为 {fp_fb['date_from']}..{fp_fb['date_to']}，fallback耗时 {time_anchor_latency_ms}ms"
                        )
                    else:
                        # 总开关关闭：仅在缺失时补齐，不覆盖已有值
                        for key in ("date_from", "date_to"):
                            if key not in normalized_fp and fp_fb.get(key):
                                normalized_fp[key] = fp_fb[key]
                                warnings.append("LLM 时间感知纠偏关闭：仅补齐缺失的时间字段")
                else:
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

            # 若依然缺失时间且开启窄域提取，则调用窄域 LLM 进行补齐
            if settings.enable_time_enhancements and settings.enable_narrow_time_llm:
                missing_from = not normalized_fp.get("date_from")
                missing_to = not normalized_fp.get("date_to")
                if missing_from or missing_to:
                    picked, latency_ms2 = self._extract_time_narrow(text)
                    time_narrow_latency_ms = latency_ms2
                    if picked and picked.get("date_from") and picked.get("date_to"):
                        normalized_fp["date_from"] = picked["date_from"]
                        normalized_fp["date_to"] = picked["date_to"]
                        time_narrow_used = True
                        warnings.append("Narrow 时间抽取触发：主/降级路径均缺失时间，采用窄域 LLM 提取")
                        logger.info(
                            f"LLMProxy: 窄域时间提取触发（主路径补齐），区间 {picked['date_from']}..{picked['date_to']}，耗时 {latency_ms2}ms"
                        )

            result: Dict[str, Any] = {
                "filter_params": normalized_fp,
                "systems": normalized_sys,
                "warnings": warnings,
                "metrics": {
                    "llm_used": llm_used,
                    "llm_status": llm_status,
                    "llm_latency_ms": latency_ms,
                    "llm_model": get_model_config()["model"],
                    "time_narrow_used": time_narrow_used,
                    "time_narrow_latency_ms": time_narrow_latency_ms,
                    "time_anchor_override_used": time_anchor_override_used,
                    "time_anchor_latency_ms": time_anchor_latency_ms,
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
                    "time_narrow_used": time_narrow_used,
                    "time_narrow_latency_ms": time_narrow_latency_ms,
                    "time_anchor_override_used": time_anchor_override_used,
                    "time_anchor_latency_ms": time_anchor_latency_ms,
                },
            }


__all__ = [
    "LLMProxy",
    "FILTER_PARAMS_JSON_SCHEMA",
    "validate_filter_params_for_llm",
]