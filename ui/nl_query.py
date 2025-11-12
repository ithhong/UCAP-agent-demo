"""
Streamlit 自然语言查询入口（方案B）

功能：
- 文本输入自然语言，调用 orchestrator.nl_query 获取跨系统聚合结果
- 可选侧边栏参数：systems、entity_type、limit、timeout_ms
- 展示 warnings/errors/metrics 以及各实体的数量与原始数据（可展开）

作者: Tom
创建时间: 2025-11-11T18:07:08+08:00 (Asia/Shanghai)
"""

import sys
from pathlib import Path

# 保障在以 Streamlit 或 IDE 方式运行时，能够解析到项目根目录的包
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st
from typing import Any, Dict, List, Optional
import pandas as pd
from datetime import datetime

from orchestrator import nl_query
from orchestrator.router import SUPPORTED_ENTITY_TYPES


st.set_page_config(page_title="UCAP NL Query", layout="wide")
st.title("UCAP 查询系统")
st.caption("输入你的查询意图，系统将解析并跨系统聚合结果。")


def to_dict(item: Any) -> Any:
    try:
        # Pydantic v2
        return item.model_dump()
    except Exception:
        try:
            return item.__dict__
        except Exception:
            return str(item)


with st.sidebar:
    st.header("高级参数")
    systems: List[str] = st.multiselect("选择系统", options=["erp", "hr", "fin"], default=[])
    entity_type: Optional[str] = st.selectbox("实体类型", options=[""] + SUPPORTED_ENTITY_TYPES, index=0)
    limit: int = st.number_input("返回上限 (1..1000)", min_value=1, max_value=1000, value=50, step=1)
    timeout_ms: int = st.number_input("并发超时 (ms)", min_value=50, max_value=60000, value=5000, step=50)

text: str = st.text_area("查询文本", placeholder="例如：查询近三个月的财务流水，限制100条")

run = st.button("运行查询")

if run:
    default_filters: Dict[str, Any] = {}
    if entity_type:
        default_filters["entity_type"] = entity_type
    if limit:
        default_filters["limit"] = int(limit)

    final_systems = systems if systems else None
    result = nl_query(
        text=text,
        default_filters=default_filters or None,
        systems=final_systems,
        timeout_ms=int(timeout_ms),
    )

    st.subheader("告警与错误")
    st.write({
        "warnings": result.get("warnings", []),
        "errors": result.get("errors", []),
    })

    st.subheader("指标与统计")
    st.write(result.get("metrics", {}))

    st.subheader("结果（数量）")
    counts = {}
    for k in ("organizations", "persons", "customers", "transactions"):
        counts[k] = len(result.get(k, []))
    st.write(counts)

    # 图表展示区域
    st.subheader("图表展示")

    # 实体计数柱状图
    try:
        counts_df = pd.DataFrame({
            "entity": ["organizations", "persons", "customers", "transactions"],
            "count": [
                counts.get("organizations", 0),
                counts.get("persons", 0),
                counts.get("customers", 0),
                counts.get("transactions", 0),
            ],
        })
        counts_df = counts_df.set_index("entity")
        st.bar_chart(counts_df, use_container_width=True)
    except Exception as e:
        st.warning(f"实体计数柱状图绘制失败: {e}")

    # 交易数据图表
    tx_list = result.get("transactions", [])
    if tx_list:
        # 准备金额序列与日期序列
        amounts: List[float] = []
        dates: List[datetime] = []
        for tx in tx_list:
            try:
                amt = float(getattr(tx, "amount", 0))
                dt = getattr(tx, "tx_date", None)
                if dt is None:
                    continue
                amounts.append(amt)
                # 确保为 datetime 类型
                if isinstance(dt, datetime):
                    dates.append(dt)
                else:
                    # 尝试解析字符串时间
                    dates.append(pd.to_datetime(dt))
            except Exception:
                # 跳过异常条目，避免阻断整体绘制
                continue

        # 金额分布直方图
        try:
            if len(amounts) > 0:
                amt_series = pd.Series(amounts, name="amount")
                # 使用 10 等距分箱；如需要可后续改为自适应分箱
                bins = pd.cut(amt_series, bins=10, precision=2)
                hist_df = bins.value_counts().sort_index().rename_axis("bin").reset_index(name="count")
                hist_df = hist_df.set_index("bin")
                st.caption("交易金额分布（直方图，10 分箱）")
                st.bar_chart(hist_df, use_container_width=True)
            else:
                st.info("暂无可用于分布的交易金额数据")
        except Exception as e:
            st.warning(f"交易金额分布直方图绘制失败: {e}")

        # 交易金额时间序列折线（按日聚合总额）
        try:
            if len(dates) > 0 and len(amounts) == len(dates):
                df = pd.DataFrame({
                    "tx_date": pd.to_datetime(dates).floor("D"),
                    "amount": amounts,
                })
                agg_sum = df.groupby("tx_date")["amount"].sum()
                agg_cnt = df.groupby("tx_date")["amount"].count().rename("count")

                st.caption("交易金额时间序列（按日总额）")
                st.line_chart(agg_sum, use_container_width=True)

                st.caption("交易数量时间序列（按日条数）")
                st.line_chart(agg_cnt, use_container_width=True)
            else:
                st.info("暂无可用于时间序列的交易数据")
        except Exception as e:
            st.warning(f"交易金额时间序列绘制失败: {e}")
    else:
        st.info("当前筛选条件下无交易数据，图表暂不展示")

    with st.expander("查看原始数据（organizations）"):
        st.json([to_dict(x) for x in result.get("organizations", [])])
    with st.expander("查看原始数据（persons）"):
        st.json([to_dict(x) for x in result.get("persons", [])])
    with st.expander("查看原始数据（customers）"):
        st.json([to_dict(x) for x in result.get("customers", [])])
    with st.expander("查看原始数据（transactions）"):
        st.json([to_dict(x) for x in result.get("transactions", [])])

    # LLM 指标图表（会话级追踪）
    st.subheader("LLM 指标图表")
    llm_metrics = result.get("metrics", {}).get("llm", {})
    latency = llm_metrics.get("llm_latency_ms")
    status = llm_metrics.get("llm_status")

    # 初始化会话状态
    if "llm_latency_history" not in st.session_state:
        st.session_state["llm_latency_history"] = []
    if "llm_status_history" not in st.session_state:
        st.session_state["llm_status_history"] = []

    # 追加当前查询的指标
    try:
        if isinstance(latency, (int, float)):
            st.session_state["llm_latency_history"].append(float(latency))
        if isinstance(status, str) and status:
            st.session_state["llm_status_history"].append(status)
    except Exception:
        pass

    # 绘制延迟折线（以查询序号为横轴）
    try:
        if st.session_state["llm_latency_history"]:
            lat_df = pd.DataFrame({
                "run": list(range(1, len(st.session_state["llm_latency_history"]) + 1)),
                "latency_ms": st.session_state["llm_latency_history"],
            }).set_index("run")
            st.caption("LLM 延迟（ms）")
            st.line_chart(lat_df, use_container_width=True)
        else:
            st.info("暂无 LLM 延迟数据")
    except Exception as e:
        st.warning(f"LLM 延迟折线绘制失败: {e}")

    # 绘制状态分布（柱状图代替饼图）
    try:
        if st.session_state["llm_status_history"]:
            status_series = pd.Series(st.session_state["llm_status_history"], name="status")
            status_df = status_series.value_counts().rename_axis("status").reset_index(name="count").set_index("status")
            st.caption("LLM 状态分布（used/degraded/bypassed 等）")
            st.bar_chart(status_df, use_container_width=True)
        else:
            st.info("暂无 LLM 状态分布数据")
    except Exception as e:
        st.warning(f"LLM 状态分布绘制失败: {e}")