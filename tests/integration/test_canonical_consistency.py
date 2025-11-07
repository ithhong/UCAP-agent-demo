"""
Agent集成测试：Canonical统一性与关键约束验证

作者: Tom
创建时间: 2025-11-07T17:05:04+08:00
"""

from datetime import datetime
from decimal import Decimal

from agents.erp import ERPAgent
from agents.hr import HRAgent
from agents.fin import FINAgent
from canonical.models import (
    Organization, Person, Customer, Transaction,
    OrganizationType, StatusType, SystemType
)


def _get_agents():
    """构造待测的三类 Agent"""
    return (
        ERPAgent(),
        HRAgent(),
        FINAgent(),
    )


def _map(agent):
    """拉取并映射为 Canonical 结构"""
    raw = agent.pull_raw()
    return agent.map_to_canonical(raw)


def test_canonical_entity_presence_and_types():
    """
    实体存在与类型校验：四类实体均存在且类型符合 Canonical 模型
    """
    expected_keys = {"organizations", "persons", "customers", "transactions"}

    for agent in _get_agents():
        mapped = _map(agent)
        assert set(mapped.keys()) == expected_keys

        orgs = mapped["organizations"]
        persons = mapped["persons"]
        customers = mapped["customers"]
        txns = mapped["transactions"]

        assert len(orgs) > 0
        assert len(persons) > 0
        assert len(customers) > 0
        assert len(txns) > 0

        assert isinstance(orgs[0], Organization)
        assert isinstance(persons[0], Person)
        assert isinstance(customers[0], Customer)
        assert isinstance(txns[0], Transaction)


def test_canonical_transaction_constraints():
    """
    交易约束：
    - currency 必须为 CNY
    - amount 为 Decimal 且 > 0
    - created_at 为 datetime；tx_date 若存在则为 datetime
    - status 为 StatusType 枚举
    """
    for agent in _get_agents():
        txns = _map(agent)["transactions"]
        assert len(txns) > 0

        for tx in txns[:30]:  # 采样前30条
            assert isinstance(tx, Transaction)
            assert tx.currency == "CNY"
            # HR 事务可能为流程类事务（如培训/请假）可出现 0 金额；ERP/FIN 必须 > 0
            if tx.source_system == SystemType.HR:
                assert isinstance(tx.amount, Decimal) and tx.amount >= 0
            else:
                assert isinstance(tx.amount, Decimal) and tx.amount > 0
            assert isinstance(tx.created_at, datetime)
            if tx.tx_date:
                assert isinstance(tx.tx_date, datetime)
            assert isinstance(tx.status, StatusType)


def test_canonical_organization_constraints():
    """
    组织约束：
    - org_type 在允许集合 {company, dept, team, cost_center}
    - org_id 非空字符串
    - created_at 为 datetime
    """
    allowed_types = {
        OrganizationType.COMPANY,
        OrganizationType.DEPARTMENT,
        OrganizationType.TEAM,
        OrganizationType.COST_CENTER,
    }

    for agent in _get_agents():
        orgs = _map(agent)["organizations"]
        assert len(orgs) > 0
        for org in orgs[:30]:
            assert isinstance(org, Organization)
            assert org.org_type in allowed_types
            assert isinstance(org.org_id, str) and org.org_id.strip() != ""
            assert isinstance(org.created_at, datetime)


def test_canonical_person_customer_constraints():
    """
    人员与客户约束：
    - 主键 ID 非空字符串
    - created_at 为 datetime
    """
    for agent in _get_agents():
        persons = _map(agent)["persons"]
        customers = _map(agent)["customers"]

        assert len(persons) > 0
        assert len(customers) > 0

        for p in persons[:30]:
            assert isinstance(p, Person)
            assert isinstance(p.person_id, str) and p.person_id.strip() != ""
            assert isinstance(p.created_at, datetime)

        for c in customers[:30]:
            assert isinstance(c, Customer)
            assert isinstance(c.customer_id, str) and c.customer_id.strip() != ""
            assert isinstance(c.created_at, datetime)