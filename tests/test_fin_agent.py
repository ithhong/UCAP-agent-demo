"""
FINAgent测试
作者: Tom
创建时间: 2025-11-06T17:35:20+08:00
"""

from decimal import Decimal
from datetime import datetime

from agents.fin import FINAgent
from canonical.models import Organization, Person, Customer, Transaction


def test_pull_raw_fin_agent_entities():
    agent = FINAgent()
    raw = agent.pull_raw()
    assert isinstance(raw, list)
    assert len(raw) > 0

    entities = {item.get("_entity") for item in raw}
    assert {"organization", "person", "customer", "transaction"}.issubset(entities)


def test_map_to_canonical_fin_agent_shapes():
    agent = FINAgent()
    raw = agent.pull_raw()
    mapped = agent.map_to_canonical(raw)

    assert set(mapped.keys()) == {"organizations", "persons", "customers", "transactions"}

    # 组织
    orgs = mapped["organizations"]
    assert len(orgs) > 0
    assert isinstance(orgs[0], Organization)

    # 人员
    persons = mapped["persons"]
    assert len(persons) > 0
    assert isinstance(persons[0], Person)

    # 客户
    customers = mapped["customers"]
    assert len(customers) > 0
    assert isinstance(customers[0], Customer)

    # 交易
    txns = mapped["transactions"]
    assert len(txns) > 0
    assert isinstance(txns[0], Transaction)


def test_fin_agent_transaction_amount_and_dates():
    agent = FINAgent()
    raw = agent.pull_raw()
    mapped = agent.map_to_canonical(raw)

    txns = mapped["transactions"]
    assert len(txns) > 0
    for tx in txns[:10]:  # 检查前10条
        assert isinstance(tx.amount, Decimal)
        if tx.tx_date:
            assert isinstance(tx.tx_date, datetime)