"""
HRAgent单元测试

作者: Tom
创建时间: 2025-11-06T16:57:00+08:00
"""

import unittest

from agents.hr import HRAgent
from canonical.models import Organization, Person, Customer, Transaction


class TestHRAgent(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.agent = HRAgent()

    def test_pull_raw_has_data(self):
        raw = self.agent.pull_raw()
        self.assertIsInstance(raw, list)
        self.assertGreater(len(raw), 0, "HR原始数据应不为空")

        # 检查是否包含四类实体标记
        kinds = {item.get("_entity") for item in raw}
        self.assertTrue({"organization", "person", "customer", "transaction"}.issubset(kinds))

    def test_map_to_canonical_shapes(self):
        raw = self.agent.pull_raw()
        mapped = self.agent.map_to_canonical(raw)

        self.assertIn("organizations", mapped)
        self.assertIn("persons", mapped)
        self.assertIn("customers", mapped)
        self.assertIn("transactions", mapped)

        # 基本类型校验
        orgs = mapped["organizations"]
        persons = mapped["persons"]
        customers = mapped["customers"]
        txns = mapped["transactions"]

        if orgs:
            self.assertIsInstance(orgs[0], Organization)
        if persons:
            self.assertIsInstance(persons[0], Person)
        if customers:
            self.assertIsInstance(customers[0], Customer)
        if txns:
            self.assertIsInstance(txns[0], Transaction)


if __name__ == "__main__":
    unittest.main()