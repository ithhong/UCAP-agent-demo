"""
FIN系统Agent
读取SQLite中的FIN数据，映射为Canonical统一模型

作者: Tom
创建时间: 2025-11-06T17:35:20+08:00
"""

import sqlite3
from typing import List, Dict, Any
from loguru import logger

from agents.base import BaseAgent, DataSourceError, DataMappingError
from canonical.models import (
    Organization, Person, Customer, Transaction,
    SystemType
)
from canonical.mapper import DataMapper


class FINAgent(BaseAgent):
    """
    FIN系统Agent
    - 从SQLite数据库读取FIN原始数据
    - 使用DataMapper标准化并映射到Canonical模型
    """

    def __init__(self):
        super().__init__(system_name="财务系统", system_type=SystemType.FIN)
        self.db_path = self.settings.database_path

    def pull_raw(self) -> List[Dict[str, Any]]:
        """
        拉取FIN原始数据（组织、人员、客户、交易）

        Returns:
            包含entity标记的原始数据列表
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            raw: List[Dict[str, Any]] = []

            # 组织
            cursor.execute("SELECT * FROM fin_organizations")
            org_rows = cursor.fetchall()
            for row in org_rows:
                item = dict(row)
                item["_entity"] = "organization"
                raw.append(item)

            # 人员
            cursor.execute("SELECT * FROM fin_persons")
            person_rows = cursor.fetchall()
            for row in person_rows:
                item = dict(row)
                item["_entity"] = "person"
                raw.append(item)

            # 客户
            cursor.execute("SELECT * FROM fin_customers")
            cust_rows = cursor.fetchall()
            for row in cust_rows:
                item = dict(row)
                item["_entity"] = "customer"
                raw.append(item)

            # 交易
            cursor.execute("SELECT * FROM fin_transactions")
            txn_rows = cursor.fetchall()
            for row in txn_rows:
                item = dict(row)
                item["_entity"] = "transaction"
                raw.append(item)

            conn.close()

            logger.info(
                f"FIN原始数据拉取完成: 组织={len(org_rows)}, 人员={len(person_rows)}, 客户={len(cust_rows)}, 交易={len(txn_rows)}"
            )

            return raw

        except Exception as e:
            logger.error(f"FIN数据源访问失败: {e}")
            raise DataSourceError(f"FIN数据源访问失败: {e}")

    def map_to_canonical(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
        """
        将FIN原始数据映射为Canonical模型

        Args:
            raw_data: 带有`_entity`标记的原始数据列表

        Returns:
            各实体的Canonical列表字典
        """
        try:
            mapper = DataMapper()

            organizations: List[Organization] = []
            persons: List[Person] = []
            customers: List[Customer] = []
            transactions: List[Transaction] = []

            # 先构建索引以支持跨表关联
            org_code_to_id: Dict[str, str] = {}
            person_index_by_id: Dict[str, Dict[str, Any]] = {}
            customer_index_by_id: Dict[str, Dict[str, Any]] = {}

            for item in raw_data:
                entity = item.get("_entity")
                if entity == "organization":
                    fin_org_id = item.get("fin_org_id")
                    org_code = item.get("org_code")
                    if fin_org_id and org_code:
                        org_code_to_id[org_code] = mapper.normalize_id(fin_org_id, "fin")
                elif entity == "person":
                    pid = item.get("fin_person_id")
                    if pid:
                        person_index_by_id[pid] = item
                elif entity == "customer":
                    cid = item.get("fin_customer_id")
                    if cid:
                        customer_index_by_id[cid] = item

            # 再进行实体映射
            for item in raw_data:
                entity = item.get("_entity")

                if entity == "organization":
                    org = Organization(
                        org_id=mapper.normalize_id(item.get("fin_org_id"), "fin"),
                        source_system=SystemType.FIN,
                        org_name=item.get("org_name"),
                        org_type=mapper.normalize_org_type(item.get("org_type"), item.get("org_name")),
                        parent_org_id=(
                            mapper.normalize_id(item.get("parent_org_id"), "fin")
                            if item.get("parent_org_id") else None
                        ),
                        org_code=item.get("org_code"),
                        manager_name=mapper.clean_text(item.get("manager_name")),
                        address=mapper.clean_text(item.get("address")),
                        status=mapper.normalize_status(item.get("status"), "fin"),
                        created_at=mapper.normalize_date(item.get("created_time"), "fin")
                                   or mapper.normalize_date(item.get("updated_time"), "fin"),
                        raw_data=item,
                    )
                    organizations.append(org)

                elif entity == "person":
                    cost_center_code = item.get("cost_center")
                    mapped_org_id = org_code_to_id.get(cost_center_code) if cost_center_code else None
                    if not mapped_org_id and cost_center_code:
                        # 回退到以成本中心编码作为ID（尽量保证必填字段有值）
                        mapped_org_id = mapper.normalize_id(cost_center_code, "fin")

                    person = Person(
                        person_id=mapper.normalize_id(item.get("fin_person_id"), "fin"),
                        source_system=SystemType.FIN,
                        person_name=item.get("person_name"),
                        employee_number=item.get("employee_code"),
                        org_id=mapped_org_id or mapper.normalize_id("unknown_org", "fin"),
                        position=mapper.clean_text(item.get("position")),
                        department=mapper.clean_text(item.get("department")),
                        email=mapper.validate_email(item.get("email")),
                        phone=mapper.validate_phone(item.get("phone")),
                        hire_date=mapper.normalize_date(item.get("hire_date"), "fin"),
                        status=mapper.normalize_status(item.get("status"), "fin"),
                        created_at=mapper.normalize_date(item.get("created_time"), "fin")
                                   or mapper.normalize_date(item.get("updated_time"), "fin")
                                   or mapper.normalize_date(item.get("hire_date"), "fin"),
                        raw_data=item,
                    )
                    persons.append(person)

                elif entity == "customer":
                    customer = Customer(
                        customer_id=mapper.normalize_id(item.get("fin_customer_id"), "fin"),
                        source_system=SystemType.FIN,
                        customer_name=item.get("customer_name"),
                        customer_code=item.get("customer_code"),
                        customer_type=item.get("customer_type"),
                        tax_num=item.get("tax_number"),
                        industry=item.get("industry"),
                        contact_person=mapper.clean_text(item.get("contact_person")),
                        contact_phone=mapper.validate_phone(item.get("contact_phone")),
                        contact_email=mapper.validate_email(item.get("contact_email")),
                        address=mapper.clean_text(item.get("billing_address")),
                        credit_level=mapper.clean_text(item.get("credit_rating")),
                        status=mapper.normalize_status(item.get("status"), "fin"),
                        created_at=mapper.normalize_date(item.get("registration_date"), "fin")
                                   or mapper.normalize_date(item.get("created_time"), "fin")
                                   or mapper.normalize_date(item.get("updated_time"), "fin"),
                        raw_data=item,
                    )
                    customers.append(customer)

                elif entity == "transaction":
                    cost_center_code = item.get("cost_center")
                    mapped_org_id = org_code_to_id.get(cost_center_code) if cost_center_code else None
                    if not mapped_org_id and cost_center_code:
                        mapped_org_id = mapper.normalize_id(cost_center_code, "fin")

                    # 人员关联优先使用created_by，其次approver
                    person_id_raw = item.get("created_by") or item.get("approver")

                    txn = Transaction(
                        tx_id=mapper.normalize_id(item.get("fin_transaction_id"), "fin"),
                        source_system=SystemType.FIN,
                        transaction_number=item.get("transaction_number"),
                        tx_type=mapper.normalize_transaction_type(item.get("transaction_type"), "fin"),
                        amount=mapper.normalize_amount(item.get("amount")),
                        currency="CNY",  # Canonical仅支持CNY，统一回落
                        tx_date=mapper.normalize_date(item.get("transaction_date"), "fin"),
                        customer_id=(
                            mapper.normalize_id(item.get("customer_id"), "fin")
                            if item.get("customer_id") else None
                        ),
                        person_id=(
                            mapper.normalize_id(person_id_raw, "fin") if person_id_raw else None
                        ),
                        org_id=mapped_org_id or mapper.normalize_id("unknown_org", "fin"),
                        description=mapper.clean_text(item.get("description")),
                        product_info=None,
                        payment_method=mapper.clean_text(item.get("payment_method")),
                        status=mapper.normalize_status(item.get("status"), "fin"),
                        created_at=mapper.normalize_date(item.get("created_time"), "fin")
                                   or mapper.normalize_date(item.get("value_date"), "fin")
                                   or mapper.normalize_date(item.get("transaction_date"), "fin"),
                        raw_data=item,
                    )
                    transactions.append(txn)

            return {
                "organizations": organizations,
                "persons": persons,
                "customers": customers,
                "transactions": transactions,
            }

        except Exception as e:
            logger.error(f"FIN数据映射失败: {e}")
            raise DataMappingError(f"FIN数据映射失败: {e}")


__all__ = ["FINAgent"]