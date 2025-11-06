"""
HR系统Agent
读取SQLite中的HR数据，映射为Canonical统一模型

作者: Tom
创建时间: 2025-11-06T16:57:00+08:00
"""

import sqlite3
from datetime import datetime
from typing import List, Dict, Any
from loguru import logger

from agents.base import BaseAgent, DataSourceError, DataMappingError
from canonical.models import (
    Organization, Person, Customer, Transaction,
    SystemType
)
from canonical.mapper import DataMapper


class HRAgent(BaseAgent):
    """
    HR系统Agent
    - 从SQLite数据库读取HR原始数据
    - 使用DataMapper标准化并映射到Canonical模型
    """

    def __init__(self):
        super().__init__(system_name="HR系统", system_type=SystemType.HR)
        self.db_path = self.settings.database_path

    def pull_raw(self) -> List[Dict[str, Any]]:
        """
        拉取HR原始数据（组织、人员、客户、交易）

        Returns:
            包含entity标记的原始数据列表
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            raw: List[Dict[str, Any]] = []

            # 组织
            cursor.execute("SELECT * FROM hr_organizations")
            org_rows = cursor.fetchall()
            for row in org_rows:
                item = dict(row)
                item["_entity"] = "organization"
                raw.append(item)

            # 人员
            cursor.execute("SELECT * FROM hr_persons")
            person_rows = cursor.fetchall()
            for row in person_rows:
                item = dict(row)
                item["_entity"] = "person"
                raw.append(item)

            # 客户
            cursor.execute("SELECT * FROM hr_customers")
            cust_rows = cursor.fetchall()
            for row in cust_rows:
                item = dict(row)
                item["_entity"] = "customer"
                raw.append(item)

            # 交易
            cursor.execute("SELECT * FROM hr_transactions")
            txn_rows = cursor.fetchall()
            for row in txn_rows:
                item = dict(row)
                item["_entity"] = "transaction"
                raw.append(item)

            conn.close()

            logger.info(
                f"HR原始数据拉取完成: 组织={len(org_rows)}, 人员={len(person_rows)}, 客户={len(cust_rows)}, 交易={len(txn_rows)}"
            )

            return raw

        except Exception as e:
            logger.error(f"HR数据源访问失败: {e}")
            raise DataSourceError(f"HR数据源访问失败: {e}")

    def map_to_canonical(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
        """
        将HR原始数据映射为Canonical模型

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

            # 构建索引（用于组织负责人、交易-人员关联）
            person_index_by_id: Dict[str, Dict[str, Any]] = {}

            for item in raw_data:
                entity = item.get("_entity")

                if entity == "organization":
                    org_id = mapper.normalize_id(item.get("hr_org_id"), "hr")
                    # 组织类型从部门名称推断（DataMapper支持从名称推断常见部门）
                    org_type = mapper.normalize_org_type(item.get("org_name"), item.get("org_name"))

                    # 负责人名称解析
                    manager_name = None
                    manager_id = item.get("manager_id")
                    if manager_id and manager_id in person_index_by_id:
                        manager_name = person_index_by_id[manager_id].get("person_name")

                    org = Organization(
                        org_id=org_id,
                        source_system=SystemType.HR,
                        org_name=item.get("org_name"),
                        org_type=org_type,
                        parent_org_id=(
                            mapper.normalize_id(item.get("parent_org_id"), "hr")
                            if item.get("parent_org_id") else None
                        ),
                        manager_name=manager_name,
                        address=item.get("location"),
                        status=mapper.normalize_status(item.get("status")),
                        created_at=mapper.normalize_date(item.get("establishment_date"), "hr")
                                   or mapper.normalize_date(item.get("created_time"), "hr"),
                        raw_data=item,
                    )
                    organizations.append(org)

                elif entity == "person":
                    person_index_by_id[item.get("hr_person_id")] = item

                    person = Person(
                        person_id=mapper.normalize_id(item.get("hr_person_id"), "hr"),
                        source_system=SystemType.HR,
                        person_name=item.get("person_name"),
                        employee_number=item.get("employee_id"),
                        org_id=mapper.normalize_id(item.get("department_id"), "hr"),
                        position=item.get("position"),
                        department=item.get("department_id"),
                        email=mapper.validate_email(item.get("email")),
                        phone=mapper.validate_phone(item.get("phone")),
                        hire_date=mapper.normalize_date(item.get("hire_date"), "hr"),
                        status=mapper.normalize_status("active"),
                        created_at=mapper.normalize_date(item.get("created_time"), "hr")
                                   or mapper.normalize_date(item.get("hire_date"), "hr"),
                        raw_data=item,
                    )
                    persons.append(person)

                elif entity == "customer":
                    customer = Customer(
                        customer_id=mapper.normalize_id(item.get("hr_customer_id"), "hr"),
                        source_system=SystemType.HR,
                        customer_name=item.get("customer_name"),
                        customer_type=item.get("customer_type"),
                        tax_num=item.get("business_license"),
                        contact_person=item.get("contact_person"),
                        contact_phone=item.get("contact_phone"),
                        contact_email=mapper.validate_email(item.get("contact_email")),
                        address=item.get("company_address"),
                        status=mapper.normalize_status("active"),
                        created_at=mapper.normalize_date(item.get("created_time"), "hr")
                                   or mapper.normalize_date(item.get("updated_time"), "hr"),
                        raw_data=item,
                    )
                    customers.append(customer)

                elif entity == "transaction":
                    # 注意：hr_transactions.employee_id 实际生成格式与 hr_person_id 相同（HR_PER_xxxxx）
                    person_id_val = item.get("employee_id")
                    txn = Transaction(
                        tx_id=mapper.normalize_id(item.get("hr_transaction_id"), "hr"),
                        source_system=SystemType.HR,
                        transaction_number=item.get("transaction_number"),
                        tx_type=mapper.normalize_transaction_type(item.get("transaction_type"), "hr"),
                        amount=mapper.normalize_amount(item.get("amount")),
                        currency="CNY",
                        tx_date=mapper.normalize_date(item.get("transaction_date"), "hr"),
                        customer_id=None,
                        person_id=(
                            mapper.normalize_id(person_id_val, "hr") if person_id_val else None
                        ),
                        org_id=mapper.normalize_id(item.get("department_id"), "hr"),
                        description=mapper.clean_text(item.get("transaction_description") or item.get("comments")),
                        product_info=None,
                        payment_method=None,
                        status=mapper.normalize_status(item.get("status"), "hr"),
                        created_at=mapper.normalize_date(item.get("created_time"), "hr")
                                   or mapper.normalize_date(item.get("transaction_date"), "hr"),
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
            logger.error(f"HR数据映射失败: {e}")
            raise DataMappingError(f"HR数据映射失败: {e}")

__all__ = ["HRAgent"]