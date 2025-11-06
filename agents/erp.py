"""
ERPAgent实现
读取SQLite中的ERP数据，映射为Canonical统一模型

作者: Tom
创建时间: 2025-11-05T23:21:13+08:00
"""

import sqlite3
from typing import List, Dict, Any
from loguru import logger

from agents.base import BaseAgent, DataSourceError, DataMappingError
from canonical.models import (
    Organization, Person, Customer, Transaction,
    SystemType, StatusType
)
from canonical.mapper import DataMapper


class ERPAgent(BaseAgent):
    """
    ERP系统Agent
    - 从SQLite数据库读取ERP原始数据
    - 使用DataMapper标准化并映射到Canonical模型
    """

    def __init__(self):
        super().__init__(system_name="ERP系统", system_type=SystemType.ERP)
        self.db_path = self.settings.database_path

    def pull_raw(self) -> List[Dict[str, Any]]:
        """
        拉取ERP原始数据（组织、人员、客户、交易）

        Returns:
            包含entity标记的原始数据列表
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            raw: List[Dict[str, Any]] = []

            # 组织
            cursor.execute("SELECT * FROM erp_organizations")
            org_rows = cursor.fetchall()
            for row in org_rows:
                item = dict(row)
                item["_entity"] = "organization"
                raw.append(item)

            # 人员
            cursor.execute("SELECT * FROM erp_persons")
            person_rows = cursor.fetchall()
            for row in person_rows:
                item = dict(row)
                item["_entity"] = "person"
                raw.append(item)

            # 客户
            cursor.execute("SELECT * FROM erp_customers")
            cust_rows = cursor.fetchall()
            for row in cust_rows:
                item = dict(row)
                item["_entity"] = "customer"
                raw.append(item)

            # 交易
            cursor.execute("SELECT * FROM erp_transactions")
            txn_rows = cursor.fetchall()
            for row in txn_rows:
                item = dict(row)
                item["_entity"] = "transaction"
                raw.append(item)

            conn.close()

            logger.info(
                f"ERP原始数据拉取完成: 组织={len(org_rows)}, 人员={len(person_rows)}, 客户={len(cust_rows)}, 交易={len(txn_rows)}"
            )

            return raw

        except Exception as e:
            logger.error(f"ERP数据源访问失败: {e}")
            raise DataSourceError(f"ERP数据源访问失败: {e}")

    def map_to_canonical(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
        """
        将ERP原始数据映射为Canonical模型

        Args:
            raw_data: 带有`_entity`标记的原始数据列表

        Returns:
            各实体的Canonical列表字典
        """
        try:
            mapper = self.mapper

            organizations: List[Organization] = []
            persons: List[Person] = []
            customers: List[Customer] = []
            transactions: List[Transaction] = []

            # 构建索引以进行跨表关联（如交易中的销售人员 -> 部门 -> 组织）
            person_index: Dict[str, Dict[str, Any]] = {}

            for item in raw_data:
                entity = item.get("_entity")

                if entity == "organization":
                    org_id = mapper.normalize_id(item.get("erp_org_id"), "erp")
                    org = Organization(
                        org_id=org_id,
                        source_system=SystemType.ERP,
                        org_name=item.get("org_name"),
                        org_type=mapper.normalize_org_type(item.get("org_type"), item.get("org_name")),
                        parent_org_id=(
                            mapper.normalize_id(item.get("parent_org_id"), "erp")
                            if item.get("parent_org_id") else None
                        ),
                        org_code=item.get("org_code"),
                        contact_info=mapper.clean_text(item.get("contact_info")),
                        address=mapper.clean_text(item.get("address")),
                        status=mapper.normalize_status(item.get("status")),
                        created_at=mapper.normalize_date(item.get("created_time"), "erp") or mapper.normalize_date(item.get("updated_time"), "erp") or mapper.normalize_date(item.get("established_date"), "erp") or None,
                        raw_data=item
                    )
                    organizations.append(org)

                elif entity == "person":
                    erp_person_id = item.get("erp_person_id")
                    person_index[erp_person_id] = item

                    person = Person(
                        person_id=mapper.normalize_id(erp_person_id, "erp"),
                        source_system=SystemType.ERP,
                        person_name=item.get("person_name"),
                        employee_number=item.get("employee_number"),
                        org_id=mapper.normalize_id(item.get("department_id"), "erp"),
                        position=mapper.clean_text(item.get("position")),
                        department=mapper.clean_text(item.get("department_id")),
                        email=mapper.validate_email(item.get("email")),
                        phone=mapper.validate_phone(item.get("contact_phone")),
                        hire_date=mapper.normalize_date(item.get("hire_date"), "erp"),
                        status=mapper.normalize_status(item.get("employment_status")),
                        raw_data=item
                    )
                    persons.append(person)

                elif entity == "customer":
                    customer = Customer(
                        customer_id=mapper.normalize_id(item.get("erp_customer_id"), "erp"),
                        source_system=SystemType.ERP,
                        customer_name=item.get("customer_name"),
                        customer_code=item.get("customer_code"),
                        customer_type=item.get("customer_type"),
                        tax_num=None,
                        industry=item.get("industry"),
                        contact_person=item.get("contact_person"),
                        contact_phone=mapper.validate_phone(item.get("contact_phone")),
                        contact_email=mapper.validate_email(item.get("contact_email")),
                        address=item.get("address"),
                        credit_level=item.get("credit_level"),
                        status=mapper.normalize_status(item.get("status")),
                        created_at=mapper.normalize_date(item.get("created_time"), "erp"),
                        raw_data=item
                    )
                    customers.append(customer)

                elif entity == "transaction":
                    # 关联销售人员 -> 部门 -> 组织ID
                    sales_person_code = item.get("sales_person")
                    person_row = person_index.get(sales_person_code)
                    org_id_value = person_row.get("department_id") if person_row else "ORG000001"

                    txn = Transaction(
                        tx_id=mapper.normalize_id(item.get("erp_transaction_id"), "erp"),
                        source_system=SystemType.ERP,
                        transaction_number=item.get("transaction_number"),
                        tx_type=mapper.normalize_transaction_type(item.get("transaction_type"), "erp"),
                        amount=mapper.normalize_amount(item.get("total_amount")),
                        currency="CNY",  # Canonical仅支持CNY
                        tx_date=mapper.normalize_date(item.get("transaction_date"), "erp"),
                        customer_id=(
                            mapper.normalize_id(item.get("customer_id"), "erp")
                            if item.get("customer_id") else None
                        ),
                        person_id=(
                            mapper.normalize_id(sales_person_code, "erp")
                            if sales_person_code else None
                        ),
                        org_id=mapper.normalize_id(org_id_value, "erp"),
                        description=mapper.clean_text(item.get("notes")),
                        product_info=mapper.clean_text(f"{item.get('product_code')}/{item.get('product_name')}") if item.get("product_code") or item.get("product_name") else None,
                        payment_method=mapper.clean_text(item.get("payment_terms")),
                        status=mapper.normalize_status(item.get("status")),
                        created_at=mapper.normalize_date(item.get("created_time"), "erp") or mapper.normalize_date(item.get("updated_time"), "erp"),
                        raw_data=item
                    )
                    transactions.append(txn)

            return {
                "organizations": organizations,
                "persons": persons,
                "customers": customers,
                "transactions": transactions
            }

        except Exception as e:
            logger.error(f"ERP数据映射失败: {e}")
            raise DataMappingError(f"ERP数据映射失败: {e}")


__all__ = ["ERPAgent"]