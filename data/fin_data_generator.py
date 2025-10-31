"""
FIN系统模拟数据生成器
特征：客户导向，交易频繁，金额精确
"""

import random
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any
import uuid
from decimal import Decimal
from config.settings import get_settings

settings = get_settings()


class FINDataGenerator:
    """FIN系统数据生成器"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or settings.database_path
        self.account_types = ["资产", "负债", "所有者权益", "收入", "费用"]
        self.transaction_types = ["收款", "付款", "转账", "调整", "结算"]
        self.currencies = ["CNY", "USD", "EUR", "JPY", "GBP"]
        self.payment_methods = ["银行转账", "现金", "支票", "信用卡", "支付宝", "微信支付"]
        
    def _generate_fin_id(self, prefix: str, sequence: int) -> str:
        """生成FIN系统ID"""
        return f"FIN{prefix}{sequence:08d}"
    
    def _generate_account_number(self) -> str:
        """生成银行账号"""
        return f"{random.randint(1000, 9999)}{random.randint(1000000000000000, 9999999999999999)}"
    
    def _generate_organization_data(self, count: int) -> List[Dict[str, Any]]:
        """生成FIN组织数据（成本中心/利润中心）"""
        organizations = []
        cost_centers = ["销售中心", "生产中心", "研发中心", "管理中心", "财务中心"]
        
        for i in range(count):
            org_data = {
                "fin_org_id": self._generate_fin_id("ORG", i + 1),
                "org_name": f"{random.choice(cost_centers)}{random.randint(1, 10)}",
                "org_code": f"CC{random.randint(1000, 9999)}",
                "org_type": random.choice(["成本中心", "利润中心", "投资中心"]),
                "parent_org_id": None if i < 3 else self._generate_fin_id("ORG", random.randint(1, 3)),
                "manager_name": f"{random.choice(['张', '王', '李'])}总",
                "budget_amount": random.randint(1000000, 50000000),
                "actual_amount": random.randint(800000, 45000000),
                "currency": "CNY",
                "account_number": self._generate_account_number(),
                "bank_name": random.choice(["工商银行", "建设银行", "农业银行", "中国银行", "招商银行"]),
                "tax_number": f"91{random.randint(100000000000000, 999999999999999)}",
                "address": f"上海市{random.choice(['浦东新区', '黄浦区', '徐汇区'])}财务大厦{random.randint(1, 50)}层",
                "status": "active",
                "created_time": datetime.now(),
                "updated_time": datetime.now()
            }
            organizations.append(org_data)
        
        return organizations
    
    def _generate_person_data(self, count: int) -> List[Dict[str, Any]]:
        """生成FIN人员数据（财务人员）"""
        persons = []
        surnames = ["张", "王", "李", "赵", "刘", "陈", "杨", "黄", "周", "吴"]
        given_names = ["会计", "出纳", "审计", "分析", "经理", "主管", "专员", "助理"]
        positions = ["CFO", "财务总监", "会计经理", "出纳", "成本会计", "税务专员", "审计师"]
        
        for i in range(count):
            person_data = {
                "fin_person_id": self._generate_fin_id("PER", i + 1),
                "person_name": f"{random.choice(surnames)}{random.choice(given_names)}",
                "employee_code": f"FIN{random.randint(1000, 9999)}",
                "position": random.choice(positions),
                "department": random.choice(["财务部", "会计部", "审计部", "税务部"]),
                "cost_center": f"CC{random.randint(1000, 9999)}",
                "authorization_level": random.choice(["初级", "中级", "高级", "总监级"]),
                "max_approval_amount": random.choice([10000, 50000, 100000, 500000, 1000000]),
                "phone": f"021-{random.randint(10000000, 99999999)}",
                "email": f"fin.{i+1}@company.com",
                "hire_date": datetime.now() - timedelta(days=random.randint(30, 3650)),
                "certification": random.choice(["CPA", "ACCA", "CMA", "CIA", "无"]),
                "status": "active",
                "created_time": datetime.now(),
                "updated_time": datetime.now()
            }
            persons.append(person_data)
        
        return persons
    
    def _generate_customer_data(self, count: int) -> List[Dict[str, Any]]:
        """生成FIN客户数据"""
        customers = []
        company_types = ["制造业", "服务业", "贸易", "科技", "金融", "房地产", "教育"]
        
        for i in range(count):
            customer_data = {
                "fin_customer_id": self._generate_fin_id("CUST", i + 1),
                "customer_name": f"{random.choice(['华为', '腾讯', '阿里', '百度', '京东', '美团', '滴滴'])}{random.choice(['科技', '集团', '控股'])}有限公司",
                "customer_code": f"C{random.randint(100000, 999999)}",
                "customer_type": random.choice(["企业客户", "个人客户", "政府机构", "金融机构"]),
                "industry": random.choice(company_types),
                "tax_number": f"91{random.randint(100000000000000, 999999999999999)}",
                "credit_rating": random.choice(["AAA", "AA+", "AA", "AA-", "A+", "A", "A-"]),
                "credit_limit": random.randint(1000000, 50000000),
                "payment_terms": random.choice(["现金", "30天", "60天", "90天", "120天"]),
                "account_manager": self._generate_fin_id("PER", random.randint(1, min(20, count))),
                "bank_account": self._generate_account_number(),
                "bank_name": random.choice(["工商银行", "建设银行", "农业银行", "中国银行", "招商银行"]),
                "contact_person": f"{random.choice(['张', '王', '李'])}总",
                "contact_phone": f"400-{random.randint(1000000, 9999999)}",
                "contact_email": f"finance{i+1}@customer.com",
                "billing_address": f"北京市{random.choice(['朝阳区', '海淀区', '西城区'])}金融街{random.randint(1, 999)}号",
                "registration_date": datetime.now() - timedelta(days=random.randint(1, 1825)),
                "last_transaction_date": datetime.now() - timedelta(days=random.randint(0, 365)),
                "total_receivable": random.randint(0, 5000000),
                "total_payable": random.randint(0, 3000000),
                "status": random.choice(["活跃", "暂停", "黑名单"]),
                "created_time": datetime.now(),
                "updated_time": datetime.now()
            }
            customers.append(customer_data)
        
        return customers
    
    def _generate_transaction_data(self, count: int) -> List[Dict[str, Any]]:
        """生成FIN交易数据"""
        transactions = []
        
        for i in range(count):
            transaction_date = datetime.now() - timedelta(days=random.randint(0, 365))
            amount = round(random.uniform(1000.0, 1000000.0), 2)
            
            transaction_data = {
                "fin_transaction_id": self._generate_fin_id("TXN", i + 1),
                "transaction_number": f"FIN{transaction_date.strftime('%Y%m%d')}{random.randint(100000, 999999)}",
                "customer_id": self._generate_fin_id("CUST", random.randint(1, min(100, count))),
                "transaction_type": random.choice(self.transaction_types),
                "transaction_category": random.choice(["营业收入", "营业成本", "管理费用", "销售费用", "财务费用"]),
                "amount": amount,
                "currency": random.choice(self.currencies),
                "exchange_rate": 1.0 if random.choice(self.currencies) == "CNY" else round(random.uniform(0.1, 10.0), 4),
                "amount_cny": amount,  # 简化处理，实际应根据汇率计算
                "debit_account": f"{random.randint(1000, 9999)}.{random.randint(10, 99)}",
                "credit_account": f"{random.randint(1000, 9999)}.{random.randint(10, 99)}",
                "cost_center": f"CC{random.randint(1000, 9999)}",
                "project_code": f"PRJ{random.randint(1000, 9999)}",
                "transaction_date": transaction_date,
                "value_date": transaction_date + timedelta(days=random.randint(0, 3)),
                "due_date": transaction_date + timedelta(days=random.randint(30, 90)),
                "payment_method": random.choice(self.payment_methods),
                "reference_number": f"REF{random.randint(100000000, 999999999)}",
                "invoice_number": f"INV{transaction_date.strftime('%Y%m%d')}{random.randint(1000, 9999)}",
                "description": f"财务交易{i+1} - {random.choice(['销售收入', '采购付款', '费用报销', '投资收益'])}",
                "status": random.choice(["已确认", "待审核", "已审核", "已入账", "已取消"]),
                "approver": self._generate_fin_id("PER", random.randint(1, min(10, count))),
                "approval_date": transaction_date + timedelta(days=random.randint(1, 5)),
                "created_by": self._generate_fin_id("PER", random.randint(1, min(20, count))),
                "tax_amount": round(amount * random.uniform(0.06, 0.13), 2),
                "tax_rate": round(random.uniform(0.06, 0.13), 4),
                "bank_reference": f"BANK{random.randint(100000000000, 999999999999)}",
                "reconciliation_status": random.choice(["已对账", "未对账", "差异"]),
                "created_time": datetime.now(),
                "updated_time": datetime.now()
            }
            transactions.append(transaction_data)
        
        return transactions
    
    def create_tables(self):
        """创建FIN系统数据表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 组织表（成本中心）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fin_organizations (
                fin_org_id TEXT PRIMARY KEY,
                org_name TEXT NOT NULL,
                org_code TEXT UNIQUE,
                org_type TEXT,
                parent_org_id TEXT,
                manager_name TEXT,
                budget_amount INTEGER,
                actual_amount INTEGER,
                currency TEXT,
                account_number TEXT,
                bank_name TEXT,
                tax_number TEXT,
                address TEXT,
                status TEXT,
                created_time DATETIME,
                updated_time DATETIME
            )
        """)
        
        # 人员表（财务人员）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fin_persons (
                fin_person_id TEXT PRIMARY KEY,
                person_name TEXT NOT NULL,
                employee_code TEXT UNIQUE,
                position TEXT,
                department TEXT,
                cost_center TEXT,
                authorization_level TEXT,
                max_approval_amount INTEGER,
                phone TEXT,
                email TEXT,
                hire_date DATETIME,
                certification TEXT,
                status TEXT,
                created_time DATETIME,
                updated_time DATETIME
            )
        """)
        
        # 客户表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fin_customers (
                fin_customer_id TEXT PRIMARY KEY,
                customer_name TEXT NOT NULL,
                customer_code TEXT UNIQUE,
                customer_type TEXT,
                industry TEXT,
                tax_number TEXT,
                credit_rating TEXT,
                credit_limit INTEGER,
                payment_terms TEXT,
                account_manager TEXT,
                bank_account TEXT,
                bank_name TEXT,
                contact_person TEXT,
                contact_phone TEXT,
                contact_email TEXT,
                billing_address TEXT,
                registration_date DATETIME,
                last_transaction_date DATETIME,
                total_receivable INTEGER,
                total_payable INTEGER,
                status TEXT,
                created_time DATETIME,
                updated_time DATETIME
            )
        """)
        
        # 交易表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fin_transactions (
                fin_transaction_id TEXT PRIMARY KEY,
                transaction_number TEXT UNIQUE,
                customer_id TEXT,
                transaction_type TEXT,
                transaction_category TEXT,
                amount REAL,
                currency TEXT,
                exchange_rate REAL,
                amount_cny REAL,
                debit_account TEXT,
                credit_account TEXT,
                cost_center TEXT,
                project_code TEXT,
                transaction_date DATETIME,
                value_date DATETIME,
                due_date DATETIME,
                payment_method TEXT,
                reference_number TEXT,
                invoice_number TEXT,
                description TEXT,
                status TEXT,
                approver TEXT,
                approval_date DATETIME,
                created_by TEXT,
                tax_amount REAL,
                tax_rate REAL,
                bank_reference TEXT,
                reconciliation_status TEXT,
                created_time DATETIME,
                updated_time DATETIME
            )
        """)
        
        conn.commit()
        conn.close()
    
    def generate_and_save_data(self, org_count: int = None, person_count: int = None, 
                              customer_count: int = None, transaction_count: int = None):
        """生成并保存FIN数据"""
        # 使用配置中的默认值
        org_count = org_count or settings.fin_data_size // 40
        person_count = person_count or settings.fin_data_size // 20
        customer_count = customer_count or settings.fin_data_size // 4
        transaction_count = transaction_count or settings.fin_data_size
        
        # 创建表
        self.create_tables()
        
        # 生成数据
        organizations = self._generate_organization_data(org_count)
        persons = self._generate_person_data(person_count)
        customers = self._generate_customer_data(customer_count)
        transactions = self._generate_transaction_data(transaction_count)
        
        # 保存到数据库
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 插入组织数据
        for org in organizations:
            cursor.execute("""
                INSERT OR REPLACE INTO fin_organizations 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(org.values()))
        
        # 插入人员数据
        for person in persons:
            cursor.execute("""
                INSERT OR REPLACE INTO fin_persons 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(person.values()))
        
        # 插入客户数据
        for customer in customers:
            cursor.execute("""
                INSERT OR REPLACE INTO fin_customers 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(customer.values()))
        
        # 插入交易数据
        for transaction in transactions:
            cursor.execute("""
                INSERT OR REPLACE INTO fin_transactions 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(transaction.values()))
        
        conn.commit()
        conn.close()
        
        return {
            "organizations": len(organizations),
            "persons": len(persons),
            "customers": len(customers),
            "transactions": len(transactions)
        }


if __name__ == "__main__":
    generator = FINDataGenerator()
    result = generator.generate_and_save_data()
    print(f"FIN数据生成完成: {result}")