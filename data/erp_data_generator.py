"""
ERP系统模拟数据生成器
特征：层级化组织结构，业务流程导向，编码规范化
"""

import random
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any
import uuid
from config.settings import get_settings

settings = get_settings()


class ERPDataGenerator:
    """ERP系统数据生成器"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or settings.database_path
        self.company_codes = ["COMP001", "COMP002", "COMP003", "COMP004", "COMP005"]
        self.department_types = ["销售部", "生产部", "采购部", "财务部", "人事部", "技术部", "市场部"]
        self.business_processes = ["采购流程", "销售流程", "生产流程", "财务流程", "人事流程"]
        
    def _generate_erp_code(self, prefix: str, sequence: int) -> str:
        """生成ERP标准编码"""
        return f"{prefix}{sequence:06d}"
    
    def _generate_organization_data(self, count: int) -> List[Dict[str, Any]]:
        """生成组织架构数据"""
        organizations = []
        
        for i in range(count):
            # 生成层级结构
            level = random.choice([1, 2, 3])  # 1=公司, 2=部门, 3=小组
            
            if level == 1:  # 公司级别
                org_data = {
                    "erp_org_id": self._generate_erp_code("ORG", i + 1),
                    "org_name": f"{random.choice(['华东', '华南', '华北', '西南', '东北'])}分公司",
                    "org_type": "公司",
                    "org_level": 1,
                    "parent_org_id": None,
                    "org_code": random.choice(self.company_codes),
                    "business_scope": random.choice(["制造业", "服务业", "贸易", "科技"]),
                    "established_date": datetime.now() - timedelta(days=random.randint(365, 3650)),
                    "status": "active",
                    "contact_info": f"021-{random.randint(10000000, 99999999)}",
                    "address": f"上海市{random.choice(['浦东新区', '黄浦区', '徐汇区'])}",
                    "created_time": datetime.now(),
                    "updated_time": datetime.now()
                }
            elif level == 2:  # 部门级别
                org_data = {
                    "erp_org_id": self._generate_erp_code("DEPT", i + 1),
                    "org_name": random.choice(self.department_types),
                    "org_type": "部门",
                    "org_level": 2,
                    "parent_org_id": self._generate_erp_code("ORG", random.randint(1, min(10, count))),
                    "org_code": f"DEPT{random.randint(100, 999)}",
                    "business_scope": random.choice(self.business_processes),
                    "established_date": datetime.now() - timedelta(days=random.randint(30, 1825)),
                    "status": "active",
                    "contact_info": f"分机{random.randint(1000, 9999)}",
                    "address": f"办公楼{random.randint(1, 20)}层",
                    "created_time": datetime.now(),
                    "updated_time": datetime.now()
                }
            else:  # 小组级别
                org_data = {
                    "erp_org_id": self._generate_erp_code("TEAM", i + 1),
                    "org_name": f"{random.choice(['第一', '第二', '第三'])}小组",
                    "org_type": "小组",
                    "org_level": 3,
                    "parent_org_id": self._generate_erp_code("DEPT", random.randint(1, min(20, count))),
                    "org_code": f"TEAM{random.randint(100, 999)}",
                    "business_scope": "具体业务执行",
                    "established_date": datetime.now() - timedelta(days=random.randint(1, 365)),
                    "status": "active",
                    "contact_info": f"内线{random.randint(100, 999)}",
                    "address": f"工位区域{random.choice(['A', 'B', 'C', 'D'])}",
                    "created_time": datetime.now(),
                    "updated_time": datetime.now()
                }
            
            organizations.append(org_data)
        
        return organizations
    
    def _generate_person_data(self, count: int) -> List[Dict[str, Any]]:
        """生成人员数据"""
        persons = []
        surnames = ["张", "王", "李", "赵", "刘", "陈", "杨", "黄", "周", "吴"]
        given_names = ["伟", "芳", "娜", "敏", "静", "丽", "强", "磊", "军", "洋"]
        
        for i in range(count):
            person_data = {
                "erp_person_id": self._generate_erp_code("EMP", i + 1),
                "person_name": f"{random.choice(surnames)}{random.choice(given_names)}",
                "employee_number": f"E{random.randint(100000, 999999)}",
                "position": random.choice(["经理", "主管", "专员", "助理", "总监"]),
                "department_id": self._generate_erp_code("DEPT", random.randint(1, 20)),
                "hire_date": datetime.now() - timedelta(days=random.randint(30, 1825)),
                "employment_status": random.choice(["在职", "试用期", "离职"]),
                "work_location": random.choice(["总部", "分部A", "分部B"]),
                "contact_phone": f"138{random.randint(10000000, 99999999)}",
                "email": f"emp{i+1}@company.com",
                "salary_grade": random.choice(["P1", "P2", "P3", "M1", "M2"]),
                "created_time": datetime.now(),
                "updated_time": datetime.now()
            }
            persons.append(person_data)
        
        return persons
    
    def _generate_customer_data(self, count: int) -> List[Dict[str, Any]]:
        """生成客户数据"""
        customers = []
        company_suffixes = ["有限公司", "股份有限公司", "科技有限公司", "贸易有限公司"]
        
        for i in range(count):
            customer_data = {
                "erp_customer_id": self._generate_erp_code("CUST", i + 1),
                "customer_name": f"{random.choice(['华为', '腾讯', '阿里', '百度', '京东'])}{random.choice(company_suffixes)}",
                "customer_code": f"C{random.randint(100000, 999999)}",
                "customer_type": random.choice(["企业客户", "个人客户", "政府客户"]),
                "industry": random.choice(["制造业", "IT", "金融", "教育", "医疗"]),
                "credit_level": random.choice(["AAA", "AA", "A", "BBB", "BB"]),
                "contact_person": f"{random.choice(['张', '王', '李'])}总",
                "contact_phone": f"021-{random.randint(10000000, 99999999)}",
                "contact_email": f"contact{i+1}@customer.com",
                "address": f"上海市{random.choice(['浦东新区', '黄浦区', '徐汇区'])}某某路{random.randint(1, 999)}号",
                "registration_date": datetime.now() - timedelta(days=random.randint(1, 1825)),
                "status": random.choice(["活跃", "潜在", "暂停"]),
                "created_time": datetime.now(),
                "updated_time": datetime.now()
            }
            customers.append(customer_data)
        
        return customers
    
    def _generate_transaction_data(self, count: int) -> List[Dict[str, Any]]:
        """生成交易数据"""
        transactions = []
        
        for i in range(count):
            transaction_data = {
                "erp_transaction_id": self._generate_erp_code("TXN", i + 1),
                "transaction_number": f"TXN{datetime.now().strftime('%Y%m%d')}{random.randint(1000, 9999)}",
                "customer_id": self._generate_erp_code("CUST", random.randint(1, min(100, count))),
                "transaction_type": random.choice(["销售订单", "采购订单", "退货单", "换货单"]),
                "product_code": f"PROD{random.randint(1000, 9999)}",
                "product_name": random.choice(["产品A", "产品B", "产品C", "服务X", "服务Y"]),
                "quantity": random.randint(1, 100),
                "unit_price": round(random.uniform(10.0, 1000.0), 2),
                "total_amount": 0,  # 将在后面计算
                "currency": "CNY",
                "transaction_date": datetime.now() - timedelta(days=random.randint(0, 365)),
                "status": random.choice(["已确认", "处理中", "已完成", "已取消"]),
                "sales_person": self._generate_erp_code("EMP", random.randint(1, min(50, count))),
                "payment_terms": random.choice(["现金", "30天账期", "60天账期"]),
                "delivery_date": datetime.now() + timedelta(days=random.randint(1, 30)),
                "notes": f"ERP系统生成的交易记录{i+1}",
                "created_time": datetime.now(),
                "updated_time": datetime.now()
            }
            
            # 计算总金额
            transaction_data["total_amount"] = round(
                transaction_data["quantity"] * transaction_data["unit_price"], 2
            )
            
            transactions.append(transaction_data)
        
        return transactions
    
    def create_tables(self):
        """创建ERP系统数据表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 组织表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS erp_organizations (
                erp_org_id TEXT PRIMARY KEY,
                org_name TEXT NOT NULL,
                org_type TEXT,
                org_level INTEGER,
                parent_org_id TEXT,
                org_code TEXT UNIQUE,
                business_scope TEXT,
                established_date DATETIME,
                status TEXT,
                contact_info TEXT,
                address TEXT,
                created_time DATETIME,
                updated_time DATETIME
            )
        """)
        
        # 人员表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS erp_persons (
                erp_person_id TEXT PRIMARY KEY,
                person_name TEXT NOT NULL,
                employee_number TEXT UNIQUE,
                position TEXT,
                department_id TEXT,
                hire_date DATETIME,
                employment_status TEXT,
                work_location TEXT,
                contact_phone TEXT,
                email TEXT,
                salary_grade TEXT,
                created_time DATETIME,
                updated_time DATETIME
            )
        """)
        
        # 客户表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS erp_customers (
                erp_customer_id TEXT PRIMARY KEY,
                customer_name TEXT NOT NULL,
                customer_code TEXT UNIQUE,
                customer_type TEXT,
                industry TEXT,
                credit_level TEXT,
                contact_person TEXT,
                contact_phone TEXT,
                contact_email TEXT,
                address TEXT,
                registration_date DATETIME,
                status TEXT,
                created_time DATETIME,
                updated_time DATETIME
            )
        """)
        
        # 交易表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS erp_transactions (
                erp_transaction_id TEXT PRIMARY KEY,
                transaction_number TEXT UNIQUE,
                customer_id TEXT,
                transaction_type TEXT,
                product_code TEXT,
                product_name TEXT,
                quantity INTEGER,
                unit_price REAL,
                total_amount REAL,
                currency TEXT,
                transaction_date DATETIME,
                status TEXT,
                sales_person TEXT,
                payment_terms TEXT,
                delivery_date DATETIME,
                notes TEXT,
                created_time DATETIME,
                updated_time DATETIME
            )
        """)
        
        conn.commit()
        conn.close()
    
    def generate_and_save_data(self, org_count: int = None, person_count: int = None, 
                              customer_count: int = None, transaction_count: int = None):
        """生成并保存ERP数据"""
        # 使用配置中的默认值
        org_count = org_count or settings.erp_data_size // 10
        person_count = person_count or settings.erp_data_size // 5
        customer_count = customer_count or settings.erp_data_size // 8
        transaction_count = transaction_count or settings.erp_data_size
        
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
                INSERT OR REPLACE INTO erp_organizations 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(org.values()))
        
        # 插入人员数据
        for person in persons:
            cursor.execute("""
                INSERT OR REPLACE INTO erp_persons 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(person.values()))
        
        # 插入客户数据
        for customer in customers:
            cursor.execute("""
                INSERT OR REPLACE INTO erp_customers 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(customer.values()))
        
        # 插入交易数据
        for transaction in transactions:
            cursor.execute("""
                INSERT OR REPLACE INTO erp_transactions 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    generator = ERPDataGenerator()
    result = generator.generate_and_save_data()
    print(f"ERP数据生成完成: {result}")