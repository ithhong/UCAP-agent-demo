"""
HR系统模拟数据生成器
特征：人员详细信息，组织关系复杂，字段丰富
"""

import random
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any
import uuid
from config.settings import get_settings

settings = get_settings()


class HRDataGenerator:
    """HR系统数据生成器"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or settings.database_path
        self.departments = ["人力资源部", "财务部", "技术部", "市场部", "销售部", "运营部", "法务部"]
        self.positions = ["总监", "经理", "主管", "高级专员", "专员", "助理", "实习生"]
        self.education_levels = ["博士", "硕士", "本科", "大专", "高中"]
        self.skills = ["Python", "Java", "项目管理", "数据分析", "市场营销", "财务分析", "法律事务"]
        
    def _generate_hr_id(self, prefix: str, sequence: int) -> str:
        """生成HR系统ID"""
        return f"HR_{prefix}_{sequence:05d}"
    
    def _generate_organization_data(self, count: int) -> List[Dict[str, Any]]:
        """生成HR组织数据"""
        organizations = []
        
        for i in range(count):
            org_data = {
                "hr_org_id": self._generate_hr_id("ORG", i + 1),
                "org_name": random.choice(self.departments),
                "org_description": f"{random.choice(self.departments)}负责公司相关业务",
                "manager_id": None,  # 稍后关联
                "parent_org_id": None if i < 5 else self._generate_hr_id("ORG", random.randint(1, 5)),
                "org_level": 1 if i < 5 else 2,
                "employee_count": random.randint(5, 50),
                "budget": random.randint(100000, 5000000),
                "cost_center": f"CC{random.randint(1000, 9999)}",
                "location": random.choice(["北京总部", "上海分部", "深圳分部", "广州分部"]),
                "establishment_date": datetime.now() - timedelta(days=random.randint(365, 3650)),
                "status": "active",
                "created_time": datetime.now(),
                "updated_time": datetime.now()
            }
            organizations.append(org_data)
        
        return organizations
    
    def _generate_person_data(self, count: int) -> List[Dict[str, Any]]:
        """生成HR人员数据"""
        persons = []
        surnames = ["张", "王", "李", "赵", "刘", "陈", "杨", "黄", "周", "吴", "徐", "孙", "马", "朱", "胡"]
        given_names = ["伟", "芳", "娜", "敏", "静", "丽", "强", "磊", "军", "洋", "艳", "勇", "涛", "明", "超"]
        
        for i in range(count):
            birth_date = datetime.now() - timedelta(days=random.randint(7300, 18250))  # 20-50岁
            hire_date = datetime.now() - timedelta(days=random.randint(30, 3650))
            
            person_data = {
                "hr_person_id": self._generate_hr_id("PER", i + 1),
                "employee_id": f"HR{random.randint(100000, 999999)}",
                "person_name": f"{random.choice(surnames)}{random.choice(given_names)}",
                "english_name": f"Employee{i+1}",
                "gender": random.choice(["男", "女"]),
                "birth_date": birth_date,
                "age": (datetime.now() - birth_date).days // 365,
                "id_card": f"{random.randint(100000, 999999)}{birth_date.strftime('%Y%m%d')}{random.randint(1000, 9999)}",
                "phone": f"1{random.choice([3,5,7,8])}{random.randint(100000000, 999999999)}",
                "email": f"hr.emp{i+1}@company.com",
                "address": f"{random.choice(['北京市', '上海市', '深圳市'])}{random.choice(['朝阳区', '浦东新区', '南山区'])}某某街道{random.randint(1, 999)}号",
                "emergency_contact": f"{random.choice(surnames)}{random.choice(given_names)}",
                "emergency_phone": f"1{random.choice([3,5,7,8])}{random.randint(100000000, 999999999)}",
                "department_id": self._generate_hr_id("ORG", random.randint(1, min(10, count//10))),
                "position": random.choice(self.positions),
                "job_level": random.choice(["P1", "P2", "P3", "P4", "M1", "M2", "M3"]),
                "hire_date": hire_date,
                "probation_end_date": hire_date + timedelta(days=90),
                "contract_type": random.choice(["正式员工", "合同工", "实习生", "顾问"]),
                "employment_status": random.choice(["在职", "试用期", "离职", "休假"]),
                "work_location": random.choice(["北京总部", "上海分部", "深圳分部", "远程办公"]),
                "manager_id": None,  # 稍后关联
                "salary": random.randint(5000, 50000),
                "education_level": random.choice(self.education_levels),
                "major": random.choice(["计算机科学", "工商管理", "会计学", "市场营销", "法学", "金融学"]),
                "university": random.choice(["清华大学", "北京大学", "复旦大学", "上海交大", "浙江大学"]),
                "skills": ",".join(random.sample(self.skills, random.randint(1, 3))),
                "performance_rating": random.choice(["优秀", "良好", "合格", "待改进"]),
                "last_promotion_date": hire_date + timedelta(days=random.randint(365, 1095)),
                "created_time": datetime.now(),
                "updated_time": datetime.now()
            }
            persons.append(person_data)
        
        return persons
    
    def _generate_customer_data(self, count: int) -> List[Dict[str, Any]]:
        """生成HR客户数据（内部客户/业务伙伴）"""
        customers = []
        partner_types = ["供应商", "外包商", "咨询公司", "培训机构", "招聘机构"]
        
        for i in range(count):
            customer_data = {
                "hr_customer_id": self._generate_hr_id("CUST", i + 1),
                "customer_name": f"{random.choice(['智联', '前程无忧', '猎聘', '拉勾', 'BOSS直聘'])}{random.choice(['科技', '咨询', '服务'])}有限公司",
                "customer_type": random.choice(partner_types),
                "business_license": f"91{random.randint(100000000000000, 999999999999999)}",
                "contact_person": f"{random.choice(['张', '王', '李'])}经理",
                "contact_title": random.choice(["总经理", "业务经理", "项目经理", "客户经理"]),
                "contact_phone": f"010-{random.randint(10000000, 99999999)}",
                "contact_email": f"contact{i+1}@partner.com",
                "company_address": f"北京市{random.choice(['朝阳区', '海淀区', '西城区'])}某某大厦{random.randint(1, 50)}层",
                "service_scope": random.choice(["人才招聘", "培训服务", "薪酬咨询", "组织发展", "法律服务"]),
                "cooperation_start_date": datetime.now() - timedelta(days=random.randint(30, 1825)),
                "contract_amount": random.randint(10000, 500000),
                "payment_terms": random.choice(["月付", "季付", "年付", "项目完成后付"]),
                "service_rating": random.choice(["优秀", "良好", "一般", "待改进"]),
                "status": random.choice(["合作中", "暂停合作", "终止合作"]),
                "created_time": datetime.now(),
                "updated_time": datetime.now()
            }
            customers.append(customer_data)
        
        return customers
    
    def _generate_transaction_data(self, count: int) -> List[Dict[str, Any]]:
        """生成HR交易数据（人事事务处理）"""
        transactions = []
        transaction_types = ["入职办理", "离职办理", "薪资调整", "职位变更", "培训申请", "请假申请", "绩效评估"]
        
        for i in range(count):
            transaction_date = datetime.now() - timedelta(days=random.randint(0, 365))
            
            transaction_data = {
                "hr_transaction_id": self._generate_hr_id("TXN", i + 1),
                "transaction_number": f"HR{transaction_date.strftime('%Y%m%d')}{random.randint(1000, 9999)}",
                "employee_id": self._generate_hr_id("PER", random.randint(1, min(100, count))),
                "transaction_type": random.choice(transaction_types),
                "transaction_description": f"员工{random.choice(transaction_types)}相关事务处理",
                "initiator": self._generate_hr_id("PER", random.randint(1, min(50, count))),
                "approver": self._generate_hr_id("PER", random.randint(1, min(20, count))),
                "transaction_date": transaction_date,
                "effective_date": transaction_date + timedelta(days=random.randint(1, 30)),
                "status": random.choice(["待审批", "已审批", "已执行", "已拒绝"]),
                "priority": random.choice(["高", "中", "低"]),
                "department_id": self._generate_hr_id("ORG", random.randint(1, min(10, count//10))),
                "cost_center": f"CC{random.randint(1000, 9999)}",
                "amount": random.randint(0, 10000) if random.choice([True, False]) else 0,
                "currency": "CNY",
                "attachment_count": random.randint(0, 5),
                "comments": f"HR系统事务处理记录{i+1}",
                "created_by": self._generate_hr_id("PER", random.randint(1, min(30, count))),
                "created_time": datetime.now(),
                "updated_time": datetime.now()
            }
            transactions.append(transaction_data)
        
        return transactions
    
    def create_tables(self):
        """创建HR系统数据表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 组织表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hr_organizations (
                hr_org_id TEXT PRIMARY KEY,
                org_name TEXT NOT NULL,
                org_description TEXT,
                manager_id TEXT,
                parent_org_id TEXT,
                org_level INTEGER,
                employee_count INTEGER,
                budget INTEGER,
                cost_center TEXT,
                location TEXT,
                establishment_date DATETIME,
                status TEXT,
                created_time DATETIME,
                updated_time DATETIME
            )
        """)
        
        # 人员表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hr_persons (
                hr_person_id TEXT PRIMARY KEY,
                employee_id TEXT UNIQUE,
                person_name TEXT NOT NULL,
                english_name TEXT,
                gender TEXT,
                birth_date DATETIME,
                age INTEGER,
                id_card TEXT,
                phone TEXT,
                email TEXT,
                address TEXT,
                emergency_contact TEXT,
                emergency_phone TEXT,
                department_id TEXT,
                position TEXT,
                job_level TEXT,
                hire_date DATETIME,
                probation_end_date DATETIME,
                contract_type TEXT,
                employment_status TEXT,
                work_location TEXT,
                manager_id TEXT,
                salary INTEGER,
                education_level TEXT,
                major TEXT,
                university TEXT,
                skills TEXT,
                performance_rating TEXT,
                last_promotion_date DATETIME,
                created_time DATETIME,
                updated_time DATETIME
            )
        """)
        
        # 客户表（业务伙伴）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hr_customers (
                hr_customer_id TEXT PRIMARY KEY,
                customer_name TEXT NOT NULL,
                customer_type TEXT,
                business_license TEXT,
                contact_person TEXT,
                contact_title TEXT,
                contact_phone TEXT,
                contact_email TEXT,
                company_address TEXT,
                service_scope TEXT,
                cooperation_start_date DATETIME,
                contract_amount INTEGER,
                payment_terms TEXT,
                service_rating TEXT,
                status TEXT,
                created_time DATETIME,
                updated_time DATETIME
            )
        """)
        
        # 交易表（人事事务）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hr_transactions (
                hr_transaction_id TEXT PRIMARY KEY,
                transaction_number TEXT UNIQUE,
                employee_id TEXT,
                transaction_type TEXT,
                transaction_description TEXT,
                initiator TEXT,
                approver TEXT,
                transaction_date DATETIME,
                effective_date DATETIME,
                status TEXT,
                priority TEXT,
                department_id TEXT,
                cost_center TEXT,
                amount INTEGER,
                currency TEXT,
                attachment_count INTEGER,
                comments TEXT,
                created_by TEXT,
                created_time DATETIME,
                updated_time DATETIME
            )
        """)
        
        conn.commit()
        conn.close()
    
    def generate_and_save_data(self, org_count: int = None, person_count: int = None, 
                              customer_count: int = None, transaction_count: int = None):
        """生成并保存HR数据"""
        # 使用配置中的默认值
        org_count = org_count or settings.hr_data_size // 20
        person_count = person_count or settings.hr_data_size
        customer_count = customer_count or settings.hr_data_size // 10
        transaction_count = transaction_count or settings.hr_data_size * 2
        
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
                INSERT OR REPLACE INTO hr_organizations 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(org.values()))
        
        # 插入人员数据
        for person in persons:
            cursor.execute("""
                INSERT OR REPLACE INTO hr_persons 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(person.values()))
        
        # 插入客户数据
        for customer in customers:
            cursor.execute("""
                INSERT OR REPLACE INTO hr_customers 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(customer.values()))
        
        # 插入交易数据
        for transaction in transactions:
            cursor.execute("""
                INSERT OR REPLACE INTO hr_transactions 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    generator = HRDataGenerator()
    result = generator.generate_and_save_data()
    print(f"HR数据生成完成: {result}")