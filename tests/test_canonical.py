"""
Canonical模型和数据转换工具单元测试
测试Pydantic模型验证和DataMapper转换功能

作者: Tom
创建时间: 2025-11-02T22:08:53+08:00
"""

import unittest
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Any
import tempfile
import os

# 导入被测试的模块
from canonical.models import (
    Organization, Person, Customer, Transaction, CanonicalDataSummary,
    SystemType, OrganizationType, TransactionType, StatusType
)
from canonical.mapper import DataMapper


class TestCanonicalModels(unittest.TestCase):
    """测试Canonical数据模型"""
    
    def setUp(self):
        """测试前准备"""
        self.test_datetime = datetime(2025, 11, 2, 22, 8, 53)
        
    def test_organization_model_creation(self):
        """测试Organization模型创建"""
        print("\n=== 测试Organization模型创建 ===")
        
        # 测试完整数据创建
        org_data = {
            "org_id": "erp_ORG000001",
            "source_system": SystemType.ERP,
            "org_name": "测试科技有限公司",
            "org_type": OrganizationType.COMPANY,
            "parent_org_id": None,
            "org_code": "TEST001",
            "contact_info": "contact@test.com",
            "address": "北京市朝阳区",
            "status": StatusType.ACTIVE,
            "raw_data": {"erp_org_id": "ORG000001"},
            "created_at": self.test_datetime
        }
        
        org = Organization(**org_data)
        
        # 验证基本属性
        self.assertEqual(org.org_id, "erp_ORG000001")
        self.assertEqual(org.source_system, SystemType.ERP)
        self.assertEqual(org.org_name, "测试科技有限公司")
        self.assertEqual(org.org_type, OrganizationType.COMPANY)
        self.assertEqual(org.status, StatusType.ACTIVE)
        
        print(f"✓ Organization创建成功: {org.org_name}")
        
    def test_organization_model_validation(self):
        """测试Organization模型验证"""
        print("\n=== 测试Organization模型验证 ===")
        
        # 测试必填字段验证
        with self.assertRaises(ValueError):
            Organization()  # 缺少必填字段
            
        # 测试ID格式验证
        with self.assertRaises(ValueError):
            Organization(
                org_id="invalid_id",  # 无效ID格式
                source_system=SystemType.ERP,
                org_name="测试公司"
            )
            
        print("✓ Organization验证规则正常工作")
        
    def test_person_model_creation(self):
        """测试Person模型创建"""
        print("\n=== 测试Person模型创建 ===")
        
        person_data = {
            "person_id": "hr_EMP000001",
            "source_system": SystemType.HR,
            "person_name": "张三",
            "employee_number": "EMP001",
            "org_id": "hr_ORG000001",
            "department": "技术部",
            "position": "软件工程师",
            "email": "zhangsan@test.com",
            "phone": "13800138000",
            "hire_date": self.test_datetime,
            "status": StatusType.ACTIVE,
            "raw_data": {"hr_person_id": "EMP000001"},
            "created_at": self.test_datetime
        }
        
        person = Person(**person_data)
        
        # 验证基本属性
        self.assertEqual(person.person_id, "hr_EMP000001")
        self.assertEqual(person.source_system, SystemType.HR)
        self.assertEqual(person.person_name, "张三")
        self.assertEqual(person.email, "zhangsan@test.com")
        
        print(f"✓ Person创建成功: {person.person_name}")
        
    def test_customer_model_creation(self):
        """测试Customer模型创建"""
        print("\n=== 测试Customer模型创建 ===")
        
        customer_data = {
            "customer_id": "erp_CUST000001",
            "source_system": SystemType.ERP,
            "customer_name": "客户A公司",
            "customer_code": "CUST001",
            "customer_type": "企业客户",
            "contact_person": "李四",
            "contact_email": "lisi@customer.com",
            "contact_phone": "13900139000",
            "address": "上海市浦东新区",
            "credit_level": "A",
            "status": StatusType.ACTIVE,
            "raw_data": {"erp_customer_id": "CUST000001"},
            "created_at": self.test_datetime
        }
        
        customer = Customer(**customer_data)
        
        # 验证基本属性
        self.assertEqual(customer.customer_id, "erp_CUST000001")
        self.assertEqual(customer.customer_name, "客户A公司")
        self.assertEqual(customer.credit_level, "A")
        
        print(f"✓ Customer创建成功: {customer.customer_name}")
        
    def test_transaction_model_creation(self):
        """测试Transaction模型创建"""
        print("\n=== 测试Transaction模型创建 ===")
        
        transaction_data = {
            "tx_id": "fin_TXN000001",
            "source_system": SystemType.FIN,
            "transaction_number": "TXN001",
            "customer_id": "erp_CUST000001",
            "tx_type": TransactionType.SALES,
            "amount": Decimal("999.90"),
            "currency": "CNY",
            "tx_date": self.test_datetime,
            "org_id": "erp_ORG000001",
            "description": "测试交易",
            "product_info": "测试产品",
            "payment_method": "现金支付",
            "status": StatusType.COMPLETED,
            "raw_data": {"fin_transaction_id": "TXN000001"},
            "created_at": self.test_datetime
        }
        
        transaction = Transaction(**transaction_data)
        
        # 验证基本属性
        self.assertEqual(transaction.tx_id, "fin_TXN000001")
        self.assertEqual(transaction.tx_type, TransactionType.SALES)
        self.assertEqual(transaction.amount, Decimal("999.90"))
        self.assertEqual(transaction.currency, "CNY")
        
        print(f"✓ Transaction创建成功: {transaction.transaction_number}")
        
    def test_canonical_data_summary(self):
        """测试CanonicalDataSummary模型"""
        print("\n=== 测试CanonicalDataSummary模型 ===")
        
        summary_data = {
            "total_organizations": 10,
            "total_persons": 50,
            "total_customers": 30,
            "total_transactions": 100,
            "erp_data_count": {"organizations": 40, "persons": 35, "customers": 25},
            "hr_data_count": {"persons": 35},
            "fin_data_count": {"transactions": 25},
            "data_quality_score": 95.0,
            "last_updated": self.test_datetime
        }
        
        summary = CanonicalDataSummary(**summary_data)
        
        # 验证统计数据
        self.assertEqual(summary.total_organizations, 10)
        self.assertEqual(summary.total_transactions, 100)
        self.assertEqual(summary.data_quality_score, 95.0)
        self.assertEqual(summary.erp_data_count["organizations"], 40)
        
        print("✓ CanonicalDataSummary创建成功")


class TestDataMapper(unittest.TestCase):
    """测试DataMapper数据转换工具"""
    
    def setUp(self):
        """测试前准备"""
        self.mapper = DataMapper()
        
    def test_normalize_id(self):
        """测试ID标准化"""
        print("\n=== 测试ID标准化 ===")
        
        # 测试ERP系统ID
        erp_id = DataMapper.normalize_id("ORG000001", "erp")
        self.assertEqual(erp_id, "erp_ORG000001")
        
        # 测试HR系统ID
        hr_id = DataMapper.normalize_id("HR_EMP_00001", "hr")
        self.assertEqual(hr_id, "hr_HR_EMP_00001")
        
        # 测试FIN系统ID
        fin_id = DataMapper.normalize_id("FIN12345678", "fin")
        self.assertEqual(fin_id, "fin_FIN12345678")
        
        # 测试已标准化的ID
        existing_id = DataMapper.normalize_id("erp_ORG000001", "erp")
        self.assertEqual(existing_id, "erp_ORG000001")
        
        # 测试特殊字符清理
        special_id = DataMapper.normalize_id("ORG@001#", "erp")
        self.assertEqual(special_id, "erp_ORG_001_")
        
        # 测试异常情况
        with self.assertRaises(ValueError):
            DataMapper.normalize_id("", "erp")  # 空ID
            
        with self.assertRaises(ValueError):
            DataMapper.normalize_id("ORG001", "invalid")  # 无效系统类型
            
        print("✓ ID标准化测试通过")
        
    def test_normalize_date(self):
        """测试日期标准化"""
        print("\n=== 测试日期标准化 ===")
        
        # 测试ERP系统日期格式 (YYYY-MM-DD)
        erp_date = DataMapper.normalize_date("2025-11-02", "erp")
        expected_date = datetime(2025, 11, 2)
        self.assertEqual(erp_date, expected_date)
        
        # 测试HR系统日期格式 (DD/MM/YYYY)
        hr_date = DataMapper.normalize_date("02/11/2025", "hr")
        self.assertEqual(hr_date, expected_date)
        
        # 测试FIN系统日期格式 (MM-DD-YYYY HH:mm:ss)
        fin_date = DataMapper.normalize_date("11-02-2025 22:08:53", "fin")
        expected_datetime = datetime(2025, 11, 2, 22, 8, 53)
        self.assertEqual(fin_date, expected_datetime)
        
        # 测试datetime对象直接返回
        dt_obj = datetime(2025, 11, 2)
        result = DataMapper.normalize_date(dt_obj, "erp")
        self.assertEqual(result, dt_obj)
        
        # 测试空值处理
        self.assertIsNone(DataMapper.normalize_date(None, "erp"))
        self.assertIsNone(DataMapper.normalize_date("", "erp"))
        self.assertIsNone(DataMapper.normalize_date("null", "erp"))
        
        # 测试无效日期格式
        invalid_date = DataMapper.normalize_date("invalid-date", "erp")
        self.assertIsNone(invalid_date)
        
        print("✓ 日期标准化测试通过")
        
    def test_normalize_amount(self):
        """测试金额标准化"""
        print("\n=== 测试金额标准化 ===")
        
        # 测试基本数字
        amount1 = DataMapper.normalize_amount(100.50)
        self.assertEqual(amount1, Decimal("100.50"))
        
        # 测试字符串数字
        amount2 = DataMapper.normalize_amount("999.99")
        self.assertEqual(amount2, Decimal("999.99"))
        
        # 测试带货币符号的金额
        amount3 = DataMapper.normalize_amount("¥1,234.56")
        self.assertEqual(amount3, Decimal("1234.56"))
        
        amount4 = DataMapper.normalize_amount("$999.99")
        self.assertEqual(amount4, Decimal("999.99"))
        
        # 测试负数
        amount5 = DataMapper.normalize_amount("-500.00")
        self.assertEqual(amount5, Decimal("-500.00"))
        
        # 测试Decimal对象
        decimal_amount = Decimal("123.456")
        result = DataMapper.normalize_amount(decimal_amount)
        self.assertEqual(result, Decimal("123.46"))  # 保留两位小数
        
        # 测试空值处理
        self.assertEqual(DataMapper.normalize_amount(None), Decimal("0.00"))
        self.assertEqual(DataMapper.normalize_amount(""), Decimal("0.00"))
        
        # 测试无效金额
        with self.assertRaises(ValueError):
            DataMapper.normalize_amount("invalid-amount")
            
        print("✓ 金额标准化测试通过")
        
    def test_normalize_currency(self):
        """测试货币标准化"""
        print("\n=== 测试货币标准化 ===")
        
        # 测试货币代码
        self.assertEqual(DataMapper.normalize_currency("CNY"), "CNY")
        self.assertEqual(DataMapper.normalize_currency("USD"), "USD")
        self.assertEqual(DataMapper.normalize_currency("eur"), "EUR")  # 小写转大写
        
        # 测试货币符号
        self.assertEqual(DataMapper.normalize_currency("¥"), "CNY")
        self.assertEqual(DataMapper.normalize_currency("￥"), "CNY")
        self.assertEqual(DataMapper.normalize_currency("$"), "USD")
        self.assertEqual(DataMapper.normalize_currency("€"), "EUR")
        self.assertEqual(DataMapper.normalize_currency("£"), "GBP")
        
        # 测试空值和默认值
        self.assertEqual(DataMapper.normalize_currency(None), "CNY")
        self.assertEqual(DataMapper.normalize_currency(""), "CNY")
        self.assertEqual(DataMapper.normalize_currency("unknown"), "CNY")
        
        print("✓ 货币标准化测试通过")
        
    def test_normalize_status(self):
        """测试状态标准化"""
        print("\n=== 测试状态标准化 ===")
        
        # 测试英文状态
        self.assertEqual(DataMapper.normalize_status("active"), StatusType.ACTIVE)
        self.assertEqual(DataMapper.normalize_status("pending"), StatusType.PENDING)
        self.assertEqual(DataMapper.normalize_status("completed"), StatusType.COMPLETED)
        
        # 测试中文状态
        self.assertEqual(DataMapper.normalize_status("已确认"), StatusType.COMPLETED)
        self.assertEqual(DataMapper.normalize_status("处理中"), StatusType.PENDING)
        self.assertEqual(DataMapper.normalize_status("待审批"), StatusType.PENDING)
        
        # 测试模糊匹配
        self.assertEqual(DataMapper.normalize_status("已完成订单"), StatusType.COMPLETED)
        
        # 测试默认值
        self.assertEqual(DataMapper.normalize_status(None), StatusType.ACTIVE)
        self.assertEqual(DataMapper.normalize_status("unknown"), StatusType.ACTIVE)
        
        print("✓ 状态标准化测试通过")
        
    def test_normalize_transaction_type(self):
        """测试交易类型标准化"""
        print("\n=== 测试交易类型标准化 ===")
        
        # 测试ERP系统交易类型
        self.assertEqual(DataMapper.normalize_transaction_type("销售订单"), TransactionType.SALES)
        self.assertEqual(DataMapper.normalize_transaction_type("采购订单"), TransactionType.EXPENSE)
        
        # 测试HR系统交易类型
        self.assertEqual(DataMapper.normalize_transaction_type("薪资调整", "hr"), TransactionType.SALARY)
        self.assertEqual(DataMapper.normalize_transaction_type("培训申请", "hr"), TransactionType.EXPENSE)
        
        # 测试FIN系统交易类型
        self.assertEqual(DataMapper.normalize_transaction_type("收款", "fin"), TransactionType.RECEIPT)
        self.assertEqual(DataMapper.normalize_transaction_type("付款", "fin"), TransactionType.PAYMENT)
        
        # 测试默认值
        self.assertEqual(DataMapper.normalize_transaction_type(None), TransactionType.SALES)
        self.assertEqual(DataMapper.normalize_transaction_type("unknown", "hr"), TransactionType.SALARY)
        
        print("✓ 交易类型标准化测试通过")
        
    def test_normalize_org_type(self):
        """测试组织类型标准化"""
        print("\n=== 测试组织类型标准化 ===")
        
        # 测试直接类型匹配
        self.assertEqual(DataMapper.normalize_org_type("公司"), OrganizationType.COMPANY)
        self.assertEqual(DataMapper.normalize_org_type("部门"), OrganizationType.DEPARTMENT)
        
        # 测试从组织名称推断
        self.assertEqual(DataMapper.normalize_org_type(None, "测试科技有限公司"), OrganizationType.COMPANY)
        self.assertEqual(DataMapper.normalize_org_type(None, "人事部"), OrganizationType.DEPARTMENT)
        self.assertEqual(DataMapper.normalize_org_type(None, "技术中心"), OrganizationType.COST_CENTER)
        self.assertEqual(DataMapper.normalize_org_type(None, "开发小组"), OrganizationType.TEAM)
        
        # 测试默认值
        self.assertEqual(DataMapper.normalize_org_type(None, None), OrganizationType.DEPARTMENT)
        
        print("✓ 组织类型标准化测试通过")
        
    def test_extract_system_type(self):
        """测试系统类型提取"""
        print("\n=== 测试系统类型提取 ===")
        
        # 测试从ID字段提取
        erp_data = {"erp_org_id": "ORG000001", "org_name": "测试公司"}
        self.assertEqual(DataMapper.extract_system_type(erp_data), SystemType.ERP)
        
        hr_data = {"hr_person_id": "EMP000001", "person_name": "张三"}
        self.assertEqual(DataMapper.extract_system_type(hr_data), SystemType.HR)
        
        fin_data = {"fin_transaction_id": "TXN000001", "amount": "1000.00"}
        self.assertEqual(DataMapper.extract_system_type(fin_data), SystemType.FIN)
        
        # 测试从字段名提取
        erp_data2 = {"organization_id": "123", "erp_code": "ERP001"}
        self.assertEqual(DataMapper.extract_system_type(erp_data2), SystemType.ERP)
        
        # 测试默认值
        unknown_data = {"id": "123", "name": "test"}
        self.assertEqual(DataMapper.extract_system_type(unknown_data), SystemType.ERP)
        
        print("✓ 系统类型提取测试通过")
        
    def test_clean_text(self):
        """测试文本清理"""
        print("\n=== 测试文本清理 ===")
        
        # 测试基本清理
        cleaned = DataMapper.clean_text("  测试文本  ")
        self.assertEqual(cleaned, "测试文本")
        
        # 测试特殊字符清理
        cleaned2 = DataMapper.clean_text("测试@#$文本123")
        self.assertEqual(cleaned2, "测试文本123")
        
        # 测试长度限制
        long_text = "这是一个很长的测试文本" * 10
        cleaned3 = DataMapper.clean_text(long_text, max_length=10)
        self.assertEqual(len(cleaned3), 10)
        
        # 测试空值处理
        self.assertIsNone(DataMapper.clean_text(None))
        self.assertIsNone(DataMapper.clean_text(""))
        self.assertIsNone(DataMapper.clean_text("null"))
        
        print("✓ 文本清理测试通过")
        
    def test_validate_email(self):
        """测试邮箱验证"""
        print("\n=== 测试邮箱验证 ===")
        
        # 测试有效邮箱
        valid_emails = [
            "test@example.com",
            "user.name@domain.co.uk",
            "user+tag@example.org"
        ]
        
        for email in valid_emails:
            result = DataMapper.validate_email(email)
            self.assertEqual(result, email.lower())
            
        # 测试无效邮箱
        invalid_emails = [
            "invalid-email",
            "@domain.com",
            "user@",
            "user@domain",
            ""
        ]
        
        for email in invalid_emails:
            result = DataMapper.validate_email(email)
            self.assertIsNone(result)
            
        # 测试空值
        self.assertIsNone(DataMapper.validate_email(None))
        
        print("✓ 邮箱验证测试通过")
        
    def test_validate_phone(self):
        """测试电话验证"""
        print("\n=== 测试电话验证 ===")
        
        # 测试有效电话
        valid_phones = [
            "13800138000",
            "+86-138-0013-8000",
            "(010) 8888-8888",
            "400-123-4567"
        ]
        
        for phone in valid_phones:
            result = DataMapper.validate_phone(phone)
            self.assertIsNotNone(result)
            
        # 测试无效电话
        invalid_phones = [
            "123",  # 太短
            "abc123",  # 包含字母
            "",
            None
        ]
        
        for phone in invalid_phones:
            result = DataMapper.validate_phone(phone)
            self.assertIsNone(result)
            
        print("✓ 电话验证测试通过")


class TestIntegration(unittest.TestCase):
    """集成测试"""
    
    def test_complete_data_transformation(self):
        """测试完整的数据转换流程"""
        print("\n=== 测试完整数据转换流程 ===")
        
        # 模拟ERP系统原始数据
        erp_org_data = {
            "erp_org_id": "ORG000001",
            "org_name": "测试科技有限公司",
            "org_type": "公司",
            "established_date": "2025-01-15",
            "status": "active",
            "contact_info": "contact@test.com",
            "address": "北京市朝阳区"
        }
        
        # 使用DataMapper转换数据
        canonical_id = DataMapper.normalize_id(erp_org_data["erp_org_id"], "erp")
        system_type = DataMapper.extract_system_type(erp_org_data)
        org_type = DataMapper.normalize_org_type(erp_org_data["org_type"], erp_org_data["org_name"])
        established_date = DataMapper.normalize_date(erp_org_data["established_date"], "erp")
        status = DataMapper.normalize_status(erp_org_data["status"])
        
        # 创建Canonical模型
        org = Organization(
            org_id=canonical_id,
            source_system=system_type,
            org_name=erp_org_data["org_name"],
            org_type=org_type,
            status=status,
            contact_info=erp_org_data["contact_info"],
            address=erp_org_data["address"],
            raw_data=erp_org_data,
            created_at=datetime.now()
        )
        
        # 验证转换结果
        self.assertEqual(org.org_id, "erp_ORG000001")
        self.assertEqual(org.source_system, SystemType.ERP)
        self.assertEqual(org.org_type, OrganizationType.COMPANY)
        self.assertEqual(org.status, StatusType.ACTIVE)
        
        print(f"✓ 完整数据转换成功: {org.org_name}")
        
    def test_cross_system_data_consistency(self):
        """测试跨系统数据一致性"""
        print("\n=== 测试跨系统数据一致性 ===")
        
        # 创建来自不同系统的相同实体数据
        systems_data = {
            "erp": {
                "person_id": "EMP000001",
                "name": "张三",
                "email": "zhangsan@company.com",
                "phone": "13800138000"
            },
            "hr": {
                "person_id": "HR_EMP_00001", 
                "name": "张三",
                "email": "zhangsan@company.com",
                "phone": "138-0013-8000"
            }
        }
        
        persons = []
        for system, data in systems_data.items():
            canonical_id = DataMapper.normalize_id(data["person_id"], system)
            system_type = SystemType.ERP if system == "erp" else SystemType.HR
            email = DataMapper.validate_email(data["email"])
            phone = DataMapper.validate_phone(data["phone"])
            
            person = Person(
                person_id=canonical_id,
                source_system=system_type,
                person_name=data["name"],
                email=email,
                phone=phone,
                org_id="erp_ORG000001",  # 添加必需的org_id字段
                raw_data=data,
                created_at=datetime.now()
            )
            persons.append(person)
        
        # 验证数据一致性
        self.assertEqual(persons[0].person_name, persons[1].person_name)
        self.assertEqual(persons[0].email, persons[1].email)
        # 电话号码经过标准化后应该包含相同的数字
        
        print("✓ 跨系统数据一致性验证通过")


def run_canonical_tests():
    """运行所有Canonical测试"""
    print("=" * 60)
    print("开始运行Canonical模型和数据转换工具测试")
    print("=" * 60)
    
    # 创建测试套件
    test_suite = unittest.TestSuite()
    
    # 添加测试类
    test_classes = [
        TestCanonicalModels,
        TestDataMapper,
        TestIntegration
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # 输出测试结果摘要
    print("\n" + "=" * 60)
    print("测试结果摘要:")
    print(f"总测试数: {result.testsRun}")
    print(f"成功: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"失败: {len(result.failures)}")
    print(f"错误: {len(result.errors)}")
    
    if result.failures:
        print("\n失败的测试:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")
    
    if result.errors:
        print("\n错误的测试:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")
    
    success = len(result.failures) == 0 and len(result.errors) == 0
    print(f"\n测试{'成功' if success else '失败'}!")
    print("=" * 60)
    
    return success


if __name__ == "__main__":
    success = run_canonical_tests()
    exit(0 if success else 1)