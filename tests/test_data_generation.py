"""
数据生成测试脚本
测试ERP、HR、FIN三个系统的数据生成功能
"""

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime
from typing import Dict, Any

from data.erp_data_generator import ERPDataGenerator
from data.hr_data_generator import HRDataGenerator
from data.fin_data_generator import FINDataGenerator
from data.init_database import DatabaseInitializer


class TestDataGeneration(unittest.TestCase):
    """数据生成测试类"""
    
    def setUp(self):
        """测试前准备"""
        # 创建临时数据库文件
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
        # 初始化生成器
        self.erp_generator = ERPDataGenerator(self.db_path)
        self.hr_generator = HRDataGenerator(self.db_path)
        self.fin_generator = FINDataGenerator(self.db_path)
        self.db_initializer = DatabaseInitializer(self.db_path)
    
    def tearDown(self):
        """测试后清理"""
        # 删除临时数据库文件
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def test_erp_data_generation(self):
        """测试ERP数据生成"""
        print("\n=== 测试ERP数据生成 ===")
        
        # 生成小量测试数据
        result = self.erp_generator.generate_and_save_data(
            org_count=5, person_count=10, customer_count=15, transaction_count=20
        )
        
        # 验证返回结果
        self.assertIsInstance(result, dict)
        self.assertEqual(result["organizations"], 5)
        self.assertEqual(result["persons"], 10)
        self.assertEqual(result["customers"], 15)
        self.assertEqual(result["transactions"], 20)
        
        # 验证数据库中的数据
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 检查表是否创建
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        expected_tables = ["erp_organizations", "erp_persons", "erp_customers", "erp_transactions"]
        for table in expected_tables:
            self.assertIn(table, tables, f"表 {table} 未创建")
        
        # 检查数据数量
        cursor.execute("SELECT COUNT(*) FROM erp_organizations")
        self.assertEqual(cursor.fetchone()[0], 5)
        
        cursor.execute("SELECT COUNT(*) FROM erp_persons")
        self.assertEqual(cursor.fetchone()[0], 10)
        
        cursor.execute("SELECT COUNT(*) FROM erp_customers")
        self.assertEqual(cursor.fetchone()[0], 15)
        
        cursor.execute("SELECT COUNT(*) FROM erp_transactions")
        self.assertEqual(cursor.fetchone()[0], 20)
        
        # 检查数据结构
        cursor.execute("SELECT * FROM erp_organizations LIMIT 1")
        org_data = cursor.fetchone()
        self.assertIsNotNone(org_data)
        self.assertTrue(org_data[0].startswith("ERPORG"))  # erp_org_id
        
        cursor.execute("SELECT * FROM erp_persons LIMIT 1")
        person_data = cursor.fetchone()
        self.assertIsNotNone(person_data)
        self.assertTrue(person_data[0].startswith("ERPPER"))  # erp_person_id
        
        conn.close()
        print("✓ ERP数据生成测试通过")
    
    def test_hr_data_generation(self):
        """测试HR数据生成"""
        print("\n=== 测试HR数据生成 ===")
        
        # 生成小量测试数据
        result = self.hr_generator.generate_and_save_data(
            org_count=3, person_count=8, customer_count=12, transaction_count=15
        )
        
        # 验证返回结果
        self.assertIsInstance(result, dict)
        self.assertEqual(result["organizations"], 3)
        self.assertEqual(result["persons"], 8)
        self.assertEqual(result["customers"], 12)
        self.assertEqual(result["transactions"], 15)
        
        # 验证数据库中的数据
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 检查表是否创建
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        expected_tables = ["hr_organizations", "hr_persons", "hr_customers", "hr_transactions"]
        for table in expected_tables:
            self.assertIn(table, tables, f"表 {table} 未创建")
        
        # 检查数据数量
        cursor.execute("SELECT COUNT(*) FROM hr_organizations")
        self.assertEqual(cursor.fetchone()[0], 3)
        
        cursor.execute("SELECT COUNT(*) FROM hr_persons")
        self.assertEqual(cursor.fetchone()[0], 8)
        
        # 检查数据结构
        cursor.execute("SELECT * FROM hr_persons LIMIT 1")
        person_data = cursor.fetchone()
        self.assertIsNotNone(person_data)
        self.assertTrue(person_data[0].startswith("HRPER"))  # hr_person_id
        
        conn.close()
        print("✓ HR数据生成测试通过")
    
    def test_fin_data_generation(self):
        """测试FIN数据生成"""
        print("\n=== 测试FIN数据生成 ===")
        
        # 生成小量测试数据
        result = self.fin_generator.generate_and_save_data(
            org_count=4, person_count=6, customer_count=10, transaction_count=25
        )
        
        # 验证返回结果
        self.assertIsInstance(result, dict)
        self.assertEqual(result["organizations"], 4)
        self.assertEqual(result["persons"], 6)
        self.assertEqual(result["customers"], 10)
        self.assertEqual(result["transactions"], 25)
        
        # 验证数据库中的数据
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 检查表是否创建
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        expected_tables = ["fin_organizations", "fin_persons", "fin_customers", "fin_transactions"]
        for table in expected_tables:
            self.assertIn(table, tables, f"表 {table} 未创建")
        
        # 检查数据数量
        cursor.execute("SELECT COUNT(*) FROM fin_organizations")
        self.assertEqual(cursor.fetchone()[0], 4)
        
        cursor.execute("SELECT COUNT(*) FROM fin_transactions")
        self.assertEqual(cursor.fetchone()[0], 25)
        
        # 检查数据结构和特征
        cursor.execute("SELECT * FROM fin_transactions LIMIT 1")
        txn_data = cursor.fetchone()
        self.assertIsNotNone(txn_data)
        self.assertTrue(txn_data[0].startswith("FINTXN"))  # fin_transaction_id
        self.assertIsNotNone(txn_data[5])  # amount
        self.assertIsNotNone(txn_data[6])  # currency
        
        conn.close()
        print("✓ FIN数据生成测试通过")
    
    def test_database_initialization(self):
        """测试数据库初始化"""
        print("\n=== 测试数据库初始化 ===")
        
        # 测试初始化
        result = self.db_initializer.initialize_database(backup_existing=False)
        
        # 验证初始化结果
        self.assertEqual(result["status"], "success")
        self.assertIn("systems", result)
        self.assertIn("ERP", result["systems"])
        self.assertIn("HR", result["systems"])
        self.assertIn("FIN", result["systems"])
        
        # 验证数据库信息
        db_info = self.db_initializer.get_database_info()
        self.assertEqual(db_info["status"], "exists")
        self.assertGreater(len(db_info["tables"]), 10)  # 应该有12个主要表
        
        # 验证元数据表
        self.assertIn("system_metadata", db_info["tables"])
        self.assertIn("initialization_log", db_info["tables"])
        
        print("✓ 数据库初始化测试通过")
    
    def test_data_consistency(self):
        """测试数据一致性"""
        print("\n=== 测试数据一致性 ===")
        
        # 初始化数据库
        self.db_initializer.initialize_database(backup_existing=False)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 测试ERP数据一致性
        cursor.execute("SELECT COUNT(*) FROM erp_organizations WHERE erp_org_id IS NULL")
        self.assertEqual(cursor.fetchone()[0], 0, "ERP组织ID不能为空")
        
        cursor.execute("SELECT COUNT(*) FROM erp_persons WHERE person_name IS NULL")
        self.assertEqual(cursor.fetchone()[0], 0, "ERP人员姓名不能为空")
        
        # 测试HR数据一致性
        cursor.execute("SELECT COUNT(*) FROM hr_persons WHERE employee_id IS NULL")
        self.assertEqual(cursor.fetchone()[0], 0, "HR员工ID不能为空")
        
        # 测试FIN数据一致性
        cursor.execute("SELECT COUNT(*) FROM fin_transactions WHERE amount IS NULL")
        self.assertEqual(cursor.fetchone()[0], 0, "FIN交易金额不能为空")
        
        cursor.execute("SELECT COUNT(*) FROM fin_transactions WHERE amount <= 0")
        self.assertEqual(cursor.fetchone()[0], 0, "FIN交易金额必须大于0")
        
        conn.close()
        print("✓ 数据一致性测试通过")
    
    def test_data_uniqueness(self):
        """测试数据唯一性"""
        print("\n=== 测试数据唯一性 ===")
        
        # 初始化数据库
        self.db_initializer.initialize_database(backup_existing=False)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 测试ERP ID唯一性
        cursor.execute("SELECT COUNT(*) FROM erp_organizations")
        total_orgs = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT erp_org_id) FROM erp_organizations")
        unique_orgs = cursor.fetchone()[0]
        self.assertEqual(total_orgs, unique_orgs, "ERP组织ID必须唯一")
        
        # 测试HR ID唯一性
        cursor.execute("SELECT COUNT(*) FROM hr_persons")
        total_persons = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT hr_person_id) FROM hr_persons")
        unique_persons = cursor.fetchone()[0]
        self.assertEqual(total_persons, unique_persons, "HR人员ID必须唯一")
        
        # 测试FIN ID唯一性
        cursor.execute("SELECT COUNT(*) FROM fin_transactions")
        total_txns = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT fin_transaction_id) FROM fin_transactions")
        unique_txns = cursor.fetchone()[0]
        self.assertEqual(total_txns, unique_txns, "FIN交易ID必须唯一")
        
        conn.close()
        print("✓ 数据唯一性测试通过")


class TestDataCharacteristics(unittest.TestCase):
    """数据特征测试类"""
    
    def setUp(self):
        """测试前准备"""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        self.db_initializer = DatabaseInitializer(self.db_path)
        
        # 初始化数据库
        self.db_initializer.initialize_database(backup_existing=False)
    
    def tearDown(self):
        """测试后清理"""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def test_erp_characteristics(self):
        """测试ERP系统数据特征"""
        print("\n=== 测试ERP数据特征 ===")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 测试产品导向特征
        cursor.execute("SELECT COUNT(*) FROM erp_customers WHERE customer_type = '企业客户'")
        enterprise_customers = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM erp_customers")
        total_customers = cursor.fetchone()[0]
        
        # 企业客户应该占大部分
        self.assertGreater(enterprise_customers / total_customers, 0.5, "ERP应该以企业客户为主")
        
        # 测试交易金额范围（ERP通常金额较大）
        cursor.execute("SELECT AVG(amount) FROM erp_transactions")
        avg_amount = cursor.fetchone()[0]
        self.assertGreater(avg_amount, 10000, "ERP交易平均金额应该较大")
        
        conn.close()
        print("✓ ERP数据特征测试通过")
    
    def test_hr_characteristics(self):
        """测试HR系统数据特征"""
        print("\n=== 测试HR数据特征 ===")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 测试人员导向特征
        cursor.execute("SELECT COUNT(*) FROM hr_persons WHERE position LIKE '%经理%' OR position LIKE '%主管%'")
        managers = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM hr_persons")
        total_persons = cursor.fetchone()[0]
        
        # 管理人员比例应该合理
        manager_ratio = managers / total_persons
        self.assertGreater(manager_ratio, 0.1, "HR系统应该有合理的管理人员比例")
        self.assertLess(manager_ratio, 0.5, "管理人员比例不应过高")
        
        # 测试薪资数据存在
        cursor.execute("SELECT COUNT(*) FROM hr_persons WHERE salary > 0")
        persons_with_salary = cursor.fetchone()[0]
        self.assertEqual(persons_with_salary, total_persons, "所有HR人员都应该有薪资数据")
        
        conn.close()
        print("✓ HR数据特征测试通过")
    
    def test_fin_characteristics(self):
        """测试FIN系统数据特征"""
        print("\n=== 测试FIN数据特征 ===")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 测试客户导向特征
        cursor.execute("SELECT COUNT(DISTINCT customer_id) FROM fin_transactions")
        unique_customers = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM fin_transactions")
        total_transactions = cursor.fetchone()[0]
        
        # 平均每个客户的交易数应该较多（客户导向）
        avg_txns_per_customer = total_transactions / unique_customers
        self.assertGreater(avg_txns_per_customer, 2, "FIN系统客户应该有多笔交易")
        
        # 测试金额精确性（应该有小数）
        cursor.execute("SELECT COUNT(*) FROM fin_transactions WHERE amount != ROUND(amount)")
        decimal_amounts = cursor.fetchone()[0]
        self.assertGreater(decimal_amounts, 0, "FIN系统应该有精确的小数金额")
        
        # 测试多币种特征
        cursor.execute("SELECT COUNT(DISTINCT currency) FROM fin_transactions")
        currencies = cursor.fetchone()[0]
        self.assertGreater(currencies, 1, "FIN系统应该支持多币种")
        
        conn.close()
        print("✓ FIN数据特征测试通过")


def run_data_generation_tests():
    """运行数据生成测试"""
    print("开始运行数据生成测试...")
    print("=" * 60)
    
    # 创建测试套件
    test_suite = unittest.TestSuite()
    
    # 添加基础功能测试
    test_suite.addTest(TestDataGeneration('test_erp_data_generation'))
    test_suite.addTest(TestDataGeneration('test_hr_data_generation'))
    test_suite.addTest(TestDataGeneration('test_fin_data_generation'))
    test_suite.addTest(TestDataGeneration('test_database_initialization'))
    test_suite.addTest(TestDataGeneration('test_data_consistency'))
    test_suite.addTest(TestDataGeneration('test_data_uniqueness'))
    
    # 添加数据特征测试
    test_suite.addTest(TestDataCharacteristics('test_erp_characteristics'))
    test_suite.addTest(TestDataCharacteristics('test_hr_characteristics'))
    test_suite.addTest(TestDataCharacteristics('test_fin_characteristics'))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    print("\n" + "=" * 60)
    print(f"测试完成！")
    print(f"运行测试: {result.testsRun}")
    print(f"失败: {len(result.failures)}")
    print(f"错误: {len(result.errors)}")
    
    if result.failures:
        print("\n失败的测试:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")
    
    if result.errors:
        print("\n错误的测试:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_data_generation_tests()
    exit(0 if success else 1)