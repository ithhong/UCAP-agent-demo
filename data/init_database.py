"""
数据库初始化脚本
统一管理ERP、HR、FIN三个系统的数据生成
"""

import os
import sqlite3
from datetime import datetime
from typing import Dict, Any
from loguru import logger

from config.settings import get_settings
from data.erp_data_generator import ERPDataGenerator
from data.hr_data_generator import HRDataGenerator
from data.fin_data_generator import FINDataGenerator

settings = get_settings()


class DatabaseInitializer:
    """数据库初始化器"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or settings.database_path
        self.erp_generator = ERPDataGenerator(self.db_path)
        self.hr_generator = HRDataGenerator(self.db_path)
        self.fin_generator = FINDataGenerator(self.db_path)
        
        # 配置日志
        logger.add(
            settings.log_file,
            rotation="10 MB",
            retention="30 days",
            level=settings.log_level
        )
    
    def check_database_exists(self) -> bool:
        """检查数据库是否存在"""
        return os.path.exists(self.db_path)
    
    def backup_existing_database(self) -> str:
        """备份现有数据库"""
        if not self.check_database_exists():
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{self.db_path}.backup_{timestamp}"
        
        try:
            # 复制数据库文件
            import shutil
            shutil.copy2(self.db_path, backup_path)
            logger.info(f"数据库已备份到: {backup_path}")
            return backup_path
        except Exception as e:
            logger.error(f"数据库备份失败: {e}")
            raise
    
    def create_database_directory(self):
        """创建数据库目录"""
        db_dir = os.path.dirname(self.db_path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
            logger.info(f"创建数据库目录: {db_dir}")
    
    def initialize_database(self, backup_existing: bool = True, force_recreate: bool = False) -> Dict[str, Any]:
        """
        初始化数据库
        
        Args:
            backup_existing: 是否备份现有数据库
            force_recreate: 是否强制重新创建
        
        Returns:
            初始化结果统计
        """
        logger.info("开始初始化数据库...")
        
        # 创建数据库目录
        self.create_database_directory()
        
        # 备份现有数据库
        backup_path = None
        if backup_existing and self.check_database_exists():
            if force_recreate:
                backup_path = self.backup_existing_database()
                os.remove(self.db_path)
                logger.info("已删除现有数据库，准备重新创建")
            else:
                logger.info("数据库已存在，跳过初始化")
                return {"status": "skipped", "message": "数据库已存在"}
        
        try:
            # 初始化结果统计
            results = {
                "status": "success",
                "backup_path": backup_path,
                "start_time": datetime.now(),
                "systems": {}
            }
            
            # 1. 创建ERP系统数据
            logger.info("开始生成ERP系统数据...")
            erp_result = self.erp_generator.generate_and_save_data()
            results["systems"]["ERP"] = erp_result
            logger.info(f"ERP数据生成完成: {erp_result}")
            
            # 2. 创建HR系统数据
            logger.info("开始生成HR系统数据...")
            hr_result = self.hr_generator.generate_and_save_data()
            results["systems"]["HR"] = hr_result
            logger.info(f"HR数据生成完成: {hr_result}")
            
            # 3. 创建FIN系统数据
            logger.info("开始生成FIN系统数据...")
            fin_result = self.fin_generator.generate_and_save_data()
            results["systems"]["FIN"] = fin_result
            logger.info(f"FIN数据生成完成: {fin_result}")
            
            # 4. 创建系统元数据表
            self._create_metadata_table()
            
            results["end_time"] = datetime.now()
            results["duration"] = (results["end_time"] - results["start_time"]).total_seconds()
            
            self._insert_system_metadata(results)
            
            logger.info(f"数据库初始化完成，耗时: {results['duration']:.2f}秒")
            return results
            
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            # 如果有备份，尝试恢复
            if backup_path and os.path.exists(backup_path):
                try:
                    import shutil
                    shutil.copy2(backup_path, self.db_path)
                    logger.info("已从备份恢复数据库")
                except Exception as restore_error:
                    logger.error(f"数据库恢复失败: {restore_error}")
            raise
    
    def _create_metadata_table(self):
        """创建系统元数据表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                system_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                record_count INTEGER,
                last_updated DATETIME,
                data_version TEXT,
                description TEXT
            )
        """)
        
        # 创建数据库初始化日志表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS initialization_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                init_time DATETIME,
                duration_seconds REAL,
                total_records INTEGER,
                backup_path TEXT,
                status TEXT,
                error_message TEXT
            )
        """)
        
        conn.commit()
        conn.close()
    
    def _insert_system_metadata(self, results: Dict[str, Any]):
        """插入系统元数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 清除旧的元数据
        cursor.execute("DELETE FROM system_metadata")
        
        # 插入新的元数据
        metadata_entries = [
            ("ERP", "erp_organizations", results["systems"]["ERP"]["organizations"], "ERP组织架构数据"),
            ("ERP", "erp_persons", results["systems"]["ERP"]["persons"], "ERP人员数据"),
            ("ERP", "erp_customers", results["systems"]["ERP"]["customers"], "ERP客户数据"),
            ("ERP", "erp_transactions", results["systems"]["ERP"]["transactions"], "ERP交易数据"),
            ("HR", "hr_organizations", results["systems"]["HR"]["organizations"], "HR组织架构数据"),
            ("HR", "hr_persons", results["systems"]["HR"]["persons"], "HR人员数据"),
            ("HR", "hr_customers", results["systems"]["HR"]["customers"], "HR内部客户数据"),
            ("HR", "hr_transactions", results["systems"]["HR"]["transactions"], "HR事务数据"),
            ("FIN", "fin_organizations", results["systems"]["FIN"]["organizations"], "FIN成本中心数据"),
            ("FIN", "fin_persons", results["systems"]["FIN"]["persons"], "FIN财务人员数据"),
            ("FIN", "fin_customers", results["systems"]["FIN"]["customers"], "FIN客户数据"),
            ("FIN", "fin_transactions", results["systems"]["FIN"]["transactions"], "FIN财务交易数据"),
        ]
        
        for system_name, table_name, count, description in metadata_entries:
            cursor.execute("""
                INSERT INTO system_metadata 
                (system_name, table_name, record_count, last_updated, data_version, description)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (system_name, table_name, count, datetime.now(), "1.0", description))
        
        # 插入初始化日志
        total_records = sum(
            sum(system_data.values()) for system_data in results["systems"].values()
        )
        
        cursor.execute("""
            INSERT INTO initialization_log 
            (init_time, duration_seconds, total_records, backup_path, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            results["start_time"],
            results["duration"],
            total_records,
            results.get("backup_path"),
            "success",
            None
        ))
        
        conn.commit()
        conn.close()
    
    def get_database_info(self) -> Dict[str, Any]:
        """获取数据库信息"""
        if not self.check_database_exists():
            return {"status": "not_exists", "message": "数据库不存在"}
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # 获取所有表信息
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            # 获取每个表的记录数
            table_counts = {}
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                table_counts[table] = cursor.fetchone()[0]
            
            # 获取元数据信息
            metadata = {}
            if "system_metadata" in tables:
                cursor.execute("SELECT * FROM system_metadata")
                metadata_rows = cursor.fetchall()
                for row in metadata_rows:
                    system = row[1]
                    if system not in metadata:
                        metadata[system] = []
                    metadata[system].append({
                        "table": row[2],
                        "count": row[3],
                        "last_updated": row[4],
                        "version": row[5],
                        "description": row[6]
                    })
            
            # 获取最近的初始化日志
            last_init = None
            if "initialization_log" in tables:
                cursor.execute("""
                    SELECT * FROM initialization_log 
                    ORDER BY init_time DESC LIMIT 1
                """)
                log_row = cursor.fetchone()
                if log_row:
                    last_init = {
                        "init_time": log_row[1],
                        "duration": log_row[2],
                        "total_records": log_row[3],
                        "backup_path": log_row[4],
                        "status": log_row[5]
                    }
            
            return {
                "status": "exists",
                "database_path": self.db_path,
                "database_size": os.path.getsize(self.db_path),
                "tables": tables,
                "table_counts": table_counts,
                "metadata": metadata,
                "last_initialization": last_init
            }
            
        except Exception as e:
            logger.error(f"获取数据库信息失败: {e}")
            return {"status": "error", "message": str(e)}
        finally:
            conn.close()
    
    def reset_database(self) -> Dict[str, Any]:
        """重置数据库（删除并重新创建）"""
        logger.info("开始重置数据库...")
        return self.initialize_database(backup_existing=True, force_recreate=True)


def main():
    """主函数"""
    initializer = DatabaseInitializer()
    
    # 显示当前数据库状态
    db_info = initializer.get_database_info()
    print(f"数据库状态: {db_info['status']}")
    
    if db_info["status"] == "exists":
        print(f"数据库路径: {db_info['database_path']}")
        print(f"数据库大小: {db_info['database_size']} bytes")
        print(f"表数量: {len(db_info['tables'])}")
        print("表记录统计:")
        for table, count in db_info["table_counts"].items():
            print(f"  {table}: {count} 条记录")
        
        # 询问是否重新初始化
        choice = input("\n数据库已存在，是否重新初始化？(y/N): ").lower()
        if choice == 'y':
            result = initializer.reset_database()
        else:
            print("跳过初始化")
            return
    else:
        # 初始化数据库
        result = initializer.initialize_database()
    
    # 显示结果
    if result["status"] == "success":
        print(f"\n数据库初始化成功！")
        print(f"耗时: {result['duration']:.2f}秒")
        print("数据生成统计:")
        for system, data in result["systems"].items():
            total = sum(data.values())
            print(f"  {system}系统: {total} 条记录")
            for table, count in data.items():
                print(f"    {table}: {count}")
    else:
        print(f"初始化失败: {result.get('message', '未知错误')}")


if __name__ == "__main__":
    main()