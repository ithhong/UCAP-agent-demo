"""
数据转换工具 (DataMapper)
将ERP、HR、FIN三个异构系统的数据转换为Canonical标准格式

作者: Tom
创建时间: 2025-10-31T11:36:48+08:00
"""

import re
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Union
from loguru import logger

from canonical.models import (
    Organization, Person, Customer, Transaction,
    SystemType, OrganizationType, TransactionType, StatusType
)


class DataMapper:
    """
    数据转换映射器
    
    提供统一的数据转换方法，将三个异构系统的数据标准化为Canonical格式
    支持日期、金额、ID、货币等各种数据类型的标准化转换
    """
    
    # 系统ID前缀映射
    SYSTEM_ID_MAPPING = {
        "erp": "erp",
        "hr": "hr", 
        "fin": "fin"
    }
    
    # 日期格式映射
    DATE_FORMATS = {
        "erp": ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"],  # ERP: YYYY-MM-DD
        "hr": ["%d/%m/%Y", "%d/%m/%Y %H:%M:%S"],   # HR: DD/MM/YYYY
        "fin": ["%m-%d-%Y %H:%M:%S", "%m-%d-%Y"]   # FIN: MM-DD-YYYY HH:mm:ss
    }
    
    # 货币符号映射
    CURRENCY_SYMBOLS = {
        "¥": "CNY",
        "￥": "CNY", 
        "$": "USD",
        "€": "EUR",
        "£": "GBP",
        "¢": "CNY"  # 分转换为CNY
    }
    
    # 状态映射
    STATUS_MAPPING = {
        # 通用状态
        "active": StatusType.ACTIVE,
        "inactive": StatusType.INACTIVE,
        "pending": StatusType.PENDING,
        "completed": StatusType.COMPLETED,
        "cancelled": StatusType.CANCELLED,
        
        # ERP系统状态
        "已确认": StatusType.COMPLETED,
        "处理中": StatusType.PENDING,
        "已完成": StatusType.COMPLETED,
        "已取消": StatusType.CANCELLED,
        
        # HR系统状态
        "待审批": StatusType.PENDING,
        "已审批": StatusType.ACTIVE,
        "已执行": StatusType.COMPLETED,
        "已拒绝": StatusType.CANCELLED,
        
        # FIN系统状态
        "已确认": StatusType.COMPLETED,
        "待审核": StatusType.PENDING,
        "已审核": StatusType.ACTIVE,
        "已入账": StatusType.COMPLETED,
        "已取消": StatusType.CANCELLED
    }
    
    # 交易类型映射
    TRANSACTION_TYPE_MAPPING = {
        # ERP系统
        "销售订单": TransactionType.SALES,
        "采购订单": TransactionType.EXPENSE,
        "退货单": TransactionType.SALES,
        "换货单": TransactionType.SALES,
        
        # HR系统
        "入职办理": TransactionType.SALARY,
        "离职办理": TransactionType.SALARY,
        "薪资调整": TransactionType.SALARY,
        "职位变更": TransactionType.SALARY,
        "培训申请": TransactionType.EXPENSE,
        "请假申请": TransactionType.SALARY,
        "绩效评估": TransactionType.SALARY,
        
        # FIN系统
        "收款": TransactionType.RECEIPT,
        "付款": TransactionType.PAYMENT,
        "转账": TransactionType.TRANSFER,
        "调整": TransactionType.EXPENSE,
        "结算": TransactionType.PAYMENT
    }
    
    # 组织类型映射
    ORG_TYPE_MAPPING = {
        # ERP系统
        "公司": OrganizationType.COMPANY,
        "部门": OrganizationType.DEPARTMENT,
        "小组": OrganizationType.TEAM,
        
        # HR系统 (从部门名称推断)
        "人事部": OrganizationType.DEPARTMENT,
        "财务部": OrganizationType.DEPARTMENT,
        "技术部": OrganizationType.DEPARTMENT,
        "销售部": OrganizationType.DEPARTMENT,
        "市场部": OrganizationType.DEPARTMENT,
        
        # FIN系统
        "成本中心": OrganizationType.COST_CENTER,
        "利润中心": OrganizationType.COST_CENTER,
        "投资中心": OrganizationType.COST_CENTER
    }

    @staticmethod
    def normalize_id(original_id: str, system_type: str) -> str:
        """
        标准化ID格式
        
        Args:
            original_id: 原始ID
            system_type: 系统类型 (erp/hr/fin)
            
        Returns:
            标准化后的ID，格式: {system}_{original_id}
            
        Raises:
            ValueError: 当系统类型不支持或ID格式无效时
        """
        if not original_id:
            raise ValueError("原始ID不能为空")
            
        if system_type not in DataMapper.SYSTEM_ID_MAPPING:
            raise ValueError(f"不支持的系统类型: {system_type}")
        
        # 清理原始ID中的特殊字符
        cleaned_id = re.sub(r'[^\w\-]', '_', str(original_id))
        
        # 如果已经是标准格式，直接返回
        if cleaned_id.startswith(f"{system_type}_"):
            return cleaned_id
            
        return f"{system_type}_{cleaned_id}"

    @staticmethod
    def normalize_date(date_value: Any, system_type: str) -> Optional[datetime]:
        """
        标准化日期格式
        
        Args:
            date_value: 日期值（可能是字符串、datetime或其他格式）
            system_type: 系统类型，用于确定日期格式
            
        Returns:
            标准化后的datetime对象，失败时返回None
        """
        if not date_value:
            return None
            
        # 如果已经是datetime对象，直接返回
        if isinstance(date_value, datetime):
            return date_value
            
        # 转换为字符串处理
        date_str = str(date_value).strip()
        if not date_str or date_str.lower() in ['none', 'null', '']:
            return None
        
        # 获取该系统的日期格式列表
        formats = DataMapper.DATE_FORMATS.get(system_type, [])
        
        # 尝试各种格式解析
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # 如果系统特定格式失败，尝试通用格式
        common_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M:%S", 
            "%d/%m/%Y",
            "%m-%d-%Y %H:%M:%S",
            "%m-%d-%Y",
            "%Y/%m/%d",
            "%d-%m-%Y"
        ]
        
        for fmt in common_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        logger.warning(f"无法解析日期格式: {date_value} (系统: {system_type})")
        return None

    @staticmethod
    def normalize_amount(amount_value: Any, system_type: str = None) -> Decimal:
        """
        标准化金额格式
        
        Args:
            amount_value: 金额值（可能包含货币符号、千分位分隔符等）
            system_type: 系统类型（可选，用于特殊处理）
            
        Returns:
            标准化后的Decimal金额
            
        Raises:
            ValueError: 当金额格式无效时
        """
        if amount_value is None:
            return Decimal('0.00')
        
        # 如果已经是Decimal类型，直接返回
        if isinstance(amount_value, Decimal):
            return amount_value.quantize(Decimal('0.01'))
            
        # 如果是数字类型，转换为Decimal
        if isinstance(amount_value, (int, float)):
            return Decimal(str(amount_value)).quantize(Decimal('0.01'))
        
        # 字符串处理
        amount_str = str(amount_value).strip()
        if not amount_str or amount_str.lower() in ['none', 'null', '']:
            return Decimal('0.00')
        
        # 保存原始字符串用于错误检查
        original_str = amount_str
        
        # 移除货币符号和空格
        for symbol in DataMapper.CURRENCY_SYMBOLS.keys():
            amount_str = amount_str.replace(symbol, '')
        
        # 移除千分位分隔符和其他非数字字符（保留小数点和负号）
        cleaned_str = re.sub(r'[^\d\.\-]', '', amount_str)
        
        # 检查是否包含无效的字母字符（如果清理后的字符串与原始字符串差异太大，可能是无效格式）
        if re.search(r'[a-zA-Z]', original_str) and not re.search(r'^\s*[\d\.\-\s,¥￥$€£¢]+\s*$', original_str):
            logger.warning(f"无法解析金额格式: {amount_value}")
            raise ValueError(f"无效的金额格式: {amount_value}")
        
        # 处理负号位置
        is_negative = cleaned_str.count('-') > 0
        cleaned_str = cleaned_str.replace('-', '')
        
        if not cleaned_str:
            return Decimal('0.00')
        
        try:
            amount = Decimal(cleaned_str)
            if is_negative:
                amount = -amount
            return amount.quantize(Decimal('0.01'))
        except (InvalidOperation, ValueError) as e:
            logger.warning(f"无法解析金额格式: {amount_value}")
            raise ValueError(f"无效的金额格式: {amount_value}") from e

    @staticmethod
    def normalize_currency(currency_value: Any, system_type: str = None) -> str:
        """
        标准化货币代码
        
        Args:
            currency_value: 货币值（可能是符号或代码）
            system_type: 系统类型（可选）
            
        Returns:
            标准化后的货币代码（默认CNY）
        """
        if not currency_value:
            return "CNY"
        
        currency_str = str(currency_value).strip().upper()
        
        # 直接匹配货币代码
        if currency_str in ["CNY", "USD", "EUR", "GBP", "JPY"]:
            return currency_str
        
        # 匹配货币符号
        for symbol, code in DataMapper.CURRENCY_SYMBOLS.items():
            if symbol in str(currency_value):
                return code
        
        # 默认返回CNY
        return "CNY"

    @staticmethod
    def normalize_status(status_value: Any, system_type: str = None) -> StatusType:
        """
        标准化状态值
        
        Args:
            status_value: 状态值
            system_type: 系统类型（可选）
            
        Returns:
            标准化后的StatusType枚举值
        """
        if not status_value:
            return StatusType.ACTIVE
        
        status_str = str(status_value).strip()
        
        # 直接匹配
        if status_str in DataMapper.STATUS_MAPPING:
            return DataMapper.STATUS_MAPPING[status_str]
        
        # 模糊匹配
        status_lower = status_str.lower()
        for key, value in DataMapper.STATUS_MAPPING.items():
            if key.lower() in status_lower or status_lower in key.lower():
                return value
        
        # 默认返回ACTIVE
        return StatusType.ACTIVE

    @staticmethod
    def normalize_transaction_type(tx_type_value: Any, system_type: str = None) -> TransactionType:
        """
        标准化交易类型
        
        Args:
            tx_type_value: 交易类型值
            system_type: 系统类型（可选）
            
        Returns:
            标准化后的TransactionType枚举值
        """
        if not tx_type_value:
            return TransactionType.SALES
        
        tx_type_str = str(tx_type_value).strip()
        
        # 直接匹配
        if tx_type_str in DataMapper.TRANSACTION_TYPE_MAPPING:
            return DataMapper.TRANSACTION_TYPE_MAPPING[tx_type_str]
        
        # 模糊匹配
        for key, value in DataMapper.TRANSACTION_TYPE_MAPPING.items():
            if key in tx_type_str or tx_type_str in key:
                return value
        
        # 根据系统类型推断默认值
        if system_type == "hr":
            return TransactionType.SALARY
        elif system_type == "fin":
            return TransactionType.PAYMENT
        else:
            return TransactionType.SALES

    @staticmethod
    def normalize_org_type(org_type_value: Any, org_name: str = None) -> OrganizationType:
        """
        标准化组织类型
        
        Args:
            org_type_value: 组织类型值
            org_name: 组织名称（用于推断类型）
            
        Returns:
            标准化后的OrganizationType枚举值
        """
        if org_type_value:
            org_type_str = str(org_type_value).strip()
            
            # 直接匹配
            if org_type_str in DataMapper.ORG_TYPE_MAPPING:
                return DataMapper.ORG_TYPE_MAPPING[org_type_str]
        
        # 从组织名称推断
        if org_name:
            name_str = str(org_name)
            if "公司" in name_str or "集团" in name_str:
                return OrganizationType.COMPANY
            elif "部" in name_str or "部门" in name_str:
                return OrganizationType.DEPARTMENT
            elif "中心" in name_str:
                return OrganizationType.COST_CENTER
            elif "组" in name_str or "团队" in name_str:
                return OrganizationType.TEAM
        
        # 默认返回部门
        return OrganizationType.DEPARTMENT

    @staticmethod
    def extract_system_type(data: Dict[str, Any]) -> SystemType:
        """
        从数据中提取系统类型
        
        Args:
            data: 原始数据字典
            
        Returns:
            SystemType枚举值
        """
        # 检查ID字段前缀
        for key, value in data.items():
            if key.endswith('_id') and value:
                value_str = str(value)
                if value_str.startswith('erp') or 'erp' in key.lower():
                    return SystemType.ERP
                elif value_str.startswith('hr') or 'hr' in key.lower():
                    return SystemType.HR
                elif value_str.startswith('fin') or 'fin' in key.lower():
                    return SystemType.FIN
        
        # 检查表名或其他标识
        for key in data.keys():
            if 'erp' in key.lower():
                return SystemType.ERP
            elif 'hr' in key.lower():
                return SystemType.HR
            elif 'fin' in key.lower():
                return SystemType.FIN
        
        # 默认返回ERP
        return SystemType.ERP

    @staticmethod
    def clean_text(text_value: Any, max_length: int = None) -> Optional[str]:
        """
        清理文本数据
        
        Args:
            text_value: 文本值
            max_length: 最大长度限制
            
        Returns:
            清理后的文本，None表示空值
        """
        if not text_value:
            return None
        
        text = str(text_value).strip()
        if not text or text.lower() in ['none', 'null', '']:
            return None
        
        # 移除特殊字符（保留中文、英文、数字、常用标点）
        text = re.sub(r'[^\w\s\u4e00-\u9fff\-\.\,\(\)（）]', '', text)
        
        # 限制长度
        if max_length and len(text) > max_length:
            text = text[:max_length]
        
        return text if text else None

    @staticmethod
    def validate_email(email_value: Any) -> Optional[str]:
        """
        验证和标准化邮箱地址
        
        Args:
            email_value: 邮箱值
            
        Returns:
            标准化后的邮箱地址，无效时返回None
        """
        if not email_value:
            return None
        
        email = str(email_value).strip().lower()
        
        # 简单的邮箱格式验证
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if re.match(email_pattern, email):
            return email
        
        return None

    @staticmethod
    def validate_phone(phone_value: Any) -> Optional[str]:
        """
        验证和标准化电话号码
        
        Args:
            phone_value: 电话号码值
            
        Returns:
            标准化后的电话号码，无效时返回None
        """
        if not phone_value:
            return None
        
        phone = str(phone_value).strip()
        
        # 移除所有非数字字符（保留+、-、()、空格）
        phone = re.sub(r'[^\d\+\-\(\)\s]', '', phone)
        
        # 检查是否包含足够的数字
        digits = re.sub(r'[^\d]', '', phone)
        if len(digits) < 7:  # 最少7位数字
            return None
        
        return phone if phone else None


# 导出主要类和方法
__all__ = [
    "DataMapper"
]