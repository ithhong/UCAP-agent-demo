"""
Canonical数据模型定义
统一的数据模型，用于将ERP、HR、FIN三个异构系统的数据映射到标准格式

作者: Tom
创建时间: 2025-10-31T11:36:48+08:00
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator, EmailStr
from enum import Enum


class SystemType(str, Enum):
    """系统类型枚举"""
    ERP = "erp"
    HR = "hr"
    FIN = "fin"


class OrganizationType(str, Enum):
    """组织类型枚举"""
    COMPANY = "company"
    DEPARTMENT = "dept"
    COST_CENTER = "cost_center"
    TEAM = "team"


class TransactionType(str, Enum):
    """交易类型枚举"""
    SALES = "sales"
    SALARY = "salary"
    EXPENSE = "expense"
    PAYMENT = "payment"
    RECEIPT = "receipt"
    TRANSFER = "transfer"


class StatusType(str, Enum):
    """状态类型枚举"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class Organization(BaseModel):
    """
    组织信息统一模型
    
    将ERP、HR、FIN三个系统的组织架构数据映射到统一格式
    支持公司、部门、成本中心等不同类型的组织单元
    """
    
    org_id: str = Field(
        ..., 
        description="统一组织ID，格式: {system}_{original_id}",
        regex=r"^(erp|hr|fin)_[A-Za-z0-9_-]+$"
    )
    
    org_name: str = Field(
        ..., 
        description="统一组织名称",
        min_length=1,
        max_length=200
    )
    
    org_type: OrganizationType = Field(
        ..., 
        description="组织类型: company/dept/cost_center/team"
    )
    
    parent_org_id: Optional[str] = Field(
        None, 
        description="父组织ID，支持层级结构",
        regex=r"^(erp|hr|fin)_[A-Za-z0-9_-]+$"
    )
    
    org_code: Optional[str] = Field(
        None, 
        description="组织编码，来源于原系统"
    )
    
    manager_name: Optional[str] = Field(
        None, 
        description="负责人姓名"
    )
    
    contact_info: Optional[str] = Field(
        None, 
        description="联系方式"
    )
    
    address: Optional[str] = Field(
        None, 
        description="地址信息"
    )
    
    status: StatusType = Field(
        default=StatusType.ACTIVE, 
        description="组织状态"
    )
    
    created_at: datetime = Field(
        default_factory=datetime.now, 
        description="创建时间，ISO格式"
    )
    
    source_system: SystemType = Field(
        ..., 
        description="数据来源系统"
    )
    
    raw_data: Optional[Dict[str, Any]] = Field(
        None, 
        description="原始数据，用于调试和追溯"
    )

    @validator('org_id')
    def validate_org_id(cls, v):
        """验证组织ID格式"""
        if not v:
            raise ValueError("组织ID不能为空")
        parts = v.split('_', 1)
        if len(parts) != 2 or parts[0] not in ['erp', 'hr', 'fin']:
            raise ValueError("组织ID格式错误，应为: {system}_{original_id}")
        return v

    @validator('source_system')
    def validate_source_system_consistency(cls, v, values):
        """验证来源系统与ID前缀一致性"""
        if 'org_id' in values:
            system_prefix = values['org_id'].split('_')[0]
            if system_prefix != v.value:
                raise ValueError(f"来源系统({v})与ID前缀({system_prefix})不一致")
        return v


class Person(BaseModel):
    """
    人员信息统一模型
    
    将ERP、HR、FIN三个系统的人员数据映射到统一格式
    支持员工、联系人等不同类型的人员信息
    """
    
    person_id: str = Field(
        ..., 
        description="统一人员ID，格式: {system}_{original_id}",
        regex=r"^(erp|hr|fin)_[A-Za-z0-9_-]+$"
    )
    
    person_name: str = Field(
        ..., 
        description="统一姓名格式",
        min_length=1,
        max_length=100
    )
    
    employee_number: Optional[str] = Field(
        None, 
        description="员工编号，来源于原系统"
    )
    
    org_id: str = Field(
        ..., 
        description="关联组织ID",
        regex=r"^(erp|hr|fin)_[A-Za-z0-9_-]+$"
    )
    
    position: Optional[str] = Field(
        None, 
        description="职位信息"
    )
    
    department: Optional[str] = Field(
        None, 
        description="部门信息"
    )
    
    email: Optional[EmailStr] = Field(
        None, 
        description="标准邮箱格式"
    )
    
    phone: Optional[str] = Field(
        None, 
        description="联系电话",
        regex=r"^[\d\-\+\(\)\s]+$"
    )
    
    hire_date: Optional[datetime] = Field(
        None, 
        description="入职日期"
    )
    
    status: StatusType = Field(
        default=StatusType.ACTIVE, 
        description="人员状态"
    )
    
    created_at: datetime = Field(
        default_factory=datetime.now, 
        description="创建时间，ISO格式"
    )
    
    source_system: SystemType = Field(
        ..., 
        description="数据来源系统"
    )
    
    raw_data: Optional[Dict[str, Any]] = Field(
        None, 
        description="原始数据，用于调试和追溯"
    )

    @validator('person_id')
    def validate_person_id(cls, v):
        """验证人员ID格式"""
        if not v:
            raise ValueError("人员ID不能为空")
        parts = v.split('_', 1)
        if len(parts) != 2 or parts[0] not in ['erp', 'hr', 'fin']:
            raise ValueError("人员ID格式错误，应为: {system}_{original_id}")
        return v


class Customer(BaseModel):
    """
    客户信息统一模型
    
    将ERP、HR、FIN三个系统的客户/业务伙伴数据映射到统一格式
    支持企业客户、个人客户等不同类型
    """
    
    customer_id: str = Field(
        ..., 
        description="统一客户ID，格式: {system}_{original_id}",
        regex=r"^(erp|hr|fin)_[A-Za-z0-9_-]+$"
    )
    
    customer_name: str = Field(
        ..., 
        description="统一客户名称",
        min_length=1,
        max_length=200
    )
    
    customer_code: Optional[str] = Field(
        None, 
        description="客户编码，来源于原系统"
    )
    
    customer_type: Optional[str] = Field(
        None, 
        description="客户类型：企业/个人/政府等"
    )
    
    tax_num: Optional[str] = Field(
        None, 
        description="统一税号格式"
    )
    
    industry: Optional[str] = Field(
        None, 
        description="所属行业"
    )
    
    contact_person: Optional[str] = Field(
        None, 
        description="联系人姓名"
    )
    
    contact_phone: Optional[str] = Field(
        None, 
        description="联系电话",
        regex=r"^[\d\-\+\(\)\s]+$"
    )
    
    contact_email: Optional[EmailStr] = Field(
        None, 
        description="联系邮箱"
    )
    
    address: Optional[str] = Field(
        None, 
        description="客户地址"
    )
    
    credit_level: Optional[str] = Field(
        None, 
        description="信用等级"
    )
    
    status: StatusType = Field(
        default=StatusType.ACTIVE, 
        description="客户状态"
    )
    
    created_at: datetime = Field(
        default_factory=datetime.now, 
        description="创建时间，ISO格式"
    )
    
    source_system: SystemType = Field(
        ..., 
        description="数据来源系统"
    )
    
    raw_data: Optional[Dict[str, Any]] = Field(
        None, 
        description="原始数据，用于调试和追溯"
    )

    @validator('customer_id')
    def validate_customer_id(cls, v):
        """验证客户ID格式"""
        if not v:
            raise ValueError("客户ID不能为空")
        parts = v.split('_', 1)
        if len(parts) != 2 or parts[0] not in ['erp', 'hr', 'fin']:
            raise ValueError("客户ID格式错误，应为: {system}_{original_id}")
        return v


class Transaction(BaseModel):
    """
    交易信息统一模型
    
    将ERP、HR、FIN三个系统的交易/事务数据映射到统一格式
    支持销售、薪资、费用等不同类型的交易
    """
    
    tx_id: str = Field(
        ..., 
        description="统一交易ID，格式: {system}_{original_id}",
        regex=r"^(erp|hr|fin)_[A-Za-z0-9_-]+$"
    )
    
    transaction_number: Optional[str] = Field(
        None, 
        description="交易编号，来源于原系统"
    )
    
    tx_type: TransactionType = Field(
        ..., 
        description="交易类型: sales/salary/expense/payment/receipt/transfer"
    )
    
    amount: Decimal = Field(
        ..., 
        description="统一为Decimal类型的金额",
        decimal_places=2,
        ge=0
    )
    
    currency: str = Field(
        default="CNY", 
        description="统一为CNY货币代码"
    )
    
    tx_date: datetime = Field(
        ..., 
        description="交易日期，ISO格式时间"
    )
    
    customer_id: Optional[str] = Field(
        None, 
        description="可选客户ID",
        regex=r"^(erp|hr|fin)_[A-Za-z0-9_-]+$"
    )
    
    person_id: Optional[str] = Field(
        None, 
        description="可选人员ID",
        regex=r"^(erp|hr|fin)_[A-Za-z0-9_-]+$"
    )
    
    org_id: str = Field(
        ..., 
        description="关联组织ID",
        regex=r"^(erp|hr|fin)_[A-Za-z0-9_-]+$"
    )
    
    description: Optional[str] = Field(
        None, 
        description="交易描述"
    )
    
    product_info: Optional[str] = Field(
        None, 
        description="产品/服务信息"
    )
    
    payment_method: Optional[str] = Field(
        None, 
        description="支付方式"
    )
    
    status: StatusType = Field(
        default=StatusType.PENDING, 
        description="交易状态"
    )
    
    created_at: datetime = Field(
        default_factory=datetime.now, 
        description="创建时间，ISO格式"
    )
    
    source_system: SystemType = Field(
        ..., 
        description="数据来源系统"
    )
    
    raw_data: Optional[Dict[str, Any]] = Field(
        None, 
        description="原始数据，用于调试和追溯"
    )

    @validator('tx_id')
    def validate_tx_id(cls, v):
        """验证交易ID格式"""
        if not v:
            raise ValueError("交易ID不能为空")
        parts = v.split('_', 1)
        if len(parts) != 2 or parts[0] not in ['erp', 'hr', 'fin']:
            raise ValueError("交易ID格式错误，应为: {system}_{original_id}")
        return v

    @validator('currency')
    def validate_currency(cls, v):
        """验证货币代码"""
        if v != "CNY":
            raise ValueError("目前只支持CNY货币代码")
        return v

    @validator('amount')
    def validate_amount(cls, v):
        """验证金额格式"""
        if v < 0:
            raise ValueError("金额不能为负数")
        # 检查小数位数
        if v.as_tuple().exponent < -2:
            raise ValueError("金额最多支持2位小数")
        return v


class CanonicalDataSummary(BaseModel):
    """
    Canonical数据汇总模型
    
    用于统计和展示各系统数据映射情况
    """
    
    total_organizations: int = Field(default=0, description="组织总数")
    total_persons: int = Field(default=0, description="人员总数")
    total_customers: int = Field(default=0, description="客户总数")
    total_transactions: int = Field(default=0, description="交易总数")
    
    erp_data_count: Dict[str, int] = Field(default_factory=dict, description="ERP系统数据统计")
    hr_data_count: Dict[str, int] = Field(default_factory=dict, description="HR系统数据统计")
    fin_data_count: Dict[str, int] = Field(default_factory=dict, description="FIN系统数据统计")
    
    last_updated: datetime = Field(
        default_factory=datetime.now, 
        description="最后更新时间"
    )
    
    data_quality_score: Optional[float] = Field(
        None, 
        description="数据质量评分 (0-100)",
        ge=0,
        le=100
    )


# 导出所有模型类
__all__ = [
    "SystemType",
    "OrganizationType", 
    "TransactionType",
    "StatusType",
    "Organization",
    "Person", 
    "Customer",
    "Transaction",
    "CanonicalDataSummary"
]