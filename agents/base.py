"""
BaseAgent抽象类
定义统一的Agent接口，为ERP、HR、FIN三个系统提供标准化的数据访问方式

作者: Tom
创建时间: 2025-11-04T10:11:46+08:00
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from dateutil.parser import isoparse
from functools import lru_cache
from typing import List, Dict, Any, Optional, Union, Type
import hashlib
import json
from loguru import logger

from canonical.models import (
    Organization, Person, Customer, Transaction, 
    SystemType, BaseModel
)
from canonical.mapper import DataMapper
from config.settings import get_settings


class AgentError(Exception):
    """Agent相关异常基类"""
    pass


class DataSourceError(AgentError):
    """数据源访问异常"""
    pass


class DataMappingError(AgentError):
    """数据映射异常"""
    pass


class BaseAgent(ABC):
    """
    Agent抽象基类
    
    提供统一的数据访问接口，支持：
    1. 原始数据拉取
    2. Canonical数据映射
    3. 缓存机制
    4. 错误处理
    5. 工具函数定义
    """
    
    def __init__(self, system_name: str, system_type: SystemType):
        """
        初始化Agent
        
        Args:
            system_name: 系统名称 (如: "ERP系统", "HR系统", "财务系统")
            system_type: 系统类型枚举
        """
        self.system_name = system_name
        self.system_type = system_type
        self.settings = get_settings()
        
        # 健康状态
        self.health_status = "healthy"
        self.last_health_check = datetime.now()
        
        # 版本信息
        self.canonical_version = "v1.0"
        self.agent_version = "v1.0"
        
        # 缓存配置
        self.cache_ttl = 300  # 5分钟缓存
        self.max_cache_size = 128
        
        # 数据映射器
        self.mapper = DataMapper()
        
        # 初始化日志
        self._setup_logger()
        
        logger.info(f"{self.system_name} Agent初始化完成")
    
    def _setup_logger(self) -> None:
        """设置专用日志器"""
        logger.add(
            f"logs/{self.system_type.value}_agent.log",
            rotation="1 day",
            retention="7 days",
            level=self.settings.log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}"
        )
    
    @abstractmethod
    def pull_raw(self) -> List[Dict[str, Any]]:
        """
        拉取原始数据
        
        Returns:
            原始数据列表
            
        Raises:
            DataSourceError: 数据源访问失败
        """
        pass
    
    @abstractmethod
    def map_to_canonical(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[BaseModel]]:
        """
        @abstractmethod：抽象方法，具体实现由子类实现
        映射到标准模型
        
        Args:
            raw_data: 原始数据列表
            
        Returns:
            标准化数据字典，格式: {
                "organizations": [Organization, ...],
                "persons": [Person, ...], 
                "customers": [Customer, ...],
                "transactions": [Transaction, ...]
            }
            
        Raises:
            DataMappingError: 数据映射失败
        """
        pass
    
    def _generate_cache_key(self, filter_params: Optional[Dict] = None) -> str:
        """
        生成缓存键
        
        Args:
            filter_params: 过滤参数
            
        Returns:
            缓存键字符串
        """
        key_data = {
            "system_type": self.system_type.value,
            "filter_params": filter_params or {},
            "timestamp": int(datetime.now().timestamp() // self.cache_ttl)
        }
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    @lru_cache(maxsize=128)
    def _cached_query(self, cache_key: str) -> Dict[str, List[BaseModel]]:
        """
        缓存查询实现
        
        Args:
            cache_key: 缓存键
            
        Returns:
            标准化数据
        """
        try:
            logger.info(f"{self.system_name} 开始拉取原始数据")
            raw_data = self.pull_raw()
            
            logger.info(f"{self.system_name} 拉取到 {len(raw_data)} 条原始数据")
            
            logger.info(f"{self.system_name} 开始数据映射")
            canonical_data = self.map_to_canonical(raw_data)
            
            # 统计映射结果
            total_count = sum(len(items) for items in canonical_data.values())
            logger.info(f"{self.system_name} 映射完成，共 {total_count} 条标准化数据")
            
            return canonical_data
            
        except Exception as e:
            logger.error(f"{self.system_name} 数据查询失败: {str(e)}")
            raise AgentError(f"{self.system_name} 数据查询失败: {str(e)}") from e
    
    def query_canonical(self, filter_params: Optional[Dict] = None) -> Dict[str, List[BaseModel]]:
        """
        查询标准化数据（带缓存）
        
        Args:
            filter_params: 过滤参数，支持:
                - date_from: 开始日期
                - date_to: 结束日期
                - entity_type: 实体类型 (organization/person/customer/transaction)
                - limit: 限制数量
                
        Returns:
            标准化数据字典
            
        Raises:
            AgentError: 查询失败
        """
        try:
            cache_key = self._generate_cache_key(filter_params)
            canonical_data = self._cached_query(cache_key)
            
            # 应用过滤条件
            if filter_params:
                canonical_data = self._apply_filters(canonical_data, filter_params)
            
            return canonical_data
            
        except Exception as e:
            logger.error(f"{self.system_name} 查询失败: {str(e)}")
            raise
    
    def _apply_filters(self, data: Dict[str, List[BaseModel]], filters: Dict) -> Dict[str, List[BaseModel]]:
        """
        应用过滤条件
        
        Args:
            data: 标准化数据
            filters: 过滤条件
            
        Returns:
            过滤后的数据
        """
        filtered_data = {}
        
        for entity_type, items in data.items():
            filtered_items = items
            
            # 实体类型过滤
            if filters.get("entity_type") and filters["entity_type"] != entity_type:
                filtered_items = []
            
            # 日期范围过滤
            # 注意：不同实体的时间语义不同，优先使用域字段，其次回退到 created_at
            if filtered_items and (filters.get("date_from") or filters.get("date_to")):
                # 解析为更兼容的 ISO8601（支持Z后缀）
                date_from = None
                date_to = None
                try:
                    if filters.get("date_from"):
                        date_from = isoparse(str(filters["date_from"]))
                    if filters.get("date_to"):
                        date_to = isoparse(str(filters["date_to"]))
                except Exception:
                    # 解析失败则保持 None，不做时间过滤
                    date_from = None
                    date_to = None

                # 记录一次过滤信息，便于定位问题
                time_field = "hire_date" if entity_type == "persons" else ("tx_date" if entity_type == "transactions" else "created_at")
                logger.debug(
                    f"{self.system_name} 时间过滤生效: entity_type={entity_type}, time_field={time_field}, date_from={date_from}, date_to={date_to}"
                )

                def get_item_time(it: BaseModel) -> Optional[datetime]:
                    try:
                        if entity_type == "persons" and hasattr(it, "hire_date") and getattr(it, "hire_date"):
                            return getattr(it, "hire_date")
                        if entity_type == "transactions" and hasattr(it, "tx_date") and getattr(it, "tx_date"):
                            return getattr(it, "tx_date")
                        # organizations/customers 或无域时间时回退
                        if hasattr(it, "created_at") and getattr(it, "created_at"):
                            return getattr(it, "created_at")
                    except Exception:
                        return None
                    return None

                if date_from or date_to:
                    tmp: List[BaseModel] = []
                    for it in filtered_items:
                        t = get_item_time(it)
                        if t is None:
                            continue
                        if date_from and t < date_from:
                            continue
                        if date_to and t > date_to:
                            continue
                        tmp.append(it)
                    filtered_items = tmp
            
            # 数量限制
            if filtered_items and filters.get("limit"):
                limit = int(filters["limit"])
                filtered_items = filtered_items[:limit]
            
            filtered_data[entity_type] = filtered_items
        
        return filtered_data
    
    def health_check(self) -> Dict[str, Any]:
        """
        健康检查
        
        Returns:
            健康状态信息
        """
        try:
            # 尝试拉取少量数据验证连接
            test_data = self.pull_raw()
            
            self.health_status = "healthy"
            self.last_health_check = datetime.now()
            
            return {
                "system_name": self.system_name,
                "system_type": self.system_type.value,
                "status": self.health_status,
                "last_check": self.last_health_check.isoformat(),
                "data_count": len(test_data),
                "agent_version": self.agent_version,
                "canonical_version": self.canonical_version
            }
            
        except Exception as e:
            self.health_status = "unhealthy"
            self.last_health_check = datetime.now()
            
            logger.error(f"{self.system_name} 健康检查失败: {str(e)}")
            
            return {
                "system_name": self.system_name,
                "system_type": self.system_type.value,
                "status": self.health_status,
                "last_check": self.last_health_check.isoformat(),
                "error": str(e),
                "agent_version": self.agent_version,
                "canonical_version": self.canonical_version
            }
    
    def get_schema(self) -> Dict[str, Any]:
        """
        获取数据模式信息
        
        Returns:
            数据模式描述
        """
        return {
            "system_name": self.system_name,
            "system_type": self.system_type.value,
            "supported_entities": {
                "organizations": Organization.model_json_schema(),
                "persons": Person.model_json_schema(),
                "customers": Customer.model_json_schema(),
                "transactions": Transaction.model_json_schema()
            },
            "canonical_version": self.canonical_version
        }
    
    def tools(self) -> Dict[str, Any]:
        """
        返回Agent工具定义（用于LLM集成）
        
        Returns:
            工具定义字典
        """
        return {
            "name": f"{self.system_type.value}_query",
            "description": f"查询{self.system_name}数据，支持组织、人员、客户、交易等信息",
            "function": self.query_canonical,
            "parameters": {
                "type": "object",
                "properties": {
                    "filter_params": {
                        "type": "object",
                        "properties": {
                            "entity_type": {
                                "type": "string",
                                "enum": ["organizations", "persons", "customers", "transactions"],
                                "description": "实体类型过滤"
                            },
                            "date_from": {
                                "type": "string",
                                "format": "date-time",
                                "description": "开始日期过滤"
                            },
                            "date_to": {
                                "type": "string", 
                                "format": "date-time",
                                "description": "结束日期过滤"
                            },
                            "limit": {
                                "type": "integer",
                                "minimum": 1,
                                "maximum": 1000,
                                "description": "限制返回数量"
                            }
                        }
                    }
                }
            }
        }
    
    def clear_cache(self) -> None:
        """清空缓存"""
        self._cached_query.cache_clear()
        logger.info(f"{self.system_name} 缓存已清空")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """
        获取缓存信息
        
        Returns:
            缓存统计信息
        """
        cache_info = self._cached_query.cache_info()
        return {
            "hits": cache_info.hits,
            "misses": cache_info.misses,
            "maxsize": cache_info.maxsize,
            "currsize": cache_info.currsize,
            "hit_rate": cache_info.hits / (cache_info.hits + cache_info.misses) if (cache_info.hits + cache_info.misses) > 0 else 0
        }
    
    def __str__(self) -> str:
        return f"{self.system_name}Agent({self.system_type.value})"
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(system_name='{self.system_name}', system_type='{self.system_type.value}', status='{self.health_status}')>"


# 导出类
__all__ = [
    "BaseAgent",
    "AgentError", 
    "DataSourceError",
    "DataMappingError"
]