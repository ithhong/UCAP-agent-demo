"""
配置管理模块
负责加载和管理应用程序的所有配置项
"""

import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv
from pathlib import Path

# 项目根目录（确保无论从何处运行，都定位到项目根的 .env）
_ROOT_DIR = Path(__file__).resolve().parents[1]

# 加载环境变量（显式指定项目根 .env，避免因工作目录变化导致加载错误）
load_dotenv(dotenv_path=_ROOT_DIR / ".env")


class Settings(BaseSettings):
    """应用程序配置类"""
    
    # 应用基础配置
    app_name: str = Field(default="UCAP-Agent-Demo", env="APP_NAME")
    app_version: str = Field(default="1.0.0", env="APP_VERSION")
    debug: bool = Field(default=True, env="DEBUG")
    
    # 通义千问API配置
    dashscope_api_key: str = Field(..., env="DASHSCOPE_API_KEY")
    default_model: str = Field(default="qwen3-flash", env="DEFAULT_MODEL")
    max_tokens: int = Field(default=2000, env="MAX_TOKENS")
    temperature: float = Field(default=0.7, env="TEMPERATURE")
    
    # 数据库配置
    database_path: str = Field(default="./data/ucap_demo.db", env="DATABASE_PATH")
    
    # 日志配置
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_file: str = Field(default="./logs/app.log", env="LOG_FILE")
    
    # Streamlit配置
    streamlit_port: int = Field(default=8501, env="STREAMLIT_PORT")
    streamlit_host: str = Field(default="localhost", env="STREAMLIT_HOST")
    
    # 数据生成配置
    erp_data_size: int = Field(default=1000, env="ERP_DATA_SIZE")
    hr_data_size: int = Field(default=500, env="HR_DATA_SIZE")
    fin_data_size: int = Field(default=800, env="FIN_DATA_SIZE")

    # 时间增强开关
    enable_time_enhancements: bool = Field(default=True, env="ENABLE_TIME_ENHANCEMENTS")
    enable_narrow_time_llm: bool = Field(default=True, env="ENABLE_NARROW_TIME_LLM")
    
    class Config:
        # 显式指定项目根的 .env 文件
        env_file = str(_ROOT_DIR / ".env")
        env_file_encoding = "utf-8"
        case_sensitive = False
    
    def get_database_dir(self) -> str:
        """获取数据库目录路径"""
        return os.path.dirname(self.database_path)
    
    def get_log_dir(self) -> str:
        """获取日志目录路径"""
        return os.path.dirname(self.log_file)
    
    def ensure_directories(self) -> None:
        """确保必要的目录存在"""
        directories = [
            self.get_database_dir(),
            self.get_log_dir(),
            "./data",
            "./logs"
        ]
        
        for directory in directories:
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)


# 全局配置实例
settings = Settings()

# 确保目录存在
settings.ensure_directories()


def get_settings() -> Settings:
    """获取配置实例"""
    return settings


def validate_api_key() -> bool:
    """验证API密钥是否配置"""
    return bool(settings.dashscope_api_key and settings.dashscope_api_key != "your_dashscope_api_key_here")


def get_model_config() -> dict:
    """获取模型配置"""
    return {
        "model": settings.default_model,
        "max_tokens": settings.max_tokens,
        "temperature": settings.temperature
    }