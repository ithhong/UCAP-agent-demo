# UCAP Agent Demo

## 项目简介

UCAP Agent Demo 是一个基于通义千问的多系统数据统一查询代理演示项目。该项目展示了如何通过AI代理技术，将来自不同异构系统（ERP、HR、FIN）的数据进行统一查询和处理。

## 核心特性

- 🤖 **智能代理**: 基于通义千问API的智能查询代理
- 🔄 **数据统一**: 将异构系统数据映射到统一的Canonical模型
- 🎯 **意图识别**: 自动识别用户查询意图并路由到相应系统
- 📊 **可视化界面**: 基于Streamlit的直观Web界面
- 🧪 **模拟数据**: 内置三种不同特征的模拟数据系统

## 系统架构

```
UCAP-Agent-Demo/
├── agents/          # 智能代理模块
├── canonical/       # 统一数据模型
├── orchestrator/    # 编排器模块
├── ui/             # 用户界面
├── data/           # 数据存储
├── config/         # 配置管理
├── tests/          # 测试模块
└── logs/           # 日志文件
```

## 快速开始

### 1. 环境准备

```bash
# 克隆项目
git clone <repository-url>
cd UCAP-agent-demo

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置设置

```bash
# 复制配置模板
cp .env.example .env

# 编辑配置文件，设置你的通义千问API密钥与数据库路径
# DASHSCOPE_API_KEY=your_dashscope_api_key_here
# DATABASE_PATH=d:/Vsproject/UCAP-agent-demo/ui/data/ucap_demo.db
```

### 3. 运行应用

```bash
# 启动Streamlit应用
streamlit run ui/nl_query.py

### 4. 运行目录与路径解析指引

- 相对路径基于“当前工作目录”解析。推荐在项目根目录运行：`streamlit run ui/nl_query.py`。
- 若从 `ui` 子目录运行，请使用脚本名：`streamlit run nl_query.py`，并确保 `.env` 中的 `DATABASE_PATH` 为绝对路径，避免产生 `ui/ui/data` 等误路径。
- 修改 `.env` 后需要重启应用以使配置生效（配置在导入阶段加载）。
 
 重要说明：
 - `.env` 的加载已固定为“项目根目录”的 `.env` 文件（无论从根目录还是 `ui` 子目录运行，均使用项目根 `.env`），避免工作目录变化导致读取错误配置。
 - 财务系统（FIN）数据库连接使用 SQLite 只读 URI（`mode=ro`）。若 `DATABASE_PATH` 配置的文件不存在或路径拼写错误，将直接报出只读连接失败或缺表错误，而不会静默创建空库。请根据错误信息修正路径。
 - 运行时日志会输出以下字段用于定位：`FINAgent 使用数据库路径`、`resolved_uri`、`database_list`（来自 `PRAGMA database_list`）、`fin_tables_present`/`fin_tables_missing`。建议在出现数据源错误时查看日志。
```

## 项目结构

### 数据层
- **ERP系统**: 模拟企业资源规划数据，包含组织架构和业务流程信息
- **HR系统**: 模拟人力资源数据，包含员工信息和组织关系
- **FIN系统**: 模拟财务数据，包含客户信息和交易记录

### 统一模型
- **Organization**: 组织信息统一模型
- **Person**: 人员信息统一模型  
- **Customer**: 客户信息统一模型
- **Transaction**: 交易信息统一模型

### 核心组件
- **BaseAgent**: 基础代理类
- **DataMapper**: 数据映射器
- **IntentRouter**: 意图路由器
- **QianwenProxy**: 通义千问代理

## 开发指南

### 环境要求
- Python 3.9+
- 通义千问API密钥

### 开发流程
1. 环境准备和项目初始化
2. 数据层开发（模拟数据生成）
3. Canonical模型定义
4. 基础代理开发
5. 用户界面开发

### 测试
```bash
# 运行单元测试
pytest tests/

# 运行覆盖率测试
pytest --cov=. tests/
```

## 配置说明

主要配置项：
- `DASHSCOPE_API_KEY`: 通义千问API密钥
- `DEFAULT_MODEL`: 默认使用的模型（qwen-turbo）
- `DATABASE_PATH`: 数据库文件路径
- `LOG_LEVEL`: 日志级别

示例（Windows绝对路径推荐）：

```
DASHSCOPE_API_KEY=your_dashscope_api_key_here
DATABASE_PATH=d:/Vsproject/UCAP-agent-demo/ui/data/ucap_demo.db
DEFAULT_MODEL=qwen-turbo
MAX_TOKENS=2000
TEMPERATURE=0.7
```

## 许可证

本项目采用 MIT 许可证。

## 贡献指南

欢迎提交Issue和Pull Request来改进项目。

## 联系方式

如有问题，请通过Issue联系我们。