# UCAP Agent 前后端对接参考

版本: v1.0  
最后更新: 2025-11-18T13:00:28+08:00 Asia/Shanghai  
维护者: Tom

## 基础约定
- 基础路径 `baseURL`: 由环境变量 `VITE_API_BASE_URL` 指定；未设置时默认 `/api`
- 请求头：
  - `Authorization: Bearer <token>`（从前端本地存储读取）
  - `X-Request-ID`: `req_<timestamp>_<random>`（用于链路跟踪）
  - `X-Request-Timestamp`: 毫秒级时间戳字符串
  - `Content-Type: application/json`
- 响应包裹统一结构 `ApiResponse<T>`：
  ```json
  {
    "code": 0,
    "message": "OK",
    "success": true,
    "data": { /* 具体数据 */ }
  }
  ```
- 错误约定：
  - 业务失败：`success: false`，`message`为错误提示
  - 401：清理令牌并跳转登录；403/404/500/502/503：直接提示错误
  - 超时与网络错误分别提示

## 分页与查询约定
- 请求参数：`page`, `pageSize`, `sortBy`, `sortOrder ('asc'|'desc')`, `filters: Record<string, any>`
- 列表响应统一为 `PaginatedResponse<T>`：
  ```json
  {
    "items": [],
    "total": 1324,
    "page": 1,
    "pageSize": 20,
    "totalPages": 67
  }
  ```

## 核心数据模型
- Organization
  ```json
  {
    "org_id": "string",
    "org_name": "string",
    "org_type": "company|dept|cost_center|team",
    "parent_org_id": "string|null",
    "org_code": "string|null",
    "manager_name": "string|null",
    "contact_info": "string|null",
    "address": "string|null",
    "status": "active|inactive|pending|completed|cancelled",
    "created_at": "ISO-8601",
    "source_system": "erp|hr|fin",
    "raw_data": {}
  }
  ```
- Person
  ```json
  {
    "person_id": "string",
    "person_name": "string",
    "employee_number": "string|null",
    "org_id": "string",
    "position": "string|null",
    "department": "string|null",
    "email": "string|null",
    "phone": "string|null",
    "hire_date": "ISO-8601|null",
    "status": "active|inactive|pending|completed|cancelled",
    "created_at": "ISO-8601",
    "source_system": "erp|hr|fin",
    "raw_data": {}
  }
  ```
- Customer
  ```json
  {
    "customer_id": "string",
    "customer_name": "string",
    "customer_code": "string|null",
    "contact_person": "string|null",
    "email": "string|null",
    "phone": "string|null",
    "address": "string|null",
    "registration_date": "ISO-8601|null",
    "status": "active|inactive|pending|completed|cancelled",
    "created_at": "ISO-8601",
    "source_system": "erp|hr|fin",
    "raw_data": {}
  }
  ```
- Transaction
  ```json
  {
    "tx_id": "string",
    "transaction_number": "string|null",
    "tx_type": "sales|salary|expense|payment|receipt|transfer",
    "amount": 123.45,
    "currency": "CNY",
    "tx_date": "ISO-8601",
    "customer_id": "string|null",
    "person_id": "string|null",
    "org_id": "string",
    "description": "string|null",
    "product_info": "string|null",
    "payment_method": "string|null",
    "status": "active|inactive|pending|completed|cancelled",
    "created_at": "ISO-8601",
    "source_system": "erp|hr|fin",
    "raw_data": {}
  }
  ```

## 接口清单

### 智能查询
- POST `/nl-query`
  - Request（NLQueryRequest）：
    ```json
    {
      "text": "查询最近一周的交易数据",
      "default_filters": { "dateRange": ["2025-11-11", "2025-11-18"] },
      "systems": ["erp", "fin"],
      "timeout_ms": 30000
    }
    ```
  - Response（ApiResponse<NLQueryResponse>）：
    ```json
    {
      "code": 0,
      "message": "OK",
      "success": true,
      "data": {
        "organizations": [],
        "persons": [],
        "customers": [],
        "transactions": [],
        "warnings": [],
        "errors": [],
        "metrics": {
          "executionTime": 1234,
          "resultCount": 256,
          "systemCount": 2,
          "dataQuality": 97.5
        }
      }
    }
    ```
- POST `/query`
  - Request（QueryRequest）：
    ```json
    {
      "filter_params": {
        "dateRange": ["2025-11-01", "2025-11-18"],
        "transactionTypes": ["sales", "payment"]
      },
      "systems": ["erp", "fin"],
      "timeout_ms": 30000
    }
    ```
  - Response：同 `/nl-query`
- GET `/nl-query/suggestions?query=...`
  - Response：`ApiResponse<string[]>`
- POST `/nl-query/validate`
  - Request：`{ "query": "..." }`
  - Response：`ApiResponse<{ "valid": true, "errors": [] }>`

### 组织（Organizations）
- GET `/organizations` → `ApiResponse<PaginatedResponse<Organization>>`
- GET `/organizations/{id}` → `ApiResponse<Organization>`
- POST `/organizations` → `ApiResponse<Organization>`
- PUT `/organizations/{id}` → `ApiResponse<Organization>`
- DELETE `/organizations/{id}` → `ApiResponse<void>`
- GET `/organizations/statistics` → `ApiResponse<{ "total": number, "byType": Record<string, number> }>`

### 人员（Persons）
- GET `/persons` → `ApiResponse<PaginatedResponse<Person>>`
- GET `/persons/{id}` → `ApiResponse<Person>`
- POST `/persons` → `ApiResponse<Person>`
- PUT `/persons/{id}` → `ApiResponse<Person>`
- DELETE `/persons/{id}` → `ApiResponse<void>`
- GET `/persons/statistics` → `ApiResponse<{ "total": number, "byDepartment": Record<string, number> }>`

### 客户（Customers）
- GET `/customers` → `ApiResponse<PaginatedResponse<Customer>>`
- GET `/customers/{id}` → `ApiResponse<Customer>`
- POST `/customers` → `ApiResponse<Customer>`
- PUT `/customers/{id}` → `ApiResponse<Customer>`
- DELETE `/customers/{id}` → `ApiResponse<void>`
- GET `/customers/statistics` → `ApiResponse<{ "total": number, "byStatus": Record<string, number> }>`

### 交易（Transactions）
- GET `/transactions` → `ApiResponse<PaginatedResponse<Transaction>>`
- GET `/transactions/{id}` → `ApiResponse<Transaction>`
- POST `/transactions` → `ApiResponse<Transaction>`
- PUT `/transactions/{id}` → `ApiResponse<Transaction>`
- DELETE `/transactions/{id}` → `ApiResponse<void>`
- GET `/transactions/statistics` → `ApiResponse<{ "total": number, "totalAmount": number, "byType": Record<string, number>, "byCurrency": Record<string, number> }>`
- GET `/transactions/timeseries?dateRange=YYYY-MM-DD,YYYY-MM-DD` → `ApiResponse<Array<{ "date": string, "amount": number, "count": number }>>`

### 系统监控与配置
- GET `/system/health` → `ApiResponse<SystemHealth[]>`
- GET `/system/data-quality` → `ApiResponse<DataQualityMetrics>`
- GET `/system/statistics` → `ApiResponse<StatisticsOverview>`
- GET `/system/config` → `ApiResponse<Record<string, any>>`
- PUT `/system/config` → `ApiResponse<void>`

## Java后端实现建议
- 集合类型统一使用 `java.util.List`
- Lombok注解可用：示例 `@Data @Builder @NoArgsConstructor @AllArgsConstructor`
- 统一返回类型：
  ```java
  public class ApiResponse<T> {
    private int code;
    private String message;
    private boolean success;
    private T data;
  }
  ```
- 分页返回类型：
  ```java
  public class PaginatedResponse<T> {
    private java.util.List<T> items;
    private long total;
    private int page;
    private int pageSize;
    private int totalPages;
  }
  ```
- 时间字段统一序列化为 ISO 8601 字符串；金额使用 `BigDecimal` 映射为 JSON 数字

## 联调与校验清单
- 鉴权校验：无/过期/有效令牌三态
- 响应包裹：所有接口均按 `ApiResponse<T>` 返回
- 分页一致性：`page` 从 1 开始；`totalPages = ceil(total/pageSize)`
- 时间与时区：统一 ISO 8601；建议使用 UTC 或约定时区
- 错误语义：集中维护错误码，如 `1001 参数错误`, `2001 资源不存在`, `3001 权限不足`

## 环境与部署
- 环境变量：`VITE_API_BASE_URL`（生产建议设置为完整域名或反向代理路径）
- 反向代理：确保 `/api/*` 路径正确转发到后端服务

## 变更控制
- 如需调整字段或端点，请同步更新本文档并通知前端以保持契约一致