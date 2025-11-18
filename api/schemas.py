from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, model_validator


class FilterParams(BaseModel):
    entity_type: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    dateRange: Optional[List[str]] = None
    limit: Optional[int] = Field(default=None, ge=1, le=1000)

    @model_validator(mode="after")
    def _normalize_date_range(self):
        rng = getattr(self, "dateRange", None)
        if isinstance(rng, list) and len(rng) == 2:
            self.date_from = self.date_from or rng[0]
            self.date_to = self.date_to or rng[1]
        return self


class NLQueryRequest(BaseModel):
    text: str
    default_filters: Optional[FilterParams] = None
    systems: Optional[List[str]] = None
    timeout_ms: Optional[int] = Field(default=5000, ge=50, le=60000)


class QueryRequest(BaseModel):
    filter_params: Optional[FilterParams] = None
    systems: Optional[List[str]] = None
    timeout_ms: Optional[int] = Field(default=5000, ge=50, le=60000)


class QueryResponse(BaseModel):
    organizations: List[Dict[str, Any]] = Field(default_factory=list)
    persons: List[Dict[str, Any]] = Field(default_factory=list)
    customers: List[Dict[str, Any]] = Field(default_factory=list)
    transactions: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    metrics: Dict[str, Any] = Field(default_factory=dict)


class ApiResponse(BaseModel):
    code: int = 0
    message: str = "OK"
    success: bool = True
    data: QueryResponse