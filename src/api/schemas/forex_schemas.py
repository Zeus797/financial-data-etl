from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime, date
from typing import Optional, List
from decimal import Decimal


class CountryBase(BaseModel):
    """Base schema for country data"""
    code: str = Field(..., min_length=2, max_length=3, description="ISO country code")
    name: str = Field(..., min_length=1, max_length=100)
    currency_code: str = Field(..., min_length=3, max_length=3, DESCRIPTION="ISO currency code")
    currency_name: str = Field(..., min_length=1, max_length=100)
    region: Optional[str] = Field(None, max_length=50)


class CountryResponse(CountryBase):
    """Schema for country API responses"""
    id: int
    created_at: datetime

model_config = ConfigDict(from_attributes=True)

class CurrencyPairBase(BaseModel):
    """Base chema for currency pair data"""
    base_currency: str = Field(..., min_length=3, max_length=3)
    quote_currency: str = Field(..., min_length=3, max_length=3)
    pair_symbol: str = Field(..., description="e.g., USD/KES")
    is_active: bool = True


class CurrencyPairResponse(CurrencyPairBase):
    """Schema for currency pair API responses"""
    id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)

class ForexRateBase(BaseModel):
    """Base schema for forex rate data"""
    currency_pair_id: int
    date: date
    open_rate: Decimal = Field(..., decimal_places=6)
    high_rate: Decimal = Field(..., decimal_places=6)
    low_rate: Decimal = Field(..., decimal_places=6)
    close_rate: Decimal = Field(..., decimal_places=6)
    volume: Optional[int] = None

class ForexRateResponse(ForexRateBase):
    """Schema for forex rate API responses"""
    id: int
    created_at: datetime
    
    # Calculated fields
    daily_change: Optional[Decimal] = Field(None, description="Daily price change")
    daily_change_percent: Optional[Decimal] = Field(None, description="Daily percentage change")
    
    model_config = ConfigDict(from_attributes=True)


class ForexRateDetailed(ForexRateResponse):
    """Detailed forex rate response with technical indicators"""
    ma_20: Optional[Decimal] = Field(None, description="20-day moving average")
    ma_50: Optional[Decimal] = Field(None, description="50-day moving average")
    ma_200: Optional[Decimal] = Field(None, description="200-day moving average")
    volatility_20: Optional[Decimal] = Field(None, description="20-day volatility")
    monthly_change: Optional[Decimal] = Field(None, description="Month-over-month change")
    yearly_change: Optional[Decimal] = Field(None, description="Year-over-year change")
    is_significant_move: Optional[bool] = Field(None, description="Significant price movement flag")

class ForexRatePowerBI(BaseModel):
    """Optimized schema for PowerBI consumption"""
    date: date
    pair_symbol: str
    base_currency: str
    quote_currency: str
    open_rate: float
    high_rate: float
    low_rate: float
    close_rate: float
    volume: Optional[int] = None
    daily_change: Optional[float] = None
    daily_change_percent: Optional[float] = None
    ma_20: Optional[float] = None
    ma_50: Optional[float] = None
    ma_200: Optional[float] = None
    volatility_20: Optional[float] = None
    monthly_change: Optional[float] = None
    yearly_change: Optional[float] = None
    country_name: Optional[str] = None
    region: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class PaginatedResponse(BaseModel):
    """Schema for paginated API responses"""
    data: List[dict]
    total: int
    page: int
    page_size: int
    total_pages: int

class ForexSummaryStats(BaseModel):
    """Summary statistics for forex data"""
    pair_symbol: str
    start_date: date
    end_date: date
    total_records: int
    average_rate: Decimal
    min_rate: Decimal
    max_rate: Decimal
    volatility: Decimal
    total_change: Decimal
    total_change_percent: Decimal

class TimeSeriesRequest(BaseModel):
    """Request schema for time series data"""
    currency_pairs: List[str] = Field(..., min_items=1, description="List of currency pairs")
    start_date: date
    end_date: date
    interval: Optional[str] = Field("daily", pattern="^(daily|weekly|monthly)$")
    include_technical_indicators: bool = True

class BatchUpdateRequest(BaseModel):
    """Request schema for batch forex rate updates"""
    rates: List[ForexRateBase]
    
class ErrorResponse(BaseModel):
    """Standard error response schema"""
    error: str
    message: str
    timestamp: datetime
    details: Optional[dict] = None 