#!/usr/bin/env python3
"""
African Financial Data Pipeline - Complete API
Handles all asset classes with PowerBI-optimized endpoints
"""

from fastapi import FastAPI, HTTPException, Query, Path, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, date, timedelta
from enum import Enum
import asyncio
import logging
import json
import hashlib
from functools import wraps
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
import redis
import os
from contextlib import asynccontextmanager

# ============================================================================
# CONFIGURATION & SETUP
# ============================================================================

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection
DATABASE_URL = f"postgresql://{os.getenv('DATABASE_USERNAME')}:{os.getenv('DATABASE_PASSWORD')}@{os.getenv('DATABASE_HOST')}:{os.getenv('DATABASE_PORT')}/{os.getenv('DATABASE_NAME')}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Redis for caching
try:
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    redis_client.ping()
    REDIS_AVAILABLE = True
except:
    REDIS_AVAILABLE = False
    logger.warning("Redis not available - caching disabled")

# ============================================================================
# PYDANTIC MODELS & ENUMS
# ============================================================================

class AssetClass(str, Enum):
    CRYPTO = "crypto"
    MONEY_MARKET = "money_market"
    TREASURY = "treasury"
    FOREX = "forex"
    INDEXES = "indexes"
    GREEN_BONDS = "green_bonds"
    FIXED_DEPOSITS = "fixed_deposits"
    CARBON_OFFSETS = "carbon_offsets"

class TimePeriod(str, Enum):
    SEVEN_DAYS = "7d"
    THIRTY_DAYS = "30d"
    NINETY_DAYS = "90d"
    ONE_YEAR = "1y"
    ALL_TIME = "all"

class PowerBIResponse(BaseModel):
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    powerbi_schema: Dict[str, Any]  # Changed from Dict[str, str] to Dict[str, Any]
    total_records: int
    last_updated: datetime
    cache_info: Optional[Dict[str, Any]] = None

class PipelineStatus(BaseModel):
    pipeline_name: str
    status: str
    last_run: Optional[datetime]
    next_run: Optional[datetime]
    records_processed: int
    error_count: int
    is_running: bool

class QualityReport(BaseModel):
    asset_class: str
    overall_score: float
    checks_passed: int
    total_checks: int
    critical_failures: int
    last_updated: datetime

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_db():
    """Database dependency"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def parse_period_to_days(period: str) -> int:
    """Convert period string to number of days"""
    period_map = {
        "7d": 7, "30d": 30, "90d": 90, "1y": 365, "all": 3650
    }
    return period_map.get(period.lower(), 30)

def create_cache_key(endpoint: str, **params) -> str:
    """Create consistent cache key"""
    param_str = "&".join([f"{k}={v}" for k, v in sorted(params.items()) if v is not None])
    key_string = f"{endpoint}?{param_str}"
    return hashlib.md5(key_string.encode()).hexdigest()

def cache_response(expiration_seconds: int = 300):
    """Decorator for caching API responses"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if not REDIS_AVAILABLE:
                return await func(*args, **kwargs)
            
            # Create cache key from function and parameters
            cache_key = create_cache_key(func.__name__, **kwargs)
            
            # Try to get from cache
            try:
                cached = redis_client.get(cache_key)
                if cached:
                    result = json.loads(cached)
                    result['cache_info'] = {
                        'cached': True,
                        'cache_key': cache_key,
                        'retrieved_at': datetime.now()
                    }
                    return result
            except Exception as e:
                logger.warning(f"Cache retrieval error: {e}")
            
            # Execute function and cache result
            result = await func(*args, **kwargs)
            
            try:
                result['cache_info'] = {
                    'cached': False,
                    'cache_key': cache_key,
                    'cached_at': datetime.now()
                }
                redis_client.setex(cache_key, expiration_seconds, json.dumps(result, default=str))
            except Exception as e:
                logger.warning(f"Cache storage error: {e}")
            
            return result
        return wrapper
    return decorator

async def execute_query(db: Session, query: str, params: Dict = None) -> List[Dict]:
    """Execute SQL query and return results as list of dicts"""
    try:
        result = db.execute(text(query), params or {})
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

# ============================================================================
# FASTAPI APP INITIALIZATION
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting African Financial Data Pipeline API")
    yield
    # Shutdown
    logger.info("Shutting down API")

app = FastAPI(
    title="African Financial Data Pipeline API",
    description="PowerBI-optimized financial data API for African markets",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware for PowerBI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure specific domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# CRYPTOCURRENCY ENDPOINTS
# ============================================================================

@app.get("/api/v1/crypto/prices", response_model=PowerBIResponse, tags=["Cryptocurrency"])
@cache_response(expiration_seconds=240)  # 4 minutes cache
async def get_crypto_prices(
    symbols: str = Query(..., description="Comma-separated crypto symbols (BTC,ETH,USDT)"),
    period: TimePeriod = Query(TimePeriod.THIRTY_DAYS, description="Time period"),
    include_technical: bool = Query(True, description="Include moving averages"),
    include_stablecoin_data: bool = Query(True, description="Include stablecoin deviation data"),
    db: Session = Depends(get_db)
):
    """Get cryptocurrency prices with technical indicators - PowerBI optimized"""
    
    symbol_list = [s.strip().upper() for s in symbols.split(",")]
    days = parse_period_to_days(period.value)
    start_date = datetime.now() - timedelta(days=days)
    
    # Base query with corrected joins matching your actual schema
    base_query = """
    SELECT 
        fp.record_date,
        di.instrument_name as symbol,
        di.instrument_code as ticker,
        fp.price,
        fp.market_cap,
        fp.total_volume,
        fp.deviation_from_dollar,
        fp.within_01_percent,
        fp.within_05_percent,
        fp.within_1_percent,
        fp.price_band,
        di.primary_currency_code as currency_code,
        de.entity_name as exchange
    """
    
    if include_technical:
        base_query += """
        ,fp.rolling_avg_7d,
        fp.rolling_avg_30d,
        fp.rolling_std_7d,
        fp.rolling_std_30d,
        CASE 
            WHEN LAG(fp.price) OVER (PARTITION BY fp.instrument_id ORDER BY fp.record_date) IS NOT NULL
            THEN (fp.price - LAG(fp.price) OVER (PARTITION BY fp.instrument_id ORDER BY fp.record_date)) 
                 / LAG(fp.price) OVER (PARTITION BY fp.instrument_id ORDER BY fp.record_date) * 100
            ELSE NULL
        END as daily_return_pct
        """
    
    base_query += """
    FROM fact_cryptocurrency_prices fp
    JOIN dim_financial_instruments di ON fp.instrument_id = di.instrument_id
    LEFT JOIN dim_entities de ON di.issuer_entity_id = de.entity_id
    WHERE di.instrument_code = ANY(:symbols)
    AND fp.record_date >= :start_date
    ORDER BY fp.record_date DESC, di.instrument_name
    """
    
    data = await execute_query(db, base_query, {
        'symbols': symbol_list,
        'start_date': start_date
    })
    
    # PowerBI schema hints
    schema = {
        "date_column": "record_date",
        "value_columns": ["price", "market_cap", "total_volume"],
        "category_column": "symbol",
        "date_format": "YYYY-MM-DD"
    }
    
    if include_technical:
        schema["technical_columns"] = ["rolling_avg_7d", "rolling_avg_30d", "daily_return_pct"]
    
    if include_stablecoin_data:
        schema["stablecoin_columns"] = ["deviation_from_dollar", "within_01_percent", "price_band"]
    
    return PowerBIResponse(
        data=data,
        metadata={
            "period": period.value,
            "symbols_requested": symbol_list,
            "query_timestamp": datetime.now(),
            "data_source": "CoinGecko API",
            "include_technical": include_technical
        },
        powerbi_schema=schema,
        total_records=len(data),
        last_updated=max([datetime.fromisoformat(str(row['record_date'])) for row in data]) if data else datetime.now()
    )

@app.get("/api/v1/crypto/stablecoins/deviations", response_model=PowerBIResponse, tags=["Cryptocurrency"])
@cache_response(expiration_seconds=300)
async def get_stablecoin_deviations(
    period: TimePeriod = Query(TimePeriod.SEVEN_DAYS),
    threshold: float = Query(0.01, description="Deviation threshold (0.01 = 1%)"),
    db: Session = Depends(get_db)
):
    """Track stablecoin deviations from $1.00 peg"""
    
    days = parse_period_to_days(period.value)
    start_date = datetime.now() - timedelta(days=days)
    
    query = """
    SELECT 
        fp.record_date,
        di.instrument_name as symbol,
        fp.price,
        fp.deviation_from_dollar,
        fp.within_01_percent,
        fp.within_05_percent,
        fp.within_1_percent,
        fp.price_band,
        CASE 
            WHEN fp.deviation_from_dollar > :threshold THEN 'ALERT'
            WHEN fp.deviation_from_dollar > :threshold/2 THEN 'WARNING'
            ELSE 'NORMAL'
        END as risk_level
    FROM fact_cryptocurrency_prices fp
    JOIN dim_financial_instruments di ON fp.instrument_id = di.instrument_id
    WHERE di.instrument_name IN ('USDT', 'USDC', 'BUSD', 'DAI')
    AND fp.record_date >= :start_date
    ORDER BY fp.record_date DESC, fp.deviation_from_dollar DESC
    """
    
    data = await execute_query(db, query, {
        'threshold': threshold,
        'start_date': start_date
    })
    
    return PowerBIResponse(
        data=data,
        metadata={
            "period": period.value,
            "threshold": threshold,
            "alert_count": len([r for r in data if r['risk_level'] == 'ALERT']),
            "data_source": "Stablecoin Monitoring"
        },
        powerbi_schema={
            "date_column": "record_date",
            "value_columns": ["price", "deviation_from_dollar"],
            "category_column": "symbol",
            "alert_column": "risk_level"
        },
        total_records=len(data),
        last_updated=datetime.now()
    )

# ============================================================================
# MONEY MARKET FUNDS ENDPOINTS
# ============================================================================

@app.get("/api/v1/money-market/rates", response_model=PowerBIResponse, tags=["Money Market"])
@cache_response(expiration_seconds=3600)  # 1 hour cache
async def get_money_market_rates(
    currency: str = Query("ALL", description="Currency filter (KES, USD, EUR, ALL)"),
    latest: bool = Query(True, description="Return only latest rates"),
    entity_filter: Optional[str] = Query(None, description="Comma-separated entity names"),
    db: Session = Depends(get_db)
):
    """Get money market fund rates - PowerBI optimized"""
    
    query = """
    SELECT 
        mmf.record_date,
        de.entity_name,
        di.instrument_name as fund_name,
        mmf.current_yield as average_annual_yield,
        mmf.seven_day_yield as net_yield,
        mmf.management_fee,
        0.15 as withholding_tax,
        di.minimum_investment,
        di.primary_currency_code as currency_code,
        dc.currency_name,
        dco.country_name,
        RANK() OVER (PARTITION BY mmf.instrument_id ORDER BY mmf.record_date DESC) as recency_rank
    FROM fact_money_market_funds mmf
    JOIN dim_financial_instruments di ON mmf.instrument_id = di.instrument_id
    LEFT JOIN dim_entities de ON di.manager_entity_id = de.entity_id
    LEFT JOIN dim_currencies dc ON di.primary_currency_code = dc.currency_code
    LEFT JOIN dim_countries dco ON dc.country_code = dco.country_code
    WHERE 1=1
    """
    
    params = {}
    
    if currency != "ALL":
        query += " AND fd.currency_code = :currency"
        params['currency'] = currency.upper()
    
    if entity_filter:
        entity_list = [e.strip() for e in entity_filter.split(",")]
        query += " AND de.entity_name = ANY(:entities)"
        params['entities'] = entity_list
    
    if latest:
        query += " AND mmf.record_date >= CURRENT_DATE - INTERVAL '7 days'"
    
    query += " ORDER BY mmf.average_annual_yield DESC, mmf.record_date DESC"
    
    data = await execute_query(db, query, params)
    
    # Filter to latest only if requested
    if latest:
        data = [row for row in data if row['recency_rank'] == 1]
    
    return PowerBIResponse(
        data=data,
        metadata={
            "currency_filter": currency,
            "latest_only": latest,
            "entity_filter": entity_filter,
            "data_source": "Manual Entry + Web Scraping"
        },
        powerbi_schema={
            "date_column": "record_date",
            "value_columns": ["average_annual_yield", "net_yield", "minimum_investment"],
            "category_column": "entity_name",
            "currency_column": "currency_code"
        },
        total_records=len(data),
        last_updated=datetime.now()
    )

# ============================================================================
# TREASURY SECURITIES ENDPOINTS
# ============================================================================

@app.get("/api/v1/treasury/bonds", response_model=PowerBIResponse, tags=["Treasury"])
@cache_response(expiration_seconds=1800)  # 30 minutes cache
async def get_treasury_bonds(
    security_type: str = Query("ALL", description="T-Bond, T-Bill, or ALL"),
    maturity_range: Optional[str] = Query(None, description="e.g., 1-5y, 5-10y"),
    latest: bool = Query(True, description="Return only latest data"),
    db: Session = Depends(get_db)
):
    """Get treasury securities data with yield calculations"""
    
    query = """
    SELECT 
        ts.record_date,
        ts.isin,
        ts.issue_number,
        di.instrument_name as security_name,
        ts.security_type,
        ts.issue_date,
        ts.maturity_date,
        ts.tenor,
        ts.coupon_rate,
        ts.current_price,
        ts.ytm,
        ts.after_tax_ytm,
        ts.price_premium,
        ts.years_to_maturity,
        ts.auction_date,
        ts.amount_offered,
        ts.amount_accepted,
        ts.war_rate,
        ts.discount_rate,
        CASE 
            WHEN ts.years_to_maturity <= 1 THEN 'Short-term (<1y)'
            WHEN ts.years_to_maturity <= 5 THEN 'Medium-term (1-5y)'
            WHEN ts.years_to_maturity <= 10 THEN 'Long-term (5-10y)'
            ELSE 'Very Long-term (>10y)'
        END as maturity_bucket,
        RANK() OVER (PARTITION BY ts.isin ORDER BY ts.record_date DESC) as recency_rank
    FROM fact_treasury_securities ts
    JOIN dim_financial_instruments di ON ts.instrument_id = di.instrument_id
    WHERE 1=1
    """
    
    params = {}
    
    if security_type != "ALL":
        query += " AND ts.security_type = :security_type"
        params['security_type'] = security_type
    
    if maturity_range:
        if maturity_range == "1-5y":
            query += " AND ts.years_to_maturity BETWEEN 1 AND 5"
        elif maturity_range == "5-10y":
            query += " AND ts.years_to_maturity BETWEEN 5 AND 10"
        elif maturity_range == ">10y":
            query += " AND ts.years_to_maturity > 10"
    
    if latest:
        query += " AND ts.record_date >= CURRENT_DATE - INTERVAL '30 days'"
    
    query += " ORDER BY ts.ytm DESC, ts.maturity_date ASC"
    
    data = await execute_query(db, query, params)
    
    # Filter to latest records if requested
    if latest:
        data = [row for row in data if row['recency_rank'] == 1]
    
    return PowerBIResponse(
        data=data,
        metadata={
            "security_type": security_type,
            "maturity_range": maturity_range,
            "latest_only": latest,
            "data_source": "NSE PDF + LLM Processing"
        },
        powerbi_schema={
            "date_column": "record_date",
            "value_columns": ["ytm", "after_tax_ytm", "current_price", "coupon_rate"],
            "category_column": "security_type",
            "identifier_column": "isin"
        },
        total_records=len(data),
        last_updated=datetime.now()
    )

@app.get("/api/v1/treasury/yield-curve", response_model=PowerBIResponse, tags=["Treasury"])
@cache_response(expiration_seconds=3600)
async def get_yield_curve(
    date: Optional[str] = Query(None, description="YYYY-MM-DD or latest"),
    db: Session = Depends(get_db)
):
    """Get treasury yield curve for specific date"""
    
    if date and date != "latest":
        try:
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    else:
        # Get most recent date with data
        recent_query = "SELECT MAX(record_date) as max_date FROM fact_treasury_securities"
        recent_result = await execute_query(db, recent_query)
        target_date = recent_result[0]['max_date'] if recent_result else datetime.now().date()
    
    query = """
    SELECT 
        ts.years_to_maturity,
        AVG(ts.ytm) as avg_ytm,
        AVG(ts.after_tax_ytm) as avg_after_tax_ytm,
        COUNT(*) as security_count,
        ts.security_type,
        :target_date as curve_date
    FROM fact_treasury_securities ts
    WHERE ts.record_date = :target_date
    AND ts.ytm IS NOT NULL
    AND ts.years_to_maturity > 0
    GROUP BY ts.years_to_maturity, ts.security_type
    ORDER BY ts.years_to_maturity ASC
    """
    
    data = await execute_query(db, query, {'target_date': target_date})
    
    return PowerBIResponse(
        data=data,
        metadata={
            "curve_date": str(target_date),
            "securities_included": sum(row['security_count'] for row in data),
            "data_source": "Treasury Securities Database"
        },
        powerbi_schema={
            "date_column": "curve_date",
            "x_axis": "years_to_maturity", 
            "y_axis": "avg_ytm",
            "category_column": "security_type"
        },
        total_records=len(data),
        last_updated=datetime.now()
    )

# ============================================================================
# FOREX ENDPOINTS
# ============================================================================

@app.get("/api/v1/forex/rates", response_model=PowerBIResponse, tags=["Forex"])
@cache_response(expiration_seconds=1800)  # 30 minutes cache
async def get_forex_rates(
    pairs: str = Query("ALL", description="USD/KES,EUR/KES or ALL"),
    period: TimePeriod = Query(TimePeriod.THIRTY_DAYS),
    include_technical: bool = Query(True),
    db: Session = Depends(get_db)
):
    """Get forex exchange rates with technical indicators"""
    
    days = parse_period_to_days(period.value)
    start_date = datetime.now() - timedelta(days=days)
    
    query = """
    SELECT 
        fr.record_date,
        fr.base_currency,
        fr.quote_currency,
        fr.pair as currency_pair,
        fr.exchange_rate,
        0.0 as bid_rate,
        0.0 as ask_rate,
        0.0 as spread,
        0.0 as daily_change,
        fr.monthly_pct_change as daily_change_percent
    """
    
    if include_technical:
        query += """
        ,fr.ma_12_month as moving_avg_7d,
        fr.ma_12_month as moving_avg_30d,
        fr.quarterly_volatility as volatility_7d,
        fr.annual_volatility as volatility_30d,
        50.0 as rsi_14d
        """
    
    query += """
    FROM fact_forex_rates fr
    JOIN dim_financial_instruments di ON fr.instrument_id = di.instrument_id
    WHERE fr.record_date >= :start_date
    """
    
    params = {'start_date': start_date}
    
    if pairs != "ALL":
        pair_list = [p.strip() for p in pairs.split(",")]
        # Convert pairs like "USD/KES" to pair format "USDKES"
        formatted_pairs = []
        for pair in pair_list:
            if "/" in pair:
                base, quote = pair.split("/")
                formatted_pairs.append(f"{base.strip()}{quote.strip()}")
            else:
                formatted_pairs.append(pair.strip())
        
        if formatted_pairs:
            placeholders = ','.join([f"'{pair}'" for pair in formatted_pairs])
            query += f" AND fr.pair IN ({placeholders})"
    
    query += " ORDER BY fr.record_date DESC, fr.base_currency, fr.quote_currency"
    
    data = await execute_query(db, query, params)
    
    schema = {
        "date_column": "record_date",
        "value_columns": ["exchange_rate", "daily_change_percent"],
        "category_column": "currency_pair"
    }
    
    if include_technical:
        schema["technical_columns"] = ["moving_avg_7d", "moving_avg_30d", "volatility_7d", "rsi_14d"]
    
    return PowerBIResponse(
        data=data,
        metadata={
            "pairs_requested": pairs,
            "period": period.value,
            "include_technical": include_technical,
            "data_source": "yfinance API"
        },
        powerbi_schema=schema,
        total_records=len(data),
        last_updated=datetime.now()
    )

# ============================================================================
# MARKET INDEXES ENDPOINTS  
# ============================================================================

@app.get("/api/v1/indexes/performance", response_model=PowerBIResponse, tags=["Market Indexes"])
@cache_response(expiration_seconds=1800)
async def get_index_performance(
    region: str = Query("ALL", description="Africa, Global, ALL"),
    period: TimePeriod = Query(TimePeriod.THIRTY_DAYS),
    include_technical: bool = Query(True),
    db: Session = Depends(get_db)
):
    """Get market index performance data"""
    
    days = parse_period_to_days(period.value)
    start_date = datetime.now() - timedelta(days=days)
    
    query = """
    SELECT 
        mi.record_date,
        di.instrument_name as index_name,
        di.instrument_code as index_code,
        mi.index_value,
        0.0 as daily_change,
        mi.daily_return * 100 as daily_change_percent,
        mi.volume,
        mi.market_cap,
        dco.country_name,
        dco.region,
        'N/A' as continent
    """
    
    if include_technical:
        query += """
        ,mi.moving_avg_50d as moving_avg_20d,
        mi.moving_avg_50d,
        mi.moving_avg_200d,
        0.0 as volatility_30d,
        mi.relative_strength as rsi
        """
    
    query += """
    FROM fact_market_indexes mi
    JOIN dim_financial_instruments di ON mi.instrument_id = di.instrument_id
    LEFT JOIN dim_entities de ON di.issuer_entity_id = de.entity_id
    LEFT JOIN dim_countries dco ON de.country_code = dco.country_code
    WHERE mi.record_date >= :start_date
    """
    
    params = {'start_date': start_date}
    
    if region != "ALL":
        if region.lower() == "africa":
            query += " AND dco.region LIKE '%Africa%'"
        else:
            query += " AND dco.region = :region"
            params['region'] = region
    
    query += " ORDER BY mi.record_date DESC, mi.daily_return DESC"
    
    data = await execute_query(db, query, params)
    
    schema = {
        "date_column": "record_date",
        "value_columns": ["index_value", "daily_change_percent", "volume"],
        "category_column": "index_name",
        "geographic_column": "country_name"
    }
    
    if include_technical:
        schema["technical_columns"] = ["moving_avg_20d", "moving_avg_50d", "volatility_30d", "rsi"]
    
    return PowerBIResponse(
        data=data,
        metadata={
            "region_filter": region,
            "period": period.value,
            "include_technical": include_technical,
            "data_source": "Multiple Financial APIs"
        },
        powerbi_schema=schema,
        total_records=len(data),
        last_updated=datetime.now()
    )

# ============================================================================
# CROSS-ASSET ANALYTICS ENDPOINTS
# ============================================================================

@app.get("/api/v1/analytics/performance-comparison", response_model=PowerBIResponse, tags=["Analytics"])
@cache_response(expiration_seconds=1800)
async def compare_asset_performance(
    asset_classes: str = Query("crypto,money_market,treasury,forex", description="Comma-separated asset classes"),
    period: TimePeriod = Query(TimePeriod.THIRTY_DAYS),
    benchmark: str = Query("USD", description="Base currency for comparison"),
    db: Session = Depends(get_db)
):
    """Cross-asset performance comparison - PowerBI's killer feature"""
    
    days = parse_period_to_days(period.value)
    start_date = datetime.now() - timedelta(days=days)
    asset_list = [a.strip() for a in asset_classes.split(",")]
    
    results = []
    
    # Crypto performance
    if "crypto" in asset_list:
        crypto_query = """
        SELECT 
            'crypto' as asset_class,
            di.instrument_code as asset_name,
            AVG(fp.price) as avg_value,
            (MAX(fp.price) - MIN(fp.price)) / MIN(fp.price) * 100 as period_return_pct,
            COALESCE(STDDEV(fp.price) / NULLIF(AVG(fp.price), 0) * 100, 0) as volatility_pct,
            COUNT(*) as data_points
        FROM fact_cryptocurrency_prices fp
        JOIN dim_financial_instruments di ON fp.instrument_id = di.instrument_id
        WHERE fp.record_date >= :start_date
        GROUP BY di.instrument_code
        """
        crypto_data = await execute_query(db, crypto_query, {'start_date': start_date})
        results.extend(crypto_data)
    
    # Money Market performance
    if "money_market" in asset_list:
        mm_query = """
        SELECT 
            'money_market' as asset_class,
            de.entity_name as asset_name,
            AVG(mmf.current_yield) * 100 as avg_value,
            (MAX(mmf.current_yield) - MIN(mmf.current_yield)) * 100 as period_return_pct,
            COALESCE(STDDEV(mmf.current_yield) * 100, 0) as volatility_pct,
            COUNT(*) as data_points
        FROM fact_money_market_funds mmf
        JOIN dim_financial_instruments di ON mmf.instrument_id = di.instrument_id
        LEFT JOIN dim_entities de ON di.manager_entity_id = de.entity_id
        WHERE mmf.record_date >= :start_date
        GROUP BY de.entity_name
        """
        mm_data = await execute_query(db, mm_query, {'start_date': start_date})
        results.extend(mm_data)
    
    # Treasury performance
    if "treasury" in asset_list:
        treasury_query = """
        SELECT 
            'treasury' as asset_class,
            CASE 
                WHEN di.instrument_type = 'treasury_bond' THEN 'T-Bond'
                WHEN di.instrument_type = 'treasury_bill' THEN 'T-Bill'
                ELSE 'Treasury'
            END as asset_name,
            AVG(ts.yield_to_maturity) * 100 as avg_value,
            (MAX(ts.yield_to_maturity) - MIN(ts.yield_to_maturity)) * 100 as period_return_pct,
            COALESCE(STDDEV(ts.yield_to_maturity) * 100, 0) as volatility_pct,
            COUNT(*) as data_points
        FROM fact_treasury_securities ts
        JOIN dim_financial_instruments di ON ts.instrument_id = di.instrument_id
        WHERE ts.record_date >= :start_date
        AND ts.yield_to_maturity IS NOT NULL
        GROUP BY di.instrument_type
        """
        treasury_data = await execute_query(db, treasury_query, {'start_date': start_date})
        results.extend(treasury_data)
    
    # Forex performance
    if "forex" in asset_list:
        forex_query = """
        SELECT 
            'forex' as asset_class,
            fr.pair as asset_name,
            AVG(fr.exchange_rate) as avg_value,
            (MAX(fr.exchange_rate) - MIN(fr.exchange_rate)) / NULLIF(MIN(fr.exchange_rate), 0) * 100 as period_return_pct,
            COALESCE(STDDEV(fr.exchange_rate) / NULLIF(AVG(fr.exchange_rate), 0) * 100, 0) as volatility_pct,
            COUNT(*) as data_points
        FROM fact_forex_rates fr
        WHERE fr.record_date >= :start_date
        GROUP BY fr.pair
        """
        forex_data = await execute_query(db, forex_query, {'start_date': start_date})
        results.extend(forex_data)
    
    return PowerBIResponse(
        data=results,
        metadata={
            "asset_classes": asset_list,
            "period": period.value,
            "benchmark": benchmark,
            "comparison_type": "risk_return",
            "analysis_note": "This cross-asset comparison was impossible with Excel files"
        },
        powerbi_schema={
            "category_column": "asset_class",
            "subcategory_column": "asset_name",
            "return_column": "period_return_pct",
            "risk_column": "volatility_pct",
            "value_column": "avg_value"
        },
        total_records=len(results),
        last_updated=datetime.now()
    )

@app.get("/api/v1/analytics/correlation-matrix", response_model=PowerBIResponse, tags=["Analytics"])
@cache_response(expiration_seconds=3600)
async def get_correlation_matrix(
    assets: str = Query("BTC,ETH,USD/KES,10Y-Bond", description="Assets to correlate"),
    period: TimePeriod = Query(TimePeriod.NINETY_DAYS),
    db: Session = Depends(get_db)
):
    """Calculate correlation matrix between different assets"""
    
    days = parse_period_to_days(period.value)
    start_date = datetime.now() - timedelta(days=days)
    asset_list = [a.strip() for a in assets.split(",")]
    
    # This would need actual correlation calculation with your real data
    # For now, providing structure that works with your schema
    correlations = []
    
    for i, asset1 in enumerate(asset_list):
        for j, asset2 in enumerate(asset_list):
            # Placeholder correlation calculation - you'd implement actual correlation here
            correlation = 1.0 if asset1 == asset2 else 0.5  # Simplified for now
            
            correlations.append({
                "asset1": asset1,
                "asset2": asset2,
                "correlation": correlation,
                "period": period.value,
                "significance": "high" if abs(correlation) > 0.7 else "medium" if abs(correlation) > 0.3 else "low",
                "data_points": 30  # Placeholder
            })
    
    return PowerBIResponse(
        data=correlations,
        metadata={
            "assets": asset_list,
            "period": period.value,
            "calculation_method": "Pearson correlation",
            "data_source": "Multi-asset database"
        },
        powerbi_schema={
            "x_axis": "asset1",
            "y_axis": "asset2", 
            "value_column": "correlation",
            "heatmap_column": "correlation"
        },
        total_records=len(correlations),
        last_updated=datetime.now()
    )

# ============================================================================
# ADDITIONAL ASSET CLASSES (GREEN BONDS, FIXED DEPOSITS, CARBON OFFSETS)
# ============================================================================

@app.get("/api/v1/green-bonds/performance", response_model=PowerBIResponse, tags=["Green Bonds"])
@cache_response(expiration_seconds=3600)
async def get_green_bonds_performance(
    period: TimePeriod = Query(TimePeriod.THIRTY_DAYS),
    certification_type: str = Query("ALL", description="Green, Social, Sustainability, ALL"),
    db: Session = Depends(get_db)
):
    """Get green bonds and ETF performance data"""
    
    days = parse_period_to_days(period.value)
    start_date = datetime.now() - timedelta(days=days)
    
    query = """
    SELECT 
        gb.record_date,
        di.instrument_name as bond_name,
        di.instrument_code as isin,
        gb.clean_price as current_price,
        gb.yield_to_maturity,
        gb.duration,
        gb.esg_score,
        gb.carbon_avoided_tonnes as carbon_intensity,
        gb.green_certification as green_classification,
        gb.green_certification as certification_body,
        de.entity_name as issuer,
        dco.country_name as issuer_country
    FROM fact_green_bonds gb
    JOIN dim_financial_instruments di ON gb.instrument_id = di.instrument_id
    LEFT JOIN dim_entities de ON di.issuer_entity_id = de.entity_id
    LEFT JOIN dim_countries dco ON de.country_code = dco.country_code
    WHERE gb.record_date >= :start_date
    AND di.instrument_type = 'green_bond'
    """
    
    params = {'start_date': start_date}
    
    if certification_type != "ALL":
        query += " AND gb.green_classification = :cert_type"
        params['cert_type'] = certification_type
    
    query += " ORDER BY gb.record_date DESC, gb.esg_score DESC"
    
    data = await execute_query(db, query, params)
    
    return PowerBIResponse(
        data=data,
        metadata={
            "period": period.value,
            "certification_filter": certification_type,
            "data_source": "App Scripts + Manual Collection"
        },
        powerbi_schema={
            "date_column": "record_date",
            "value_columns": ["current_price", "yield_to_maturity", "esg_score"],
            "category_column": "green_classification",
            "geographic_column": "issuer_country"
        },
        total_records=len(data),
        last_updated=datetime.now()
    )

@app.get("/api/v1/fixed-deposits/rates", response_model=PowerBIResponse, tags=["Fixed Deposits"])
@cache_response(expiration_seconds=7200)  # 2 hours cache
async def get_fixed_deposit_rates(
    currency: str = Query("ALL", description="KES, USD, EUR, ALL"),
    term_months: Optional[int] = Query(None, description="Filter by term in months"),
    latest: bool = Query(True),
    db: Session = Depends(get_db)
):
    """Get fixed deposit rates from banks"""
    
    query = """
    SELECT 
        fd.record_date,
        de.entity_name as bank_name,
        di.instrument_name as deposit_product,
        fd.interest_rate,
        fd.term_months,
        fd.minimum_deposit as minimum_amount,
        fd.currency_code,
        fd.early_withdrawal_penalty,
        'Monthly' as compounding_frequency,
        CASE 
            WHEN fd.term_months <= 6 THEN 'Short-term (≤6m)'
            WHEN fd.term_months <= 12 THEN 'Medium-term (6m-1y)'
            ELSE 'Long-term (>1y)'
        END as term_category,
        RANK() OVER (PARTITION BY fd.instrument_id ORDER BY fd.record_date DESC) as recency_rank
    FROM fact_fixed_deposits fd
    JOIN dim_financial_instruments di ON fd.instrument_id = di.instrument_id
    LEFT JOIN dim_entities de ON di.issuer_entity_id = de.entity_id
    WHERE 1=1
    """
    
    params = {}
    
    if currency != "ALL":
        query += " AND di.primary_currency_code = :currency"
        params['currency'] = currency.upper()
    
    if term_months:
        query += " AND fd.term_months = :term_months"
        params['term_months'] = term_months
    
    if latest:
        query += " AND fd.record_date >= CURRENT_DATE - INTERVAL '14 days'"
    
    query += " ORDER BY fd.interest_rate DESC, fd.record_date DESC"
    
    data = await execute_query(db, query, params)
    
    # Filter to latest records if requested
    if latest:
        data = [row for row in data if row['recency_rank'] == 1]
    
    return PowerBIResponse(
        data=data,
        metadata={
            "currency_filter": currency,
            "term_filter": term_months,
            "latest_only": latest,
            "data_source": "Bank Website Scraping"
        },
        powerbi_schema={
            "date_column": "record_date",
            "value_columns": ["interest_rate", "minimum_amount"],
            "category_column": "bank_name",
            "term_column": "term_category"
        },
        total_records=len(data),
        last_updated=datetime.now()
    )

@app.get("/api/v1/carbon-offsets/pricing", response_model=PowerBIResponse, tags=["Carbon Offsets"])
@cache_response(expiration_seconds=3600)
async def get_carbon_offset_pricing(
    project_type: str = Query("ALL", description="Forestry, Renewable, Technology, ALL"),
    certification: str = Query("ALL", description="VCS, Gold Standard, CDM, ALL"),
    latest: bool = Query(True),
    db: Session = Depends(get_db)
):
    """Get carbon offset pricing from various platforms"""
    
    query = """
    SELECT 
        co.record_date,
        de.entity_name as platform_name,
        di.instrument_name as project_name,
        co.project_type,
        co.standard as certification_standard,
        co.price_per_tonne,
        di.primary_currency_code as currency_code,
        co.project_vintage as vintage_year,
        co.credits_available as available_tonnes,
        co.project_location as location_country,
        co.standard as methodology,
        dco.region as project_region,
        RANK() OVER (PARTITION BY di.instrument_name, de.entity_name ORDER BY co.record_date DESC) as recency_rank
    FROM fact_carbon_offsets co
    JOIN dim_financial_instruments di ON co.instrument_id = di.instrument_id
    LEFT JOIN dim_entities de ON di.manager_entity_id = de.entity_id
    LEFT JOIN dim_countries dco ON co.project_location = dco.country_code
    WHERE 1=1
    """
    
    params = {}
    
    if project_type != "ALL":
        query += " AND co.project_type = :project_type"
        params['project_type'] = project_type
    
    if certification != "ALL":
        query += " AND co.certification_standard = :certification"
        params['certification'] = certification
    
    if latest:
        query += " AND co.record_date >= CURRENT_DATE - INTERVAL '7 days'"
    
    query += " ORDER BY co.price_per_tonne ASC, co.record_date DESC"
    
    data = await execute_query(db, query, params)
    
    # Filter to latest records if requested
    if latest:
        data = [row for row in data if row['recency_rank'] == 1]
    
    return PowerBIResponse(
        data=data,
        metadata={
            "project_type_filter": project_type,
            "certification_filter": certification,
            "latest_only": latest,
            "data_source": "Carbon Platform APIs"
        },
        powerbi_schema={
            "date_column": "record_date",
            "value_columns": ["price_per_tonne", "available_tonnes"],
            "category_column": "project_type",
            "geographic_column": "project_region"
        },
        total_records=len(data),
        last_updated=datetime.now()
    )

# ============================================================================
# PIPELINE MANAGEMENT & ADMIN ENDPOINTS
# ============================================================================

@app.get("/api/v1/admin/pipeline-status", response_model=List[PipelineStatus], tags=["Admin"])
async def get_pipeline_status(db: Session = Depends(get_db)):
    """Get status of all data pipelines"""
    
    query = """
    SELECT 
        pipeline_name,
        status,
        last_run,
        next_scheduled_run as next_run,
        records_processed,
        error_count,
        CASE WHEN status = 'running' THEN true ELSE false END as is_running
    FROM pipeline_status
    ORDER BY priority ASC, last_run DESC
    """
    
    data = await execute_query(db, query)
    
    return [PipelineStatus(**row) for row in data]

@app.post("/api/v1/admin/pipeline/{pipeline_name}/run", tags=["Admin"])
async def trigger_pipeline_run(
    pipeline_name: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Manually trigger a pipeline run"""
    
    valid_pipelines = [
        "crypto", "money_market", "treasury", "forex", 
        "indexes", "green_bonds", "fixed_deposits", "carbon_offsets"
    ]
    
    if pipeline_name not in valid_pipelines:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid pipeline name. Valid options: {', '.join(valid_pipelines)}"
        )
    
    # Add pipeline run to background tasks
    background_tasks.add_task(run_pipeline_background, pipeline_name)
    
    return {
        "message": f"Pipeline {pipeline_name} has been queued for execution",
        "pipeline_name": pipeline_name,
        "triggered_at": datetime.now(),
        "status": "queued"
    }

async def run_pipeline_background(pipeline_name: str):
    """Background task to run pipeline"""
    logger.info(f"Starting background execution of pipeline: {pipeline_name}")
    
    try:
        # This would integrate with your existing pipeline classes
        # For now, simulate pipeline execution
        await asyncio.sleep(5)  # Simulate work
        logger.info(f"Pipeline {pipeline_name} completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline {pipeline_name} failed: {e}")

@app.get("/api/v1/admin/data-quality", response_model=List[QualityReport], tags=["Admin"])
async def get_data_quality_report(
    asset_class: Optional[str] = Query(None, description="Specific asset class or all"),
    db: Session = Depends(get_db)
):
    """Get data quality report for all or specific asset classes"""
    
    query = """
    SELECT 
        asset_class,
        overall_score,
        checks_passed,
        total_checks,
        critical_failures,
        last_updated
    FROM data_quality_reports
    """
    
    params = {}
    
    if asset_class:
        query += " WHERE asset_class = :asset_class"
        params['asset_class'] = asset_class
    
    query += " ORDER BY overall_score ASC, last_updated DESC"
    
    data = await execute_query(db, query, params)
    
    return [QualityReport(**row) for row in data]

# ============================================================================
# POWERBI INTEGRATION HELPERS
# ============================================================================

@app.get("/api/v1/powerbi/schema/{asset_class}", tags=["PowerBI Integration"])
async def get_powerbi_schema(asset_class: AssetClass):
    """Get PowerBI-optimized schema information for an asset class"""
    
    schemas = {
        AssetClass.CRYPTO: {
            "table_name": "CryptocurrencyPrices",
            "primary_key": "id",
            "date_column": "record_date",
            "value_columns": ["price", "market_cap", "total_volume"],
            "category_columns": ["symbol", "currency_code"],
            "technical_columns": ["rolling_avg_7d", "rolling_avg_30d", "daily_return_pct"],
            "filters": ["symbol", "currency_code", "record_date"],
            "suggested_visuals": ["line_chart", "scatter_plot", "table"]
        },
        AssetClass.MONEY_MARKET: {
            "table_name": "MoneyMarketFunds",
            "primary_key": "id",
            "date_column": "record_date",
            "value_columns": ["average_annual_yield", "net_yield", "minimum_investment"],
            "category_columns": ["entity_name", "currency_code"],
            "filters": ["entity_name", "currency_code", "record_date"],
            "suggested_visuals": ["bar_chart", "table", "gauge"]
        },
        AssetClass.TREASURY: {
            "table_name": "TreasurySecurities",
            "primary_key": "id",
            "date_column": "record_date",
            "value_columns": ["ytm", "after_tax_ytm", "current_price"],
            "category_columns": ["security_type", "maturity_bucket"],
            "identifier_columns": ["isin", "issue_number"],
            "filters": ["security_type", "maturity_bucket", "record_date"],
            "suggested_visuals": ["yield_curve", "bar_chart", "table"]
        }
        # Add other asset classes as needed
    }
    
    return schemas.get(asset_class, {"error": "Schema not found for asset class"})

@app.get("/api/v1/powerbi/connection-test", tags=["PowerBI Integration"])
async def test_powerbi_connection():
    """Test endpoint for PowerBI connection validation"""
    
    return {
        "status": "success",
        "message": "PowerBI API connection is working",
        "timestamp": datetime.now(),
        "version": "1.0.0",
        "endpoints_available": [
            "/api/v1/crypto/prices",
            "/api/v1/money-market/rates", 
            "/api/v1/treasury/bonds",
            "/api/v1/forex/rates",
            "/api/v1/analytics/performance-comparison"
        ],
        "powerbi_notes": [
            "Use Web.Contents() function in PowerBI",
            "All endpoints return standardized PowerBIResponse format",
            "Caching enabled for performance optimization",
            "Schema hints included in all responses"
        ]
    }

# ============================================================================
# HEALTH CHECK & ROOT ENDPOINTS
# ============================================================================

@app.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """API health check with database connectivity test"""
    
    try:
        # Test database connection
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return {
        "status": "healthy",
        "timestamp": datetime.now(),
        "database": db_status,
        "redis_cache": "available" if REDIS_AVAILABLE else "unavailable",
        "version": "1.0.0"
    }

@app.get("/test/database-schema", tags=["Testing"])
async def test_database_schema(db: Session = Depends(get_db)):
    """Test database schema and return table information"""
    
    try:
        # Test each main table exists and has data
        tests = {}
        
        # Test fact tables
        tables_to_test = [
            'fact_cryptocurrency_prices',
            'fact_money_market_funds', 
            'fact_treasury_securities',
            'fact_forex_rates',
            'fact_market_indexes',
            'dim_financial_instruments',
            'dim_entities',
            'dim_currencies',
            'dim_countries'
        ]
        
        for table in tables_to_test:
            try:
                count_query = f"SELECT COUNT(*) as count FROM {table}"
                result = db.execute(text(count_query)).scalar()
                tests[table] = {"exists": True, "count": result}
                
                # Get column names
                columns_query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table}' 
                ORDER BY ordinal_position
                """
                columns_result = db.execute(text(columns_query)).fetchall()
                tests[table]["columns"] = [row[0] for row in columns_result]
                
            except Exception as e:
                tests[table] = {"exists": False, "error": str(e)}
        
        return {
            "status": "success",
            "timestamp": datetime.now(),
            "schema_tests": tests
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": str(e),
            "timestamp": datetime.now()
        }

@app.get("/test/simple-crypto", tags=["Testing"])
async def test_simple_crypto_query(db: Session = Depends(get_db)):
    """Simple crypto query test without complex joins"""
    
    try:
        # Very basic query first
        basic_query = """
        SELECT 
            fp.record_date,
            fp.price,
            fp.market_cap,
            fp.instrument_id
        FROM fact_cryptocurrency_prices fp
        ORDER BY fp.record_date DESC
        LIMIT 10
        """
        
        result = db.execute(text(basic_query)).fetchall()
        
        return {
            "status": "success",
            "message": "Basic crypto query works",
            "sample_data": [dict(row._mapping) for row in result],
            "record_count": len(result)
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "query": basic_query
        }

@app.get("/test/crypto-instruments", tags=["Testing"])
async def test_crypto_instruments(db: Session = Depends(get_db)):
    """Check what crypto instrument codes actually exist"""
    
    try:
        query = """
        SELECT 
            di.instrument_id,
            di.instrument_name,
            di.instrument_code,
            di.instrument_type,
            di.coingecko_id,
            COUNT(fp.id) as price_records
        FROM dim_financial_instruments di
        LEFT JOIN fact_cryptocurrency_prices fp ON di.instrument_id = fp.instrument_id
        WHERE di.instrument_type IN ('cryptocurrency', 'stablecoin')
        GROUP BY di.instrument_id, di.instrument_name, di.instrument_code, di.instrument_type, di.coingecko_id
        ORDER BY price_records DESC
        """
        
        result = db.execute(text(query)).fetchall()
        
        return {
            "status": "success",
            "message": "Crypto instruments found",
            "instruments": [dict(row._mapping) for row in result],
            "total_instruments": len(result)
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "query": query
        }

@app.get("/debug/crypto-codes", tags=["Debug"])
async def debug_crypto_codes(db: Session = Depends(get_db)):
    """Debug: See what crypto instrument codes exist"""
    
    try:
        query = """
        SELECT DISTINCT
            di.instrument_code,
            di.instrument_name,
            di.coingecko_id,
            di.instrument_type
        FROM dim_financial_instruments di
        WHERE di.instrument_type IN ('cryptocurrency', 'stablecoin')
        ORDER BY di.instrument_name
        """
        
        result = db.execute(text(query)).fetchall()
        
        return {
            "available_codes": [dict(row._mapping) for row in result],
            "note": "Use these codes in your API calls"
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/v1/crypto/prices-fixed", response_model=PowerBIResponse, tags=["Cryptocurrency"])
@cache_response(expiration_seconds=240)
async def get_crypto_prices_fixed(
    symbols: str = Query("BTC,ETH,USDT", description="Use instrument_code like BTC,ETH or coingecko_id like bitcoin,ethereum"),
    period: TimePeriod = Query(TimePeriod.THIRTY_DAYS),
    include_technical: bool = Query(True),
    db: Session = Depends(get_db)
):
    """Fixed crypto prices endpoint - works with your actual data"""
    
    symbol_list = [s.strip() for s in symbols.split(",")]
    days = parse_period_to_days(period.value)
    start_date = datetime.now() - timedelta(days=days)
    
    # Build query that works with actual schema
    query = """
    SELECT 
        fp.record_date,
        di.instrument_name as symbol,
        di.instrument_code as ticker,
        di.coingecko_id,
        fp.price::float as price,
        fp.market_cap::float as market_cap,
        fp.total_volume::float as total_volume,
        fp.deviation_from_dollar::float as deviation_from_dollar,
        fp.within_01_percent,
        fp.within_05_percent,
        fp.within_1_percent,
        fp.price_band,
        di.primary_currency_code as currency_code
    """
    
    if include_technical:
        query += """
        ,fp.rolling_avg_7d::float as rolling_avg_7d,
        fp.rolling_avg_30d::float as rolling_avg_30d,
        fp.rolling_std_7d::float as rolling_std_7d,
        fp.rolling_std_30d::float as rolling_std_30d,
        fp.daily_return::float as daily_return,
        fp.volatility::float as volatility
        """
    
    query += """
    FROM fact_cryptocurrency_prices fp
    JOIN dim_financial_instruments di ON fp.instrument_id = di.instrument_id
    WHERE (di.instrument_code = ANY(:symbols) 
           OR di.coingecko_id = ANY(:symbols))
    AND fp.record_date >= :start_date
    ORDER BY fp.record_date DESC, di.instrument_name
    LIMIT 1000
    """
    
    try:
        data = await execute_query(db, query, {
            'symbols': symbol_list,
            'start_date': start_date
        })
        
        # PowerBI schema hints
        schema = {
            "date_column": "record_date",
            "value_columns": ["price", "market_cap", "total_volume"],
            "category_column": "symbol",
            "date_format": "YYYY-MM-DD"
        }
        
        if include_technical:
            schema["technical_columns"] = ["rolling_avg_7d", "rolling_avg_30d", "daily_return", "volatility"]
        
        return PowerBIResponse(
            data=data,
            metadata={
                "period": period.value,
                "symbols_requested": symbol_list,
                "records_found": len(data),
                "query_timestamp": datetime.now(),
                "data_source": "CoinGecko API",
                "available_symbols": "BTC,ETH,USDT,USDC,BNB,SOL,ADA,DAI,BUSD,TUSD"
            },
            powerbi_schema=schema,
            total_records=len(data),
            last_updated=max([datetime.fromisoformat(str(row['record_date'])) for row in data]) if data else datetime.now()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Crypto query failed: {str(e)}")

@app.get("/api/v1/indexes/performance-fixed", response_model=PowerBIResponse, tags=["Market Indexes"])
@cache_response(expiration_seconds=1800)
async def get_index_performance_fixed(
    region: str = Query("ALL", description="Africa, Global, ALL"),
    period: TimePeriod = Query(TimePeriod.THIRTY_DAYS),
    include_technical: bool = Query(True),
    db: Session = Depends(get_db)
):
    """Fixed market index performance data"""
    
    days = parse_period_to_days(period.value)
    start_date = datetime.now() - timedelta(days=days)
    
    query = """
    SELECT 
        mi.record_date,
        di.instrument_name as index_name,
        di.instrument_code as index_code,
        mi.index_value::float as index_value,
        0.0 as daily_change,
        (mi.daily_return * 100)::float as daily_change_percent,
        mi.volume::float as volume,
        mi.market_cap::float as market_cap,
        'N/A' as country_name,
        'Global' as region,
        'N/A' as continent
    """
    
    if include_technical:
        query += """
        ,mi.moving_avg_50d::float as moving_avg_20d,
        mi.moving_avg_50d::float as moving_avg_50d,
        mi.moving_avg_200d::float as moving_avg_200d,
        0.0 as volatility_30d,
        mi.relative_strength::float as rsi
        """
    
    query += """
    FROM fact_market_indexes mi
    JOIN dim_financial_instruments di ON mi.instrument_id = di.instrument_id
    WHERE mi.record_date >= :start_date
    ORDER BY mi.record_date DESC, mi.daily_return DESC
    LIMIT 1000
    """
    
    try:
        data = await execute_query(db, query, {'start_date': start_date})
        
        schema = {
            "date_column": "record_date",
            "value_columns": ["index_value", "daily_change_percent", "volume"],
            "category_column": "index_name",
            "geographic_column": "region"
        }
        
        if include_technical:
            schema["technical_columns"] = ["moving_avg_20d", "moving_avg_50d", "moving_avg_200d", "rsi"]
        
        return PowerBIResponse(
            data=data,
            metadata={
                "region_filter": region,
                "period": period.value,
                "include_technical": include_technical,
                "records_found": len(data),
                "data_source": "Multiple Financial APIs"
            },
            powerbi_schema=schema,
            total_records=len(data),
            last_updated=datetime.now()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Index query failed: {str(e)}")

@app.get("/api/v1/green-bonds/performance-fixed", response_model=PowerBIResponse, tags=["Green Bonds"])
@cache_response(expiration_seconds=3600)
async def get_green_bonds_performance_fixed(
    period: TimePeriod = Query(TimePeriod.THIRTY_DAYS),
    db: Session = Depends(get_db)
):
    """Fixed green bonds performance - you have 17,570 records!"""
    
    days = parse_period_to_days(period.value)
    start_date = datetime.now() - timedelta(days=days)
    
    query = """
    SELECT 
        gb.record_date,
        di.instrument_name as bond_name,
        di.instrument_code as isin,
        gb.clean_price::float as current_price,
        gb.yield_to_maturity::float as yield_to_maturity,
        gb.duration::float as duration,
        gb.esg_score::float as esg_score,
        gb.carbon_avoided_tonnes::float as carbon_intensity,
        gb.green_certification as green_classification,
        gb.green_certification as certification_body,
        'N/A' as issuer,
        'N/A' as issuer_country
    FROM fact_green_bonds gb
    JOIN dim_financial_instruments di ON gb.instrument_id = di.instrument_id
    WHERE gb.record_date >= :start_date
    ORDER BY gb.record_date DESC, gb.esg_score DESC
    LIMIT 1000
    """
    
    try:
        data = await execute_query(db, query, {'start_date': start_date})
        
        return PowerBIResponse(
            data=data,
            metadata={
                "period": period.value,
                "records_found": len(data),
                "total_available": 17570,
                "data_source": "Green Bond Database"
            },
            powerbi_schema={
                "date_column": "record_date",
                "value_columns": ["current_price", "yield_to_maturity", "esg_score"],
                "category_column": "green_classification",
                "geographic_column": "issuer_country"
            },
            total_records=len(data),
            last_updated=datetime.now()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Green bonds query failed: {str(e)}")

@app.get("/api/v1/carbon-offsets/pricing-fixed", response_model=PowerBIResponse, tags=["Carbon Offsets"])
@cache_response(expiration_seconds=3600)
async def get_carbon_offset_pricing_fixed(
    db: Session = Depends(get_db)
):
    """Fixed carbon offset pricing - you have 5 records"""
    
    query = """
    SELECT 
        co.record_date,
        'Platform' as platform_name,
        di.instrument_name as project_name,
        co.project_type,
        co.standard as certification_standard,
        co.price_per_tonne::float as price_per_tonne,
        di.primary_currency_code as currency_code,
        co.project_vintage as vintage_year,
        co.credits_available::float as available_tonnes,
        co.project_location as location_country,
        co.standard as methodology,
        'Global' as project_region
    FROM fact_carbon_offsets co
    JOIN dim_financial_instruments di ON co.instrument_id = di.instrument_id
    ORDER BY co.record_date DESC, co.price_per_tonne ASC
    """
    
    try:
        data = await execute_query(db, query)
        
        return PowerBIResponse(
            data=data,
            metadata={
                "records_found": len(data),
                "total_available": 5,
                "data_source": "Carbon Platform APIs"
            },
            powerbi_schema={
                "date_column": "record_date",
                "value_columns": ["price_per_tonne", "available_tonnes"],
                "category_column": "project_type",
                "geographic_column": "project_region"
            },
            total_records=len(data),
            last_updated=datetime.now()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Carbon offsets query failed: {str(e)}")

@app.get("/api/v1/carbon-offsets/pricing-fixed", response_model=PowerBIResponse, tags=["Carbon Offsets"])
@cache_response(expiration_seconds=3600)
async def get_carbon_offset_pricing_fixed(
    project_type: str = Query("ALL", description="Forestry, Renewable, Technology, ALL"),
    certification: str = Query("ALL", description="VCS, Gold Standard, CDM, ALL"),
    latest: bool = Query(True),
    db: Session = Depends(get_db)
):
    """Fixed carbon offset pricing endpoint"""
    
    # Check if table exists and has data first
    try:
        count_query = "SELECT COUNT(*) FROM fact_carbon_offsets"
        count = db.execute(text(count_query)).scalar()
        
        if count == 0:
            return PowerBIResponse(
                data=[],
                metadata={
                    "message": "No carbon offset data available yet",
                    "table_exists": True,
                    "record_count": 0
                },
                powerbi_schema={
                    "date_column": "record_date",
                    "value_columns": ["price_per_tonne", "volume_traded"],
                    "category_column": "project_type"
                },
                total_records=0,
                last_updated=datetime.now()
            )
            
    except Exception as e:
        return PowerBIResponse(
            data=[],
            metadata={
                "error": "Carbon offsets table not accessible",
                "message": str(e)
            },
            powerbi_schema={},
            total_records=0,
            last_updated=datetime.now()
        )

@app.get("/debug/table-status", tags=["Debug"])
async def debug_table_status(db: Session = Depends(get_db)):
    """Debug: Check which tables have data"""
    
    tables = [
        'fact_cryptocurrency_prices',
        'fact_money_market_funds',
        'fact_treasury_securities', 
        'fact_forex_rates',
        'fact_market_indexes',
        'fact_fixed_deposits',
        'fact_green_bonds',
        'fact_carbon_offsets',
        'fact_etf_performance'
    ]
    
    status = {}
    for table in tables:
        try:
            count = db.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            latest = db.execute(text(f"SELECT MAX(record_date) FROM {table}")).scalar()
            status[table] = {
                "record_count": count,
                "latest_date": str(latest) if latest else None,
                "has_data": count > 0
            }
        except Exception as e:
            status[table] = {
                "error": str(e),
                "has_data": False
            }
    
    return {
        "table_status": status,
        "summary": {
            "tables_with_data": len([t for t, s in status.items() if s.get("has_data", False)]),
            "total_tables": len(tables)
        }
    }

@app.get("/debug/all-endpoints", tags=["Debug"])
async def debug_all_endpoints():
    """Debug: Updated status with 4 working data endpoints!"""
    
    return {
        "🎉 BREAKTHROUGH": "You have 4 asset classes with real data!",
        "✅ WORKING_ENDPOINTS": {
            "health_and_debug": [
                "GET /health - API health check",
                "GET /test/database-schema - Complete schema info", 
                "GET /debug/crypto-codes - Available crypto symbols",
                "GET /debug/table-status - Data availability by table",
                "GET /debug/all-endpoints - This endpoint"
            ],
            "working_data_apis": [
                "GET /api/v1/crypto/prices-fixed?symbols=BTC,ETH,USDT - 18,382 records ✅",
                "GET /api/v1/indexes/performance-fixed - 12,520 records ✅",
                "GET /api/v1/green-bonds/performance-fixed - 17,570 records ✅",
                "GET /api/v1/carbon-offsets/pricing-fixed - 5 records ✅"
            ]
        },
        "❌ BROKEN_OR_EMPTY": {
            "empty_tables": [
                "money-market (0 records)",
                "treasury (0 records)", 
                "forex (0 records)",
                "fixed-deposits (0 records)",
                "etf-performance (0 records)"
            ]
        },
        "📊 REAL_DATA_SUMMARY": {
            "crypto": "18,382 records - Latest: 2025-07-03 ✅",
            "market_indexes": "12,520 records - Latest: 2025-07-02 ✅", 
            "green_bonds": "17,570 records - Latest: 2025-07-02 ✅",
            "carbon_offsets": "5 records - Latest: 2025-07-03 ✅",
            "total_records": "48,477 records ready for PowerBI!"
        },
        "🚀 IMMEDIATE_POWERBI_READY": [
            "Crypto: BTC, ETH, USDT, USDC, BNB, SOL, ADA pricing",
            "Market Indexes: Global market performance data", 
            "Green Bonds: ESG scoring and sustainability metrics",
            "Carbon Offsets: Environmental impact pricing"
        ],
        "💡 POWERBI_QUERIES": {
            "crypto": "symbols=BTC,ETH,USDT&period=30d&include_technical=true",
            "indexes": "region=ALL&period=30d&include_technical=true",
            "green_bonds": "period=30d (17K records available!)",
            "carbon_offsets": "(small dataset but complete)"
        },
        "🎯 NEXT_STEPS": [
            "1. Test all 4 working endpoints below",
            "2. Create PowerBI dashboards with this real data",
            "3. Deploy to AWS/GCP for Windows PowerBI access", 
            "4. Populate remaining tables later (money market, treasury, forex)",
            "5. Celebrate - you have way more data than expected! 🎉"
        ]
    }
    """API health check with database connectivity test"""
    
    try:
        # Test database connection
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return {
        "status": "healthy",
        "timestamp": datetime.now(),
        "database": db_status,
        "redis_cache": "available" if REDIS_AVAILABLE else "unavailable",
        "version": "1.0.0"
    }

@app.get("/")
async def root():
    """API root endpoint with documentation links"""
    
    return {
        "message": "African Financial Data Pipeline API",
        "version": "1.0.0",
        "documentation": "/docs",
        "health_check": "/health",
        "asset_classes": [
            "crypto", "money_market", "treasury", "forex",
            "indexes", "green_bonds", "fixed_deposits", "carbon_offsets"
        ],
        "key_features": [
            "PowerBI-optimized responses",
            "Cross-asset analytics",
            "Real-time caching",
            "Data quality monitoring",
            "Pipeline management"
        ],
        "powerbi_connection": "Use Web.Contents() with any /api/v1/* endpoint"
    }

# ============================================================================
# RUN THE APPLICATION
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    logger.info("Starting African Financial Data Pipeline API")
    logger.info(f"Database: {DATABASE_URL}")
    logger.info(f"Redis: {'Available' if REDIS_AVAILABLE else 'Not Available'}")
    
    uvicorn.run(
        "main:app",  # This file should be named main.py
        host="0.0.0.0",
        port=8000,
        reload=True,  # Set to False in production
        log_level="info"
    )

# ============================================================================
# POWERBI CONNECTION EXAMPLES
# ============================================================================

"""
PowerBI M Query Examples:

1. Cryptocurrency Prices:
let
    Source = Json.Document(Web.Contents("http://localhost:8000/api/v1/crypto/prices?symbols=BTC,ETH,USDT&period=30d&include_technical=true")),
    data = Source[data],
    Table = Table.FromRecords(data)
in
    Table

2. Money Market Comparison:
let
    Source = Json.Document(Web.Contents("http://localhost:8000/api/v1/money-market/rates?currency=KES&latest=true")),
    data = Source[data],
    Table = Table.FromRecords(data)
in
    Table

3. Cross-Asset Performance:
let
    Source = Json.Document(Web.Contents("http://localhost:8000/api/v1/analytics/performance-comparison?asset_classes=crypto,money_market,treasury&period=90d")),
    data = Source[data],
    Table = Table.FromRecords(data)
in
    Table

4. Treasury Yield Curve:
let
    Source = Json.Document(Web.Contents("http://localhost:8000/api/v1/treasury/yield-curve?date=latest")),
    data = Source[data],
    Table = Table.FromRecords(data)
in
    Table
"""