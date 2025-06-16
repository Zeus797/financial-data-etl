# src/api/routes/forex.py
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
from datetime import datetime, date
import pandas as pd
import logging

from src.models.models import CurrencyPair, ForexRate
from src.models.base import SessionLocal

# Configure logger
logger = logging.getLogger(__name__)

router = APIRouter()

# Database dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/pairs")
async def get_currency_pairs(db: Session = Depends(get_db)):
    """Get all available currency pairs"""
    try:
        pairs = db.query(CurrencyPair).all()
        return [{"symbol": pair.pair_symbol, "id": pair.id} for pair in pairs]
    except Exception as e:
        logger.error(f"Error fetching currency pairs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/rates/powerbi")
async def get_powerbi_data(
    start_date: date = Query(default=None),
    end_date: date = Query(default=None),
    currency_pairs: str = Query(default=None),
    include_technical: bool = Query(default=False),
    db: Session = Depends(get_db)
):
    """Get data formatted for PowerBI"""
    try:
        query = db.query(ForexRate, CurrencyPair).join(CurrencyPair)
        
        if start_date:
            query = query.filter(ForexRate.date >= start_date)
        if end_date:
            query = query.filter(ForexRate.date <= end_date)
        if currency_pairs:
            pairs = [p.strip() for p in currency_pairs.split(",")]
            query = query.filter(CurrencyPair.pair_symbol.in_(pairs))
            
        results = query.all()
        
        return [{
            "date": rate.date,
            "pair": pair.pair_symbol,
            "rate": rate.exchange_rate,
            "high": rate.high,
            "low": rate.low,
            "open": rate.open,
            "volume": rate.volume,
            "technical_indicators": {
                "sma_20": rate.sma_20,
                "rsi": rate.rsi,
                "volatility": rate.volatility
            } if include_technical else None
        } for rate, pair in results]
    except Exception as e:
        logger.error(f"Error fetching PowerBI data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/rates/timeseries")
async def get_timeseries_data(
    pair: str = Query(...),
    start_date: date = Query(None),
    end_date: date = Query(None),
    db: Session = Depends(get_db)
):
    """Get time series data for analysis"""
    try:
        query = db.query(ForexRate, CurrencyPair).join(CurrencyPair)
        query = query.filter(CurrencyPair.pair_symbol == pair)
        
        if start_date:
            query = query.filter(ForexRate.date >= start_date)
        if end_date:
            query = query.filter(ForexRate.date <= end_date)
            
        results = query.order_by(ForexRate.date).all()
        
        return [{
            "date": rate.date,
            "rate": rate.exchange_rate,
            "high": rate.high,
            "low": rate.low,
            "open": rate.open,
            "volume": rate.volume
        } for rate, _ in results]
    except Exception as e:
        logger.error(f"Error fetching time series data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/rates/summary/{base}/{quote}")
async def get_summary_statistics(
    base: str,
    quote: str,
    db: Session = Depends(get_db)
):
    """Get summary statistics for a currency pair"""
    try:
        pair = db.query(CurrencyPair).filter(
            CurrencyPair.pair_symbol == f"{base}/{quote}"
        ).first()
        
        if not pair:
            raise HTTPException(status_code=404, detail="Currency pair not found")
            
        stats = db.query(
            func.avg(ForexRate.exchange_rate).label('avg_rate'),
            func.min(ForexRate.exchange_rate).label('min_rate'),
            func.max(ForexRate.exchange_rate).label('max_rate'),
            func.stddev(ForexRate.exchange_rate).label('std_dev')
        ).filter(ForexRate.currency_pair_id == pair.id).first()
        
        return {
            "pair": pair.pair_symbol,
            "average_rate": float(stats.avg_rate),
            "minimum_rate": float(stats.min_rate),
            "maximum_rate": float(stats.max_rate),
            "standard_deviation": float(stats.std_dev) if stats.std_dev else 0
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error calculating summary statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))