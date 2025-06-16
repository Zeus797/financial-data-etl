# src/api/routes/health.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime
import psutil
import os

from src.models.base import SessionLocal

router = APIRouter()

def get_db():
    """Database dependency"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """
    Health check endpoint for monitoring
    
    Returns system status and database connectivity
    """
    try:
        # Check database connection - Fixed for SQLAlchemy 2.0
        db.execute(text("SELECT 1"))
        db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"
    
    # Get system metrics
    memory = psutil.virtual_memory()
    
    return {
        "status": "healthy" if db_status == "healthy" else "degraded",
        "timestamp": datetime.utcnow().isoformat(),
        "database": db_status,
        "system": {
            "memory_usage_percent": memory.percent,
            "cpu_percent": psutil.cpu_percent(interval=1),
            "pid": os.getpid()
        },
        "version": "1.0.0"
    }

@router.get("/health/detailed")
async def detailed_health_check(db: Session = Depends(get_db)):
    """
    Detailed health check with table counts
    """
    health_info = await health_check(db)
    
    try:
        # Get record counts
        from src.models.models import Country, CurrencyPair, ForexRate
        
        country_count = db.query(Country).count()
        pair_count = db.query(CurrencyPair).count()
        rate_count = db.query(ForexRate).count()
        
        health_info["data_stats"] = {
            "countries": country_count,
            "currency_pairs": pair_count,
            "forex_rates": rate_count
        }
    except Exception as e:
        health_info["data_stats"] = {"error": str(e)}
    
    return health_info 
