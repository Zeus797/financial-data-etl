from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session
import logging
from datetime import datetime

from src.models.base import engine, SessionLocal
from src.models.models import Base, CurrencyPair, ForexRate
from src.api.routes import forex, health, countries 

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Create tables if they don't exist
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown"""
    # Startup
    logger.info("Starting up Financial Data Pipeline API...")
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created/verified successfully")
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        raise 
    yield

    #Shutdown
    logger.info("Shutting down Financial Data Pipeline API...")

# Create FastAPI app
app = FastAPI(
    title="Financial Data Pipeline API",
    description="API for African financial and economic data, optimized for PowerBI integration",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS for PowerBI access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Configure specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, tags=["health"])
app.include_router(countries.router, prefix="/api/v1/countries", tags=["countries"])
app.include_router(forex.router, prefix="/api/v1/forex", tags=["forex"])


@app.get("/")
async def root():
    """Root endpoints with API information"""
    return {
        "message": "Financial Data Pipeline API",
        "version": "1.0.0",
        "documentation": "/docs",
        "health_check": "/heallth",
        "current_time": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health_check():
    """Simple health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Financial Data Pipeline API"
    }

@app.get("/api/v1/forex/pairs")
def get_currency_pairs(db: Session = Depends(get_db)):
    try:
        pairs = db.query(CurrencyPair).all()
        return [{"symbol": pair.pair_symbol, "id": pair.id} for pair in pairs]
    except Exception as e:
        logger.error(f"Error fetching currency pairs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/forex/rates/{base}/{quote}/latest")
def get_latest_rate(base: str, quote: str, db: Session = Depends(get_db)):
    try:
        pair = db.query(CurrencyPair).filter(
            CurrencyPair.pair_symbol == f"{base}/{quote}"
        ).first()
        
        if not pair:
            raise HTTPException(status_code=404, detail="Currency pair not found")
            
        latest_rate = db.query(ForexRate).filter(
            ForexRate.currency_pair_id == pair.id
        ).order_by(ForexRate.date.desc()).first()
        
        if not latest_rate:
            raise HTTPException(status_code=404, detail="No rates found")
            
        return {
            "pair": pair.pair_symbol,
            "rate": latest_rate.exchange_rate,
            "date": latest_rate.date
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching latest rate: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/forex/rates/{base}/{quote}/history")
def get_historical_rates(
    base: str, 
    quote: str, 
    limit: int = 30,
    db: Session = Depends(get_db)
):
    try:
        pair = db.query(CurrencyPair).filter(
            CurrencyPair.pair_symbol == f"{base}/{quote}"
        ).first()
        
        if not pair:
            raise HTTPException(status_code=404, detail="Currency pair not found")
            
        rates = db.query(ForexRate).filter(
            ForexRate.currency_pair_id == pair.id
        ).order_by(ForexRate.date.desc()).limit(limit).all()
        
        if not rates:
            raise HTTPException(status_code=404, detail="No rates found")
            
        return [{
            "date": rate.date,
            "rate": rate.exchange_rate,
            "high": rate.high,
            "low": rate.low,
            "open": rate.open,
            "volume": rate.volume
        } for rate in rates]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching historical rates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}")
    return{
        "error": "Internal server error",
        "message": str(exc),
        "timestamp": datetime.utcnow().isoformat()
    }

