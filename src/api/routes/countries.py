from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional 

from src.models.base import SessionLocal
from src.models.models import Country, CurrencyPair
from src.api.schemas.forex_schemas import CountryResponse, CurrencyPairResponse

router = APIRouter()

def get_db():
    """Database dependency"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/", response_model=List[CountryResponse])
async def get_countries(
    region: Optional[str] = Query(None, description="Filter by region"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=500, description = "Maximum records to return"),
    db: Session = Depends(get_db)
) :
    
    """
    Get all countries with optional filtering
    
    -**region**: Filter countries by region (e.g., 'East Africa', 'West Africa')
    -**skip**: Number of records to skip for pagination
    --**limit**: MMaximum number of records to return
    """
    query = db.query(Country)

    if region:
        query = query.filter(Country.region == region)

    countries = query.offset(skip).limit(limit).all()
    return countries

@router.get("/{country_code}", response_model=CountryResponse)
async def get_country(
    country_code: str,
    db: Session = Depends(get_db)
):
    """
    Get a specific country by its ISO code
    
    - **country_code**: ISO 2 or 3 letter country code (e.g., 'KE', 'KEN')
    """
    country = db.query(Country).filter(
        (Country.code == country_code.upper())
    ).first()
    
    if not country:
        raise HTTPException(status_code=404, detail=f"Country {country_code} not found")
    
    return country

@router.get("/{country_code}/currency-pairs", response_model=List[CurrencyPairResponse])
async def get_country_currency_pairs(
    country_code: str,
    db: Session = Depends(get_db)
):
    """
    Get all currency pairs for a specific country
    
    Returns pairs where the country's currency is either base or quote
    """
    # First get the country to find its currency
    country = db.query(Country).filter(
        Country.code == country_code.upper()
    ).first()
    
    if not country:
        raise HTTPException(status_code=404, detail=f"Country {country_code} not found")
    
    # Find all currency pairs with this currency
    pairs = db.query(CurrencyPair).filter(
        (CurrencyPair.base_currency == country.currency_code) |
        (CurrencyPair.quote_currency == country.currency_code)
    ).filter(CurrencyPair.is_active == True).all()
    
    return pairs

@router.post("/", response_model=CountryResponse)
async def create_country(
    country: CountryResponse,
    db: Session = Depends(get_db)
):
    """
    Create a new country entry
    
    Required fields:
    - **code**: ISO country code
    - **name**: Country name
    - **currency_code**: ISO currency code
    - **currency_name**: Currency name
    """
    # Check if country already exists
    existing = db.query(Country).filter(
        Country.code == country.code.upper()
    ).first()
    
    if existing:
        raise HTTPException(status_code=400, detail="Country already exists")
    
    db_country = Country(
        code=country.code.upper(),
        name=country.name,
        currency_code=country.currency_code.upper(),
        currency_name=country.currency_name,
        region=country.region
    )
    
    db.add(db_country)
    db.commit()
    db.refresh(db_country)
    
    return db_country

@router.get("/regions/list")
async def get_regions(db: Session = Depends(get_db)):
    """
    Get list of all unique regions
    """
    regions = db.query(Country.region).distinct().filter(
        Country.region != None
    ).all()
    
    return {"regions": [r[0] for r in regions if r[0]]}

@router.get("/currencies/list")
async def get_currencies(db: Session = Depends(get_db)):
    """
    Get list of all unique currencies
    """
    currencies = db.query(
        Country.currency_code,
        Country.currency_name
    ).distinct().all()
    
    return {
        "currencies": [
            {"code": c[0], "name": c[1]} 
            for c in currencies
        ]
    } 