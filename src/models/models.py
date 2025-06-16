from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Date, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base

class Country(Base):
    __tablename__ = 'countries'
    
    id = Column(Integer, primary_key=True)
    country_name = Column(String(100), nullable=False)
    currency_name = Column(String(100), nullable=False)
    currency_code = Column(String(10), nullable=False)  # Increased length
    
    currency_pairs = relationship("CurrencyPair", back_populates="country")

class CurrencyPair(Base):
    __tablename__ = 'currency_pairs'
    
    id = Column(Integer, primary_key=True)
    pair_symbol = Column(String(20), unique=True, nullable=False)  # Increased length
    base_currency = Column(String(10), nullable=False)  # Increased length
    quote_currency = Column(String(10), nullable=False)  # Increased length
    country_id = Column(Integer, ForeignKey('countries.id'), nullable=True)
    pair_name = Column(String(100), nullable=False)  # Increased length
    active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    country = relationship("Country", back_populates="currency_pairs")
    forex_rates = relationship("ForexRate", back_populates="currency_pair", cascade="all, delete-orphan")

class ForexRate(Base):
    __tablename__ = 'forex_rates'
    
    id = Column(Integer, primary_key=True)
    currency_pair_id = Column(Integer, ForeignKey('currency_pairs.id'), nullable=False)
    date = Column(Date, nullable=False)
    exchange_rate = Column(Float, nullable=False)
    open = Column(Float, nullable=False)  # This should be 'open', not 'open_rate'
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    volume = Column(Float)
    monthly_pct_change = Column(Float)
    yoy_pct_change = Column(Float)
    quarterly_volatility = Column(Float)
    significant_move = Column(Boolean, default=False)
    
    currency_pair = relationship("CurrencyPair", back_populates="forex_rates")