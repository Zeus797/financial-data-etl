"""
SQLAlchemy models for cryptocurrency data pipeline
"""
from sqlalchemy import Column, Integer, String, Boolean, DECIMAL, DateTime, Text, Date, ForeignKey, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime

Base = declarative_base()

class DimCurrency(Base):
    """Currency dimension table"""
    __tablename__ = 'dim_currencies'

    currency_code = Column(String(3), primary_key=True)
    currency_name = Column(String(100), nulllable=False)
    currency_symbol = Column(String(10))
    is_active = Column(Boolean, default = True)
    created_at = Column(DateTime, default=func.now())

    # Relationships
    instruments = relationship("DimFinancialInstrument", back_populates="currency")

    def __repr__(self):
        return f"<DimCurrency(code='{self.currency_code}', name='{self.currency_name}')>"
    
class DimEntity(Base):
    """Entity dimension table for issuers, exchanges, platforms"""
    __tablename__ = 'dim_entities'

    entity_id = Column(Integer, primary_key = True, autoincrement=True)
    entity_name = Column(String(255), nullable=False)
    entity_type = Column(String(50), nullable=False) # 'exchange', 'platform', 'issuer'
    entity_code = Column(String(20))
    country_code = (String(3))
    website_url = Column(String(3))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    instruments = relationship("DimFinancialInstrument", back_populates="entity")

    def __repr__(self):
        return f"<DimEntity(id={self.entity_id}, name='{self.entity_name}', type='{self.entity_type}')>"
    
class DimFinancialInstrument(Base):
    """Financial instruments dimension table"""
    __tablename__ = "dim_financial_instruments"

    instrument_id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_type = Column(String(50), nullable=False) # 'cryptocurrency', 'stablecoin'
    instrument_name = Column(String(255), nullable=False)
    instrument_code = Column(String(50)) # Symbol like BTC, ETH
    currency_code = Column(String(3), ForeignKey('dim_currencies.currency_code'), nullable=False)
    entity_id = Column(Integer, ForeignKey('dim_entities.entity_id'))
    description = Column(Text)
    coingecko_id = Column(String(100)) #CoinGecko API Identifier
    is_stablecoin = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())

    # Relationships
    currency = relationship("DimCurrency", back_populates="instruments")
    entity = relationship("DimEntity", back_populates="instruments")
    price_data = relationship("FactCryptocurrencyPrice", backpopulates="instrument")

    # Indexes
    __table_args__ = (
        Index('idx_instruments_type_currency', 'instrument_type', 'currency_code'),
        Index('ids_instruments_coingecko', 'coingecko_id'),
        Index('idx_instruments_active', 'is_active'),
    )

    def __repr__(self):
        return f"<DimFinancialInstrument(id={self.instrument_id}, code='{self.instrument_code}', name='{self.instrument_name}')>"
    

class DimDate(Base):
    """Date dimension table for PoweBI time intelligence"""
    __table_name__ = 'dim_dates'

    