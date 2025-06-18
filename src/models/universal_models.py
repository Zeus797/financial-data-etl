"""
Universal SQLAlchemy models for all asses classes
Shared dimensions enable cross-asset analysis in PowerBI
"""

from sqlalchemy import Column, Integer, String, Boolean, DECIMAL, DateTime, Date, Text, ForeignKey, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime, date
import calendar

Base = declarative_base()

# ==========================================
#SHARED DIMENSION TABLES (Used by ALL asset classes) 
# ==========================================

class DimCurrency(Base):
    """Universal currency dimension - fiat + crypto"""
    __tablename__ = 'dim_currencies'
    
    currency_id = Column(Integer, primary_key=True, autoincrement=True)
    currency_code = Column(String(10), unique=True, nullable=False)  # Changed from String(3)
    currency_name = Column(String(100), nullable=False)
    country_code = Column(String(2), ForeignKey('dim_countries.country_code'), nullable=False)
    
    # Relationships
    country = relationship(
        "DimCountry",
        foreign_keys=[country_code],
        back_populates="currencies"
    )
    
    # Fix: Use back_populates instead of backref
    instruments_primary = relationship(
        "DimFinancialInstrument",
        foreign_keys="[DimFinancialInstrument.primary_currency_code]",
        back_populates="primary_currency"  # Changed from backref
    )
    
    instruments_secondary = relationship(
        "DimFinancialInstrument",
        foreign_keys="[DimFinancialInstrument.secondary_currency_code]",
        back_populates="secondary_currency"  # Changed from backref
    )

    def __repr__(self):
        return f"<DimCurrency(code='{self.currency_code}', name='{self.currency_name}')>"
    
# First define DimEntity since others reference it
class DimEntity(Base):
    """Universal entity dimension - banks, fund managers, exchanges, governments, platforms"""
    __tablename__ = 'dim_entities'
    
    entity_id = Column(Integer, primary_key=True, autoincrement=True)
    entity_name = Column(String(255), nullable=False)
    entity_type = Column(String(50), nullable=False)  # 'bank', 'fund_manager', 'exchange', 'government', 'platform', 'issuer'
    entity_code = Column(String(20))
    country_code = Column(String(3), ForeignKey('dim_countries.country_code'))
    website_url = Column(Text)
    regulatory_status = Column(String(50))  # 'regulated', 'licensed', 'unregulated'
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    country = relationship(
        "DimCountry",
        foreign_keys=[country_code],
        back_populates="entities"
    )
    instruments_issued = relationship("DimFinancialInstrument", 
                                    foreign_keys="DimFinancialInstrument.issuer_entity_id",
                                    back_populates="issuer")
    instruments_managed = relationship("DimFinancialInstrument", 
                                     foreign_keys="DimFinancialInstrument.manager_entity_id",
                                     back_populates="manager")
    

    # Indexes
    __table_args__ = (
        Index('idx_entities_type_country', 'entity_type', 'country_code'),
        Index('idx_entities_active', 'is_active'),
    )
    
    def __repr__(self):
        return f"<DimEntity(id={self.entity_id}, name='{self.entity_name}', type='{self.entity_type}')>"
    

class DimCountry(Base):
    """Universal country/region dimension"""
    __tablename__ = 'dim_countries'
    
    country_code = Column(String(2), primary_key=True)
    country_name = Column(String(100), nullable=False)
    region = Column(String(50))  # Added missing field
    
    # Relationships
    entities = relationship(
        "DimEntity",
        foreign_keys=[DimEntity.country_code],
        back_populates="country"
    )
    currencies = relationship(
        "DimCurrency",
        foreign_keys=[DimCurrency.country_code],
        back_populates="country"
    )

    def __repr__(self):
        return f"<DimCountry(code='{self.country_code}', name='{self.country_name}', region='{self.region}')>"
    

class DimDate(Base):
    """Universal date dimension for ALL PowerBI time intelligence"""
    __tablename__ = 'dim_dates'

    date_key = Column(Integer, primary_key=True) # YYYYMMDD format
    full_date = Column(Date, nullable=False, unique=True)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    month_name = Column(String(20), nullable=False)
    week = Column(Integer, nullable=False)
    day_of_year = Column(Integer, nullable=False)
    day_of_month = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    day_name = Column(String(20), nullable=False)
    is_weekend = Column(Boolean, nullable=False)
    is_holiday = Column(Boolean, default=False)
    holiday_name = Column(String(100))
    fiscal_year = Column(Integer) # Kenya fiscal year July to June
    fiscal_quarter = Column(Integer)
    is_market_day = Column(Boolean, default=True) # For market-specific calendars

    # Relationships - complete list
    crypto_prices = relationship("FactCryptocurrencyPrice", back_populates="date_dim")
    treasury_securities = relationship("FactTreasurySecurity", back_populates="date_dim")
    money_market_funds = relationship("FactMoneyMarketFund", back_populates="date_dim")
    forex_rates = relationship("FactForexRate", back_populates="date_dim")
    market_indexes = relationship("FactMarketIndex", back_populates="date_dim") 
    fixed_deposits = relationship("FactFixedDeposit", back_populates="date_dim")
    green_bonds = relationship("FactGreenBond", back_populates="date_dim") 
    carbon_offsets = relationship("FactCarbonOffset", back_populates="date_dim")
    etf_performance = relationship("FactETFPerformance", back_populates="date_dim")  # Added this line

    # Indexes
    __table_args__ = (
        Index('idx_dates_year_month', 'year', 'month'),
        Index('idx_dates_quarter', 'fiscal_year', 'fiscal_quarter'),
        Index('idx_dates_market_day', 'is_market_day', 'full_date'),
    )
    
    def __repr__(self):
        return f"<DimDate(date_key={self.date_key}, date='{self.full_date}')>"
    

class DimFinancialInstrument(Base):
    """Universal financial instruments dimension - ALL asset types"""
    __tablename__ = 'dim_financial_instruments'
    
    instrument_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Core identification
    instrument_type = Column(String(50), nullable=False)
    # 'cryptocurrency', 'stablecoin', 'treasury_bond', 'treasury_bill', 
    # 'money_market_fund', 'fixed_deposit', 'etf', 'green_bond', 
    # 'forex_pair', 'market_index', 'carbon_offset'
    
    instrument_name = Column(String(255), nullable=False)
    instrument_code = Column(String(50))  # BTC, KE_10Y_BOND, USD_KES, etc.
    display_name = Column(String(255))  # User-friendly name for PowerBI
    
    # Currency information
    primary_currency_code = Column(String(10), ForeignKey('dim_currencies.currency_code'), nullable=False)
    secondary_currency_code = Column(String(10), ForeignKey('dim_currencies.currency_code'))  # For forex pairs
    
    # Entity relationships
    issuer_entity_id = Column(Integer, ForeignKey('dim_entities.entity_id'))  # Who issued it
    manager_entity_id = Column(Integer, ForeignKey('dim_entities.entity_id'))  # Who manages it
    
    # Asset classification
    asset_class = Column(String(50))  # 'fixed_income', 'equity', 'currency', 'alternative'
    risk_level = Column(String(20))  # 'low', 'medium', 'high'
    
    # External identifiers
    isin = Column(String(20))  # International Securities Identification Number
    cusip = Column(String(20))  # Committee on Uniform Securities Identification Procedures
    coingecko_id = Column(String(100))  # For cryptocurrencies
    bloomberg_ticker = Column(String(50))  # For market data
    
    # Instrument-specific flags
    is_stablecoin = Column(Boolean, default=False)
    is_esg_compliant = Column(Boolean, default=False)  # Environmental, Social, Governance
    is_sharia_compliant = Column(Boolean, default=False)
    
    # Metadata
    description = Column(Text)
    inception_date = Column(Date)
    maturity_date = Column(Date)  # For bonds, deposits
    minimum_investment = Column(DECIMAL(15, 2))
    
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    primary_currency = relationship("DimCurrency", 
                                  foreign_keys=[primary_currency_code], 
                                  back_populates="instruments_primary")
    secondary_currency = relationship("DimCurrency", 
                                    foreign_keys=[secondary_currency_code], 
                                    back_populates="instruments_secondary")
    issuer = relationship("DimEntity", 
                         foreign_keys=[issuer_entity_id], 
                         back_populates="instruments_issued")
    manager = relationship("DimEntity", 
                          foreign_keys=[manager_entity_id], 
                          back_populates="instruments_managed")
    


    # Current relationships in DimFinancialInstrument:
    crypto_prices = relationship("FactCryptocurrencyPrice", back_populates="instrument")
    treasury_securities = relationship("FactTreasurySecurity", back_populates="instrument")
    money_market_funds = relationship("FactMoneyMarketFund", back_populates="instrument")
    forex_rates = relationship("FactForexRate", back_populates="instrument")
    market_indexes = relationship("FactMarketIndex", back_populates="instrument")
    fixed_deposits = relationship("FactFixedDeposit", back_populates="instrument")
    green_bonds = relationship("FactGreenBond", back_populates="instrument") 
    carbon_offsets = relationship("FactCarbonOffset", back_populates="instrument")
    etf_performance = relationship("FactETFPerformance", back_populates="instrument")

    __table_args__ = (
        # Existing indexes
        Index('idx_instruments_type_currency', 'instrument_type', 'primary_currency_code'),
        Index('idx_instruments_active_type', 'is_active', 'instrument_type'),
        Index('idx_instruments_coingecko', 'coingecko_id'),
        Index('idx_instruments_isin', 'isin'),
        Index('idx_instruments_asset_class', 'asset_class', 'risk_level'),
    
        # NEW indexes needed for ALL asset classes:
        Index('idx_instruments_issuer', 'issuer_entity_id'),
        Index('idx_instruments_manager', 'manager_entity_id'), 
        Index('idx_instruments_maturity', 'maturity_date'),
        Index('idx_instruments_inception', 'inception_date'),
        Index('idx_instruments_bloomberg', 'bloomberg_ticker'),
        Index('idx_instruments_cusip', 'cusip'),
    
        # Composite indexes for common PowerBI queries
        Index('idx_instruments_active_asset_class', 'is_active', 'asset_class', 'instrument_type'),
        Index('idx_instruments_currency_asset', 'primary_currency_code', 'asset_class'),
        Index('idx_instruments_issuer_type', 'issuer_entity_id', 'instrument_type'),
        Index('idx_instruments_esg_compliance', 'is_esg_compliant', 'asset_class'),
    
        # Specialized indexes for different asset types
        Index('idx_instruments_stablecoin', 'is_stablecoin'),
        Index('idx_instruments_bonds_maturity', 'instrument_type', 'maturity_date'),
    )

    def __repr__(self):
        return f"<DimFinancialInstrument(id={self.instrument_id}, code='{self.instrument_code}', type='{self.instrument_type}')>"

class FactCryptocurrencyPrice(Base):
    """Cryptocurrency prices fact table - optimized for all crypto data types"""
    __tablename__ = 'fact_cryptocurrency_prices'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_id = Column(Integer, ForeignKey('dim_financial_instruments.instrument_id'), nullable=False)
    date_key = Column(Integer, ForeignKey('dim_dates.date_key'), nullable=False)
    record_date = Column(Date, nullable=False)
    
    # Core price data - handles Bitcoin prices up to $99,999,999
    price = Column(DECIMAL(20, 8))
    market_cap = Column(DECIMAL(25, 2))  # Handles trillion-dollar market caps
    total_volume = Column(DECIMAL(25, 2))  # Large trading volumes
    
    # OHLC data for historical records
    high = Column(DECIMAL(20, 8))
    low = Column(DECIMAL(20, 8))
    open = Column(DECIMAL(20, 8))
    close = Column(DECIMAL(20, 8))
    
    # 24-hour price ranges
    high_24h = Column(DECIMAL(20, 8))
    low_24h = Column(DECIMAL(20, 8))
    
    # Supply metrics - handles tokens with massive supplies
    circulating_supply = Column(DECIMAL(25, 8))
    total_supply = Column(DECIMAL(25, 8))
    max_supply = Column(DECIMAL(25, 8))
    fully_diluted_valuation = Column(DECIMAL(25, 2))
    
    # Market position changes
    market_cap_change_24h = Column(DECIMAL(25, 2))
    market_cap_change_percentage_24h = Column(DECIMAL(8, 4))
    
    # All-time metrics - handles extreme percentage changes
    ath = Column(DECIMAL(20, 8))  # All-time high price
    ath_change_percentage = Column(DECIMAL(15, 8))  # Can be millions of %
    ath_date = Column(Date)
    atl = Column(DECIMAL(20, 8))  # All-time low price
    atl_change_percentage = Column(DECIMAL(15, 8))  # Can be millions of %
    atl_date = Column(Date)
    
    # Technical indicators - Bollinger Bands
    bollinger_upper_20d = Column(DECIMAL(20, 8))
    bollinger_lower_20d = Column(DECIMAL(20, 8))
    
    # Technical indicators - RSI
    rsi_14 = Column(DECIMAL(6, 2))
    
    # Price changes
    price_change_24h = Column(DECIMAL(20, 8))
    price_change_percentage_24h = Column(DECIMAL(8, 4))
    price_change_percentage_7d = Column(DECIMAL(8, 4))
    price_change_percentage_30d = Column(DECIMAL(8, 4))
    
    # Returns and volatility
    daily_return = Column(DECIMAL(8, 6))
    volatility = Column(DECIMAL(8, 6))
    volatility_7d = Column(DECIMAL(8, 6))
    volatility_30d = Column(DECIMAL(8, 6))
    
    # Stablecoin specific fields
    deviation_from_dollar = Column(DECIMAL(12, 10))
    within_01_percent = Column(Boolean)
    within_05_percent = Column(Boolean)
    within_1_percent = Column(Boolean)
    price_band = Column(String(20))  # 'normal', 'moderate', 'high', 'critical'
    
    # Moving averages and standard deviations
    rolling_avg_7d = Column(DECIMAL(20, 8))
    rolling_avg_30d = Column(DECIMAL(20, 8))
    rolling_std_7d = Column(DECIMAL(20, 8))
    rolling_std_30d = Column(DECIMAL(20, 8))
    
    # Market position metrics
    market_cap_rank = Column(Integer)
    market_dominance_percentage = Column(DECIMAL(6, 4))
    
    # Metadata and audit fields
    data_source = Column(String(50), default='coingecko')
    last_updated = Column(DateTime)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    instrument = relationship("DimFinancialInstrument", back_populates="crypto_prices")
    date_dim = relationship("DimDate", back_populates="crypto_prices")
    
    # Constraints and Indexes for performance
    __table_args__ = (
        UniqueConstraint('instrument_id', 'record_date', name='uq_crypto_instrument_date'),
        Index('idx_crypto_date_instrument', 'record_date', 'instrument_id'),
        Index('idx_crypto_market_cap', 'record_date', 'market_cap'),
        Index('idx_crypto_stablecoin', 'instrument_id', 'deviation_from_dollar'),
        Index('idx_crypto_price_date', 'price', 'record_date'),
    )


class FactTreasurySecurity(Base):
    """Treasury securities fact table"""
    __tablename__ = 'fact_treasury_securities'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_id = Column(Integer, ForeignKey('dim_financial_instruments.instrument_id'), nullable=False)
    date_key = Column(Integer, ForeignKey('dim_dates.date_key'), nullable=False)
    record_date = Column(Date, nullable=False)
    
    # Pricing data
    current_price = Column(DECIMAL(10, 4))
    yield_to_maturity = Column(DECIMAL(8, 6))
    after_tax_ytm = Column(DECIMAL(8, 6))
    coupon_rate = Column(DECIMAL(8, 6))
    
    # Risk metrics
    duration = Column(DECIMAL(8, 4))
    convexity = Column(DECIMAL(8, 4))
    price_premium = Column(DECIMAL(8, 4))
    years_to_maturity = Column(DECIMAL(8, 4))
    
    # Auction data
    auction_date = Column(Date)
    amount_offered = Column(DECIMAL(15, 2))
    amount_accepted = Column(DECIMAL(15, 2))
    war_rate = Column(DECIMAL(8, 6))  # Weighted Average Rate
    discount_rate = Column(DECIMAL(8, 6))
    
    # Performance
    total_return_1m = Column(DECIMAL(8, 4))
    total_return_3m = Column(DECIMAL(8, 4))
    total_return_ytd = Column(DECIMAL(8, 4))
    
    # Metadata
    data_source = Column(String(50), default='nse_kenya')
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    instrument = relationship("DimFinancialInstrument", back_populates="treasury_securities")
    date_dim = relationship("DimDate", back_populates="treasury_securities")
    
    __table_args__ = (
        UniqueConstraint('instrument_id', 'record_date', name='uq_treasury_instrument_date'),
        Index('idx_treasury_date_instrument', 'record_date', 'instrument_id'),
        Index('idx_treasury_ytm', 'record_date', 'yield_to_maturity'),
    )

class FactMoneyMarketFund(Base):
    """Money market funds fact table"""
    __tablename__ = 'fact_money_market_funds'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_id = Column(Integer, ForeignKey('dim_financial_instruments.instrument_id'), nullable=False)
    date_key = Column(Integer, ForeignKey('dim_dates.date_key'), nullable=False)
    record_date = Column(Date, nullable=False)
    
    # Yield data
    current_yield = Column(DECIMAL(8, 6))
    seven_day_yield = Column(DECIMAL(8, 6))
    thirty_day_yield = Column(DECIMAL(8, 6))
    year_to_date_yield = Column(DECIMAL(8, 6))
    
    # Fund metrics
    net_asset_value = Column(DECIMAL(12, 4))
    total_assets = Column(DECIMAL(15, 2))
    expense_ratio = Column(DECIMAL(6, 4))
    management_fee = Column(DECIMAL(6, 4))
    
    # Performance
    sharpe_ratio = Column(DECIMAL(8, 4))
    volatility = Column(DECIMAL(8, 6))
    max_drawdown = Column(DECIMAL(8, 4))
    
    # Metadata
    data_source = Column(String(50), default='fund_website')
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    instrument = relationship("DimFinancialInstrument", back_populates="money_market_funds")
    date_dim = relationship("DimDate", back_populates="money_market_funds")
    
    __table_args__ = (
        UniqueConstraint('instrument_id', 'record_date', name='uq_mmf_instrument_date'),
        Index('idx_mmf_date_instrument', 'record_date', 'instrument_id'),
        Index('idx_mmf_yield', 'record_date', 'current_yield'),
    )

class FactForexRate(Base):
    """Forex rates fact table - updated to match actual data format"""
    __tablename__ = 'fact_forex_rates'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_id = Column(Integer, ForeignKey('dim_financial_instruments.instrument_id'), nullable=False)
    date_key = Column(Integer, ForeignKey('dim_dates.date_key'), nullable=False)
    record_date = Column(Date, nullable=False)
    
    # OHLC Data (matches your format)
    open_rate = Column(DECIMAL(12, 8))
    high_rate = Column(DECIMAL(12, 8))
    low_rate = Column(DECIMAL(12, 8))
    exchange_rate = Column(DECIMAL(12, 8))  # Close price
    volume = Column(DECIMAL(20, 2))
    
    # Corporate actions
    dividends = Column(DECIMAL(12, 8))
    stock_splits = Column(DECIMAL(8, 4))
    
    # Currency pair info (from your data)
    base_currency = Column(String(3))
    quote_currency = Column(String(3))
    pair = Column(String(10))  # e.g., "USDKES"
    
    # Time-based fields
    year = Column(Integer)
    month = Column(Integer)
    quarter = Column(Integer)
    year_quarter = Column(String(10))  # e.g., "2025Q2"
    
    # Performance metrics (from your data)
    monthly_pct_change = Column(DECIMAL(8, 4))
    yoy_pct_change = Column(DECIMAL(8, 4))
    quarterly_volatility = Column(DECIMAL(8, 6))
    annual_volatility = Column(DECIMAL(8, 6))
    significant_move = Column(Boolean)
    ma_12_month = Column(DECIMAL(12, 8))
    
    # Metadata
    data_source = Column(String(50), default='yfinance')
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    instrument = relationship("DimFinancialInstrument", back_populates="forex_rates")
    date_dim = relationship("DimDate", back_populates="forex_rates")
    
    __table_args__ = (
        UniqueConstraint('instrument_id', 'record_date', name='uq_forex_instrument_date'),
        Index('idx_forex_date_instrument', 'record_date', 'instrument_id'),
        Index('idx_forex_pair_date', 'pair', 'record_date'),
        Index('idx_forex_significant_moves', 'significant_move', 'record_date'),
        Index('idx_forex_volatility', 'record_date', 'quarterly_volatility'),
    )

class FactMarketIndex(Base):
    """Market indexes fact table"""
    __tablename__ = 'fact_market_indexes'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_id = Column(Integer, ForeignKey('dim_financial_instruments.instrument_id'), nullable=False)
    date_key = Column(Integer, ForeignKey('dim_dates.date_key'), nullable=False)
    record_date = Column(Date, nullable=False)
    
    # Index values
    index_value = Column(DECIMAL(15, 4))
    opening_value = Column(DECIMAL(15, 4))
    high_value = Column(DECIMAL(15, 4))
    low_value = Column(DECIMAL(15, 4))
    closing_value = Column(DECIMAL(15, 4))
    
    # Performance
    daily_return = Column(DECIMAL(8, 6))
    volume = Column(DECIMAL(20, 2))
    market_cap = Column(DECIMAL(20, 2))
    
    # Technical indicators
    moving_avg_50d = Column(DECIMAL(15, 4))
    moving_avg_200d = Column(DECIMAL(15, 4))
    relative_strength = Column(DECIMAL(6, 2))
    
    # Metadata
    data_source = Column(String(50))
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    instrument = relationship("DimFinancialInstrument", back_populates="market_indexes")
    date_dim = relationship("DimDate", back_populates="market_indexes")
    
    __table_args__ = (
        UniqueConstraint('instrument_id', 'record_date', name='uq_index_instrument_date'),
        Index('idx_index_date_instrument', 'record_date', 'instrument_id'),
        Index('idx_index_value', 'record_date', 'index_value'),
    )

# Additional fact tables to complete the universal schema
# Add these to your universal_models.py file

class FactFixedDeposit(Base):
    """Fixed deposit rates and terms fact table"""
    __tablename__ = 'fact_fixed_deposits'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_id = Column(Integer, ForeignKey('dim_financial_instruments.instrument_id'), nullable=False)
    date_key = Column(Integer, ForeignKey('dim_dates.date_key'), nullable=False)
    record_date = Column(Date, nullable=False)
    
    # Deposit terms
    deposit_amount = Column(DECIMAL(15, 2))  # Minimum/actual deposit amount
    term_months = Column(Integer)  # 1, 3, 6, 12, 24, 36 months
    interest_rate = Column(DECIMAL(8, 6))  # Annual interest rate
    effective_rate = Column(DECIMAL(8, 6))  # After taxes and fees
    
    # Rate structure
    base_rate = Column(DECIMAL(8, 6))  # Base rate before negotiations
    negotiated_rate = Column(DECIMAL(8, 6))  # Rate after negotiation
    promotional_rate = Column(DECIMAL(8, 6))  # Special promotional offers
    senior_citizen_bonus = Column(DECIMAL(6, 4))  # Additional rate for seniors
    
    # Terms and conditions
    minimum_deposit = Column(DECIMAL(15, 2))
    maximum_deposit = Column(DECIMAL(15, 2))
    early_withdrawal_penalty = Column(DECIMAL(6, 4))  # Penalty percentage
    auto_renewal = Column(Boolean, default=True)
    
    # Tax implications
    tax_rate = Column(DECIMAL(6, 4), default=0.15)  # Withholding tax
    after_tax_return = Column(DECIMAL(8, 6))  # Net return after tax
    
    # Competitive analysis
    market_rank = Column(Integer)  # Ranking among banks for same term
    rate_premium_to_market = Column(DECIMAL(6, 4))  # Premium over market average
    
    # Metadata
    rate_type = Column(String(20))  # 'fixed', 'variable', 'stepped'
    currency_code = Column(String(3))  # KES, USD, EUR
    data_source = Column(String(50), default='bank_website')
    last_verified = Column(DateTime)
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    instrument = relationship("DimFinancialInstrument", back_populates="fixed_deposits")
    date_dim = relationship("DimDate", back_populates="fixed_deposits")
    
    __table_args__ = (
        UniqueConstraint('instrument_id', 'record_date', 'term_months', name='uq_fd_instrument_date_term'),
        Index('idx_fd_date_instrument', 'record_date', 'instrument_id'),
        Index('idx_fd_rate_term', 'interest_rate', 'term_months'),
        Index('idx_fd_currency_term', 'currency_code', 'term_months'),
        Index('idx_fd_amount_range', 'minimum_deposit', 'maximum_deposit'),
    )

class FactGreenBond(Base):
    """Green bond performance and ESG metrics fact table"""
    __tablename__ = 'fact_green_bonds'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_id = Column(Integer, ForeignKey('dim_financial_instruments.instrument_id'), nullable=False)
    date_key = Column(Integer, ForeignKey('dim_dates.date_key'), nullable=False)
    record_date = Column(Date, nullable=False)
    
    # Bond pricing
    clean_price = Column(DECIMAL(10, 4))  # Price without accrued interest
    dirty_price = Column(DECIMAL(10, 4))  # Price with accrued interest
    yield_to_maturity = Column(DECIMAL(8, 6))
    green_premium = Column(DECIMAL(6, 4))  # Premium over conventional bonds
    
    # Performance metrics
    total_return_1m = Column(DECIMAL(8, 4))
    total_return_3m = Column(DECIMAL(8, 4))
    total_return_ytd = Column(DECIMAL(8, 4))
    duration = Column(DECIMAL(8, 4))
    convexity = Column(DECIMAL(8, 4))
    
    # Green/ESG specific metrics
    green_certification = Column(String(50))  # 'Climate Bonds Standard', 'Green Bond Principles'
    esg_score = Column(DECIMAL(5, 2))  # ESG rating score
    carbon_avoided_tonnes = Column(DECIMAL(15, 2))  # CO2 equivalent avoided
    renewable_energy_mwh = Column(DECIMAL(15, 2))  # Renewable energy generated
    
    # Use of proceeds tracking
    proceeds_allocated_pct = Column(DECIMAL(5, 2))  # % of proceeds allocated
    renewable_energy_pct = Column(DECIMAL(5, 2))  # % for renewable energy
    energy_efficiency_pct = Column(DECIMAL(5, 2))  # % for energy efficiency
    sustainable_transport_pct = Column(DECIMAL(5, 2))  # % for sustainable transport
    water_management_pct = Column(DECIMAL(5, 2))  # % for water projects
    
    # Impact reporting
    impact_report_available = Column(Boolean, default=False)
    impact_verification = Column(String(50))  # 'Third-party verified', 'Self-reported'
    sdg_alignment = Column(Text)  # UN Sustainable Development Goals alignment
    
    # Market data
    outstanding_amount = Column(DECIMAL(15, 2))  # Total outstanding
    trading_volume = Column(DECIMAL(15, 2))  # Daily trading volume
    bid_ask_spread = Column(DECIMAL(6, 4))  # Market liquidity indicator
    
    # Credit metrics
    credit_rating = Column(String(10))  # Moody's/S&P rating
    credit_spread = Column(DECIMAL(6, 4))  # Spread over government bonds
    default_probability = Column(DECIMAL(6, 4))  # Implied default probability
    
    # Metadata
    data_source = Column(String(50), default='bloomberg')
    verification_date = Column(Date)  # Last impact verification
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    instrument = relationship("DimFinancialInstrument", back_populates="green_bonds")
    date_dim = relationship("DimDate", back_populates="green_bonds")
    
    __table_args__ = (
        UniqueConstraint('instrument_id', 'record_date', name='uq_gb_instrument_date'),
        Index('idx_gb_date_instrument', 'record_date', 'instrument_id'),
        Index('idx_gb_yield_rating', 'yield_to_maturity', 'credit_rating'),
        Index('idx_gb_esg_score', 'esg_score', 'record_date'),
        Index('idx_gb_certification', 'green_certification'),
        Index('idx_gb_impact', 'carbon_avoided_tonnes', 'renewable_energy_mwh'),
    )

class FactCarbonOffset(Base):
    """Carbon offset prices and project details fact table"""
    __tablename__ = 'fact_carbon_offsets'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_id = Column(Integer, ForeignKey('dim_financial_instruments.instrument_id'), nullable=False)
    date_key = Column(Integer, ForeignKey('dim_dates.date_key'), nullable=False)
    record_date = Column(Date, nullable=False)
    
    # Pricing data
    price_per_tonne = Column(DECIMAL(10, 4))  # Price per tonne CO2 equivalent
    volume_traded = Column(DECIMAL(15, 2))  # Tonnes traded
    total_value = Column(DECIMAL(15, 2))  # Total transaction value
    
    # Price movements
    price_change_1d = Column(DECIMAL(8, 4))
    price_change_7d = Column(DECIMAL(8, 4))
    price_change_30d = Column(DECIMAL(8, 4))
    price_volatility = Column(DECIMAL(8, 6))
    
    # Project details
    project_type = Column(String(50))  # 'Forestry', 'Renewable Energy', 'Methane Capture'
    project_location = Column(String(100))  # Country/region
    project_vintage = Column(Integer)  # Year credits were generated
    methodology = Column(String(50))  # 'VCS', 'CDM', 'Gold Standard'
    
    # Certification and quality
    standard = Column(String(30))  # 'Verra VCS', 'Gold Standard', 'Climate Action Reserve'
    additionality_verified = Column(Boolean, default=False)
    permanence_rating = Column(String(20))  # 'High', 'Medium', 'Low'
    co_benefits = Column(Text)  # Biodiversity, community development, etc.
    
    # Supply and demand
    credits_issued = Column(DECIMAL(15, 2))  # Total credits issued for project
    credits_retired = Column(DECIMAL(15, 2))  # Credits permanently retired
    credits_available = Column(DECIMAL(15, 2))  # Credits available for purchase
    retirement_rate = Column(DECIMAL(5, 2))  # % of credits retired vs issued
    
    # Market metrics
    market_share = Column(DECIMAL(5, 2))  # % of total voluntary carbon market
    buyer_type = Column(String(30))  # 'Corporate', 'Individual', 'Government'
    geographic_demand = Column(String(50))  # Primary demand geography
    
    # Risk factors
    reversal_risk = Column(DECIMAL(5, 2))  # Risk of carbon release (forestry)
    political_risk = Column(String(20))  # Country stability rating
    delivery_risk = Column(String(20))  # Risk of non-delivery
    
    # Impact metrics
    co2_equivalent_tonnes = Column(DECIMAL(15, 2))  # CO2 equivalent impact
    monitoring_frequency = Column(String(20))  # 'Annual', 'Bi-annual', 'Continuous'
    verification_date = Column(Date)  # Last third-party verification
    
    # Platform data
    platform = Column(String(50))  # Trading platform/marketplace
    transaction_fee = Column(DECIMAL(6, 4))  # Platform fee percentage
    settlement_period = Column(Integer)  # Days to settlement
    
    # Metadata
    data_source = Column(String(50), default='carbon_platform')
    quality_score = Column(DECIMAL(5, 2))  # Internal quality assessment
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    instrument = relationship("DimFinancialInstrument", back_populates="carbon_offsets")
    date_dim = relationship("DimDate", back_populates="carbon_offsets")
    
    __table_args__ = (
        UniqueConstraint('instrument_id', 'record_date', 'project_vintage', name='uq_co_instrument_date_vintage'),
        Index('idx_co_date_instrument', 'record_date', 'instrument_id'),
        Index('idx_co_price_volume', 'price_per_tonne', 'volume_traded'),
        Index('idx_co_project_type', 'project_type', 'project_location'),
        Index('idx_co_standard_vintage', 'standard', 'project_vintage'),
        Index('idx_co_quality_risk', 'quality_score', 'reversal_risk'),
    )

class FactETFPerformance(Base):
    """ETF performance and fund metrics fact table"""
    __tablename__ = 'fact_etf_performance'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_id = Column(Integer, ForeignKey('dim_financial_instruments.instrument_id'), nullable=False)
    date_key = Column(Integer, ForeignKey('dim_dates.date_key'), nullable=False)
    record_date = Column(Date, nullable=False)
    
    # Pricing data
    nav_per_share = Column(DECIMAL(12, 4))  # Net Asset Value per share
    market_price = Column(DECIMAL(12, 4))  # Trading price
    premium_discount = Column(DECIMAL(8, 4))  # Premium/discount to NAV
    
    # Trading data
    volume = Column(DECIMAL(15, 2))  # Shares traded
    dollar_volume = Column(DECIMAL(15, 2))  # Dollar volume traded
    bid_ask_spread = Column(DECIMAL(6, 4))  # Market spread
    
    # Performance metrics
    total_return_1d = Column(DECIMAL(8, 4))
    total_return_1w = Column(DECIMAL(8, 4))
    total_return_1m = Column(DECIMAL(8, 4))
    total_return_3m = Column(DECIMAL(8, 4))
    total_return_ytd = Column(DECIMAL(8, 4))
    total_return_1y = Column(DECIMAL(8, 4))
    
    # Risk metrics
    volatility_30d = Column(DECIMAL(8, 6))
    beta = Column(DECIMAL(6, 4))  # Beta vs benchmark
    sharpe_ratio = Column(DECIMAL(8, 4))
    max_drawdown = Column(DECIMAL(8, 4))
    
    # Fund metrics
    total_assets = Column(DECIMAL(15, 2))  # Assets under management
    shares_outstanding = Column(DECIMAL(15, 0))
    expense_ratio = Column(DECIMAL(6, 4))  # Annual expense ratio
    dividend_yield = Column(DECIMAL(6, 4))  # Annual dividend yield
    
    # Holdings data
    top_10_holdings_pct = Column(DECIMAL(5, 2))  # % in top 10 holdings
    number_of_holdings = Column(Integer)
    portfolio_turnover = Column(DECIMAL(6, 2))  # Annual turnover rate
    cash_percentage = Column(DECIMAL(5, 2))  # % cash holdings
    
    # Sector/geographic allocation (for thematic ETFs)
    technology_pct = Column(DECIMAL(5, 2))
    healthcare_pct = Column(DECIMAL(5, 2))
    financials_pct = Column(DECIMAL(5, 2))
    energy_pct = Column(DECIMAL(5, 2))
    developed_markets_pct = Column(DECIMAL(5, 2))
    emerging_markets_pct = Column(DECIMAL(5, 2))
    africa_allocation_pct = Column(DECIMAL(5, 2))  # Specific to African focus
    
    # ESG metrics (for ESG/Green ETFs)
    esg_score = Column(DECIMAL(5, 2))
    carbon_intensity = Column(DECIMAL(10, 4))  # Tonnes CO2/$M revenue
    green_revenue_pct = Column(DECIMAL(5, 2))  # % revenue from green activities
    
    # Flow data
    net_flows_1d = Column(DECIMAL(15, 2))  # Daily net flows
    net_flows_1w = Column(DECIMAL(15, 2))  # Weekly net flows
    net_flows_1m = Column(DECIMAL(15, 2))  # Monthly net flows
    
    # Tracking metrics
    tracking_error = Column(DECIMAL(6, 4))  # vs benchmark
    tracking_difference = Column(DECIMAL(6, 4))  # Return difference vs benchmark
    correlation_to_benchmark = Column(DECIMAL(6, 4))
    
    # Dividend/distribution data
    distribution_frequency = Column(String(20))  # 'Monthly', 'Quarterly', 'Annual'
    last_distribution_amount = Column(DECIMAL(8, 4))
    distribution_yield_ttm = Column(DECIMAL(6, 4))  # Trailing 12 months
    
    # Metadata
    benchmark_index = Column(String(100))  # Primary benchmark
    fund_inception_date = Column(Date)
    data_source = Column(String(50), default='fund_provider')
    fund_family = Column(String(100))  # Fund management company
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    instrument = relationship("DimFinancialInstrument", back_populates="etf_performance")
    date_dim = relationship("DimDate", back_populates="etf_performance")
    
    __table_args__ = (
        UniqueConstraint('instrument_id', 'record_date', name='uq_etf_instrument_date'),
        Index('idx_etf_date_instrument', 'record_date', 'instrument_id'),
        Index('idx_etf_nav_volume', 'nav_per_share', 'dollar_volume'),
        Index('idx_etf_performance', 'total_return_1m', 'total_return_ytd'),
        Index('idx_etf_assets_flows', 'total_assets', 'net_flows_1m'),
        Index('idx_etf_esg_green', 'esg_score', 'green_revenue_pct'),
        Index('idx_etf_expense_yield', 'expense_ratio', 'dividend_yield'),
    )

# ================================
# UTILITY FUNCTIONS
# ================================

def get_date_key(date_obj):
    """Convert date to YYYYMMDD format for date_key"""
    if isinstance(date_obj, str):
        from datetime import datetime
        date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()
    return int(date_obj.strftime('%Y%m%d'))

def create_date_dimension_record(date_obj):
    """Create a DimDate record from a dateobject"""
    if isinstance(date_obj, str):
        from datetime import datetime
        date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()

    # Kenya fiscal year calculation (July-June)
    if date_obj.month >= 7:
        fiscal_year = date_obj.year
        fiscal_quarter = ((date_obj.month - 7) // 3) + 1
    else:
        fiscal_year = date_obj.year - 1
        fiscal_quarter = ((date_obj.month + 5)//3) + 1
    
    return DimDate(
        date_key=get_date_key(date_obj),
        full_date=date_obj,
        year=date_obj.year,
        quarter = (date_obj.month-1)//3 + 1,
        month=date_obj.month,
        month_name=calendar.month_name[date_obj.month],
        week=date_obj.isocalendar()[1],
        day_of_year=date_obj.timetuple().tm_yday,
        day_of_month=date_obj.day,
        day_of_week=date_obj.weekday() + 1, #Monday = 1
        day_name=calendar.day_name[date_obj.weekday()],
        is_weekend=date_obj.weekday() >= 5,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        is_market_day=date_obj.weekday()< 5
    )