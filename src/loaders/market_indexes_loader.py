"""
src/loaders/market_index_loader.py

Complete Market Index Loader for Universal Database Schema
Loads transformed market index data into PostgreSQL with star schema design
"""

import pandas as pd
import logging
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text, and_
from sqlalchemy.orm import Session
import os

# Import your database components
from src.models.base import db_manager
from src.models.universal_models import (
    DimFinancialInstrument, DimCurrency, DimCountry, 
    DimEntity, DimDate, FactMarketIndex, get_date_key, 
    create_date_dimension_record
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketIndexLoader:
    """
    Enterprise-grade loader for market index data using universal schema
    
    Features:
    - Populates dimension tables automatically
    - Handles upserts (insert new, update existing)
    - Comprehensive data validation
    - Performance optimized batch processing
    - Detailed logging and error handling
    """
    
    def __init__(self):
        self.db_manager = db_manager
        self.batch_size = 1000  # Process records in batches
        
        # Index metadata mapping (from your transformer)
        self.index_metadata = {
            "S&P_500": {
                "country": "US", "region": "North America", "continent": "North America",
                "currency": "USD", "bloomberg_ticker": "SPX"
            },
            "Dow_Jones": {
                "country": "US", "region": "North America", "continent": "North America", 
                "currency": "USD", "bloomberg_ticker": "DJI"
            },
            "NASDAQ_Composite": {
                "country": "US", "region": "North America", "continent": "North America",
                "currency": "USD", "bloomberg_ticker": "IXIC"
            },
            "FTSE_100": {
                "country": "GB", "region": "Europe", "continent": "Europe",
                "currency": "GBP", "bloomberg_ticker": "UKX"
            },
            "DAX_40": {
                "country": "DE", "region": "Europe", "continent": "Europe",
                "currency": "EUR", "bloomberg_ticker": "DAX"
            },
            "CAC_40": {
                "country": "FR", "region": "Europe", "continent": "Europe",
                "currency": "EUR", "bloomberg_ticker": "CAC"
            },
            "JSE_Top_40": {
                "country": "ZA", "region": "Africa", "continent": "Africa",
                "currency": "ZAR", "bloomberg_ticker": "TOP40"
            },
            "Nikkei_225": {
                "country": "JP", "region": "Asia-Pacific", "continent": "Asia",
                "currency": "JPY", "bloomberg_ticker": "N225"
            },
            "Hang_Seng": {
                "country": "HK", "region": "Asia-Pacific", "continent": "Asia",
                "currency": "HKD", "bloomberg_ticker": "HSI"
            },
            "EURO_STOXX_50": {
                "country": "EU", "region": "Europe", "continent": "Europe",
                "currency": "EUR", "bloomberg_ticker": "SX5E"
            }
        }
        
        logger.info("MarketIndexLoader initialized with universal schema")

    def load_transformed_data(self, csv_file_path: str) -> bool:
        """
        Main method to load transformed market index data into database
        
        Args:
            csv_file_path: Path to the transformed_indexes.csv file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info("="*60)
            logger.info("🚀 STARTING MARKET INDEX DATA LOAD")
            logger.info("="*60)
            logger.info(f"Source file: {csv_file_path}")
            
            # Validate file exists
            if not os.path.exists(csv_file_path):
                raise FileNotFoundError(f"Transformed data file not found: {csv_file_path}")
            
            # Step 1: Load and validate CSV data
            logger.info("📄 Step 1: Loading and validating CSV data...")
            df = self._load_and_validate_csv(csv_file_path)
            
            # Step 2: Create database tables if they don't exist
            logger.info("🏗️  Step 2: Ensuring database tables exist...")
            self._create_tables_if_needed()
            
            # Step 3: Populate dimension tables
            logger.info("📊 Step 3: Populating dimension tables...")
            self._populate_dimension_tables(df)
            
            # Step 4: Load fact data
            logger.info("💾 Step 4: Loading fact data...")
            records_loaded = self._load_fact_data(df)
            
            # Step 5: Validate loaded data
            logger.info("✅ Step 5: Validating loaded data...")
            self._validate_loaded_data()
            
            logger.info("="*60)
            logger.info("🎉 MARKET INDEX DATA LOAD COMPLETED SUCCESSFULLY!")
            logger.info(f"📈 Records loaded: {records_loaded:,}")
            logger.info("="*60)
            return True
            
        except Exception as e:
            logger.error("="*60)
            logger.error("❌ MARKET INDEX DATA LOAD FAILED")
            logger.error(f"Error: {str(e)}")
            logger.error("="*60)
            return False

    def _load_and_validate_csv(self, csv_file_path: str) -> pd.DataFrame:
        """Load CSV and perform basic validation"""
        
        # Load the CSV
        df = pd.read_csv(csv_file_path)
        logger.info(f"📄 Loaded CSV with {len(df):,} rows and {len(df.columns)} columns")
        
        # Show sample of data
        logger.info(f"📅 Date range: {df['Date'].min()} to {df['Date'].max()}")
        logger.info(f"📊 Unique indexes: {df['Index_Name'].nunique()}")
        logger.info(f"🏷️  Index names: {sorted(df['Index_Name'].unique())}")
        
        # Validate required columns
        required_columns = [
            'Date', 'Index_Name', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume',
            'Country', 'Region', 'Currency', 'Daily_Return', 'Cumulative_Return',
            'MA_50', 'MA_200', 'Volatility_10D', 'Volatility_30D', 'Normalized_Value'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Convert Date column to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Validate data types and handle nulls
        df = self._clean_numeric_columns(df)
        
        logger.info("✅ CSV validation completed successfully")
        return df

    def _clean_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate numeric columns"""
        
        numeric_columns = [
            'Open', 'High', 'Low', 'Close', 'Volume', 'Daily_Return', 
            'Cumulative_Return', 'MA_50', 'MA_200', 'Volatility_10D', 
            'Volatility_30D', 'Normalized_Value'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                # Convert to numeric, coerce errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Fill NaN values with 0 for most columns
                if col == 'Volume':
                    df[col] = df[col].fillna(0)
                elif col in ['Daily_Return', 'Cumulative_Return']:
                    df[col] = df[col].fillna(0)
                elif col in ['MA_50', 'MA_200']:
                    # For moving averages, keep NaN for initial periods
                    pass
                else:
                    df[col] = df[col].fillna(0)
        
        return df

    def _create_tables_if_needed(self):
        """Create database tables if they don't exist"""
        try:
            self.db_manager.create_tables()
            logger.info("✅ Database tables verified/created")
        except Exception as e:
            logger.error(f"❌ Error creating tables: {e}")
            raise

    def _populate_dimension_tables(self, df: pd.DataFrame):
        """Populate all dimension tables with data from the DataFrame"""
        
        with self.db_manager.get_session() as session:
            
            # 1. Populate Countries
            logger.info("   📍 Populating countries dimension...")
            self._populate_countries(session, df)
            
            # 2. Populate Currencies  
            logger.info("   💱 Populating currencies dimension...")
            self._populate_currencies(session, df)
            
            # 3. Populate Entities (exchanges/providers)
            logger.info("   🏢 Populating entities dimension...")
            self._populate_entities(session, df)
            
            # 4. Populate Financial Instruments
            logger.info("   📈 Populating financial instruments dimension...")
            self._populate_instruments(session, df)
            
            # 5. Populate Date Dimension
            logger.info("   📅 Populating date dimension...")
            self._populate_dates(session, df)
            
            session.commit()
            logger.info("✅ All dimension tables populated successfully")

    def _populate_countries(self, session: Session, df: pd.DataFrame):
        """Populate countries dimension table"""
        
        unique_countries = df[['Country', 'Region']].drop_duplicates()
        
        for _, row in unique_countries.iterrows():
            country_code = self._get_country_code(row['Country'])
            
            # Check if country exists
            existing = session.query(DimCountry).filter_by(country_code=country_code).first()
            if not existing:
                country = DimCountry(
                    country_code=country_code,
                    country_name=row['Country'],
                    region=row['Region']
                )
                session.add(country)
                logger.debug(f"   Added country: {row['Country']} ({country_code})")

    def _populate_currencies(self, session: Session, df: pd.DataFrame):
        """Populate currencies dimension table"""
        
        unique_currencies = df['Currency'].unique()
        
        for currency_code in unique_currencies:
            # Check if currency exists
            existing = session.query(DimCurrency).filter_by(currency_code=currency_code).first()
            if not existing:
                currency_name = self._get_currency_name(currency_code)
                country_code = self._get_currency_country(currency_code)
                
                currency = DimCurrency(
                    currency_code=currency_code,
                    currency_name=currency_name,
                    country_code=country_code
                )
                session.add(currency)
                logger.debug(f"   Added currency: {currency_code}")

    def _populate_entities(self, session: Session, df: pd.DataFrame):
        """Populate entities dimension table (exchanges/index providers)"""
        
        # Create entities for index providers/exchanges
        index_entities = {
            "S&P_500": {"name": "S&P Dow Jones Indices", "type": "index_provider", "country": "US"},
            "Dow_Jones": {"name": "S&P Dow Jones Indices", "type": "index_provider", "country": "US"},
            "NASDAQ_Composite": {"name": "NASDAQ", "type": "exchange", "country": "US"},
            "FTSE_100": {"name": "FTSE Russell", "type": "index_provider", "country": "GB"},
            "DAX_40": {"name": "Deutsche Börse", "type": "exchange", "country": "DE"},
            "CAC_40": {"name": "Euronext", "type": "exchange", "country": "FR"},
            "JSE_Top_40": {"name": "Johannesburg Stock Exchange", "type": "exchange", "country": "ZA"},
            "Nikkei_225": {"name": "Nikkei Inc", "type": "index_provider", "country": "JP"},
            "Hang_Seng": {"name": "Hang Seng Indexes", "type": "index_provider", "country": "HK"},
            "EURO_STOXX_50": {"name": "STOXX Ltd", "type": "index_provider", "country": "EU"}
        }
        
        for index_name, entity_info in index_entities.items():
            # Check if entity exists
            existing = session.query(DimEntity).filter_by(
                entity_name=entity_info["name"],
                entity_type=entity_info["type"]
            ).first()
            
            if not existing:
                entity = DimEntity(
                    entity_name=entity_info["name"],
                    entity_type=entity_info["type"],
                    country_code=entity_info["country"],
                    is_active=True
                )
                session.add(entity)
                logger.debug(f"   Added entity: {entity_info['name']}")

    def _populate_instruments(self, session: Session, df: pd.DataFrame):
        """Populate financial instruments dimension table"""
        
        unique_instruments = df[['Index_Name', 'Ticker', 'Currency', 'Country']].drop_duplicates()
        
        for _, row in unique_instruments.iterrows():
            index_name = row['Index_Name']
            
            # Check if instrument exists
            existing = session.query(DimFinancialInstrument).filter_by(
                instrument_code=row['Ticker']
            ).first()
            
            if not existing:
                # Get metadata for this index
                metadata = self.index_metadata.get(index_name, {})
                
                # Find the issuer entity
                issuer = session.query(DimEntity).filter(
                    DimEntity.country_code == row['Country'],
                    DimEntity.entity_type.in_(['exchange', 'index_provider'])
                ).first()
                
                instrument = DimFinancialInstrument(
                    instrument_type='market_index',
                    instrument_name=index_name.replace('_', ' '),
                    instrument_code=row['Ticker'],
                    display_name=index_name.replace('_', ' '),
                    primary_currency_code=row['Currency'],
                    issuer_entity_id=issuer.entity_id if issuer else None,
                    asset_class='equity_index',
                    risk_level='medium',
                    bloomberg_ticker=metadata.get('bloomberg_ticker'),
                    description=f"{index_name.replace('_', ' ')} market index",
                    is_active=True
                )
                session.add(instrument)
                logger.debug(f"   Added instrument: {index_name}")

    def _populate_dates(self, session: Session, df: pd.DataFrame):
        """Populate date dimension table"""
        
        # Get unique dates from the data
        unique_dates = df['Date'].dt.date.unique()
        
        for date_obj in unique_dates:
            date_key = get_date_key(date_obj)
            
            # Check if date exists
            existing = session.query(DimDate).filter_by(date_key=date_key).first()
            if not existing:
                date_record = create_date_dimension_record(date_obj)
                session.add(date_record)
        
        logger.debug(f"   Added {len(unique_dates)} date records")

    def _load_fact_data(self, df: pd.DataFrame) -> int:
        """Load fact table data in batches"""
        
        total_records = 0
        batch_count = 0
        
        # Process data in batches
        for batch_start in range(0, len(df), self.batch_size):
            batch_end = min(batch_start + self.batch_size, len(df))
            batch_df = df.iloc[batch_start:batch_end]
            
            batch_count += 1
            logger.info(f"   Processing batch {batch_count}: rows {batch_start+1}-{batch_end}")
            
            with self.db_manager.get_session() as session:
                batch_records = self._process_fact_batch(session, batch_df)
                total_records += batch_records
                session.commit()
        
        logger.info(f"✅ Loaded {total_records:,} fact records in {batch_count} batches")
        return total_records

    def _process_fact_batch(self, session: Session, batch_df: pd.DataFrame) -> int:
        """Process a single batch of fact data"""
        
        records_processed = 0
        
        for _, row in batch_df.iterrows():
            try:
                # Get instrument ID
                instrument = session.query(DimFinancialInstrument).filter_by(
                    instrument_code=row['Ticker']
                ).first()
                
                if not instrument:
                    logger.warning(f"Instrument not found for ticker: {row['Ticker']}")
                    continue
                
                # Get date key
                date_key = get_date_key(row['Date'].date())
                
                # Check if record already exists (upsert logic)
                existing = session.query(FactMarketIndex).filter(
                    and_(
                        FactMarketIndex.instrument_id == instrument.instrument_id,
                        FactMarketIndex.record_date == row['Date'].date()
                    )
                ).first()
                
                if existing:
                    # Update existing record
                    self._update_fact_record(existing, row)
                else:
                    # Create new record
                    fact_record = self._create_fact_record(instrument.instrument_id, date_key, row)
                    session.add(fact_record)
                
                records_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing record for {row.get('Ticker', 'unknown')}: {e}")
                continue
        
        return records_processed

    def _create_fact_record(self, instrument_id: int, date_key: int, row: pd.Series) -> FactMarketIndex:
        """Create a new fact record"""
        
        return FactMarketIndex(
            instrument_id=instrument_id,
            date_key=date_key,
            record_date=row['Date'].date(),
            index_value=float(row['Close']) if pd.notna(row['Close']) else None,
            opening_value=float(row['Open']) if pd.notna(row['Open']) else None,
            high_value=float(row['High']) if pd.notna(row['High']) else None,
            low_value=float(row['Low']) if pd.notna(row['Low']) else None,
            closing_value=float(row['Close']) if pd.notna(row['Close']) else None,
            daily_return=float(row['Daily_Return']) if pd.notna(row['Daily_Return']) else None,
            volume=float(row['Volume']) if pd.notna(row['Volume']) else None,
            moving_avg_50d=float(row['MA_50']) if pd.notna(row['MA_50']) else None,
            moving_avg_200d=float(row['MA_200']) if pd.notna(row['MA_200']) else None,
            data_source='yfinance'
        )

    def _update_fact_record(self, existing_record: FactMarketIndex, row: pd.Series):
        """Update an existing fact record"""
        
        existing_record.index_value = float(row['Close']) if pd.notna(row['Close']) else existing_record.index_value
        existing_record.opening_value = float(row['Open']) if pd.notna(row['Open']) else existing_record.opening_value
        existing_record.high_value = float(row['High']) if pd.notna(row['High']) else existing_record.high_value
        existing_record.low_value = float(row['Low']) if pd.notna(row['Low']) else existing_record.low_value
        existing_record.closing_value = float(row['Close']) if pd.notna(row['Close']) else existing_record.closing_value
        existing_record.daily_return = float(row['Daily_Return']) if pd.notna(row['Daily_Return']) else existing_record.daily_return
        existing_record.volume = float(row['Volume']) if pd.notna(row['Volume']) else existing_record.volume
        existing_record.moving_avg_50d = float(row['MA_50']) if pd.notna(row['MA_50']) else existing_record.moving_avg_50d
        existing_record.moving_avg_200d = float(row['MA_200']) if pd.notna(row['MA_200']) else existing_record.moving_avg_200d

    def _validate_loaded_data(self):
        """Validate that data was loaded correctly"""
        
        with self.db_manager.get_session() as session:
            # Count records in each table
            counts = {
                'Countries': session.query(DimCountry).count(),
                'Currencies': session.query(DimCurrency).count(),
                'Entities': session.query(DimEntity).count(),
                'Instruments': session.query(DimFinancialInstrument).count(),
                'Dates': session.query(DimDate).count(),
                'Market Index Facts': session.query(FactMarketIndex).count()
            }
            
            logger.info("📊 Final data counts:")
            for table, count in counts.items():
                logger.info(f"   {table}: {count:,} records")
            
            # Validate date range
            date_range = session.query(
                session.query(FactMarketIndex.record_date).order_by(FactMarketIndex.record_date.asc()).limit(1).scalar_subquery(),
                session.query(FactMarketIndex.record_date).order_by(FactMarketIndex.record_date.desc()).limit(1).scalar_subquery()
            ).first()
            
            if date_range and date_range[0] and date_range[1]:
                logger.info(f"📅 Date range: {date_range[0]} to {date_range[1]}")

    # Helper methods for data mapping
    def _get_country_code(self, country_name: str) -> str:
        """Map country names to ISO codes"""
        country_mapping = {
            'United States': 'US', 'United Kingdom': 'GB', 'Germany': 'DE',
            'France': 'FR', 'South Africa': 'ZA', 'Japan': 'JP', 
            'Hong Kong': 'HK', 'Eurozone': 'EU'
        }
        return country_mapping.get(country_name, country_name[:2].upper())

    def _get_currency_name(self, currency_code: str) -> str:
        """Map currency codes to names"""
        currency_mapping = {
            'USD': 'US Dollar', 'GBP': 'British Pound', 'EUR': 'Euro',
            'ZAR': 'South African Rand', 'JPY': 'Japanese Yen', 'HKD': 'Hong Kong Dollar'
        }
        return currency_mapping.get(currency_code, f"{currency_code} Currency")

    def _get_currency_country(self, currency_code: str) -> str:
        """Map currency codes to primary countries"""
        currency_country_mapping = {
            'USD': 'US', 'GBP': 'GB', 'EUR': 'EU',
            'ZAR': 'ZA', 'JPY': 'JP', 'HKD': 'HK'
        }
        return currency_country_mapping.get(currency_code, 'US')

# Convenience function for easy usage
def load_market_index_data(csv_file_path: str = None) -> bool:
    """
    Convenience function to load market index data
    
    Args:
        csv_file_path: Path to transformed data file. 
                      Defaults to 'data/processed_data/transformed_indexes.csv'
    
    Returns:
        bool: True if successful, False otherwise
    """
    if csv_file_path is None:
        csv_file_path = "data/processed_data/transformed_indexes.csv"
    
    loader = MarketIndexLoader()
    return loader.load_transformed_data(csv_file_path)

# Example usage and testing
if __name__ == "__main__":
    print("🚀 Testing Market Index Loader...")
    
    # Test database connection first
    from src.models.base import db_manager
    
    if db_manager.test_connection():
        print("✅ Database connection successful")
        
        # Load the data
        csv_path = "data/processed_data/transformed_indexes.csv"
        if load_market_index_data(csv_path):
            print("🎉 Market index data loaded successfully!")
        else:
            print("❌ Failed to load market index data")
    else:
        print("❌ Database connection failed")
        print("Please check your database configuration")