"""
src/loaders/green_bonds_loader.py

Complete Green Bonds/ESG ETF Loader for Universal Database Schema
Loads transformed green bonds data into PostgreSQL using FactGreenBond table
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
    DimEntity, DimDate, FactGreenBond, get_date_key, 
    create_date_dimension_record
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GreenBondsLoader:
    """
    Enterprise-grade loader for green bonds/ESG ETF data using FactGreenBond table
    """
    
    def __init__(self):
        self.db_manager = db_manager
        self.batch_size = 1000  # Process records in batches
        
        # ETF metadata mapping
        self.etf_metadata = {
            "iShares_Global_Green_Bond": {
                "ticker": "BGRN", "issuer": "BlackRock", "category": "Green Bonds",
                "asset_class": "Fixed Income", "geographic_focus": "Global", "currency": "USD"
            },
            "Franklin_Liberty_Green_Bond": {
                "ticker": "FLGR", "issuer": "Franklin Templeton", "category": "Green Bonds",
                "asset_class": "Fixed Income", "geographic_focus": "United States", "currency": "USD"
            },
            "VanEck_Green_Bond": {
                "ticker": "GRNB", "issuer": "VanEck", "category": "Green Bonds",
                "asset_class": "Fixed Income", "geographic_focus": "Global", "currency": "USD"
            },
            "Schwab_US_Large_Cap_Growth": {
                "ticker": "SCHG", "issuer": "Charles Schwab", "category": "ESG Equity",
                "asset_class": "Equity", "geographic_focus": "United States", "currency": "USD"
            },
            "iShares_MSCI_USA_ESG_Select": {
                "ticker": "ESGU", "issuer": "BlackRock", "category": "ESG Equity",
                "asset_class": "Equity", "geographic_focus": "United States", "currency": "USD"
            },
            "iShares_MSCI_EAFE_ESG_Select": {
                "ticker": "ESGD", "issuer": "BlackRock", "category": "ESG Equity",
                "asset_class": "Equity", "geographic_focus": "International", "currency": "USD"
            },
            "SPDR_SSGA_Gender_Diversity": {
                "ticker": "SHE", "issuer": "State Street", "category": "ESG Equity",
                "asset_class": "Equity", "geographic_focus": "United States", "currency": "USD"
            },
            "iShares_MSCI_KLD_400_Social": {
                "ticker": "DSI", "issuer": "BlackRock", "category": "ESG Equity",
                "asset_class": "Equity", "geographic_focus": "United States", "currency": "USD"
            },
            "iShares_Global_Clean_Energy": {
                "ticker": "ICLN", "issuer": "BlackRock", "category": "Clean Energy",
                "asset_class": "Equity", "geographic_focus": "Global", "currency": "USD"
            },
            "Invesco_WilderHill_Clean_Energy": {
                "ticker": "PBW", "issuer": "Invesco", "category": "Clean Energy",
                "asset_class": "Equity", "geographic_focus": "United States", "currency": "USD"
            },
            "First_Trust_Clean_Edge_Green": {
                "ticker": "QCLN", "issuer": "First Trust", "category": "Clean Energy",
                "asset_class": "Equity", "geographic_focus": "Global", "currency": "USD"
            },
            "Invesco_Water_Resources": {
                "ticker": "PHO", "issuer": "Invesco", "category": "Water/Environmental",
                "asset_class": "Equity", "geographic_focus": "Global", "currency": "USD"
            },
            "Invesco_Global_Water": {
                "ticker": "PIO", "issuer": "Invesco", "category": "Water/Environmental",
                "asset_class": "Equity", "geographic_focus": "Global", "currency": "USD"
            },
            "Vanguard_ESG_US_Stock": {
                "ticker": "ESGV", "issuer": "Vanguard", "category": "ESG Equity",
                "asset_class": "Equity", "geographic_focus": "United States", "currency": "USD"
            }
        }
        
        logger.info("GreenBondsLoader initialized with FactGreenBond table")

    def load_transformed_data(self, csv_file_path: str) -> bool:
        """
        Main method to load transformed green bonds data into database
        """
        try:
            logger.info("="*60)
            logger.info("🌱 STARTING GREEN BONDS DATA LOAD")
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
            logger.info("🎉 GREEN BONDS DATA LOAD COMPLETED SUCCESSFULLY!")
            logger.info(f"🌱 Records loaded: {records_loaded:,}")
            logger.info("="*60)
            return True
            
        except Exception as e:
            logger.error("="*60)
            logger.error("❌ GREEN BONDS DATA LOAD FAILED")
            logger.error(f"Error: {str(e)}")
            logger.error("="*60)
            return False

    def _load_and_validate_csv(self, csv_file_path: str) -> pd.DataFrame:
        """Load CSV and perform basic validation"""
        
        df = pd.read_csv(csv_file_path)
        logger.info(f"📄 Loaded CSV with {len(df):,} rows and {len(df.columns)} columns")
        
        logger.info(f"📅 Date range: {df['Date'].min()} to {df['Date'].max()}")
        logger.info(f"📊 Unique ETFs: {df['ETF_Name'].nunique()}")
        logger.info(f"🏷️  ETF names: {sorted(df['ETF_Name'].unique())}")
        
        # Validate required columns
        required_columns = [
            'Date', 'ETF_Name', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume',
            'Category', 'Asset_Class', 'Geographic_Focus', 'Currency', 'Issuer',
            'Daily_Return', 'Cumulative_Return', 'MA_20', 'MA_50', 'MA_200',
            'Volatility_10D', 'Volatility_30D', 'ESG_Score', 'Sustainability_Theme',
            'Risk_Level', 'Normalized_Value'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Convert Date column to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Clean numeric columns
        numeric_columns = [
            'Open', 'High', 'Low', 'Close', 'Volume', 'Daily_Return', 
            'Cumulative_Return', 'MA_20', 'MA_50', 'MA_200', 'Volatility_10D', 
            'Volatility_30D', 'ESG_Score', 'Normalized_Value', 'RSI_14',
            'Price_Change_1M', 'Price_Change_3M', 'Price_Change_1Y', 'Expense_Ratio'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if col == 'Volume':
                    df[col] = df[col].fillna(0)
                elif col in ['Daily_Return', 'Cumulative_Return']:
                    df[col] = df[col].fillna(0)
                elif col == 'ESG_Score':
                    df[col] = df[col].fillna(50)
                else:
                    df[col] = df[col].fillna(0)
        
        logger.info("✅ CSV validation completed successfully")
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
            
            # 3. Populate Entities (ETF issuers)
            logger.info("   🏢 Populating entities dimension...")
            self._populate_entities(session, df)
            
            # 4. Populate Financial Instruments
            logger.info("   🌱 Populating financial instruments dimension...")
            self._populate_instruments(session, df)
            
            # 5. Populate Date Dimension
            logger.info("   📅 Populating date dimension...")
            self._populate_dates(session, df)
            
            session.commit()
            logger.info("✅ All dimension tables populated successfully")

    def _populate_countries(self, session: Session, df: pd.DataFrame):
        """Populate countries dimension table"""
        
        geographic_focuses = df['Geographic_Focus'].unique()
        
        # Use proper 2-character country codes for database compatibility
        country_mapping = {
            'United States': {'code': 'US', 'region': 'North America'},
            'International': {'code': 'IN', 'region': 'International'},  # Use IN for International
            'Global': {'code': 'GL', 'region': 'Global'},                # Use GL for Global
            'Europe': {'code': 'EU', 'region': 'Europe'},
            'Asia-Pacific': {'code': 'AP', 'region': 'Asia-Pacific'}      # Use AP for Asia-Pacific
        }
        
        for focus in geographic_focuses:
            if focus in country_mapping:
                country_info = country_mapping[focus]
                
                existing = session.query(DimCountry).filter_by(
                    country_code=country_info['code']
                ).first()
                
                if not existing:
                    country = DimCountry(
                        country_code=country_info['code'],
                        country_name=focus,
                        region=country_info['region']
                    )
                    session.add(country)
                    logger.debug(f"   Added country: {focus} ({country_info['code']})")

    def _populate_currencies(self, session: Session, df: pd.DataFrame):
        """Populate currencies dimension table"""
        
        unique_currencies = df['Currency'].unique()
        
        for currency_code in unique_currencies:
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
        """Populate entities dimension table"""
        
        unique_issuers = df['Issuer'].unique()
        
        for issuer in unique_issuers:
            existing = session.query(DimEntity).filter_by(
                entity_name=issuer,
                entity_type='etf_issuer'
            ).first()
            
            if not existing:
                entity = DimEntity(
                    entity_name=issuer,
                    entity_type='etf_issuer',
                    country_code='US',
                    is_active=True
                )
                session.add(entity)
                logger.debug(f"   Added entity: {issuer}")

    def _populate_instruments(self, session: Session, df: pd.DataFrame):
        """Populate financial instruments dimension table"""
        
        unique_instruments = df[['ETF_Name', 'Ticker', 'Currency', 'Issuer', 'Category', 
                                'Asset_Class', 'Geographic_Focus', 'ESG_Score', 'Risk_Level']].drop_duplicates()
        
        for _, row in unique_instruments.iterrows():
            etf_name = row['ETF_Name']
            
            existing = session.query(DimFinancialInstrument).filter_by(
                instrument_code=row['Ticker']
            ).first()
            
            if not existing:
                issuer_entity = session.query(DimEntity).filter_by(
                    entity_name=row['Issuer'],
                    entity_type='etf_issuer'
                ).first()
                
                instrument = DimFinancialInstrument(
                    instrument_type='etf',
                    instrument_name=etf_name.replace('_', ' '),
                    instrument_code=row['Ticker'],
                    display_name=etf_name.replace('_', ' '),
                    primary_currency_code=row['Currency'],
                    issuer_entity_id=issuer_entity.entity_id if issuer_entity else None,
                    asset_class=row['Asset_Class'].lower().replace(' ', '_'),
                    risk_level=row['Risk_Level'].lower(),
                    description=f"{etf_name.replace('_', ' ')} - {row['Category']} ETF",
                    is_active=True
                )
                session.add(instrument)
                logger.debug(f"   Added instrument: {etf_name}")

    def _populate_dates(self, session: Session, df: pd.DataFrame):
        """Populate date dimension table"""
        
        unique_dates = df['Date'].dt.date.unique()
        
        for date_obj in unique_dates:
            date_key = get_date_key(date_obj)
            
            existing = session.query(DimDate).filter_by(date_key=date_key).first()
            if not existing:
                date_record = create_date_dimension_record(date_obj)
                session.add(date_record)
        
        logger.debug(f"   Added {len(unique_dates)} date records")

    def _load_fact_data(self, df: pd.DataFrame) -> int:
        """Load fact table data in batches"""
        
        total_records = 0
        batch_count = 0
        
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
                instrument = session.query(DimFinancialInstrument).filter_by(
                    instrument_code=row['Ticker']
                ).first()
                
                if not instrument:
                    logger.warning(f"Instrument not found for ticker: {row['Ticker']}")
                    continue
                
                date_key = get_date_key(row['Date'].date())
                
                existing = session.query(FactGreenBond).filter(
                    and_(
                        FactGreenBond.instrument_id == instrument.instrument_id,
                        FactGreenBond.record_date == row['Date'].date()
                    )
                ).first()
                
                if existing:
                    self._update_fact_record(existing, row)
                else:
                    fact_record = self._create_fact_record(instrument.instrument_id, date_key, row)
                    session.add(fact_record)
                
                records_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing record for {row.get('Ticker', 'unknown')}: {e}")
                continue
        
        return records_processed

    def _create_fact_record(self, instrument_id: int, date_key: int, row: pd.Series) -> FactGreenBond:
        """Create a new fact record using FactGreenBond table for ETF data"""
        
        return FactGreenBond(
            instrument_id=instrument_id,
            date_key=date_key,
            record_date=row['Date'].date(),
            
            # Bond pricing -> ETF pricing adaptation
            clean_price=float(row['Close']) if pd.notna(row['Close']) else None,
            dirty_price=float(row['Close']) if pd.notna(row['Close']) else None,
            yield_to_maturity=None,
            green_premium=0.0,
            
            # Performance metrics
            total_return_1m=float(row['Price_Change_1M']) if pd.notna(row['Price_Change_1M']) else None,
            total_return_3m=float(row['Price_Change_3M']) if pd.notna(row['Price_Change_3M']) else None,
            total_return_ytd=float(row['Cumulative_Return']) if pd.notna(row['Cumulative_Return']) else None,
            duration=None,
            convexity=None,
            
            # Green/ESG specific metrics
            green_certification=row['Category'] if 'Category' in row else None,
            esg_score=float(row['ESG_Score']) if pd.notna(row['ESG_Score']) else None,
            carbon_avoided_tonnes=None,
            renewable_energy_mwh=None,
            
            # Use of proceeds tracking
            proceeds_allocated_pct=100.0,
            renewable_energy_pct=self._get_renewable_energy_allocation(row),
            energy_efficiency_pct=self._get_energy_efficiency_allocation(row),
            sustainable_transport_pct=self._get_transport_allocation(row),
            water_management_pct=self._get_water_allocation(row),
            
            # Impact reporting
            impact_report_available=True,
            impact_verification='Fund Provider',
            sdg_alignment=self._get_sdg_alignment(row),
            
            # Market data
            outstanding_amount=None,
            trading_volume=float(row['Volume']) if pd.notna(row['Volume']) else None,
            bid_ask_spread=None,
            
            # Credit metrics
            credit_rating=row['Risk_Level'] if 'Risk_Level' in row else None,
            credit_spread=None,
            default_probability=None,
            
            # Metadata
            data_source='yfinance',
            verification_date=row['Date'].date(),
        )

    def _update_fact_record(self, existing_record: FactGreenBond, row: pd.Series):
        """Update an existing fact record"""
        
        existing_record.clean_price = float(row['Close']) if pd.notna(row['Close']) else existing_record.clean_price
        existing_record.dirty_price = float(row['Close']) if pd.notna(row['Close']) else existing_record.dirty_price
        existing_record.total_return_1m = float(row['Price_Change_1M']) if pd.notna(row['Price_Change_1M']) else existing_record.total_return_1m
        existing_record.total_return_3m = float(row['Price_Change_3M']) if pd.notna(row['Price_Change_3M']) else existing_record.total_return_3m
        existing_record.total_return_ytd = float(row['Cumulative_Return']) if pd.notna(row['Cumulative_Return']) else existing_record.total_return_ytd
        existing_record.esg_score = float(row['ESG_Score']) if pd.notna(row['ESG_Score']) else existing_record.esg_score
        existing_record.trading_volume = float(row['Volume']) if pd.notna(row['Volume']) else existing_record.trading_volume
        existing_record.verification_date = row['Date'].date()

    def _get_renewable_energy_allocation(self, row) -> float:
        """Estimate renewable energy allocation based on ETF category"""
        category = row.get('Category', '')
        if 'Clean Energy' in category:
            return 80.0
        elif 'Green' in category:
            return 40.0
        else:
            return 10.0

    def _get_energy_efficiency_allocation(self, row) -> float:
        """Estimate energy efficiency allocation"""
        category = row.get('Category', '')
        if 'Clean Energy' in category:
            return 15.0
        elif 'ESG' in category:
            return 20.0
        else:
            return 5.0

    def _get_transport_allocation(self, row) -> float:
        """Estimate sustainable transport allocation"""
        category = row.get('Category', '')
        if 'Clean Energy' in category:
            return 10.0
        else:
            return 2.0

    def _get_water_allocation(self, row) -> float:
        """Estimate water management allocation"""
        etf_name = row.get('ETF_Name', '')
        if 'Water' in etf_name:
            return 90.0
        elif 'Environmental' in row.get('Category', ''):
            return 15.0
        else:
            return 3.0

    def _get_sdg_alignment(self, row) -> str:
        """Get UN Sustainable Development Goals alignment"""
        category = row.get('Category', '')
        etf_name = row.get('ETF_Name', '')
        
        if 'Clean Energy' in category:
            return 'SDG 7: Affordable and Clean Energy; SDG 13: Climate Action'
        elif 'Water' in etf_name:
            return 'SDG 6: Clean Water and Sanitation; SDG 14: Life Below Water'
        elif 'Green Bond' in category:
            return 'SDG 13: Climate Action; SDG 7: Affordable and Clean Energy'
        elif 'Gender' in etf_name or 'Diversity' in etf_name:
            return 'SDG 5: Gender Equality; SDG 8: Decent Work and Economic Growth'
        elif 'ESG' in category:
            return 'SDG 8: Decent Work; SDG 12: Responsible Consumption; SDG 13: Climate Action'
        else:
            return 'Multiple SDGs'

    def _validate_loaded_data(self):
        """Validate that data was loaded correctly"""
        
        with self.db_manager.get_session() as session:
            counts = {
                'countries': session.query(DimCountry).count(),
                'currencies': session.query(DimCurrency).count(),
                'entities': session.query(DimEntity).count(),
                'instruments': session.query(DimFinancialInstrument).count(),
                'dates': session.query(DimDate).count(),
                'green_bonds': session.query(FactGreenBond).count()
            }
            
            logger.info("📊 Final data counts:")
            for table, count in counts.items():
                logger.info(f"   {table}: {count:,} records")
            
            # Validate date range
            date_range = session.query(
                session.query(FactGreenBond.record_date).order_by(FactGreenBond.record_date.asc()).limit(1).scalar_subquery(),
                session.query(FactGreenBond.record_date).order_by(FactGreenBond.record_date.desc()).limit(1).scalar_subquery()
            ).first()
            
            if date_range and date_range[0] and date_range[1]:
                logger.info(f"📅 Date range: {date_range[0]} to {date_range[1]}")

    def _get_currency_name(self, currency_code: str) -> str:
        """Map currency codes to names"""
        currency_mapping = {
            'USD': 'US Dollar', 'EUR': 'Euro', 'GBP': 'British Pound',
            'JPY': 'Japanese Yen', 'CAD': 'Canadian Dollar'
        }
        return currency_mapping.get(currency_code, f"{currency_code} Currency")

    def _get_currency_country(self, currency_code: str) -> str:
        """Map currency codes to primary countries"""
        currency_country_mapping = {
            'USD': 'US', 'EUR': 'EU', 'GBP': 'GB',
            'JPY': 'JP', 'CAD': 'CA'
        }
        return currency_country_mapping.get(currency_code, 'US')


def load_green_bonds_data(csv_file_path: str = None) -> bool:
    """
    Convenience function to load green bonds data
    """
    if csv_file_path is None:
        csv_file_path = "data/processed_data/transformed_green_bonds.csv"
    
    loader = GreenBondsLoader()
    return loader.load_transformed_data(csv_file_path)


if __name__ == "__main__":
    print("🌱 Testing Green Bonds Loader...")
    
    from src.models.base import db_manager
    
    if db_manager.test_connection():
        print("✅ Database connection successful")
        
        csv_path = "data/processed_data/transformed_green_bonds.csv"
        if load_green_bonds_data(csv_path):
            print("🎉 Green bonds data loaded successfully!")
        else:
            print("❌ Failed to load green bonds data")
    else:
        print("❌ Database connection failed")
        print("Please check your database configuration")