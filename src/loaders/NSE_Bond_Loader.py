#!/usr/bin/env python3
"""
Treasury Securities Database Loader
Loads NSE bond data into the PostgreSQL database using the existing pipeline structure
"""

import os
import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
import pandas as pd
from pathlib import Path
import sys

# Add the src directory to the path to import pipeline modules
sys.path.append(str(Path(__file__).parent.parent / "src"))

from models.base import get_engine, get_session
from models.models import (
    DimCountry, DimCurrency, DimEntity, DimFinancialInstrument, 
    FactTreasurySecurity, DimDate, get_date_key, create_date_dimension_record
)
from nse_bond_processor import NSEBondPDFProcessor, BondData

logger = logging.getLogger(__name__)

class TreasurySecurityLoader:
    """Load treasury securities data into the database"""
    
    def __init__(self):
        self.engine = get_engine()
        self.Session = sessionmaker(bind=self.engine)
        self.pdf_processor = NSEBondPDFProcessor()
        
        # Cache for frequently accessed reference data
        self.entity_cache = {}
        self.instrument_cache = {}
        self.currency_cache = {}
        
    def ensure_reference_data(self, session) -> None:
        """Ensure required reference data exists in database"""
        try:
            # Ensure Kenya country exists
            kenya_country = session.query(DimCountry).filter_by(country_code='KE').first()
            if not kenya_country:
                kenya_country = DimCountry(
                    country_code='KE',
                    country_name='Kenya',
                    region='East Africa'
                )
                session.add(kenya_country)
                session.commit()
                logger.info("Created Kenya country")
            
            # Ensure KES currency exists
            kes_currency = session.query(DimCurrency).filter_by(currency_code='KES').first()
            if not kes_currency:
                kes_currency = DimCurrency(
                    currency_code='KES',
                    currency_name='Kenya Shilling',
                    country_code='KE'
                )
                session.add(kes_currency)
                session.commit()
                logger.info("Created KES currency")
            
            # Ensure Government of Kenya entity exists
            gok_entity = session.query(DimEntity).filter_by(entity_code='GOK').first()
            if not gok_entity:
                gok_entity = DimEntity(
                    entity_name='Government of Kenya',
                    entity_type='government',
                    entity_code='GOK',
                    country_code='KE',
                    regulatory_status='government',
                    is_active=True
                )
                session.add(gok_entity)
                session.commit()
                logger.info("Created Government of Kenya entity")
            
            # Cache the entities
            self.entity_cache['GOK'] = gok_entity.entity_id
            self.currency_cache['KES'] = kes_currency.currency_id
            
        except Exception as e:
            logger.error(f"Error ensuring reference data: {e}")
            session.rollback()
            raise

    def get_or_create_financial_instrument(self, session, bond_data: BondData) -> int:
        """Get or create financial instrument for the bond"""
        try:
            # Check cache first
            cache_key = f"{bond_data.isin}_{bond_data.issue_number}"
            if cache_key in self.instrument_cache:
                return self.instrument_cache[cache_key]
            
            # Check if instrument exists
            instrument = session.query(DimFinancialInstrument).filter_by(
                isin=bond_data.isin
            ).first()
            
            if not instrument:
                # Create new instrument
                instrument = DimFinancialInstrument(
                    instrument_type='treasury_bond',
                    instrument_name=f"Treasury Security {bond_data.issue_number}",
                    instrument_code=bond_data.isin,
                    display_name=f"Kenya Treasury Bond {bond_data.issue_number}",
                    primary_currency_code='KES',
                    issuer_entity_id=self.entity_cache['GOK'],
                    asset_class='fixed_income',
                    risk_level='low',
                    isin=bond_data.isin,
                    description=f"Kenya Government Treasury Security - {bond_data.bond_type}",
                    inception_date=datetime.strptime(bond_data.issue_date, '%Y-%m-%d').date() if bond_data.issue_date != 'Unknown' else None,
                    maturity_date=datetime.strptime(bond_data.maturity_date, '%Y-%m-%d').date() if bond_data.maturity_date != 'Unknown' else None,
                    is_active=True
                )
                session.add(instrument)
                session.flush()  # To get the ID
                logger.info(f"Created financial instrument for {bond_data.isin}")
            
            # Cache the result
            self.instrument_cache[cache_key] = instrument.instrument_id
            return instrument.instrument_id
            
        except Exception as e:
            logger.error(f"Error creating financial instrument for {bond_data.isin}: {e}")
            raise

    def ensure_date_dimension(self, session, date_str: str) -> int:
        """Ensure date exists in date dimension table"""
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
            date_key = get_date_key(date_obj)
            
            # Check if date exists
            date_dim = session.query(DimDate).filter_by(date_key=date_key).first()
            
            if not date_dim:
                # Create new date dimension record using utility function
                date_dim = create_date_dimension_record(date_obj)
                session.add(date_dim)
                session.flush()
                logger.debug(f"Created date dimension for {date_str}")
            
            return date_key
            
        except Exception as e:
            logger.error(f"Error ensuring date dimension for {date_str}: {e}")
            raise

    def convert_date_string(self, date_str: str) -> Optional[str]:
        """Convert date string to standard format"""
        if not date_str or date_str == 'Unknown':
            return None
        
        try:
            # If already in YYYY-MM-DD format
            if len(date_str) == 10 and date_str.count('-') == 2:
                return date_str
            
            # Try different parsing formats
            formats = [
                "%d-%b-%Y",  # 03-JUL-2025
                "%d/%m/%Y",  # 03/07/2025
                "%Y-%m-%d",  # 2025-07-03
            ]
            
            for fmt in formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            
            logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            logger.error(f"Error converting date {date_str}: {e}")
            return None

    def load_bond_data(self, session, bonds: List[BondData]) -> Dict[str, int]:
        """Load bond data into the database"""
        stats = {
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }
        
        try:
            for bond in bonds:
                try:
                    # Get or create financial instrument
                    instrument_id = self.get_or_create_financial_instrument(session, bond)
                    
                    # Ensure date dimension
                    date_key = self.ensure_date_dimension(session, bond.date)
                    
                    # Convert dates
                    issue_date = self.convert_date_string(bond.issue_date)
                    maturity_date = self.convert_date_string(bond.maturity_date)
                    
                    # Check if record already exists
                    existing = session.query(FactTreasurySecurity).filter_by(
                        instrument_id=instrument_id,
                        record_date=datetime.strptime(bond.date, '%Y-%m-%d').date()
                    ).first()
                    
                    if existing:
                        # Update existing record
                        existing.current_price = bond.current_price
                        existing.yield_to_maturity = bond.ytm
                        existing.after_tax_ytm = bond.after_tax_ytm
                        existing.coupon_rate = bond.coupon_rate
                        existing.price_premium = bond.price_premium
                        existing.years_to_maturity = bond.years_to_maturity
                        existing.auction_date = datetime.strptime(issue_date, '%Y-%m-%d').date() if issue_date else None
                        existing.war_rate = bond.coupon_rate  # Using coupon rate as proxy for WAR
                        existing.data_source = 'nse_pdf'
                        
                        stats['updated'] += 1
                        logger.debug(f"Updated bond {bond.isin}")
                    else:
                        # Create new record
                        treasury_data = FactTreasurySecurity(
                            instrument_id=instrument_id,
                            date_key=date_key,
                            record_date=datetime.strptime(bond.date, '%Y-%m-%d').date(),
                            current_price=bond.current_price,
                            yield_to_maturity=bond.ytm,
                            after_tax_ytm=bond.after_tax_ytm,
                            coupon_rate=bond.coupon_rate,
                            price_premium=bond.price_premium,
                            years_to_maturity=bond.years_to_maturity,
                            auction_date=datetime.strptime(issue_date, '%Y-%m-%d').date() if issue_date else None,
                            war_rate=bond.coupon_rate,  # Using coupon rate as proxy for WAR
                            data_source='nse_pdf'
                        )
                        session.add(treasury_data)
                        stats['inserted'] += 1
                        logger.debug(f"Inserted new bond {bond.isin}")
                
                except IntegrityError as e:
                    logger.warning(f"Integrity error for bond {bond.isin}: {e}")
                    session.rollback()
                    stats['errors'] += 1
                    continue
                except Exception as e:
                    logger.error(f"Error processing bond {bond.isin}: {e}")
                    stats['errors'] += 1
                    continue
            
            # Commit all changes
            session.commit()
            logger.info(f"Bond data loading completed. Stats: {stats}")
            
        except Exception as e:
            logger.error(f"Error in load_bond_data: {e}")
            session.rollback()
            raise
        
        return stats

    async def process_and_load_daily_bonds(self, target_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Complete workflow: download, parse, validate, and load bond data
        
        Args:
            target_date: Specific date to process (format: DD-MMM-YYYY)
            
        Returns:
            Dictionary with processing results
        """
        result = {
            'success': False,
            'message': '',
            'processed_bonds': 0,
            'database_stats': {},
            'validation_issues': []
        }
        
        session = self.Session()
        try:
            # Ensure reference data exists
            self.ensure_reference_data(session)
            
            # Process PDF and get bond data
            logger.info("Starting bond data processing...")
            df = await self.pdf_processor.process_latest_bonds(target_date)
            
            if df.empty:
                result['message'] = "No bond data found in PDF"
                return result
            
            # Validate data
            validated_df, issues = self.pdf_processor.validate_bond_data(df)
            result['validation_issues'] = issues
            
            if validated_df.empty:
                result['message'] = "No valid bond data after validation"
                return result
            
            # Convert DataFrame to BondData objects
            bonds = self.dataframe_to_bond_objects(validated_df)
            
            # Load into database
            logger.info(f"Loading {len(bonds)} bonds into database...")
            stats = self.load_bond_data(session, bonds)
            
            result['success'] = True
            result['processed_bonds'] = len(bonds)
            result['database_stats'] = stats
            result['message'] = f"Successfully processed {len(bonds)} bonds"
            
            logger.info(f"Bond processing completed successfully: {result['message']}")
            
        except Exception as e:
            logger.error(f"Error in process_and_load_daily_bonds: {e}")
            result['message'] = f"Error: {str(e)}"
        finally:
            session.close()
        
        return result

    def dataframe_to_bond_objects(self, df: pd.DataFrame) -> List[BondData]:
        """Convert DataFrame to list of BondData objects"""
        bonds = []
        
        for _, row in df.iterrows():
            bond = BondData(
                date=row.get('Date', datetime.now().strftime('%Y-%m-%d')),
                isin=row.get('ISIN', ''),
                issue_number=row.get('IssueNumber', ''),
                issue_date=row.get('IssueDate', ''),
                maturity_date=row.get('MaturityDate', ''),
                tenor=row.get('Tenor', ''),
                coupon_rate=float(row.get('CouponRate', 0)),
                current_price=float(row.get('CurrentPrice', 100)),
                ytm=float(row.get('YTM', 0)),
                bond_type=row.get('BondType', 'Treasury Bond'),
                years_to_maturity=float(row.get('YearsToMaturity', 0)),
                tax_rate=float(row.get('TaxRate', 0.1)),
                after_tax_ytm=float(row.get('AfterTaxYTM', 0)),
                price_premium=float(row.get('PricePremium', 0))
            )
            bonds.append(bond)
        
        return bonds

    def get_latest_bond_data(self, limit: int = 100) -> pd.DataFrame:
        """Get latest bond data from database"""
        session = self.Session()
        try:
            query = text("""
                SELECT 
                    ts.record_date,
                    fi.isin,
                    fi.instrument_code,
                    fi.display_name,
                    ts.current_price,
                    ts.yield_to_maturity,
                    ts.after_tax_ytm,
                    ts.coupon_rate,
                    ts.price_premium,
                    ts.years_to_maturity,
                    ts.auction_date,
                    ts.war_rate,
                    e.entity_name as issuer_name
                FROM fact_treasury_securities ts
                JOIN dim_financial_instruments fi ON ts.instrument_id = fi.instrument_id
                JOIN dim_entities e ON fi.issuer_entity_id = e.entity_id
                ORDER BY ts.record_date DESC, fi.isin
                LIMIT :limit
            """)
            
            result = session.execute(query, {'limit': limit})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            logger.info(f"Retrieved {len(df)} bond records from database")
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving bond data: {e}")
            return pd.DataFrame()
        finally:
            session.close()

    def get_bond_summary_statistics(self) -> Dict[str, Any]:
        """Get summary statistics for bond data"""
        session = self.Session()
        try:
            query = text("""
                SELECT 
                    COUNT(*) as total_bonds,
                    COUNT(DISTINCT fi.isin) as unique_isins,
                    MIN(ts.record_date) as earliest_date,
                    MAX(ts.record_date) as latest_date,
                    AVG(ts.coupon_rate) as avg_coupon_rate,
                    AVG(ts.yield_to_maturity) as avg_ytm,
                    AVG(ts.current_price) as avg_current_price,
                    AVG(ts.years_to_maturity) as avg_years_to_maturity
                FROM fact_treasury_securities ts
                JOIN dim_financial_instruments fi ON ts.instrument_id = fi.instrument_id
                WHERE ts.record_date >= CURRENT_DATE - INTERVAL '30 days'
            """)
            
            result = session.execute(query)
            row = result.fetchone()
            
            if row:
                stats = {
                    'total_bonds': row.total_bonds,
                    'unique_isins': row.unique_isins,
                    'earliest_date': row.earliest_date,
                    'latest_date': row.latest_date,
                    'avg_coupon_rate': float(row.avg_coupon_rate) if row.avg_coupon_rate else 0,
                    'avg_ytm': float(row.avg_ytm) if row.avg_ytm else 0,
                    'avg_current_price': float(row.avg_current_price) if row.avg_current_price else 0,
                    'avg_years_to_maturity': float(row.avg_years_to_maturity) if row.avg_years_to_maturity else 0
                }
                
                logger.info("Retrieved bond summary statistics")
                return stats
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error getting bond summary statistics: {e}")
            return {}
        finally:
            session.close()

    def cleanup_old_data(self, days_to_keep: int = 365) -> int:
        """Clean up old bond data beyond specified days"""
        session = self.Session()
        try:
            cutoff_date = datetime.now().date() - pd.Timedelta(days=days_to_keep)
            
            query = text("""
                DELETE FROM fact_treasury_securities 
                WHERE record_date < :cutoff_date
            """)
            
            result = session.execute(query, {'cutoff_date': cutoff_date})
            deleted_count = result.rowcount
            session.commit()
            
            logger.info(f"Deleted {deleted_count} old bond records before {cutoff_date}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
            session.rollback()
            return 0
        finally:
            session.close()


class TreasurySecurityCollector:
    """Collector class following the pipeline pattern"""
    
    def __init__(self):
        self.loader = TreasurySecurityLoader()
        self.logger = logging.getLogger(self.__class__.__name__)
        
    async def collect_and_store(self, target_date: Optional[str] = None) -> Dict[str, Any]:
        """Collect and store treasury security data"""
        try:
            self.logger.info("Starting treasury security collection")
            
            # Process and load data
            result = await self.loader.process_and_load_daily_bonds(target_date)
            
            if result['success']:
                self.logger.info(f"Treasury collection completed: {result['message']}")
            else:
                self.logger.error(f"Treasury collection failed: {result['message']}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in treasury security collection: {e}")
            return {
                'success': False,
                'message': f"Collection error: {str(e)}",
                'processed_bonds': 0,
                'database_stats': {},
                'validation_issues': []
            }

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of treasury securities data"""
        return self.loader.get_bond_summary_statistics()

    def get_latest_data(self, limit: int = 100) -> pd.DataFrame:
        """Get latest treasury securities data"""
        return self.loader.get_latest_bond_data(limit)


# Standalone script functionality
async def main():
    """Main function for standalone script execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='NSE Bond Data Processor')
    parser.add_argument('--date', type=str, help='Target date (DD-MMM-YYYY format)')
    parser.add_argument('--summary', action='store_true', help='Show data summary')
    parser.add_argument('--latest', type=int, help='Show latest N records')
    parser.add_argument('--cleanup', type=int, help='Cleanup data older than N days')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    collector = TreasurySecurityCollector()
    
    if args.summary:
        # Show summary statistics
        stats = collector.get_summary()
        print("\n=== Treasury Securities Summary ===")
        for key, value in stats.items():
            print(f"{key}: {value}")
    
    elif args.latest:
        # Show latest records
        df = collector.get_latest_data(args.latest)
        print(f"\n=== Latest {args.latest} Treasury Securities ===")
        print(df.to_string(index=False))
    
    elif args.cleanup:
        # Cleanup old data
        loader = TreasurySecurityLoader()
        deleted = loader.cleanup_old_data(args.cleanup)
        print(f"Deleted {deleted} old records")
    
    else:
        # Process and load data
        result = await collector.collect_and_store(args.date)
        
        print(f"\n=== Processing Result ===")
        print(f"Success: {result['success']}")
        print(f"Message: {result['message']}")
        print(f"Processed bonds: {result['processed_bonds']}")
        print(f"Database stats: {result['database_stats']}")
        
        if result['validation_issues']:
            print(f"Validation issues: {result['validation_issues']}")


if __name__ == "__main__":
    asyncio.run(main())