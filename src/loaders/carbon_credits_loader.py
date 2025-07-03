"""
src/loaders/carbon_credits_loader.py

Production-ready loader for carbon credits and carbon offset data
Handles database insertion with conflict resolution and validation
"""

import asyncio
import logging
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func, and_, or_
from sqlalchemy.exc import IntegrityError, DataError
import pandas as pd

# Import your database models (adjust imports based on your structure)
from ..models.dimensions import DimEntities, DimFinancialInstruments, DimCurrencies, DimCountries, DimDates
from ..models.facts import FactCarbonOffsets, FactCarbonMarketData
from ..models.base import get_async_session

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CarbonCreditsLoader:
    """
    Production-ready loader for carbon credits and offset data
    
    Features:
    - Carbon project and credit tracking
    - Market price data management
    - Quality assessment storage
    - Geographic and project type classification
    - Comprehensive audit trail
    - Batch processing for large datasets
    """
    
    def __init__(self):
        self.session: Optional[AsyncSession] = None
        self.batch_size = 500  # Smaller batch for carbon data
        self.stats = {
            'projects_inserted': 0,
            'projects_updated': 0,
            'credits_inserted': 0,
            'credits_updated': 0,
            'market_data_inserted': 0,
            'market_data_updated': 0,
            'countries_processed': set(),
            'project_types_processed': set(),
            'errors': []
        }
        
        logger.info("CarbonCreditsLoader initialized")

    async def __aenter__(self):
        """Async context manager entry"""
        self.session = await get_async_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            if exc_type:
                await self.session.rollback()
                logger.error(f"Transaction rolled back due to error: {exc_val}")
            else:
                await self.session.commit()
                logger.info("Transaction committed successfully")
            await self.session.close()

    async def load_all_carbon_data(self, transformed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Load all carbon credits data into database"""
        start_time = datetime.now()
        logger.info("🚀 Starting carbon credits data loading...")
        
        try:
            # Load carbon credits data
            if transformed_data.get('transformed_credits_data'):
                await self._load_carbon_credits_data(transformed_data['transformed_credits_data'])
            
            # Load market data
            if transformed_data.get('transformed_market_data'):
                await self._load_carbon_market_data(transformed_data['transformed_market_data'])
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            # Compile results
            load_results = {
                'load_timestamp': end_time.isoformat(),
                'load_duration_seconds': duration.total_seconds(),
                'records_processed': {
                    'projects': self.stats['projects_inserted'] + self.stats['projects_updated'],
                    'credits': self.stats['credits_inserted'] + self.stats['credits_updated'],
                    'market_data': self.stats['market_data_inserted'] + self.stats['market_data_updated'],
                    'countries_processed': len(self.stats['countries_processed']),
                    'project_types_processed': len(self.stats['project_types_processed']),
                    'total_errors': len(self.stats['errors'])
                },
                'data_summary': {
                    'countries': list(self.stats['countries_processed']),
                    'project_types': list(self.stats['project_types_processed']),
                    'error_details': self.stats['errors'][-10:]  # Last 10 errors
                },
                'loader_version': '1.0.0_carbon_credits'
            }
            
            logger.info("✅ Carbon credits data loading completed successfully!")
            return load_results
            
        except Exception as e:
            logger.error(f"❌ Carbon credits loading failed: {e}")
            self.stats['errors'].append(f"Critical error: {str(e)}")
            raise

    async def _load_carbon_credits_data(self, credits_data: List[Dict[str, Any]]) -> None:
        """Load carbon credits/projects data with dimension management"""
        logger.info(f"Loading {len(credits_data)} carbon credits records...")
        
        # Process in batches
        for i in range(0, len(credits_data), self.batch_size):
            batch = credits_data[i:i + self.batch_size]
            await self._process_carbon_credits_batch(batch)
            
            # Log progress for large datasets
            if len(credits_data) > self.batch_size:
                progress = min(i + self.batch_size, len(credits_data))
                logger.info(f"  Progress: {progress}/{len(credits_data)} records processed")

    async def _process_carbon_credits_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process a batch of carbon credits records"""
        
        carbon_records = []
        
        for record in batch:
            try:
                # Track statistics
                self.stats['countries_processed'].add(record.get('country_standardized', 'Unknown'))
                self.stats['project_types_processed'].add(record.get('project_type_standardized', 'Unknown'))
                
                # Ensure country exists
                country_code = await self._ensure_country_exists(record)
                
                # Ensure project entity exists
                entity_id = await self._ensure_project_entity_exists(record, country_code)
                
                # Ensure date dimension exists
                record_date = datetime.now().date()
                if record.get('record_date'):
                    record_date = datetime.fromisoformat(record['record_date']).date()
                date_key = await self._ensure_date_exists(record_date)
                
                # Prepare carbon offset record
                carbon_record = {
                    'project_entity_id': entity_id,
                    'date_key': date_key,
                    'record_date': record_date,
                    'project_name': record.get('project_name'),
                    'project_type': record.get('project_type_standardized'),
                    'standard_type': record.get('standard_type'),
                    'methodology': record.get('methodology'),
                    'country_code': country_code,
                    'region': record.get('region'),
                    'vintage_year': record.get('vintage_year'),
                    'price_per_tonne': record.get('price_per_tonne'),
                    'volume_tonnes': record.get('volume_tonnes'),
                    'total_value': record.get('price_per_tonne', 0) * record.get('volume_tonnes', 0) if record.get('price_per_tonne') and record.get('volume_tonnes') else None,
                    'quality_score': record.get('quality_score'),
                    'quality_tier': record.get('quality_tier'),
                    'vintage_premium': record.get('vintage_premium'),
                    'vintage_category': record.get('vintage_category'),
                    'market_type': record.get('market_type'),
                    'price_category': record.get('price_category'),
                    'additionality_assessment': record.get('additionality_assessment'),
                    'permanence_rating': record.get('permanence_rating'),
                    'co_benefits_level': record.get('co_benefits_level'),
                    'country_risk': record.get('country_risk'),
                    'permanence_risk': record.get('permanence_risk'),
                    'vintage_risk': record.get('vintage_risk'),
                    'overall_risk': record.get('overall_risk'),
                    'co2_equivalent_tonnes': record.get('co2_equivalent_tonnes'),
                    'estimated_trees_protected': record.get('estimated_trees_protected'),
                    'biodiversity_impact': record.get('biodiversity_impact'),
                    'air_quality_impact': record.get('air_quality_impact'),
                    'community_development': record.get('community_development'),
                    'poverty_alleviation': record.get('poverty_alleviation'),
                    'job_creation': record.get('job_creation'),
                    'data_completeness_score': record.get('data_completeness_score'),
                    'verification_date': record.get('verification_date'),
                    'registry': record.get('registry'),
                    'data_source': record.get('data_source', 'manual_entry'),
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                
                carbon_records.append(carbon_record)
                
            except Exception as e:
                error_msg = f"Failed to prepare carbon record for {record.get('project_name', 'unknown')}: {e}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
        
        # Batch upsert carbon records
        if carbon_records:
            await self._batch_upsert_carbon_data(carbon_records)

    async def _load_carbon_market_data(self, market_data: List[Dict[str, Any]]) -> None:
        """Load carbon market price and trend data"""
        logger.info(f"Loading {len(market_data)} carbon market records...")
        
        market_records = []
        
        for record in market_data:
            try:
                # Parse record date
                record_date = datetime.fromisoformat(record['date']).date() if record.get('date') else datetime.now().date()
                date_key = await self._ensure_date_exists(record_date)
                
                # Prepare market record
                market_record = {
                    'date_key': date_key,
                    'record_date': record_date,
                    'market_type': record.get('market_type', 'voluntary'),
                    'average_price_per_tonne': record.get('price_per_tonne'),
                    'volume_traded_tonnes': record.get('volume_traded'),
                    'number_of_transactions': record.get('number_of_transactions'),
                    'price_ma_7d': record.get('price_ma_7d'),
                    'price_ma_30d': record.get('price_ma_30d'),
                    'price_ma_90d': record.get('price_ma_90d'),
                    'daily_return': record.get('daily_return'),
                    'weekly_return': record.get('weekly_return'),
                    'monthly_return': record.get('monthly_return'),
                    'volatility_30d': record.get('volatility_30d'),
                    'volatility_90d': record.get('volatility_90d'),
                    'price_range_7d': record.get('price_range_7d'),
                    'price_range_30d': record.get('price_range_30d'),
                    'momentum_14d': record.get('momentum_14d'),
                    'trend_7d': record.get('trend_7d'),
                    'trend_30d': record.get('trend_30d'),
                    'market_pressure': record.get('market_pressure'),
                    'data_source': record.get('data_source', 'market_aggregator'),
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                
                market_records.append(market_record)
                
            except Exception as e:
                error_msg = f"Failed to prepare market record for {record.get('date', 'unknown')}: {e}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
        
        # Batch upsert market records
        if market_records:
            await self._batch_upsert_market_data(market_records)

    async def _ensure_country_exists(self, record: Dict[str, Any]) -> str:
        """Ensure country exists in dimension table"""
        country_name = record.get('country_standardized', 'Unknown')
        region = record.get('region', 'other')
        region_code = record.get('region_code', 'OTH')
        
        # Generate country code (simplified - use first 3 chars or ISO mapping)
        country_code = country_name[:3].upper() if country_name != 'Unknown' else 'UNK'
        
        # Check if country exists
        result = await self.session.execute(
            select(DimCountries.country_code)
            .where(DimCountries.country_code == country_code)
        )
        existing_country = result.scalar_one_or_none()
        
        if existing_country:
            return existing_country
        
        # Create new country
        try:
            new_country = DimCountries(
                country_code=country_code,
                country_name=country_name,
                region=region,
                continent=self._get_continent_from_region(region),
                latitude=None,  # Could be enhanced with geocoding
                longitude=None,
                currency_code='USD'  # Default for carbon markets
            )
            
            self.session.add(new_country)
            await self.session.flush()
            
            logger.debug(f"Created new country: {country_name} ({country_code})")
            return country_code
            
        except IntegrityError:
            # Handle race condition
            await self.session.rollback()
            return country_code

    async def _ensure_project_entity_exists(self, record: Dict[str, Any], country_code: str) -> int:
        """Ensure project entity exists in dimension table"""
        project_name = record.get('project_name', 'Unknown Project')
        project_type = record.get('project_type_standardized', 'Other')
        
        # Create entity name combining project name and type
        entity_name = f"{project_name[:50]} ({project_type})"  # Truncate for DB limits
        
        # Check if entity exists
        result = await self.session.execute(
            select(DimEntities.entity_id)
            .where(and_(
                DimEntities.entity_name == entity_name,
                DimEntities.entity_type == 'carbon_project'
            ))
        )
        existing_entity = result.scalar_one_or_none()
        
        if existing_entity:
            return existing_entity
        
        # Create new entity
        try:
            new_entity = DimEntities(
                entity_name=entity_name,
                entity_type='carbon_project',
                entity_code=project_name[:20].upper().replace(' ', '_'),  # Create code
                country_code=country_code,
                website_url=record.get('project_website'),
                is_active=True,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            self.session.add(new_entity)
            await self.session.flush()
            
            logger.debug(f"Created new carbon project entity: {entity_name}")
            return new_entity.entity_id
            
        except IntegrityError:
            # Handle race condition
            await self.session.rollback()
            result = await self.session.execute(
                select(DimEntities.entity_id)
                .where(and_(
                    DimEntities.entity_name == entity_name,
                    DimEntities.entity_type == 'carbon_project'
                ))
            )
            return result.scalar_one()

    async def _ensure_date_exists(self, record_date: date) -> int:
        """Ensure date exists in date dimension (shared utility)"""
        date_key = int(record_date.strftime('%Y%m%d'))
        
        # Check if date exists
        result = await self.session.execute(
            select(DimDates.date_key)
            .where(DimDates.date_key == date_key)
        )
        existing_date = result.scalar_one_or_none()
        
        if existing_date:
            return existing_date
        
        # Create new date record
        try:
            new_date = DimDates(
                date_key=date_key,
                full_date=record_date,
                year=record_date.year,
                quarter=(record_date.month - 1) // 3 + 1,
                month=record_date.month,
                month_name=record_date.strftime('%B'),
                week=record_date.isocalendar()[1],
                day_of_year=record_date.timetuple().tm_yday,
                day_of_month=record_date.day,
                day_of_week=record_date.weekday() + 1,
                day_name=record_date.strftime('%A'),
                is_weekend=record_date.weekday() >= 5,
                is_holiday=False,
                fiscal_year=record_date.year,
                fiscal_quarter=(record_date.month - 1) // 3 + 1
            )
            
            self.session.add(new_date)
            await self.session.flush()
            
            return date_key
            
        except IntegrityError:
            # Handle race condition
            await self.session.rollback()
            return date_key

    async def _batch_upsert_carbon_data(self, carbon_records: List[Dict[str, Any]]) -> None:
        """Batch upsert carbon offset data"""
        
        if not carbon_records:
            return
        
        try:
            # Use bulk insert with ON CONFLICT
            stmt = insert(FactCarbonOffsets).values(carbon_records)
            stmt = stmt.on_conflict_do_update(
                index_elements=['project_entity_id', 'record_date', 'vintage_year'],
                set_={
                    'price_per_tonne': stmt.excluded.price_per_tonne,
                    'volume_tonnes': stmt.excluded.volume_tonnes,
                    'total_value': stmt.excluded.total_value,
                    'quality_score': stmt.excluded.quality_score,
                    'quality_tier': stmt.excluded.quality_tier,
                    'market_type': stmt.excluded.market_type,
                    'price_category': stmt.excluded.price_category,
                    'data_completeness_score': stmt.excluded.data_completeness_score,
                    'updated_at': datetime.now()
                }
            )
            
            await self.session.execute(stmt)
            self.stats['credits_inserted'] += len(carbon_records)
            
        except Exception as e:
            logger.error(f"Batch carbon upsert failed: {e}")
            # Fall back to individual inserts
            for record in carbon_records:
                try:
                    stmt = insert(FactCarbonOffsets).values(record)
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=['project_entity_id', 'record_date', 'vintage_year']
                    )
                    await self.session.execute(stmt)
                    self.stats['credits_inserted'] += 1
                except Exception as individual_error:
                    logger.warning(f"Individual carbon insert failed: {individual_error}")
                    self.stats['errors'].append(f"Carbon record insert failed: {individual_error}")

    async def _batch_upsert_market_data(self, market_records: List[Dict[str, Any]]) -> None:
        """Batch upsert carbon market data"""
        
        if not market_records:
            return
        
        try:
            # Use bulk insert with ON CONFLICT
            stmt = insert(FactCarbonMarketData).values(market_records)
            stmt = stmt.on_conflict_do_update(
                index_elements=['date_key', 'market_type'],
                set_={
                    'average_price_per_tonne': stmt.excluded.average_price_per_tonne,
                    'volume_traded_tonnes': stmt.excluded.volume_traded_tonnes,
                    'number_of_transactions': stmt.excluded.number_of_transactions,
                    'daily_return': stmt.excluded.daily_return,
                    'volatility_30d': stmt.excluded.volatility_30d,
                    'price_ma_30d': stmt.excluded.price_ma_30d,
                    'trend_30d': stmt.excluded.trend_30d,
                    'market_pressure': stmt.excluded.market_pressure,
                    'updated_at': datetime.now()
                }
            )
            
            await self.session.execute(stmt)
            self.stats['market_data_inserted'] += len(market_records)
            
        except Exception as e:
            logger.error(f"Batch market data upsert failed: {e}")
            # Fall back to individual inserts
            for record in market_records:
                try:
                    stmt = insert(FactCarbonMarketData).values(record)
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=['date_key', 'market_type']
                    )
                    await self.session.execute(stmt)
                    self.stats['market_data_inserted'] += 1
                except Exception as individual_error:
                    logger.warning(f"Individual market data insert failed: {individual_error}")
                    self.stats['errors'].append(f"Market data insert failed: {individual_error}")

    def _get_continent_from_region(self, region: str) -> str:
        """Map region to continent"""
        region_continent_map = {
            'africa': 'Africa',
            'asia_pacific': 'Asia',
            'latin_america': 'South America',
            'north_america': 'North America',
            'europe': 'Europe',
            'other': 'Unknown'
        }
        return region_continent_map.get(region, 'Unknown')

# Convenience function for standalone usage
async def load_carbon_credits_data(transformed_data: Dict[str, Any]) -> Dict[str, Any]:
    """Standalone function to load carbon credits data"""
    async with CarbonCreditsLoader() as loader:
        return await loader.load_all_carbon_data(transformed_data)

if __name__ == "__main__":
    # Test the loader with sample data
    import asyncio
    
    async def test_loader():
        sample_data = {
            'transformed_credits_data': [
                {
                    'project_name': 'Kenya Forest Conservation Project',
                    'price_per_tonne': 12.50,
                    'standard_type': 'VCS',
                    'project_type_standardized': 'Forestry and Land Use',
                    'country_standardized': 'Kenya',
                    'region': 'africa',
                    'region_code': 'AFR',
                    'vintage_year': 2023,
                    'volume_tonnes': 10000,
                    'quality_score': 0.89,
                    'quality_tier': 'high',
                    'market_type': 'voluntary',
                    'additionality_assessment': 'proven',
                    'permanence_rating': 'medium',
                    'co_benefits_level': 'multiple',
                    'country_risk': 'medium',
                    'overall_risk': 'medium',
                    'data_completeness_score': 0.85,
                    'data_source': 'vcs_registry'
                }
            ],
            'transformed_market_data': [
                {
                    'date': '2025-06-01',
                    'price_per_tonne': 12.50,
                    'volume_traded': 1000,
                    'market_type': 'voluntary',
                    'daily_return': 0.02,
                    'volatility_30d': 0.15,
                    'trend_30d': 'bullish',
                    'data_source': 'market_aggregator'
                }
            ]
        }
        
        try:
            async with CarbonCreditsLoader() as loader:
                result = await loader.load_all_carbon_data(sample_data)
                print("✅ Carbon Credits Loader Test Results:")
                print(f"   Load duration: {result['load_duration_seconds']:.2f} seconds")
                print(f"   Projects processed: {result['records_processed']['projects']}")
                print(f"   Credits processed: {result['records_processed']['credits']}")
                print(f"   Market data processed: {result['records_processed']['market_data']}")
                print(f"   Countries: {result['data_summary']['countries']}")
                print(f"   Project types: {result['data_summary']['project_types']}")
                print(f"   Errors: {result['records_processed']['total_errors']}")
                
        except Exception as e:
            print(f"❌ Loader test failed: {e}")
    
    asyncio.run(test_loader())