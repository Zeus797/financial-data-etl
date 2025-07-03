"""
src/loaders/market_indexes_loader.py

Production-ready loader for market index data
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
from ..models.facts import FactMarketIndexes
from ..models.base import get_async_session

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketIndexesLoader:
    """
    Production-ready loader for market index data
    
    Features:
    - Global market index tracking
    - Regional and country classification
    - Technical indicator storage
    - Performance benchmarking
    - High-volume data processing
    - Comprehensive audit trail
    """
    
    def __init__(self):
        self.session: Optional[AsyncSession] = None
        self.batch_size = 1000  # Larger batch for index data
        self.stats = {
            'indexes_instruments_created': 0,
            'current_data_inserted': 0,
            'current_data_updated': 0,
            'historical_data_inserted': 0,
            'historical_data_updated': 0,
            'countries_processed': set(),
            'regions_processed': set(),
            'asset_classes_processed': set(),
            'errors': []
        }
        
        logger.info("MarketIndexesLoader initialized")

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

    async def load_all_market_index_data(self, transformed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Load all market index data into database"""
        start_time = datetime.now()
        logger.info("🚀 Starting market index data loading...")
        
        try:
            # Load current market data
            if transformed_data.get('current_data'):
                await self._load_current_index_data(transformed_data['current_data'])
            
            # Load historical data
            if transformed_data.get('historical_data'):
                await self._load_historical_index_data(transformed_data['historical_data'])
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            # Compile results
            load_results = {
                'load_timestamp': end_time.isoformat(),
                'load_duration_seconds': duration.total_seconds(),
                'records_processed': {
                    'indexes_created': self.stats['indexes_instruments_created'],
                    'current_records': self.stats['current_data_inserted'] + self.stats['current_data_updated'],
                    'historical_records': self.stats['historical_data_inserted'] + self.stats['historical_data_updated'],
                    'countries_processed': len(self.stats['countries_processed']),
                    'regions_processed': len(self.stats['regions_processed']),
                    'asset_classes_processed': len(self.stats['asset_classes_processed']),
                    'total_errors': len(self.stats['errors'])
                },
                'data_summary': {
                    'countries': list(self.stats['countries_processed']),
                    'regions': list(self.stats['regions_processed']),
                    'asset_classes': list(self.stats['asset_classes_processed']),
                    'error_details': self.stats['errors'][-10:]  # Last 10 errors
                },
                'loader_version': '1.0.0_market_indexes'
            }
            
            logger.info("✅ Market index data loading completed successfully!")
            return load_results
            
        except Exception as e:
            logger.error(f"❌ Market index loading failed: {e}")
            self.stats['errors'].append(f"Critical error: {str(e)}")
            raise

    async def _load_current_index_data(self, current_data: List[Dict[str, Any]]) -> None:
        """Load current market index data with dimension management"""
        logger.info(f"Loading {len(current_data)} current index records...")
        
        for record in current_data:
            try:
                # Track statistics
                self.stats['countries_processed'].add(record.get('country', 'Unknown'))
                self.stats['regions_processed'].add(record.get('region', 'Unknown'))
                self.stats['asset_classes_processed'].add(record.get('asset_class', 'Unknown'))
                
                # Ensure country exists
                country_code = await self._ensure_country_exists(record)
                
                # Ensure index entity exists (exchange or index provider)
                entity_id = await self._ensure_index_entity_exists(record, country_code)
                
                # Ensure instrument exists
                instrument_id = await self._ensure_index_instrument_exists(record, entity_id)
                
                # Ensure date dimension exists
                record_date = datetime.now().date()
                date_key = await self._ensure_date_exists(record_date)
                
                # Insert/update current index data
                await self._upsert_current_index_data(record, instrument_id, date_key)
                
            except Exception as e:
                error_msg = f"Failed to load current data for {record.get('index_name', 'unknown')}: {e}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

    async def _load_historical_index_data(self, historical_data: Dict[str, List[Dict[str, Any]]]) -> None:
        """Load historical index data in batches"""
        logger.info(f"Loading historical data for {len(historical_data)} indexes...")
        
        for index_key, records in historical_data.items():
            if not records:
                continue
                
            logger.info(f"Processing {len(records)} historical records for {index_key}...")
            
            try:
                # Process in batches for performance
                for i in range(0, len(records), self.batch_size):
                    batch = records[i:i + self.batch_size]
                    await self._process_historical_index_batch(batch, index_key)
                    
                    # Log progress for large datasets
                    if len(records) > self.batch_size:
                        progress = min(i + self.batch_size, len(records))
                        logger.info(f"  Progress: {progress}/{len(records)} records processed")
                
            except Exception as e:
                error_msg = f"Failed to load historical data for {index_key}: {e}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

    async def _process_historical_index_batch(self, batch: List[Dict[str, Any]], index_key: str) -> None:
        """Process a batch of historical index records"""
        
        # Get instrument_id for this index
        symbol = batch[0].get('symbol')
        instrument_id = await self._get_instrument_id_by_symbol(symbol)
        if not instrument_id:
            raise ValueError(f"Instrument not found for symbol: {symbol}")
        
        # Prepare batch data for insertion
        historical_records = []
        
        for record in batch:
            try:
                # Parse record date
                record_date = datetime.fromisoformat(record['record_date']).date()
                date_key = await self._ensure_date_exists(record_date)
                
                # Prepare historical record
                historical_record = {
                    'instrument_id': instrument_id,
                    'date_key': date_key,
                    'record_date': record_date,
                    'timestamp': record.get('timestamp'),
                    'open_value': record.get('open_value'),
                    'high_value': record.get('high_value'),
                    'low_value': record.get('low_value'),
                    'close_value': record.get('close_value'),
                    'volume': record.get('volume'),
                    'daily_return': record.get('daily_return'),
                    'volatility_7d': record.get('volatility_7d'),
                    'volatility_30d': record.get('volatility_30d'),
                    'volatility_90d': record.get('volatility_90d'),
                    'ma_20d': record.get('ma_20d'),
                    'ma_50d': record.get('ma_50d'),
                    'ma_200d': record.get('ma_200d'),
                    'ema_12': record.get('ema_12'),
                    'ema_26': record.get('ema_26'),
                    'macd': record.get('macd'),
                    'macd_signal': record.get('macd_signal'),
                    'rsi': record.get('rsi'),
                    'rsi_signal': record.get('rsi_signal'),
                    'bb_upper': record.get('bb_upper'),
                    'bb_middle': record.get('bb_middle'),
                    'bb_lower': record.get('bb_lower'),
                    'bb_position': record.get('bb_position'),
                    'cumulative_return': record.get('cumulative_return'),
                    'return_7d': record.get('return_7d'),
                    'return_30d': record.get('return_30d'),
                    'return_90d': record.get('return_90d'),
                    'return_252d': record.get('return_252d'),
                    'sharpe_ratio_30d': record.get('sharpe_ratio_30d'),
                    'sharpe_ratio_90d': record.get('sharpe_ratio_90d'),
                    'sharpe_ratio_252d': record.get('sharpe_ratio_252d'),
                    'drawdown': record.get('drawdown'),
                    'max_drawdown': record.get('max_drawdown'),
                    'momentum_5d': record.get('momentum_5d'),
                    'momentum_10d': record.get('momentum_10d'),
                    'momentum_20d': record.get('momentum_20d'),
                    'trend_strength_20d': record.get('trend_strength_20d'),
                    'resistance_level': record.get('resistance_level'),
                    'support_level': record.get('support_level'),
                    'var_95_30d': record.get('var_95_30d'),
                    'var_95_90d': record.get('var_95_90d'),
                    'volatility_cluster': record.get('volatility_cluster'),
                    'data_source': record.get('data_source', 'yahoo_finance'),
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                
                historical_records.append(historical_record)
                
            except Exception as e:
                error_msg = f"Failed to prepare historical index record: {e}"
                logger.warning(error_msg)
                self.stats['errors'].append(error_msg)
        
        # Batch upsert historical records
        if historical_records:
            await self._batch_upsert_historical_index_data(historical_records)

    async def _ensure_country_exists(self, record: Dict[str, Any]) -> str:
        """Ensure country exists in dimension table"""
        country = record.get('country', 'Unknown')
        region = record.get('region', 'other')
        
        # Map country to standard codes
        country_code_map = {
            'US': 'USA', 'GB': 'GBR', 'DE': 'DEU', 'FR': 'FRA', 'JP': 'JPN',
            'CN': 'CHN', 'HK': 'HKG', 'ZA': 'ZAF', 'KE': 'KEN', 'EU': 'EUR',
            'EMERGING': 'EMG'
        }
        country_code = country_code_map.get(country, country[:3].upper())
        
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
            # Map region to continent
            continent_map = {
                'North America': 'North America',
                'Europe': 'Europe',
                'Asia': 'Asia',
                'Africa': 'Africa',
                'Global': 'Global'
            }
            continent = continent_map.get(region, 'Unknown')
            
            new_country = DimCountries(
                country_code=country_code,
                country_name=self._get_full_country_name(country),
                region=region,
                continent=continent,
                latitude=None,  # Could be enhanced with geocoding
                longitude=None,
                currency_code=self._get_default_currency(country_code)
            )
            
            self.session.add(new_country)
            await self.session.flush()
            
            logger.debug(f"Created new country: {country} ({country_code})")
            return country_code
            
        except IntegrityError:
            # Handle race condition
            await self.session.rollback()
            return country_code

    async def _ensure_index_entity_exists(self, record: Dict[str, Any], country_code: str) -> int:
        """Ensure index entity (exchange/provider) exists in dimension table"""
        index_name = record.get('index_name', 'Unknown Index')
        
        # Extract exchange/provider name from index name
        entity_name = self._extract_entity_name(index_name)
        
        # Check if entity exists
        result = await self.session.execute(
            select(DimEntities.entity_id)
            .where(and_(
                DimEntities.entity_name == entity_name,
                DimEntities.entity_type == 'exchange'
            ))
        )
        existing_entity = result.scalar_one_or_none()
        
        if existing_entity:
            return existing_entity
        
        # Create new entity
        try:
            new_entity = DimEntities(
                entity_name=entity_name,
                entity_type='exchange',
                entity_code=entity_name[:10].upper().replace(' ', ''),
                country_code=country_code,
                website_url=None,
                is_active=True,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            self.session.add(new_entity)
            await self.session.flush()
            
            self.stats['indexes_instruments_created'] += 1
            logger.debug(f"Created new index entity: {entity_name}")
            
            return new_entity.entity_id
            
        except IntegrityError:
            # Handle race condition
            await self.session.rollback()
            result = await self.session.execute(
                select(DimEntities.entity_id)
                .where(and_(
                    DimEntities.entity_name == entity_name,
                    DimEntities.entity_type == 'exchange'
                ))
            )
            return result.scalar_one()

    async def _ensure_index_instrument_exists(self, record: Dict[str, Any], entity_id: int) -> int:
        """Ensure index instrument exists in dimension table"""
        symbol = record.get('symbol')
        index_name = record.get('index_name')
        asset_class = record.get('asset_class', 'equity_index')
        
        # Check if instrument exists
        result = await self.session.execute(
            select(DimFinancialInstruments.instrument_id)
            .where(and_(
                DimFinancialInstruments.instrument_code == symbol,
                DimFinancialInstruments.instrument_type == 'market_index'
            ))
        )
        existing_instrument = result.scalar_one_or_none()
        
        if existing_instrument:
            return existing_instrument
        
        # Create new instrument
        try:
            new_instrument = DimFinancialInstruments(
                instrument_type='market_index',
                instrument_name=index_name,
                instrument_code=symbol,
                currency_code=self._get_index_currency(record),
                entity_id=entity_id,
                description=f"{asset_class} - {record.get('region', 'Global')} market index",
                is_active=True,
                created_at=datetime.now()
            )
            
            self.session.add(new_instrument)
            await self.session.flush()
            
            logger.debug(f"Created new index instrument: {symbol}")
            return new_instrument.instrument_id
            
        except IntegrityError:
            # Handle race condition
            await self.session.rollback()
            result = await self.session.execute(
                select(DimFinancialInstruments.instrument_id)
                .where(and_(
                    DimFinancialInstruments.instrument_code == symbol,
                    DimFinancialInstruments.instrument_type == 'market_index'
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

    async def _upsert_current_index_data(self, record: Dict[str, Any], instrument_id: int, date_key: int) -> None:
        """Upsert current market index data"""
        
        # Prepare current index record
        current_data = {
            'instrument_id': instrument_id,
            'date_key': date_key,
            'record_date': datetime.now().date(),
            'timestamp': int(datetime.now().timestamp() * 1000),
            'current_value': record.get('current_value'),
            'daily_change': record.get('daily_change'),
            'daily_change_percent': record.get('daily_change_percent'),
            'volume': record.get('volume'),
            'high_52w': record.get('high_52w'),
            'low_52w': record.get('low_52w'),
            'market_cap': record.get('market_cap'),
            'pe_ratio': record.get('pe_ratio'),
            'data_source': record.get('data_source', 'yahoo_finance'),
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        # Use PostgreSQL ON CONFLICT for upsert
        stmt = insert(FactMarketIndexes).values(current_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=['instrument_id', 'record_date'],
            set_={
                'current_value': stmt.excluded.current_value,
                'daily_change': stmt.excluded.daily_change,
                'daily_change_percent': stmt.excluded.daily_change_percent,
                'volume': stmt.excluded.volume,
                'high_52w': stmt.excluded.high_52w,
                'low_52w': stmt.excluded.low_52w,
                'market_cap': stmt.excluded.market_cap,
                'pe_ratio': stmt.excluded.pe_ratio,
                'updated_at': datetime.now()
            }
        )
        
        await self.session.execute(stmt)
        self.stats['current_data_inserted'] += 1

    async def _batch_upsert_historical_index_data(self, historical_records: List[Dict[str, Any]]) -> None:
        """Batch upsert historical index data"""
        
        if not historical_records:
            return
        
        try:
            # Use bulk insert with ON CONFLICT
            stmt = insert(FactMarketIndexes).values(historical_records)
            stmt = stmt.on_conflict_do_update(
                index_elements=['instrument_id', 'record_date'],
                set_={
                    'close_value': stmt.excluded.close_value,
                    'high_value': stmt.excluded.high_value,
                    'low_value': stmt.excluded.low_value,
                    'volume': stmt.excluded.volume,
                    'daily_return': stmt.excluded.daily_return,
                    'volatility_30d': stmt.excluded.volatility_30d,
                    'ma_50d': stmt.excluded.ma_50d,
                    'ma_200d': stmt.excluded.ma_200d,
                    'rsi': stmt.excluded.rsi,
                    'macd': stmt.excluded.macd,
                    'sharpe_ratio_252d': stmt.excluded.sharpe_ratio_252d,
                    'max_drawdown': stmt.excluded.max_drawdown,
                    'updated_at': datetime.now()
                }
            )
            
            await self.session.execute(stmt)
            self.stats['historical_data_inserted'] += len(historical_records)
            
        except Exception as e:
            logger.error(f"Batch index upsert failed: {e}")
            # Fall back to individual inserts
            for record in historical_records:
                try:
                    stmt = insert(FactMarketIndexes).values(record)
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=['instrument_id', 'record_date']
                    )
                    await self.session.execute(stmt)
                    self.stats['historical_data_inserted'] += 1
                except Exception as individual_error:
                    logger.warning(f"Individual index insert failed: {individual_error}")
                    self.stats['errors'].append(f"Index record insert failed: {individual_error}")

    async def _get_instrument_id_by_symbol(self, symbol: str) -> Optional[int]:
        """Get instrument ID by symbol"""
        result = await self.session.execute(
            select(DimFinancialInstruments.instrument_id)
            .where(and_(
                DimFinancialInstruments.instrument_code == symbol,
                DimFinancialInstruments.instrument_type == 'market_index'
            ))
        )
        return result.scalar_one_or_none()

    def _extract_entity_name(self, index_name: str) -> str:
        """Extract exchange/provider name from index name"""
        # Mapping of index names to exchange/provider names
        entity_mapping = {
            'S&P 500': 'S&P Global',
            'Dow Jones': 'S&P Dow Jones Indices',
            'NASDAQ': 'NASDAQ',
            'FTSE 100': 'FTSE Russell',
            'DAX': 'Deutsche Börse',
            'CAC 40': 'Euronext',
            'EURO STOXX 50': 'STOXX',
            'Nikkei 225': 'Nikkei Inc.',
            'Hang Seng': 'Hang Seng Indexes',
            'Shanghai Composite': 'Shanghai Stock Exchange',
            'JSE Top 40': 'JSE Limited',
            'NSE 20': 'Nairobi Securities Exchange',
            'MSCI Emerging Markets': 'MSCI',
            'VIX': 'CBOE'
        }
        
        # Try exact match first
        for index_key, entity in entity_mapping.items():
            if index_key.lower() in index_name.lower():
                return entity
        
        # Default extraction logic
        if 'S&P' in index_name:
            return 'S&P Global'
        elif 'MSCI' in index_name:
            return 'MSCI'
        elif 'FTSE' in index_name:
            return 'FTSE Russell'
        elif 'Dow Jones' in index_name or 'DJI' in index_name:
            return 'S&P Dow Jones Indices'
        elif 'NASDAQ' in index_name:
            return 'NASDAQ'
        else:
            # Extract first word as exchange name
            return index_name.split()[0] if index_name else 'Unknown Exchange'

    def _get_full_country_name(self, country_code: str) -> str:
        """Get full country name from code"""
        country_names = {
            'US': 'United States',
            'GB': 'United Kingdom',
            'DE': 'Germany',
            'FR': 'France',
            'JP': 'Japan',
            'CN': 'China',
            'HK': 'Hong Kong',
            'ZA': 'South Africa',
            'KE': 'Kenya',
            'EU': 'European Union',
            'EMERGING': 'Emerging Markets'
        }
        return country_names.get(country_code, country_code)

    def _get_default_currency(self, country_code: str) -> str:
        """Get default currency for country"""
        currency_map = {
            'USA': 'USD', 'US': 'USD',
            'GBR': 'GBP', 'GB': 'GBP',
            'DEU': 'EUR', 'DE': 'EUR',
            'FRA': 'EUR', 'FR': 'EUR',
            'JPN': 'JPY', 'JP': 'JPY',
            'CHN': 'CNY', 'CN': 'CNY',
            'HKG': 'HKD', 'HK': 'HKD',
            'ZAF': 'ZAR', 'ZA': 'ZAR',
            'KEN': 'KES', 'KE': 'KES',
            'EUR': 'EUR',
            'EMG': 'USD'  # Emerging markets often priced in USD
        }
        return currency_map.get(country_code, 'USD')

    def _get_index_currency(self, record: Dict[str, Any]) -> str:
        """Determine index currency from record data"""
        country = record.get('country', 'US')
        return self._get_default_currency(country)

# Convenience function for standalone usage
async def load_market_index_data(transformed_data: Dict[str, Any]) -> Dict[str, Any]:
    """Standalone function to load market index data"""
    async with MarketIndexesLoader() as loader:
        return await loader.load_all_market_index_data(transformed_data)

if __name__ == "__main__":
    # Test the loader with sample data
    import asyncio
    
    async def test_loader():
        sample_data = {
            'current_data': [
                {
                    'index_key': 'sp500',
                    'index_name': 'S&P 500',
                    'symbol': '^GSPC',
                    'current_value': 4500.25,
                    'daily_change': 25.50,
                    'daily_change_percent': 0.57,
                    'volume': 3500000000,
                    'high_52w': 4800.00,
                    'low_52w': 3800.00,
                    'market_cap': 45000000000000,
                    'pe_ratio': 22.5,
                    'region': 'North America',
                    'country': 'US',
                    'asset_class': 'equity_index',
                    'data_source': 'yahoo_finance'
                }
            ],
            'historical_data': {
                'sp500': [
                    {
                        'index_key': 'sp500',
                        'index_name': 'S&P 500',
                        'symbol': '^GSPC',
                        'record_date': '2025-06-01',
                        'timestamp': 1717200000000,
                        'open_value': 4475.00,
                        'high_value': 4510.00,
                        'low_value': 4465.00,
                        'close_value': 4500.25,
                        'volume': 3500000000,
                        'daily_return': 0.0057,
                        'volatility_30d': 0.15,
                        'ma_50d': 4450.00,
                        'ma_200d': 4300.00,
                        'rsi': 65.5,
                        'macd': 12.5,
                        'sharpe_ratio_252d': 1.25,
                        'max_drawdown': -0.08,
                        'data_source': 'yahoo_finance'
                    }
                ]
            }
        }
        
        try:
            async with MarketIndexesLoader() as loader:
                result = await loader.load_all_market_index_data(sample_data)
                print("✅ Market Indexes Loader Test Results:")
                print(f"   Load duration: {result['load_duration_seconds']:.2f} seconds")
                print(f"   Indexes created: {result['records_processed']['indexes_created']}")
                print(f"   Current records: {result['records_processed']['current_records']}")
                print(f"   Historical records: {result['records_processed']['historical_records']}")
                print(f"   Countries: {result['data_summary']['countries']}")
                print(f"   Regions: {result['data_summary']['regions']}")
                print(f"   Asset classes: {result['data_summary']['asset_classes']}")
                print(f"   Errors: {result['records_processed']['total_errors']}")
                
        except Exception as e:
            print(f"❌ Loader test failed: {e}")
    
    asyncio.run(test_loader())