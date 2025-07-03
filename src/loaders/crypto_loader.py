"""
src/loaders/crypto_loader.py

ENHANCED FINAL cryptocurrency data loader 
- Better date handling for historical data
- Improved error handling for missing dates
- Added missing methods for test compatibility
"""

import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, date, timedelta
from decimal import Decimal

from sqlalchemy import text  # Import text for raw SQL queries 
from src.models.base import get_session, db_manager
from src.models.universal_models import (
    DimFinancialInstrument, DimDate, FactCryptocurrencyPrice,
    get_date_key, create_date_dimension_record
)

class CryptoLoader:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info("CryptoLoader initialized with universal schema support")
        
        # Cache for performance
        self._instrument_mappings = None
        self._date_cache = {}
        
        # Statistics tracking
        self._loading_stats = {
            'total_records_processed': 0,
            'total_records_inserted': 0,
            'total_records_updated': 0,
            'total_errors': 0,
            'last_run_timestamp': None
        }
        
    def get_instrument_mappings(self) -> Dict[str, int]:
        """Get mapping of coingecko_id to instrument_id for all crypto instruments"""
        if self._instrument_mappings is None:
            with get_session() as session:
                instruments = session.query(DimFinancialInstrument).filter(
                    DimFinancialInstrument.instrument_type.in_(['cryptocurrency', 'stablecoin']),
                    DimFinancialInstrument.coingecko_id.isnot(None),
                    DimFinancialInstrument.is_active == True
                ).all()
                
                self._instrument_mappings = {
                    instrument.coingecko_id: instrument.instrument_id
                    for instrument in instruments
                }
                
                self.logger.info(f"🔗 Found {len(self._instrument_mappings)} instrument mappings in database")
        
        return self._instrument_mappings
    
    def ensure_date_dimension(self, date_value: date) -> int:
        """Ensure date exists in dimension table and return date_key"""
        date_key = get_date_key(date_value)
        
        if date_key not in self._date_cache:
            with get_session() as session:
                existing = session.query(DimDate).filter_by(date_key=date_key).first()
                
                if not existing:
                    date_record = create_date_dimension_record(date_value)
                    session.add(date_record)
                    session.commit()
                    self.logger.info(f"Added new date record: {date_value}")
                
                self._date_cache[date_key] = True
        
        return date_key
    
    def load_market_data(self, market_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Load current market data (prices, market cap, volume)"""
        self.logger.info(f"Loading {len(market_data)} market data records...")
        
        mappings = self.get_instrument_mappings()
        results = {'inserted': 0, 'updated': 0, 'errors': 0, 'total_processed': 0}
        
        # Batch processing for performance
        batch_size = 100
        with get_session() as session:
            
            # Ensure today's date exists in dimension
            today = date.today()
            date_key = self.ensure_date_dimension(today)
            self.logger.info(f"Added {1 if date_key else 0} new date records to dimension")
            
            for i in range(0, len(market_data), batch_size):
                batch = market_data[i:i + batch_size]
                batch_results = self._process_market_batch(session, batch, mappings, date_key)
                
                # Update results
                for key in results:
                    results[key] += batch_results[key]
                
                self.logger.info(f"Processed batch {i//batch_size + 1}: "
                               f"{batch_results['inserted']} inserted, "
                               f"{batch_results['updated']} updated, "
                               f"{batch_results['errors']} errors")
        
        # Update statistics
        self._loading_stats['total_records_processed'] += results['total_processed']
        self._loading_stats['total_records_inserted'] += results['inserted']
        self._loading_stats['total_records_updated'] += results['updated']
        self._loading_stats['total_errors'] += results['errors']
        self._loading_stats['last_run_timestamp'] = datetime.utcnow()
        
        self.logger.info(f"Market data loading completed: {results}")
        return results
    
    def _process_market_batch(self, session, batch: List[Dict], mappings: Dict[str, int], 
                            date_key: int) -> Dict[str, int]:
        """Process a batch of market data records"""
        results = {'inserted': 0, 'updated': 0, 'errors': 0, 'total_processed': 0}
        
        for record in batch:
            results['total_processed'] += 1
            
            try:
                coingecko_id = record.get('coingecko_id')
                instrument_id = mappings.get(coingecko_id)
                
                if not instrument_id:
                    self.logger.warning(f"Instrument not found for {coingecko_id}")
                    results['errors'] += 1
                    continue
                
                # Check if record already exists (upsert logic)
                existing = session.query(FactCryptocurrencyPrice).filter_by(
                    instrument_id=instrument_id,
                    date_key=date_key
                ).first()
                
                # Build price data dict - only include fields that exist in the database model
                price_data = {
                    'instrument_id': instrument_id,
                    'date_key': date_key,
                    'record_date': date.today(),
                    'price': self._safe_decimal(record.get('price')),
                    'market_cap': self._safe_decimal(record.get('market_cap')),
                    'total_volume': self._safe_decimal(record.get('total_volume')),
                    'price_change_24h': self._safe_decimal(record.get('price_change_24h')),
                    'price_change_percentage_24h': self._safe_decimal(record.get('price_change_percentage_24h')),
                    'market_cap_change_24h': self._safe_decimal(record.get('market_cap_change_24h')),
                    'market_cap_change_percentage_24h': self._safe_decimal(record.get('market_cap_change_percentage_24h')),
                    'circulating_supply': self._safe_decimal(record.get('circulating_supply')),
                    'total_supply': self._safe_decimal(record.get('total_supply')),
                    'max_supply': self._safe_decimal(record.get('max_supply')),
                    'ath': self._safe_decimal(record.get('ath')),
                    'ath_change_percentage': self._safe_decimal(record.get('ath_change_percentage')),
                    'ath_date': self._safe_date(record.get('ath_date')),
                    'atl': self._safe_decimal(record.get('atl')),
                    'atl_change_percentage': self._safe_decimal(record.get('atl_change_percentage')),
                    'atl_date': self._safe_date(record.get('atl_date')),
                    'data_source': record.get('data_source', 'unknown')
                }
                
                # Handle stablecoin-specific fields
                if record.get('is_stablecoin'):
                    price_data.update({
                        'deviation_from_dollar': self._safe_decimal(record.get('deviation_from_dollar')),
                        'within_01_percent': record.get('within_01_percent'),
                        'within_05_percent': record.get('within_05_percent'),
                        'within_1_percent': record.get('within_1_percent'),
                        'price_band': record.get('price_band')
                    })
                
                if existing:
                    # Update existing record
                    for key, value in price_data.items():
                        if key not in ['instrument_id', 'date_key']:  # Don't update keys
                            setattr(existing, key, value)
                    existing.updated_at = datetime.utcnow()
                    results['updated'] += 1
                else:
                    # Insert new record
                    price_record = FactCryptocurrencyPrice(**price_data)
                    session.add(price_record)
                    results['inserted'] += 1
                
                session.commit()
                
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error processing record for {record.get('coingecko_id')}: {e}")
                results['errors'] += 1
        
        return results
    
    def load_historical_data(self, historical_data: Union[List[Dict[str, Any]], Dict[str, Any]]) -> Dict[str, int]:
        """Load historical cryptocurrency data - handles both list and dict input"""
        
        # Handle different input formats from transformer
        if isinstance(historical_data, dict):
            # If it's a dict, extract the actual historical records
            if 'historical_data' in historical_data:
                actual_data = historical_data['historical_data']
            elif 'data' in historical_data:
                actual_data = historical_data['data']
            else:
                # Assume the dict values are the data records
                actual_data = []
                for records in historical_data.values():
                    if isinstance(records, list):
                        actual_data.extend(records)
        else:
            actual_data = historical_data
        
        # Ensure we have a list
        if not isinstance(actual_data, list):
            self.logger.warning(f"Expected list of historical records, got {type(actual_data)}")
            return {'inserted': 0, 'updated': 0, 'errors': 0, 'total_processed': 0}
        
        # Filter out records with missing dates BEFORE processing
        valid_data = []
        for record in actual_data:
            if not isinstance(record, dict):
                continue
                
            record_date = record.get('date')
            if record_date is None:
                # Try alternative date fields
                record_date = record.get('timestamp') or record.get('datetime')
                
            if record_date is None:
                self.logger.warning(f"Skipping record with missing date: {record.get('coingecko_id', 'unknown')}")
                continue
            
            # If we have a date, add it to the record (normalized)
            if isinstance(record_date, str):
                try:
                    record['date'] = datetime.strptime(record_date, '%Y-%m-%d').date()
                except ValueError:
                    # Try other date formats
                    for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%m/%d/%Y']:
                        try:
                            record['date'] = datetime.strptime(record_date, fmt).date()
                            break
                        except ValueError:
                            continue
                    else:
                        self.logger.warning(f"Could not parse date {record_date} for {record.get('coingecko_id')}")
                        continue
            elif isinstance(record_date, datetime):
                record['date'] = record_date.date()
            elif isinstance(record_date, date):
                record['date'] = record_date
            else:
                self.logger.warning(f"Invalid date type {type(record_date)} for {record.get('coingecko_id')}")
                continue
                
            valid_data.append(record)
        
        self.logger.info(f"Loading historical data for {len(set(r.get('coingecko_id', 'unknown') for r in valid_data))} cryptocurrencies...")
        self.logger.info(f"Filtered {len(actual_data) - len(valid_data)} records with invalid dates")
        
        mappings = self.get_instrument_mappings()
        results = {'inserted': 0, 'updated': 0, 'errors': 0, 'total_processed': 0}
        
        # Group by cryptocurrency for batch processing
        by_crypto = {}
        for record in valid_data:
            crypto_id = record.get('coingecko_id')
            if crypto_id not in by_crypto:
                by_crypto[crypto_id] = []
            by_crypto[crypto_id].append(record)
        
        # Process each cryptocurrency's historical data
        for crypto_id, crypto_records in by_crypto.items():
            self.logger.info(f"Loading {len(crypto_records)} historical records for {crypto_id}")
            
            # Ensure dates exist in dimension
            unique_dates = set(record.get('date') for record in crypto_records if record.get('date'))
            date_keys_added = 0
            for date_value in unique_dates:
                self.ensure_date_dimension(date_value)
                date_keys_added += 1
            
            if date_keys_added > 0:
                self.logger.info(f"Added {date_keys_added} new date records to dimension")
            
            crypto_results = self._process_historical_crypto(crypto_records, mappings)
            
            # Update results
            for key in results:
                results[key] += crypto_results[key]
        
        # Update statistics
        self._loading_stats['total_records_processed'] += results['total_processed']
        self._loading_stats['total_records_inserted'] += results['inserted']
        self._loading_stats['total_records_updated'] += results['updated']
        self._loading_stats['total_errors'] += results['errors']
        
        self.logger.info(f"Historical data loading completed: {results}")
        return results
    
    def _process_historical_crypto(self, crypto_records: List[Dict], mappings: Dict[str, int]) -> Dict[str, int]:
        """Process historical data for a single cryptocurrency"""
        results = {'inserted': 0, 'updated': 0, 'errors': 0, 'total_processed': 0}
        
        if not crypto_records:
            return results
        
        coingecko_id = crypto_records[0].get('coingecko_id')
        instrument_id = mappings.get(coingecko_id)
        
        if not instrument_id:
            self.logger.warning(f"Instrument not found for {coingecko_id}")
            results['errors'] += len(crypto_records)
            return results
        
        with get_session() as session:
            for record in crypto_records:
                results['total_processed'] += 1
                
                try:
                    # Use the normalized date from our preprocessing
                    record_date = record.get('date')
                    if not record_date:
                        self.logger.warning(f"Missing date in preprocessed record for {coingecko_id}")
                        results['errors'] += 1
                        continue
                    
                    date_key = get_date_key(record_date)
                    
                    # Check if record already exists
                    existing = session.query(FactCryptocurrencyPrice).filter_by(
                        instrument_id=instrument_id,
                        date_key=date_key
                    ).first()
                    
                    # Build price data dict - only include fields that exist in the database model
                    price_data = {
                        'instrument_id': instrument_id,
                        'date_key': date_key,
                        'record_date': record_date,
                        'price': self._safe_decimal(record.get('price')),
                        'total_volume': self._safe_decimal(record.get('volume')),
                        'high': self._safe_decimal(record.get('high')),
                        'low': self._safe_decimal(record.get('low')),
                        'open': self._safe_decimal(record.get('open')),
                        'close': self._safe_decimal(record.get('close')),
                        'daily_return': self._safe_decimal(record.get('daily_return')),
                        'rolling_avg_7d': self._safe_decimal(record.get('rolling_avg_7d')),
                        'rolling_avg_30d': self._safe_decimal(record.get('rolling_avg_30d')),
                        'rolling_std_7d': self._safe_decimal(record.get('rolling_std_7d')),
                        'rolling_std_30d': self._safe_decimal(record.get('rolling_std_30d')),
                        'volatility': self._safe_decimal(record.get('volatility')),
                        'data_source': record.get('data_source', 'unknown')
                    }
                    
                    if existing:
                        # Update existing record
                        for key, value in price_data.items():
                            if key not in ['instrument_id', 'date_key']:
                                setattr(existing, key, value)
                        existing.updated_at = datetime.utcnow()
                        results['updated'] += 1
                    else:
                        # Insert new record
                        price_record = FactCryptocurrencyPrice(**price_data)
                        session.add(price_record)
                        results['inserted'] += 1
                    
                    session.commit()
                    
                except Exception as e:
                    session.rollback()
                    self.logger.error(f"Error processing historical record for {coingecko_id} on {record_date}: {e}")
                    results['errors'] += 1
        
        return results
    
    def get_loading_statistics(self) -> Dict[str, Any]:
        """Get loading statistics for test compatibility"""
        return self._loading_stats.copy()
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """Validate cryptocurrency data integrity using SQLAlchemy 2.x compatible queries"""
        self.logger.info("Performing cryptocurrency data integrity validation...")
        
        with get_session() as session:
            try:
                # Check for duplicates using text() wrapper for raw SQL
                duplicate_count = session.execute(text("""
                    SELECT COUNT(*) FROM (
                        SELECT instrument_id, date_key, COUNT(*) as cnt
                        FROM fact_cryptocurrency_prices
                        GROUP BY instrument_id, date_key
                        HAVING COUNT(*) > 1
                    ) duplicates
                """)).scalar()
                
                # Check for missing prices using text() wrapper
                missing_price_count = session.execute(text("""
                    SELECT COUNT(*)
                    FROM fact_cryptocurrency_prices
                    WHERE price IS NULL OR price <= 0
                """)).scalar()
                
                # Check data freshness using text() wrapper
                latest_date = session.execute(text("""
                    SELECT MAX(record_date)
                    FROM fact_cryptocurrency_prices
                """)).scalar()
                
                # Check total records using text() wrapper
                total_records = session.execute(text("""
                    SELECT COUNT(*)
                    FROM fact_cryptocurrency_prices
                """)).scalar()
                
                # Check instruments with data using text() wrapper
                instruments_with_data = session.execute(text("""
                    SELECT COUNT(DISTINCT instrument_id)
                    FROM fact_cryptocurrency_prices
                """)).scalar()
                
                # Check stablecoin deviations using text() wrapper
                high_deviation_count = session.execute(text("""
                    SELECT COUNT(*)
                    FROM fact_cryptocurrency_prices fcp
                    JOIN dim_financial_instruments dfi ON fcp.instrument_id = dfi.instrument_id
                    WHERE dfi.is_stablecoin = true
                    AND fcp.deviation_from_dollar > 0.05
                """)).scalar()
                
                validation_report = {
                    'total_records': total_records,
                    'instruments_with_data': instruments_with_data,
                    'duplicate_records': duplicate_count,
                    'missing_prices': missing_price_count,
                    'latest_data_date': latest_date,
                    'high_stablecoin_deviations': high_deviation_count,
                    'validation_timestamp': datetime.utcnow()
                }
                
                # Calculate data quality score
                issues = duplicate_count + missing_price_count + high_deviation_count
                validation_report['data_quality_score'] = max(0, 100 - (issues * 5))  # Deduct 5 points per issue
                
                # Determine validation status
                if issues == 0:
                    validation_report['status'] = 'EXCELLENT'
                elif issues <= 5:
                    validation_report['status'] = 'GOOD'
                elif issues <= 20:
                    validation_report['status'] = 'NEEDS_ATTENTION'
                else:
                    validation_report['status'] = 'CRITICAL'
                
                self.logger.info(f"Data validation completed: {validation_report['status']} "
                               f"(Score: {validation_report['data_quality_score']}/100)")
                
                return validation_report
                
            except Exception as e:
                self.logger.error(f"Data validation failed: {e}")
                return {
                    'status': 'ERROR',
                    'error': str(e),
                    'validation_timestamp': datetime.utcnow()
                }
    
    def get_latest_prices(self, coingecko_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get latest prices for specified cryptocurrencies"""
        with get_session() as session:
            # Build query using text() wrapper for complex SQL
            if coingecko_ids:
                placeholders = ','.join([f':coin_{i}' for i in range(len(coingecko_ids))])
                params = {f'coin_{i}': coin_id for i, coin_id in enumerate(coingecko_ids)}
                
                query = text(f"""
                    SELECT 
                        dfi.coingecko_id,
                        dfi.display_name,
                        dfi.instrument_code,
                        fcp.price,
                        fcp.market_cap,
                        fcp.total_volume,
                        fcp.price_change_percentage_24h,
                        fcp.record_date,
                        fcp.data_source
                    FROM fact_cryptocurrency_prices fcp
                    JOIN dim_financial_instruments dfi ON fcp.instrument_id = dfi.instrument_id
                    WHERE dfi.coingecko_id IN ({placeholders})
                    AND fcp.record_date = (
                        SELECT MAX(record_date)
                        FROM fact_cryptocurrency_prices fcp2
                        WHERE fcp2.instrument_id = fcp.instrument_id
                    )
                    ORDER BY dfi.display_name
                """)
                
                result = session.execute(query, params)
            else:
                query = text("""
                    SELECT 
                        dfi.coingecko_id,
                        dfi.display_name,
                        dfi.instrument_code,
                        fcp.price,
                        fcp.market_cap,
                        fcp.total_volume,
                        fcp.price_change_percentage_24h,
                        fcp.record_date,
                        fcp.data_source
                    FROM fact_cryptocurrency_prices fcp
                    JOIN dim_financial_instruments dfi ON fcp.instrument_id = dfi.instrument_id
                    WHERE fcp.record_date = (
                        SELECT MAX(record_date)
                        FROM fact_cryptocurrency_prices fcp2
                        WHERE fcp2.instrument_id = fcp.instrument_id
                    )
                    ORDER BY dfi.display_name
                """)
                
                result = session.execute(query)
            
            return [dict(row._mapping) for row in result]
    
    def get_price_history(self, coingecko_id: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get price history for a specific cryptocurrency"""
        with get_session() as session:
            query = text("""
                SELECT 
                    fcp.record_date,
                    fcp.price,
                    fcp.total_volume,
                    fcp.high,
                    fcp.low,
                    fcp.open,
                    fcp.close,
                    fcp.daily_return,
                    fcp.rolling_avg_7d,
                    fcp.rolling_avg_30d,
                    fcp.volatility
                FROM fact_cryptocurrency_prices fcp
                JOIN dim_financial_instruments dfi ON fcp.instrument_id = dfi.instrument_id
                WHERE dfi.coingecko_id = :coingecko_id
                AND fcp.record_date >= CURRENT_DATE - INTERVAL ':days days'
                ORDER BY fcp.record_date DESC
            """)
            
            result = session.execute(query, {'coingecko_id': coingecko_id, 'days': days})
            return [dict(row._mapping) for row in result]
    
    def _safe_decimal(self, value: Any) -> Optional[Decimal]:
        """Safely convert value to Decimal"""
        if value is None:
            return None
        try:
            if isinstance(value, (int, float)):
                return Decimal(str(value))
            elif isinstance(value, str):
                return Decimal(value) if value.strip() else None
            elif isinstance(value, Decimal):
                return value
            else:
                return None
        except (ValueError, TypeError):
            return None
    
    def _safe_date(self, value: Any) -> Optional[date]:
        """Safely convert value to date"""
        if value is None:
            return None
        try:
            if isinstance(value, date):
                return value
            elif isinstance(value, datetime):
                return value.date()
            elif isinstance(value, str):
                # Try common date formats
                for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f']:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
                return None
            else:
                return None
        except (ValueError, TypeError):
            return None
    
    def cleanup_old_data(self, days_to_keep: int = 365) -> int:
        """Clean up old cryptocurrency data beyond specified days"""
        with get_session() as session:
            cutoff_date = date.today() - timedelta(days=days_to_keep)
            
            deleted_count = session.execute(text("""
                DELETE FROM fact_cryptocurrency_prices
                WHERE record_date < :cutoff_date
            """), {'cutoff_date': cutoff_date}).rowcount
            
            session.commit()
            self.logger.info(f"Cleaned up {deleted_count} old records before {cutoff_date}")
            
            return deleted_count
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics for cryptocurrency data"""
        with get_session() as session:
            stats = session.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT instrument_id) as unique_instruments,
                    MIN(record_date) as earliest_date,
                    MAX(record_date) as latest_date
                FROM fact_cryptocurrency_prices
            """)).first()
            
            return dict(stats._mapping) if stats else {}