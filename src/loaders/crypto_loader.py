# src/loaders/crypto_loader.py
"""
Cryptocurrency database loader - Pure database operations
Separation of concerns: Only responsible for loading data into database
"""
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date, timedelta
import logging

# Import models and database utilities
from src.models.base import get_session, Base, engine
from src.models.universal_models import (
    DimFinancialInstrument, DimEntity, DimCurrency, DimDate,
    FactCryptocurrencyPrice, get_date_key, create_date_dimension_record
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CryptoLoader:
    """
    Pure cryptocurrency database loader
    
    Responsibilities:
    - Load transformed data into universal database schema
    - Handle upsert operations (insert new, update existing)
    - Manage dimension table relationships
    - Ensure data integrity and constraints
    
    NOT responsible for:
    - Data collection or transformation
    - Business logic or calculations
    - Data validation (assumes data is pre-validated)
    """
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        logger.info("CryptoLoader initialized with universal schema support")
    
    def load_market_data(self, market_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Load market data into fact table using universal dimensions
        
        Args:
            market_data: List of transformed market records
            
        Returns:
            Dictionary with loading statistics
        """
        logger.info(f"Loading {len(market_data)} market data records...")
        
        if not market_data:
            return {'inserted': 0, 'updated': 0, 'errors': 0, 'total_processed': 0}
        
        # Ensure required dates exist in date dimension
        dates_needed = {record['record_date'] for record in market_data if record.get('record_date')}
        self._ensure_dates_exist(dates_needed)
        
        # Process in batches for better performance
        total_inserted = 0
        total_updated = 0
        total_errors = 0
        
        for i in range(0, len(market_data), self.batch_size):
            batch = market_data[i:i + self.batch_size]
            inserted, updated, errors = self._load_market_batch(batch)
            
            total_inserted += inserted
            total_updated += updated
            total_errors += errors
            
            logger.info(f"Processed batch {i//self.batch_size + 1}: {inserted} inserted, {updated} updated, {errors} errors")
        
        result = {
            'inserted': total_inserted,
            'updated': total_updated,
            'errors': total_errors,
            'total_processed': len(market_data)
        }
        
        logger.info(f"Market data loading completed: {result}")
        return result
    
    def load_historical_data(self, historical_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
        """
        Load historical data for multiple cryptocurrencies
        
        Args:
            historical_data: Dictionary mapping coingecko_id to historical records
            
        Returns:
            Dictionary with loading statistics
        """
        logger.info(f"Loading historical data for {len(historical_data)} cryptocurrencies...")
        
        total_stats = {'inserted': 0, 'updated': 0, 'errors': 0, 'total_processed': 0}
        
        for coingecko_id, records in historical_data.items():
            if not records:
                continue
            
            logger.info(f"Loading {len(records)} historical records for {coingecko_id}")
            
            # Ensure dates exist for this coin's historical data
            dates_needed = {record['record_date'] for record in records if record.get('record_date')}
            self._ensure_dates_exist(dates_needed)
            
            # Load historical records
            stats = self._load_historical_records(coingecko_id, records)
            
            # Accumulate statistics
            for key in total_stats:
                total_stats[key] += stats.get(key, 0)
        
        logger.info(f"Historical data loading completed: {total_stats}")
        return total_stats
    
    def _ensure_dates_exist(self, dates_needed: set):
        """Ensure all required dates exist in universal date dimension"""
        if not dates_needed:
            return
        
        dates_added = 0
        
        with get_session() as session:
            for date_obj in dates_needed:
                if isinstance(date_obj, str):
                    try:
                        date_obj = datetime.fromisoformat(date_obj).date()
                    except:
                        continue
                
                if not isinstance(date_obj, date):
                    continue
                
                date_key = get_date_key(date_obj)
                
                # Check if date exists
                existing = session.query(DimDate).filter_by(date_key=date_key).first()
                if not existing:
                    # Create new date record
                    date_record = create_date_dimension_record(date_obj)
                    session.add(date_record)
                    dates_added += 1
            
            if dates_added > 0:
                session.commit()
                logger.info(f"Added {dates_added} new date records to dimension")
    
    def _load_market_batch(self, batch: List[Dict[str, Any]]) -> Tuple[int, int, int]:
        """Load a batch of market records with upsert logic"""
        inserted = 0
        updated = 0
        errors = 0
        
        with get_session() as session:
            for record in batch:
                try:
                    # Get instrument_id from coingecko_id using universal dimension
                    instrument = session.query(DimFinancialInstrument).filter_by(
                        coingecko_id=record['coingecko_id']
                    ).first()
                    
                    if not instrument:
                        logger.warning(f"Instrument not found for {record['coingecko_id']}")
                        errors += 1
                        continue
                    
                    # Prepare price record data
                    price_record_data = self._prepare_price_record(record, instrument.instrument_id)
                    
                    # Check if record exists (upsert logic)
                    existing = session.query(FactCryptocurrencyPrice).filter_by(
                        instrument_id=instrument.instrument_id,
                        record_date=price_record_data['record_date']
                    ).first()
                    
                    if existing:
                        # Update existing record
                        self._update_price_record(existing, price_record_data)
                        updated += 1
                    else:
                        # Insert new record
                        new_price = FactCryptocurrencyPrice(**price_record_data)
                        session.add(new_price)
                        inserted += 1
                
                except Exception as e:
                    logger.warning(f"Failed to load market record for {record.get('coingecko_id')}: {e}")
                    errors += 1
                    continue
            
            session.commit()
        
        return inserted, updated, errors
    
    def _load_historical_records(self, coingecko_id: str, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """Load historical records for a specific cryptocurrency"""
        inserted = 0
        updated = 0
        errors = 0
        
        with get_session() as session:
            # Get instrument
            instrument = session.query(DimFinancialInstrument).filter_by(
                coingecko_id=coingecko_id
            ).first()
            
            if not instrument:
                logger.warning(f"Instrument not found for {coingecko_id}")
                return {'inserted': 0, 'updated': 0, 'errors': len(records), 'total_processed': len(records)}
            
            # Process records in batches
            for i in range(0, len(records), self.batch_size):
                batch = records[i:i + self.batch_size]
                
                for record in batch:
                    try:
                        # Prepare historical price record
                        price_record_data = self._prepare_price_record(record, instrument.instrument_id)
                        
                        # Check if record exists
                        existing = session.query(FactCryptocurrencyPrice).filter_by(
                            instrument_id=instrument.instrument_id,
                            record_date=price_record_data['record_date']
                        ).first()
                        
                        if existing:
                            # Update existing record with historical data (merge approach)
                            self._merge_historical_data(existing, price_record_data)
                            updated += 1
                        else:
                            # Insert new historical record
                            new_price = FactCryptocurrencyPrice(**price_record_data)
                            session.add(new_price)
                            inserted += 1
                    
                    except Exception as e:
                        logger.warning(f"Failed to load historical record for {coingecko_id}: {e}")
                        errors += 1
                        continue
                
                # Commit batch
                session.commit()
        
        return {
            'inserted': inserted,
            'updated': updated,
            'errors': errors,
            'total_processed': len(records)
        }
    
    def _prepare_price_record(self, record: Dict[str, Any], instrument_id: int) -> Dict[str, Any]:
        """Prepare price record data for database insertion"""
        
        # Parse record date
        record_date = record.get('record_date')
        if isinstance(record_date, str):
            record_date = datetime.fromisoformat(record_date).date()
        
        date_key = get_date_key(record_date) if record_date else None
        
        # Prepare complete price record
        price_record_data = {
            'instrument_id': instrument_id,
            'date_key': date_key,
            'record_date': record_date,
            
            # Core price data
            'price': record.get('price'),
            'market_cap': record.get('market_cap'),
            'total_volume': record.get('total_volume'),
            'circulating_supply': record.get('circulating_supply'),
            
            # Price ranges
            'high_24h': record.get('high_24h'),
            'low_24h': record.get('low_24h'),
            
            # Price changes
            'price_change_24h': record.get('price_change_24h'),
            'price_change_percentage_24h': record.get('price_change_percentage_24h'),
            'price_change_percentage_7d': record.get('price_change_percentage_7d'),
            'price_change_percentage_30d': record.get('price_change_percentage_30d'),
            
            # Market position
            'market_cap_rank': record.get('market_cap_rank'),
            'market_cap_change_24h': record.get('market_cap_change_24h'),
            'market_cap_change_percentage_24h': record.get('market_cap_change_percentage_24h'),
            
            # Supply metrics
            'total_supply': record.get('total_supply'),
            'max_supply': record.get('max_supply'),
            'fully_diluted_valuation': record.get('fully_diluted_valuation'),
            
            # All-time metrics
            'ath': record.get('ath'),
            'ath_change_percentage': record.get('ath_change_percentage'),
            'ath_date': self._parse_date(record.get('ath_date')),
            'atl': record.get('atl'),
            'atl_change_percentage': record.get('atl_change_percentage'),
            'atl_date': self._parse_date(record.get('atl_date')),
            
            # Stablecoin specific fields
            'deviation_from_dollar': record.get('deviation_from_dollar'),
            'within_01_percent': record.get('within_01_percent'),
            'within_05_percent': record.get('within_05_percent'),
            'within_1_percent': record.get('within_1_percent'),
            'price_band': record.get('price_band'),
            
            # Technical indicators
            'rolling_avg_7d': record.get('rolling_avg_7d'),
            'rolling_avg_30d': record.get('rolling_avg_30d'),
            'rolling_std_7d': record.get('rolling_std_7d'),
            'rolling_std_30d': record.get('rolling_std_30d'),
            'volatility_7d': record.get('volatility_7d'),
            'volatility_30d': record.get('volatility_30d'),
            
            # Additional technical indicators
            'bollinger_upper_20d': record.get('bollinger_upper_20d'),
            'bollinger_lower_20d': record.get('bollinger_lower_20d'),
            'rsi_14': record.get('rsi_14'),
            
            # Metadata
            'data_source': record.get('data_source', 'coingecko'),
            'last_updated': self._parse_datetime(record.get('last_updated')),
            'created_at': datetime.now()
        }
        
        return price_record_data
    
    def _update_price_record(self, existing_record: FactCryptocurrencyPrice, new_data: Dict[str, Any]):
        """Update existing price record with new data"""
        
        # Update all fields except primary key and created_at
        excluded_fields = {'id', 'created_at'}
        
        for key, value in new_data.items():
            if key not in excluded_fields and hasattr(existing_record, key):
                setattr(existing_record, key, value)
    
    def _merge_historical_data(self, existing_record: FactCryptocurrencyPrice, historical_data: Dict[str, Any]):
        """
        Merge historical data with existing record
        Only update fields that are None in existing record or explicitly historical
        """
        
        # Fields that should always be updated from historical data
        historical_priority_fields = {
            'rolling_avg_7d', 'rolling_avg_30d', 'rolling_std_7d', 'rolling_std_30d',
            'volatility_7d', 'volatility_30d', 'bollinger_upper_20d', 'bollinger_lower_20d',
            'rsi_14', 'price_change_percentage_7d', 'price_change_percentage_30d'
        }
        
        # Fields that should only be updated if current value is None
        fill_if_empty_fields = {
            'price', 'market_cap', 'total_volume', 'price_change_24h',
            'price_change_percentage_24h'
        }
        
        for key, value in historical_data.items():
            if not hasattr(existing_record, key) or key in {'id', 'created_at'}:
                continue
            
            current_value = getattr(existing_record, key)
            
            # Always update historical priority fields
            if key in historical_priority_fields and value is not None:
                setattr(existing_record, key, value)
            
            # Update fill-if-empty fields only if current value is None
            elif key in fill_if_empty_fields and current_value is None and value is not None:
                setattr(existing_record, key, value)
    
    def _parse_date(self, date_value: Any) -> Optional[date]:
        """Parse date value to date object"""
        if date_value is None:
            return None
        
        try:
            if isinstance(date_value, str):
                dt = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                return dt.date()
            elif isinstance(date_value, datetime):
                return date_value.date()
            elif isinstance(date_value, date):
                return date_value
        except (ValueError, TypeError):
            pass
        
        return None
    
    def _parse_datetime(self, datetime_value: Any) -> Optional[datetime]:
        """Parse datetime value"""
        if datetime_value is None:
            return None
        
        try:
            if isinstance(datetime_value, str):
                return datetime.fromisoformat(datetime_value.replace('Z', '+00:00'))
            elif isinstance(datetime_value, datetime):
                return datetime_value
        except (ValueError, TypeError):
            pass
        
        return None
    
    def get_instrument_mappings(self) -> Dict[str, int]:
        """
        Get mapping of coingecko_id to instrument_id for validation
        
        Returns:
            Dictionary mapping coingecko_id to instrument_id
        """
        mappings = {}
        
        with get_session() as session:
            instruments = session.query(DimFinancialInstrument).filter(
                DimFinancialInstrument.coingecko_id.isnot(None),
                DimFinancialInstrument.is_active == True
            ).all()
            
            for instrument in instruments:
                mappings[instrument.coingecko_id] = instrument.instrument_id
        
        return mappings
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """
        Perform data integrity checks on loaded cryptocurrency data
        
        Returns:
            Validation report with integrity status
        """
        logger.info("Performing cryptocurrency data integrity validation...")
        
        validation_report = {
            'timestamp': datetime.now(),
            'checks': {},
            'issues': [],
            'summary': {}
        }
        
        with get_session() as session:
            # Check for orphaned records (prices without instruments)
            orphaned_prices = session.query(FactCryptocurrencyPrice).filter(
                ~FactCryptocurrencyPrice.instrument_id.in_(
                    session.query(DimFinancialInstrument.instrument_id)
                )
            ).count()
            
            validation_report['checks']['orphaned_price_records'] = orphaned_prices
            if orphaned_prices > 0:
                validation_report['issues'].append(f"Found {orphaned_prices} orphaned price records")
            
            # Check for missing dates in date dimension
            price_date_keys = session.query(FactCryptocurrencyPrice.date_key).distinct().all()
            missing_dates = []
            
            for (date_key,) in price_date_keys:
                if date_key and not session.query(DimDate).filter(DimDate.date_key == date_key).first():
                    missing_dates.append(date_key)
            
            validation_report['checks']['missing_date_records'] = len(missing_dates)
            if missing_dates:
                validation_report['issues'].append(f"Found {len(missing_dates)} missing date records")
            
            # Check for duplicate price records
            duplicate_count = session.execute("""
                SELECT COUNT(*) FROM (
                    SELECT instrument_id, record_date, COUNT(*) as cnt
                    FROM fact_cryptocurrency_prices
                    GROUP BY instrument_id, record_date
                    HAVING COUNT(*) > 1
                ) duplicates
            """).scalar()
            
            validation_report['checks']['duplicate_price_records'] = duplicate_count
            if duplicate_count > 0:
                validation_report['issues'].append(f"Found {duplicate_count} duplicate price records")
            
            # Check data freshness
            latest_price_date = session.query(FactCryptocurrencyPrice.record_date).order_by(
                desc(FactCryptocurrencyPrice.record_date)
            ).limit(1).scalar()
            
            if latest_price_date:
                days_old = (date.today() - latest_price_date).days
                validation_report['checks']['data_freshness_days'] = days_old
                if days_old > 2:
                    validation_report['issues'].append(f"Data is {days_old} days old")
            
            # Summary statistics
            total_instruments = session.query(DimFinancialInstrument).filter(
                DimFinancialInstrument.is_active == True,
                DimFinancialInstrument.coingecko_id.isnot(None)
            ).count()
            
            total_price_records = session.query(FactCryptocurrencyPrice).count()
            
            instruments_with_data = session.query(FactCryptocurrencyPrice.instrument_id).distinct().count()
            
            # Latest data coverage
            latest_coverage = 0
            if latest_price_date:
                latest_coverage = session.query(FactCryptocurrencyPrice.instrument_id).filter(
                    FactCryptocurrencyPrice.record_date == latest_price_date
                ).distinct().count()
            
            validation_report['summary'] = {
                'total_active_instruments': total_instruments,
                'total_price_records': total_price_records,
                'instruments_with_data': instruments_with_data,
                'latest_price_date': latest_price_date.isoformat() if latest_price_date else None,
                'latest_data_coverage': latest_coverage,
                'data_coverage_percentage': (latest_coverage / total_instruments * 100) if total_instruments > 0 else 0,
                'avg_records_per_instrument': total_price_records / instruments_with_data if instruments_with_data > 0 else 0
            }
        
        logger.info(f"Data integrity validation completed: {len(validation_report['issues'])} issues found")
        return validation_report
    
    def cleanup_old_data(self, days_to_keep: int = 365) -> int:
        """
        Clean up old cryptocurrency price data beyond retention period
        
        Args:
            days_to_keep: Number of days to retain
            
        Returns:
            Number of records deleted
        """
        cutoff_date = date.today() - timedelta(days=days_to_keep)
        
        with get_session() as session:
            deleted_count = session.query(FactCryptocurrencyPrice).filter(
                FactCryptocurrencyPrice.record_date < cutoff_date
            ).delete()
            
            session.commit()
        
        logger.info(f"Cleaned up {deleted_count} old cryptocurrency price records before {cutoff_date}")
        return deleted_count
    
    def get_loading_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive loading statistics for monitoring
        
        Returns:
            Dictionary with loading statistics
        """
        with get_session() as session:
            # Basic counts
            total_instruments = session.query(DimFinancialInstrument).filter(
                DimFinancialInstrument.coingecko_id.isnot(None),
                DimFinancialInstrument.is_active == True
            ).count()
            
            total_price_records = session.query(FactCryptocurrencyPrice).count()
            
            # Date range
            date_range = session.query(
                session.query(FactCryptocurrencyPrice.record_date).order_by(
                    FactCryptocurrencyPrice.record_date
                ).limit(1).scalar_subquery().label('earliest'),
                session.query(FactCryptocurrencyPrice.record_date).order_by(
                    desc(FactCryptocurrencyPrice.record_date)
                ).limit(1).scalar_subquery().label('latest')
            ).first()
            
            # Instrument breakdown
            crypto_count = session.query(DimFinancialInstrument).filter(
                DimFinancialInstrument.instrument_type == 'cryptocurrency',
                DimFinancialInstrument.is_active == True
            ).count()
            
            stablecoin_count = session.query(DimFinancialInstrument).filter(
                DimFinancialInstrument.instrument_type == 'stablecoin',
                DimFinancialInstrument.is_active == True
            ).count()
            
            # Recent activity
            recent_records = session.query(FactCryptocurrencyPrice).filter(
                FactCryptocurrencyPrice.record_date >= date.today() - timedelta(days=7)
            ).count()
            
            return {
                'total_instruments': total_instruments,
                'cryptocurrency_count': crypto_count,
                'stablecoin_count': stablecoin_count,
                'total_price_records': total_price_records,
                'earliest_date': date_range.earliest.isoformat() if date_range.earliest else None,
                'latest_date': date_range.latest.isoformat() if date_range.latest else None,
                'recent_records_7d': recent_records,
                'avg_records_per_instrument': total_price_records / total_instruments if total_instruments > 0 else 0,
                'statistics_timestamp': datetime.now().isoformat()
            }

# Convenience function for complete data loading
def load_crypto_data(transformed_data: Dict[str, Any], batch_size: int = 1000) -> Dict[str, Any]:
    """
    Load complete cryptocurrency dataset (market + historical)
    
    Args:
        transformed_data: Transformed data from CryptoTransformer
        batch_size: Batch size for database operations
        
    Returns:
        Loading results and statistics
    """
    loader = CryptoLoader(batch_size=batch_size)
    
    # Load market data
    market_stats = loader.load_market_data(transformed_data.get('market_data', []))
    
    # Load historical data
    historical_stats = loader.load_historical_data(transformed_data.get('historical_data', {}))
    
    # Validate data integrity
    validation_report = loader.validate_data_integrity()
    
    # Get loading statistics
    loading_statistics = loader.get_loading_statistics()
    
    return {
        'market_data_stats': market_stats,
        'historical_data_stats': historical_stats,
        'validation_report': validation_report,
        'loading_statistics': loading_statistics,
        'loading_timestamp': datetime.now().isoformat()
    }

# Example usage and testing
if __name__ == "__main__":
    # Test with sample transformed data
    sample_market_data = [
        {
            'coingecko_id': 'bitcoin',
            'symbol': 'BTC',
            'name': 'Bitcoin',
            'record_date': date.today(),
            'price': 45000.50,
            'market_cap': 850000000000,
            'total_volume': 25000000000,
            'market_cap_rank': 1,
            'price_change_percentage_24h': 2.5,
            'data_source': 'test'
        }
    ]
    
    sample_historical_data = {
        'bitcoin': [
            {
                'coingecko_id': 'bitcoin',
                'record_date': date.today() - timedelta(days=1),
                'price': 44000.0,
                'market_cap': 840000000000,
                'rolling_avg_7d': 44500.0,
                'volatility_7d': 0.02,
                'data_source': 'test_historical'
            }
        ]
    }
    
    sample_transformed_data = {
        'market_data': sample_market_data,
        'historical_data': sample_historical_data
    }
    
    try:
        loader = CryptoLoader()
        
        print("🔄 Testing cryptocurrency data loading...")
        
        # Test instrument mapping
        mappings = loader.get_instrument_mappings()
        print(f"✅ Found {len(mappings)} instrument mappings")
        
        # Test data loading (only if Bitcoin instrument exists)
        if 'bitcoin' in mappings:
            # Test market data loading
            market_stats = loader.load_market_data(sample_market_data)
            print(f"✅ Market data loading: {market_stats}")
            
            # Test historical data loading
            historical_stats = loader.load_historical_data(sample_historical_data)
            print(f"✅ Historical data loading: {historical_stats}")
        else:
            print("⚠️ Bitcoin instrument not found in database - skipping load test")
        
        # Test validation
        validation = loader.validate_data_integrity()
        print(f"✅ Data validation: {len(validation['issues'])} issues found")
        
        # Test statistics
        stats = loader.get_loading_statistics()
        print(f"✅ Loading statistics: {stats['total_price_records']} total records")
        
        print("\n✅ All loader tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()