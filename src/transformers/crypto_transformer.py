"""
src/transformers/crypto_transformer.py

FINAL FIXED cryptocurrency data transformer
- Handles actual data formats from your collector
- Properly processes dates from Binance, CoinGecko, Yahoo
- Maintains your existing architecture and config
"""
import pandas as pd 
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, date
import logging
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CryptoTransformationConfig:
    """Configuration for cryptocurrency data transformation"""
    # Technical indicator settings
    moving_averages: Optional[List[int]] = None
    volatility_periods: Optional[List[int]] = None
    
    # Stablecoin thresholds
    stablecoin_normal_threshold: float = 0.001      # 0.1%
    stablecoin_moderate_threshold: float = 0.005    # 0.5%  
    stablecoin_high_threshold: float = 0.01         # 1%
    stablecoin_critical_threshold: float = 0.05     # 5%
    
    # Data quality settings
    enable_quality_checks: bool = True
    max_price_change_24h: float = 0.50              # 50% max daily change
    min_market_cap: float = 1000000                 # $1M minimum market cap
    
    def __post_init__(self):
        """Initialize default values for None attributes"""
        if self.moving_averages is None:
            self.moving_averages = [7, 30]
        if self.volatility_periods is None:
            self.volatility_periods = [7, 30]

class CryptoTransformer:
    """
    Final fixed cryptocurrency data transformer
    """
    
    def __init__(self, config: Optional[CryptoTransformationConfig] = None):
        """Initialize the transformer with config"""
        self.config = CryptoTransformationConfig() if config is None else config
        if self.config.moving_averages is None:
            self.config.moving_averages = [7, 30]
        
        # Cryptocurrency metadata for enrichment
        self.crypto_metadata = {
            'bitcoin': {
                'symbol': 'BTC', 'name': 'Bitcoin', 'type': 'cryptocurrency', 
                'is_stablecoin': False, 'asset_class': 'alternative'
            },
            'ethereum': {
                'symbol': 'ETH', 'name': 'Ethereum', 'type': 'cryptocurrency', 
                'is_stablecoin': False, 'asset_class': 'alternative'
            },
            'binancecoin': {
                'symbol': 'BNB', 'name': 'Binance Coin', 'type': 'cryptocurrency', 
                'is_stablecoin': False, 'asset_class': 'alternative'
            },
            'solana': {
                'symbol': 'SOL', 'name': 'Solana', 'type': 'cryptocurrency', 
                'is_stablecoin': False, 'asset_class': 'alternative'
            },
            'cardano': {
                'symbol': 'ADA', 'name': 'Cardano', 'type': 'cryptocurrency', 
                'is_stablecoin': False, 'asset_class': 'alternative'
            },
            'tether': {
                'symbol': 'USDT', 'name': 'Tether', 'type': 'stablecoin', 
                'is_stablecoin': True, 'asset_class': 'currency'
            },
            'usd-coin': {
                'symbol': 'USDC', 'name': 'USD Coin', 'type': 'stablecoin', 
                'is_stablecoin': True, 'asset_class': 'currency'
            },
            'binance-usd': {
                'symbol': 'BUSD', 'name': 'Binance USD', 'type': 'stablecoin', 
                'is_stablecoin': True, 'asset_class': 'currency'
            },
            'dai': {
                'symbol': 'DAI', 'name': 'Dai', 'type': 'stablecoin', 
                'is_stablecoin': True, 'asset_class': 'currency'
            },
            'true-usd': {
                'symbol': 'TUSD', 'name': 'TrueUSD', 'type': 'stablecoin', 
                'is_stablecoin': True, 'asset_class': 'currency'
            }
        }
        
        logger.info("CryptoTransformer initialized")
    
    def transform_complete_dataset(self, collected_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main entry point - Transform complete dataset from collector
        """
        logger.info("Transforming complete cryptocurrency dataset...")
        
        # Extract market and historical data
        market_data = collected_data.get('market_data', [])
        historical_data = collected_data.get('historical_data', {})
        
        # Transform market data
        transformed_market = self.transform_market_data(market_data)
        
        # Transform historical data (FIXED to handle actual data structure)
        transformed_historical = self._transform_historical_data_fixed(historical_data)
        
        # Validate data quality
        all_records = transformed_market.copy()
        for records in transformed_historical:
            all_records.append(records)
        
        quality_report = self.validate_data_quality(all_records)
        
        result = {
            'market_data': transformed_market,
            'historical_data': transformed_historical,  # Return as list, not dict
            'quality_report': quality_report,
            'transformation_metadata': {
                'transformation_timestamp': datetime.now().isoformat(),
                'transformer_version': '2.0.0',
                'market_records_processed': len(transformed_market),
                'historical_records_processed': len(transformed_historical),
                'quality_issues_count': len(quality_report.get('issues', [])),
                'quality_warnings_count': len(quality_report.get('warnings', []))
            }
        }
        
        logger.info("✅ Complete dataset transformation completed successfully")
        return result
    
    def transform_market_data(self, market_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform market data records"""
        logger.info(f"Transforming market data for {len(market_data)} cryptocurrencies...")
        
        transformed_records = []
        
        for record in market_data:
            try:
                # Get coingecko_id from record (different sources use different field names)
                coingecko_id = (record.get('coingecko_id') or 
                              record.get('id') or 
                              record.get('symbol', '').lower())
                
                if not coingecko_id:
                    logger.warning("Record missing coingecko_id, skipping")
                    continue
                
                metadata = self.crypto_metadata.get(coingecko_id, {})
                
                # Create standardized record matching your database schema
                transformed_record = {
                    # Identifiers
                    'coingecko_id': coingecko_id,
                    'symbol': self._clean_string(record.get('symbol', metadata.get('symbol', ''))).upper(),
                    'name': self._clean_string(record.get('name', metadata.get('name', ''))),
                    'instrument_type': metadata.get('type', 'cryptocurrency'),
                    'asset_class': metadata.get('asset_class', 'alternative'),
                    'is_stablecoin': metadata.get('is_stablecoin', False),
                    
                    # Core price data (use actual field names from your collector)
                    'price': self._clean_decimal(record.get('current_price') or record.get('price')),
                    'market_cap': self._clean_decimal(record.get('market_cap')),
                    'total_volume': self._clean_decimal(record.get('total_volume')),
                    'circulating_supply': self._clean_decimal(record.get('circulating_supply')),
                    'total_supply': self._clean_decimal(record.get('total_supply')),
                    'max_supply': self._clean_decimal(record.get('max_supply')),
                    
                    # Performance metrics
                    'price_change_24h': self._clean_decimal(record.get('price_change_24h')),
                    'price_change_percentage_24h': self._clean_decimal(record.get('price_change_percentage_24h')),
                    'price_change_percentage_7d': self._clean_decimal(record.get('price_change_percentage_7d')),
                    'price_change_percentage_30d': self._clean_decimal(record.get('price_change_percentage_30d')),
                    
                    # Market position
                    'market_cap_rank': record.get('market_cap_rank'),
                    'market_cap_change_24h': self._clean_decimal(record.get('market_cap_change_24h')),
                    'market_cap_change_percentage_24h': self._clean_decimal(record.get('market_cap_change_percentage_24h')),
                    
                    # All-time metrics
                    'ath': self._clean_decimal(record.get('ath')),
                    'ath_change_percentage': self._clean_decimal(record.get('ath_change_percentage')),
                    'ath_date': self._parse_date(record.get('ath_date')),
                    'atl': self._clean_decimal(record.get('atl')),
                    'atl_change_percentage': self._clean_decimal(record.get('atl_change_percentage')),
                    'atl_date': self._parse_date(record.get('atl_date')),
                    
                    # Metadata
                    'data_source': record.get('data_source', 'coingecko'),
                    'last_updated': self._parse_datetime(record.get('last_updated')),
                }
                
                # Add stablecoin-specific metrics
                if metadata.get('is_stablecoin', False):
                    price = transformed_record['price']
                    if price is not None:
                        stablecoin_metrics = self._calculate_stablecoin_metrics(price)
                        transformed_record.update(stablecoin_metrics)
                
                transformed_records.append(transformed_record)
                
            except Exception as e:
                logger.warning(f"Failed to transform record for {record.get('coingecko_id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Successfully transformed {len(transformed_records)} market records")
        return transformed_records
    
    def _transform_historical_data_fixed(self, historical_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        FIXED: Transform historical data handling actual collector output
        """
        logger.info("Transforming historical data and calculating technical indicators...")
        
        all_historical_records = []
        
        for coingecko_id, records in historical_data.items():
            try:
                if not records:
                    logger.warning(f"No historical data for {coingecko_id}")
                    continue
                
                # Convert to DataFrame for processing
                df = pd.DataFrame(records)
                
                # CRITICAL FIX: Handle actual date formats from your collector
                df = self._fix_dates_from_collector(df, coingecko_id)
                
                if df.empty:
                    logger.warning(f"No valid historical data after date processing for {coingecko_id}")
                    continue
                
                # Calculate technical indicators
                df = self._calculate_technical_indicators_fixed(df, coingecko_id)
                
                # Convert back to records
                for _, row in df.iterrows():
                    record = self._dataframe_row_to_record(row, coingecko_id)
                    all_historical_records.append(record)
                
            except Exception as e:
                logger.warning(f"Failed to transform historical data for {coingecko_id}: {e}")
                continue
        
        logger.info(f"Successfully transformed historical data for {len(historical_data)} cryptocurrencies, {len(all_historical_records)} total records")
        return all_historical_records
    
    def _fix_dates_from_collector(self, df: pd.DataFrame, coingecko_id: str) -> pd.DataFrame:
        """
        FIXED: Handle actual date formats from your collector sources
        """
        logger.debug(f"Processing dates for {coingecko_id}. Available columns: {list(df.columns)}")
        
        # Your collector might return different date field names
        date_candidates = [
            'date',           # Standard
            'datetime',       # Alternative
            'timestamp',      # Unix timestamp
            'open_time',      # Binance
            'close_time',     # Binance
            'time',           # Generic
        ]
        
        date_field = None
        for candidate in date_candidates:
            if candidate in df.columns:
                date_field = candidate
                logger.debug(f"Found date field '{candidate}' for {coingecko_id}")
                break
        
        if date_field is None:
            # Try to use index if it looks like dates
            try:
                if not df.empty:
                    # Check if index can be converted to dates
                    pd.to_datetime(df.index[:min(5, len(df))])  # Test first few
                    df['date'] = pd.to_datetime(df.index).date
                    logger.debug(f"Used index as date for {coingecko_id}")
            except:
                logger.warning(f"Could not find or infer dates for {coingecko_id}")
                return pd.DataFrame()
        else:
            # Convert the found date field
            try:
                if pd.api.types.is_numeric_dtype(df[date_field]):
                    # Handle Unix timestamps
                    sample_val = df[date_field].iloc[0] if not df.empty else 0
                    if sample_val > 1e10:  # Milliseconds
                        df['date'] = pd.to_datetime(df[date_field], unit='ms').dt.date
                    else:  # Seconds
                        df['date'] = pd.to_datetime(df[date_field], unit='s').dt.date
                else:
                    # Handle string dates
                    df['date'] = pd.to_datetime(df[date_field], errors='coerce').dt.date
                
                # Remove rows with null dates
                df = df.dropna(subset=['date'])
                
            except Exception as e:
                logger.warning(f"Error processing dates for {coingecko_id}: {e}")
                return pd.DataFrame()
        
        return df
    
    def _calculate_technical_indicators_fixed(self, df: pd.DataFrame, coingecko_id: str) -> pd.DataFrame:
        """
        FIXED: Calculate technical indicators with actual field names
        """
        if df.empty:
            return df
        
        # Find the price field from your collector
        price_field = None
        price_candidates = ['price', 'close', 'Close', 'price_usd', 'last_price']
        
        for candidate in price_candidates:
            if candidate in df.columns:
                price_field = candidate
                break
        
        if not price_field:
            logger.warning(f"No price field found for {coingecko_id}")
            return df
        
        # Ensure numeric and sort by date
        df[price_field] = pd.to_numeric(df[price_field], errors='coerce')
        df = df.dropna(subset=[price_field])
        df = df.sort_values('date').reset_index(drop=True)
        
        try:
            # Calculate technical indicators
            for period in self.config.moving_averages:
                df[f'rolling_avg_{period}d'] = df[price_field].rolling(
                    window=period, min_periods=1
                ).mean()
                
                df[f'rolling_std_{period}d'] = df[price_field].rolling(
                    window=period, min_periods=1
                ).std()
            
            # Daily returns
            df['daily_return'] = df[price_field].pct_change()
            
            # Volatility
            if len(df) >= 7:
                df['volatility'] = df['daily_return'].rolling(window=7, min_periods=1).std()
            
            # Fill NaN values
            df = df.fillna(0)
            
        except Exception as e:
            logger.warning(f"Error calculating technical indicators for {coingecko_id}: {e}")
        
        return df
    
    def _dataframe_row_to_record(self, row: pd.Series, coingecko_id: str) -> Dict[str, Any]:
        """Convert DataFrame row to record format"""
        metadata = self.crypto_metadata.get(coingecko_id, {})
        
        # Find the price field
        price = None
        for field in ['price', 'close', 'Close', 'price_usd']:
            if field in row.index:
                price = self._clean_decimal(row[field])
                break
        
        record = {
            'coingecko_id': coingecko_id,
            'date': row['date'],
            'price': price,
            'volume': self._clean_decimal(row.get('volume')),
            'high': self._clean_decimal(row.get('high')),
            'low': self._clean_decimal(row.get('low')),
            'open': self._clean_decimal(row.get('open')),
            'close': self._clean_decimal(row.get('close')),
            'daily_return': self._clean_decimal(row.get('daily_return')),
            'rolling_avg_7d': self._clean_decimal(row.get('rolling_avg_7d')),
            'rolling_avg_30d': self._clean_decimal(row.get('rolling_avg_30d')),
            'rolling_std_7d': self._clean_decimal(row.get('rolling_std_7d')),
            'rolling_std_30d': self._clean_decimal(row.get('rolling_std_30d')),
            'volatility': self._clean_decimal(row.get('volatility')),
            'data_source': 'historical',
            'is_stablecoin': metadata.get('is_stablecoin', False),
        }
        
        return record
    
    def _calculate_stablecoin_metrics(self, price: float) -> Dict[str, Any]:
        """Calculate stablecoin deviation metrics"""
        if price is None or pd.isna(price):
            return {}
        
        deviation = abs(price - 1.0)
        
        if deviation < self.config.stablecoin_normal_threshold:
            price_band = 'normal'
        elif deviation < self.config.stablecoin_moderate_threshold:
            price_band = 'moderate'
        elif deviation < self.config.stablecoin_high_threshold:
            price_band = 'high'
        else:
            price_band = 'critical'
        
        return {
            'deviation_from_dollar': deviation,
            'within_01_percent': deviation < 0.001,
            'within_05_percent': deviation < 0.005,
            'within_1_percent': deviation < 0.01,
            'price_band': price_band
        }
    
    def validate_data_quality(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate data quality"""
        logger.info("Performing data quality validation...")
        
        quality_report = {
            'total_records': len(data),
            'issues': [],
            'warnings': [],
            'validation_timestamp': datetime.now().isoformat()
        }
        
        if not data:
            quality_report['issues'].append('No data to validate')
            return quality_report
        
        # Check for required fields
        for i, record in enumerate(data[:10]):  # Check first 10 records
            if not record.get('coingecko_id'):
                quality_report['issues'].append(f"Record {i}: Missing coingecko_id")
            if not record.get('price'):
                quality_report['issues'].append(f"Record {i}: Missing price")
        
        logger.info(f"Data quality validation completed: {len(quality_report['issues'])} issues, {len(quality_report['warnings'])} warnings")
        return quality_report
    
    # Utility methods
    def _clean_string(self, value: Any) -> str:
        """Clean string values"""
        if value is None or pd.isna(value):
            return ''
        return str(value).strip()
    
    def _clean_decimal(self, value: Any) -> Optional[float]:
        """Clean decimal values"""
        if value is None or pd.isna(value):
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _parse_date(self, date_value: Any) -> Optional[date]:
        """Parse date values"""
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
        """Parse datetime values"""
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


# Standalone function for compatibility
def transform_crypto_data(collected_data: Dict[str, Any], config: Optional[CryptoTransformationConfig] = None) -> Dict[str, Any]:
    """Standalone function to transform cryptocurrency data"""
    transformer = CryptoTransformer(config)
    return transformer.transform_complete_dataset(collected_data)