# src/transformers/crypto_transformer.py
"""
Cryptocurrency data transformer - Pure data processing and enrichment
Separation of concerns: Only responsible for cleaning and transforming data
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
    Pure cryptocurrency data transformer
    
    Responsibilities:
    - Clean and standardize raw API data
    - Calculate technical indicators
    - Enrich data with derived metrics
    - Validate data quality
    - Prepare data for database loading
    
    NOT responsible for:
    - Data collection from APIs
    - Database operations
    - Business logic decisions
    """
    
    def __init__(self, config: Optional[CryptoTransformationConfig] = None):
        """Initialize the transformer with config, ensuring default values are set"""
        self.config = CryptoTransformationConfig() if config is None else config
        # Ensure moving_averages has a default value
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
    
    def transform_market_data(self, market_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform market data from collector to database-ready format
        
        Args:
            market_data: Raw market data from collector
            
        Returns:
            List of transformed records ready for database loading
        """
        logger.info(f"Transforming market data for {len(market_data)} cryptocurrencies...")
        
        transformed_records = []
        
        for record in market_data:
            try:
                coingecko_id = record.get('coingecko_id')
                if not coingecko_id:
                    logger.warning("Record missing coingecko_id, skipping")
                    continue
                
                metadata = self.crypto_metadata.get(coingecko_id, {})
                
                # Create standardized record
                transformed_record = {
                    # Identifiers
                    'coingecko_id': coingecko_id,
                    'symbol': self._clean_string(record.get('symbol', metadata.get('symbol', ''))).upper(),
                    'name': self._clean_string(record.get('name', metadata.get('name', ''))),
                    'instrument_type': metadata.get('type', 'cryptocurrency'),
                    'asset_class': metadata.get('asset_class', 'alternative'),
                    'is_stablecoin': metadata.get('is_stablecoin', False),
                    
                    # Date information
                    'record_date': self._parse_date(record.get('collection_timestamp')),
                    'date_key': self._get_date_key(record.get('collection_timestamp')),
                    
                    # Price data (cleaned and validated)
                    'price': self._clean_decimal(record.get('current_price')),
                    'market_cap': self._clean_decimal(record.get('market_cap')),
                    'total_volume': self._clean_decimal(record.get('total_volume')),
                    'circulating_supply': self._clean_decimal(record.get('circulating_supply')),
                    
                    # Price ranges
                    'high_24h': self._clean_decimal(record.get('high_24h')),
                    'low_24h': self._clean_decimal(record.get('low_24h')),
                    
                    # Performance metrics
                    'price_change_24h': self._clean_decimal(record.get('price_change_24h')),
                    'price_change_percentage_24h': self._clean_decimal(record.get('price_change_percentage_24h')),
                    'price_change_percentage_7d': self._clean_decimal(record.get('price_change_percentage_7d')),
                    'price_change_percentage_30d': self._clean_decimal(record.get('price_change_percentage_30d')),
                    
                    # Market position
                    'market_cap_rank': record.get('market_cap_rank'),
                    'market_cap_change_24h': self._clean_decimal(record.get('market_cap_change_24h')),
                    'market_cap_change_percentage_24h': self._clean_decimal(record.get('market_cap_change_percentage_24h')),
                    
                    # Supply metrics
                    'total_supply': self._clean_decimal(record.get('total_supply')),
                    'max_supply': self._clean_decimal(record.get('max_supply')),
                    'fully_diluted_valuation': self._clean_decimal(record.get('fully_diluted_valuation')),
                    
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
                    'collection_timestamp': self._parse_datetime(record.get('collection_timestamp'))
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
    
    def transform_historical_data(self, historical_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform historical data and calculate technical indicators
        
        Args:
            historical_data: Raw historical data from collector
            
        Returns:
            Dictionary mapping coingecko_id to transformed historical records
        """
        logger.info("Transforming historical data and calculating technical indicators...")
        
        transformed_historical = {}
        
        for coingecko_id, records in historical_data.items():
            try:
                if not records:
                    logger.warning(f"No historical data for {coingecko_id}")
                    continue
                
                # Convert to DataFrame for easier calculation
                df = pd.DataFrame(records)
                
                # Clean and prepare data
                df = self._prepare_historical_dataframe(df, coingecko_id)
                
                if df.empty:
                    logger.warning(f"No valid historical data after cleaning for {coingecko_id}")
                    continue
                
                # Calculate technical indicators
                df = self._calculate_technical_indicators(df)
                
                # Convert back to list of dictionaries
                transformed_records = self._dataframe_to_records(df, coingecko_id)
                
                transformed_historical[coingecko_id] = transformed_records
                
            except Exception as e:
                logger.warning(f"Failed to transform historical data for {coingecko_id}: {e}")
                continue
        
        total_records = sum(len(records) for records in transformed_historical.values())
        logger.info(f"Successfully transformed historical data for {len(transformed_historical)} cryptocurrencies, {total_records} total records")
        
        return transformed_historical
    
    def _prepare_historical_dataframe(self, df: pd.DataFrame, coingecko_id: str) -> pd.DataFrame:
        """Prepare historical dataframe for technical indicator calculation"""
        
        # Parse and clean dates
        df['record_date'] = pd.to_datetime(df['record_date']).dt.date
        
        # Clean numeric columns
        numeric_columns = ['price_usd', 'market_cap_usd', 'volume_24h_usd']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remove rows with invalid prices
        df = df.dropna(subset=['price_usd'])
        df = df[df['price_usd'] > 0]
        
        # Sort by date
        df = df.sort_values('record_date').reset_index(drop=True)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['record_date'], keep='last')
        
        return df
    
    def _calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators on price data"""
        
        try:
            price_col = 'price_usd'
            
            # Moving averages
            for period in self.config.moving_averages:
                df[f'rolling_avg_{period}d'] = df[price_col].rolling(
                    window=period, min_periods=1
                ).mean()
                
                df[f'rolling_std_{period}d'] = df[price_col].rolling(
                    window=period, min_periods=1
                ).std()
            
            # Volatility calculations
            for period in self.config.volatility_periods:
                # Calculate returns
                returns = df[price_col].pct_change()
                
                # Rolling volatility (annualized)
                df[f'volatility_{period}d'] = returns.rolling(
                    window=period, min_periods=1
                ).std() * np.sqrt(365)
            
            # Price changes over different periods
            df['price_change_1d'] = df[price_col] - df[price_col].shift(1)
            df['price_change_percentage_1d'] = df[price_col].pct_change() * 100
            
            df['price_change_7d'] = df[price_col] - df[price_col].shift(7)
            df['price_change_percentage_7d'] = (
                (df[price_col] / df[price_col].shift(7)) - 1
            ) * 100
            
            df['price_change_30d'] = df[price_col] - df[price_col].shift(30)
            df['price_change_percentage_30d'] = (
                (df[price_col] / df[price_col].shift(30)) - 1
            ) * 100
            
            # Bollinger Bands (optional)
            for period in [20]:  # Standard 20-day Bollinger Bands
                rolling_mean = df[price_col].rolling(window=period).mean()
                rolling_std = df[price_col].rolling(window=period).std()
                df[f'bollinger_upper_{period}d'] = rolling_mean + (rolling_std * 2)
                df[f'bollinger_lower_{period}d'] = rolling_mean - (rolling_std * 2)
            
            # RSI (Relative Strength Index) - 14 period
            delta = pd.to_numeric(df[price_col].diff(), errors='coerce')
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['rsi_14'] = 100 - (100 / (1 + rs))
            
            return df
            
        except Exception as e:
            logger.error(f"Error calculating technical indicators: {e}")
            return df
    
    def _dataframe_to_records(self, df: pd.DataFrame, coingecko_id: str) -> List[Dict[str, Any]]:
        """Convert dataframe back to list of records"""
        
        metadata = self.crypto_metadata.get(coingecko_id, {})
        records = []
        
        for _, row in df.iterrows():
            record = {
                # Identifiers
                'coingecko_id': coingecko_id,
                'record_date': row['record_date'],
                'date_key': self._get_date_key_from_date(row['record_date']),
                
                # Price data
                'price': self._clean_decimal(row.get('price_usd')),
                'market_cap': self._clean_decimal(row.get('market_cap_usd')),
                'total_volume': self._clean_decimal(row.get('volume_24h_usd')),
                
                # Technical indicators
                'rolling_avg_7d': self._clean_decimal(row.get('rolling_avg_7d')),
                'rolling_avg_30d': self._clean_decimal(row.get('rolling_avg_30d')),
                'rolling_std_7d': self._clean_decimal(row.get('rolling_std_7d')),
                'rolling_std_30d': self._clean_decimal(row.get('rolling_std_30d')),
                'volatility_7d': self._clean_decimal(row.get('volatility_7d')),
                'volatility_30d': self._clean_decimal(row.get('volatility_30d')),
                
                # Price changes
                'price_change_1d': self._clean_decimal(row.get('price_change_1d')),
                'price_change_percentage_1d': self._clean_decimal(row.get('price_change_percentage_1d')),
                'price_change_7d': self._clean_decimal(row.get('price_change_7d')),
                'price_change_percentage_7d': self._clean_decimal(row.get('price_change_percentage_7d')),
                'price_change_30d': self._clean_decimal(row.get('price_change_30d')),
                'price_change_percentage_30d': self._clean_decimal(row.get('price_change_percentage_30d')),
                
                # Additional technical indicators
                'bollinger_upper_20d': self._clean_decimal(row.get('bollinger_upper_20d')),
                'bollinger_lower_20d': self._clean_decimal(row.get('bollinger_lower_20d')),
                'rsi_14': self._clean_decimal(row.get('rsi_14')),
                
                # Metadata
                'data_source': 'coingecko_historical',
                'instrument_type': metadata.get('type', 'cryptocurrency'),
                'is_stablecoin': metadata.get('is_stablecoin', False)
            }
            
            # Add stablecoin metrics if applicable
            if metadata.get('is_stablecoin', False) and record['price'] is not None:
                stablecoin_metrics = self._calculate_stablecoin_metrics(record['price'])
                record.update(stablecoin_metrics)
            
            records.append(record)
        
        return records
    
    def _calculate_stablecoin_metrics(self, price: float) -> Dict[str, Any]:
        """Calculate stablecoin deviation metrics"""
        
        if price is None or pd.isna(price):
            return {}
        
        deviation = abs(price - 1.0)
        
        # Determine price band based on configured thresholds
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
        """
        Perform comprehensive data quality validation
        
        Args:
            data: Transformed data to validate
            
        Returns:
            Quality report with issues and statistics
        """
        if not self.config.enable_quality_checks:
            return {'status': 'skipped', 'message': 'Quality checks disabled'}
        
        logger.info("Performing data quality validation...")
        
        quality_report = {
            'total_records': len(data),
            'issues': [],
            'warnings': [],
            'statistics': {},
            'validation_timestamp': datetime.now().isoformat()
        }
        
        if not data:
            quality_report['issues'].append('No data to validate')
            return quality_report
        
        # Required fields validation
        required_fields = ['coingecko_id', 'symbol', 'price', 'record_date']
        missing_fields = []
        
        for i, record in enumerate(data):
            for field in required_fields:
                if not record.get(field):
                    missing_fields.append(f"Record {i}: Missing {field}")
        
        if missing_fields:
            quality_report['issues'].extend(missing_fields[:10])  # Limit to first 10
            if len(missing_fields) > 10:
                quality_report['issues'].append(f"... and {len(missing_fields) - 10} more missing field issues")
        
        # Price validation
        price_issues = []
        stablecoin_issues = []
        extreme_changes = []
        
        for record in data:
            symbol = record.get('symbol', 'unknown')
            price = record.get('price')
            market_cap = record.get('market_cap')
            price_change_24h = record.get('price_change_percentage_24h')
            
            # Price reasonableness
            if price is not None:
                if price <= 0:
                    price_issues.append(f"Invalid price for {symbol}: {price}")
                elif price > 1000000:  # Very high price warning
                    quality_report['warnings'].append(f"Unusually high price for {symbol}: ${price:,.2f}")
            
            # Market cap validation
            if market_cap is not None and market_cap < self.config.min_market_cap:
                quality_report['warnings'].append(f"Low market cap for {symbol}: ${market_cap:,.0f}")
            
            # Extreme price changes
            if price_change_24h is not None and abs(price_change_24h) > (self.config.max_price_change_24h * 100):
                extreme_changes.append(f"Extreme 24h change for {symbol}: {price_change_24h:.1f}%")
            
            # Stablecoin deviation validation
            if record.get('is_stablecoin') and price is not None:
                deviation = record.get('deviation_from_dollar', 0)
                if deviation > self.config.stablecoin_critical_threshold:
                    stablecoin_issues.append(f"Critical stablecoin deviation for {symbol}: {deviation:.4f} ({deviation*100:.2f}%)")
        
        # Add issues to report
        quality_report['issues'].extend(price_issues)
        quality_report['issues'].extend(stablecoin_issues)
        quality_report['warnings'].extend(extreme_changes)
        
        # Calculate statistics
        valid_prices = [r['price'] for r in data if r.get('price') is not None and r['price'] > 0]
        if valid_prices:
            quality_report['statistics'] = {
                'valid_price_records': len(valid_prices),
                'price_completeness_pct': (len(valid_prices) / len(data)) * 100,
                'avg_price': np.mean(valid_prices),
                'median_price': np.median(valid_prices),
                'price_range': [min(valid_prices), max(valid_prices)],
                'stablecoin_count': len([r for r in data if r.get('is_stablecoin')]),
                'cryptocurrency_count': len([r for r in data if not r.get('is_stablecoin')])
            }
        
        logger.info(f"Data quality validation completed: {len(quality_report['issues'])} issues, {len(quality_report['warnings'])} warnings")
        return quality_report
    
    # Utility methods for data cleaning
    def _clean_string(self, value: Any) -> str:
        """Clean and standardize string values"""
        if value is None or pd.isna(value):
            return ''
        return str(value).strip()
    
    def _clean_decimal(self, value: Any) -> Optional[float]:
        """Clean and validate decimal values"""
        if value is None or pd.isna(value):
            return None
        
        try:
            cleaned_value = float(value)
            # Check for reasonable bounds
            if cleaned_value < 0 or cleaned_value > 1e15:
                return None
            return cleaned_value
        except (ValueError, TypeError):
            return None
    
    def _parse_date(self, date_value: Any) -> Optional[date]:
        """Parse various date formats to date object"""
        if date_value is None:
            return None
        
        try:
            if isinstance(date_value, str):
                # Try ISO format first
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
        """Parse various datetime formats"""
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
    
    def _get_date_key(self, date_value: Any) -> Optional[int]:
        """Convert date to YYYYMMDD format for date_key"""
        parsed_date = self._parse_date(date_value)
        if parsed_date:
            return int(parsed_date.strftime('%Y%m%d'))
        return None
    
    def _get_date_key_from_date(self, date_obj: date) -> int:
        """Convert date object to YYYYMMDD format"""
        return int(date_obj.strftime('%Y%m%d'))
    
    def transform_complete_dataset(self, collected_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform complete dataset from collector
        
        Args:
            collected_data: Complete dataset from collector
            
        Returns:
            Transformed dataset ready for loader
        """
        logger.info("Transforming complete cryptocurrency dataset...")
        
        # Transform market data
        market_data = collected_data.get('market_data', [])
        transformed_market = self.transform_market_data(market_data)
        
        # Transform historical data
        historical_data = collected_data.get('historical_data', {})
        transformed_historical = self.transform_historical_data(historical_data)
        
        # Validate data quality
        all_records = transformed_market.copy()
        for records in transformed_historical.values():
            all_records.extend(records)
        
        quality_report = self.validate_data_quality(all_records)
        
        result = {
            'market_data': transformed_market,
            'historical_data': transformed_historical,
            'quality_report': quality_report,
            'transformation_metadata': {
                'transformation_timestamp': datetime.now().isoformat(),
                'transformer_version': '1.0.0',
                'market_records_processed': len(transformed_market),
                'historical_coins_processed': len(transformed_historical),
                'total_historical_records': sum(len(records) for records in transformed_historical.values()),
                'quality_issues_count': len(quality_report.get('issues', [])),
                'quality_warnings_count': len(quality_report.get('warnings', []))
            }
        }
        
        logger.info("✅ Complete dataset transformation completed successfully")
        return result

# Convenience function for standalone usage
def transform_crypto_data(collected_data: Dict[str, Any], config: Optional[CryptoTransformationConfig] = None) -> Dict[str, Any]:
    """Standalone function to transform cryptocurrency data"""
    transformer = CryptoTransformer(config)
    return transformer.transform_complete_dataset(collected_data)

# Example usage and testing
if __name__ == "__main__":
    # Test with sample data
    sample_market_data = [
        {
            'coingecko_id': 'bitcoin',
            'symbol': 'btc',
            'name': 'Bitcoin',
            'current_price': 45000.50,
            'market_cap': 850000000000,
            'total_volume': 25000000000,
            'market_cap_rank': 1,
            'price_change_percentage_24h': 2.5,
            'collection_timestamp': datetime.now().isoformat(),
            'data_source': 'test'
        }
    ]
    
    sample_historical_data = {
        'bitcoin': [
            {
                'coingecko_id': 'bitcoin',
                'record_date': '2025-06-01',
                'price_usd': 44000.0,
                'market_cap_usd': 840000000000,
                'volume_24h_usd': 24000000000
            },
            {
                'coingecko_id': 'bitcoin',
                'record_date': '2025-06-02',
                'price_usd': 45000.0,
                'market_cap_usd': 850000000000,
                'volume_24h_usd': 25000000000
            }
        ]
    }
    
    sample_collected_data = {
        'market_data': sample_market_data,
        'historical_data': sample_historical_data
    }
    
    try:
        transformer = CryptoTransformer()
        
        # Test market data transformation
        transformed_market = transformer.transform_market_data(sample_market_data)
        print(f"✅ Transformed {len(transformed_market)} market records")
        
        # Test historical data transformation
        transformed_historical = transformer.transform_historical_data(sample_historical_data)
        print(f"✅ Transformed historical data for {len(transformed_historical)} coins")
        
        # Test complete dataset transformation
        complete_result = transformer.transform_complete_dataset(sample_collected_data)
        print(f"✅ Complete transformation: {complete_result['transformation_metadata']}")
        
        # Show sample transformed data
        if transformed_market:
            sample = transformed_market[0]
            print(f"\n📊 Sample transformed data:")
            print(f"   Coin: {sample['name']} ({sample['symbol']})")
            print(f"   Price: ${sample['price']:,.2f}")
            print(f"   Type: {sample['instrument_type']}")
            print(f"   Asset Class: {sample['asset_class']}")
        
        print("\n✅ All transformation tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()