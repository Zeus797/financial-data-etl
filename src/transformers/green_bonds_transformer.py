"""
src/transformers/green_bonds_transformer.py

Production-ready transformer for green bonds and ESG ETF data
Handles data cleaning, standardization, and enrichment for the financial pipeline
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
from dataclasses import dataclass
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class GreenBondsTransformerConfig:
    """Configuration for green bonds data transformation"""
    
    # Data quality thresholds
    min_price_threshold: float = 1.0  # Minimum valid ETF price
    max_price_threshold: float = 1000.0  # Maximum reasonable ETF price
    max_daily_return_threshold: float = 0.20  # 20% max daily return (outlier detection)
    min_volume_threshold: float = 1000  # Minimum reasonable trading volume
    
    # ESG score normalization
    esg_score_min: float = 0.0
    esg_score_max: float = 100.0
    
    # Expense ratio validation
    min_expense_ratio: float = 0.01  # 0.01% minimum
    max_expense_ratio: float = 2.0   # 2.0% maximum reasonable
    
    # Fund size validation
    min_fund_size: float = 1000000    # $1M minimum AUM
    max_fund_size: float = 50000000000  # $50B maximum reasonable
    
    # Technical indicator periods
    volatility_periods: List[int] = None
    moving_average_periods: List[int] = None
    
    def __post_init__(self):
        if self.volatility_periods is None:
            self.volatility_periods = [7, 30, 90, 252]  # 7D, 1M, 3M, 1Y
        if self.moving_average_periods is None:
            self.moving_average_periods = [20, 50, 200]  # Short, medium, long term

class GreenBondsTransformer:
    """
    Production-ready transformer for green bonds and ESG ETF data
    
    Features:
    - Comprehensive data validation and cleaning
    - ESG score normalization and categorization
    - Technical indicator calculation
    - Performance benchmarking
    - Sustainability impact metrics enhancement
    - Fund categorization and classification
    """
    
    def __init__(self, config: Optional[GreenBondsTransformerConfig] = None):
        self.config = config or GreenBondsTransformerConfig()
        
        # ESG categorization mappings
        self.esg_categories = {
            'green_bond': {
                'primary_focus': ['green bonds', 'climate bonds', 'environmental bonds'],
                'sector_keywords': ['renewable', 'clean energy', 'sustainable', 'climate']
            },
            'esg_equity': {
                'primary_focus': ['esg equity', 'sustainable equity', 'socially responsible'],
                'sector_keywords': ['esg', 'sustainable', 'responsible', 'impact']
            },
            'impact_bond': {
                'primary_focus': ['social impact', 'development bonds'],
                'sector_keywords': ['impact', 'development', 'social']
            }
        }
        
        # Fund family standardization
        self.fund_family_mapping = {
            'ishares': 'iShares',
            'vanguard': 'Vanguard',
            'vaneck': 'VanEck',
            'state street': 'State Street',
            'spdr': 'State Street',
            'nuveen': 'Nuveen',
            'flexshares': 'FlexShares'
        }
        
        logger.info("GreenBondsTransformer initialized")

    def validate_current_data(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Validate and clean current green bonds/ESG ETF data"""
        logger.info(f"Validating {len(data)} current green bonds/ESG records...")
        
        validated_data = []
        validation_errors = []
        
        for i, record in enumerate(data):
            errors = []
            
            # Required fields validation
            required_fields = ['ticker', 'fund_name', 'nav_price', 'asset_class']
            for field in required_fields:
                if not record.get(field):
                    errors.append(f"Missing required field: {field}")
            
            # Price validation
            nav_price = record.get('nav_price', 0)
            if not isinstance(nav_price, (int, float)) or nav_price <= 0:
                errors.append(f"Invalid NAV price: {nav_price}")
            elif nav_price < self.config.min_price_threshold or nav_price > self.config.max_price_threshold:
                errors.append(f"NAV price out of reasonable range: {nav_price}")
            
            # Daily return validation
            daily_return = record.get('daily_return', 0)
            if daily_return and abs(daily_return) > self.config.max_daily_return_threshold:
                errors.append(f"Extreme daily return detected: {daily_return:.4f}")
            
            # Volume validation
            volume = record.get('volume', 0)
            if volume and volume < self.config.min_volume_threshold:
                errors.append(f"Unusually low volume: {volume}")
            
            # Expense ratio validation
            expense_ratio = record.get('expense_ratio')
            if expense_ratio:
                if expense_ratio < self.config.min_expense_ratio or expense_ratio > self.config.max_expense_ratio:
                    errors.append(f"Expense ratio out of range: {expense_ratio}")
            
            # Fund size validation
            total_assets = record.get('total_assets')
            if total_assets:
                if total_assets < self.config.min_fund_size or total_assets > self.config.max_fund_size:
                    errors.append(f"Total assets out of reasonable range: {total_assets}")
            
            if errors:
                validation_errors.extend([f"Record {i} ({record.get('ticker', 'Unknown')}): {error}" for error in errors])
                logger.warning(f"Validation errors for {record.get('ticker', 'Unknown')}: {errors}")
            else:
                validated_data.append(record)
        
        logger.info(f"✅ Validation complete: {len(validated_data)}/{len(data)} records passed")
        return validated_data, validation_errors

    def standardize_current_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Standardize and enrich current green bonds/ESG data"""
        logger.info(f"Standardizing {len(data)} current records...")
        
        standardized_data = []
        
        for record in data:
            standardized_record = record.copy()
            
            # Standardize fund family
            fund_family = record.get('fund_family', '').lower()
            standardized_record['fund_family_standardized'] = self.fund_family_mapping.get(fund_family, record.get('fund_family', ''))
            
            # Normalize ESG scores
            esg_score = record.get('esg_score')
            if esg_score:
                standardized_record['esg_score_normalized'] = min(max(esg_score, self.config.esg_score_min), self.config.esg_score_max) / self.config.esg_score_max
                standardized_record['esg_category'] = self._categorize_esg_score(esg_score)
            
            # Calculate expense ratio in basis points
            expense_ratio = record.get('expense_ratio')
            if expense_ratio:
                standardized_record['expense_ratio_bps'] = expense_ratio * 10000  # Convert to basis points
                standardized_record['expense_ratio_category'] = self._categorize_expense_ratio(expense_ratio)
            
            # Fund size categorization
            total_assets = record.get('total_assets')
            if total_assets:
                standardized_record['fund_size_category'] = self._categorize_fund_size(total_assets)
                standardized_record['total_assets_billions'] = total_assets / 1000000000  # Convert to billions
            
            # Asset class enhancement
            asset_class = record.get('asset_class', '')
            focus_area = record.get('focus_area', '')
            standardized_record['enhanced_asset_class'] = self._enhance_asset_classification(asset_class, focus_area, record.get('fund_name', ''))
            
            # Performance categorization
            daily_return = record.get('daily_return', 0)
            if daily_return:
                standardized_record['daily_performance_category'] = self._categorize_daily_performance(daily_return)
            
            # Sustainability metrics enhancement
            standardized_record.update(self._calculate_sustainability_metrics(record))
            
            # Add processing metadata
            standardized_record['processing_timestamp'] = datetime.now().isoformat()
            standardized_record['data_quality_score'] = self._calculate_data_quality_score(record)
            
            standardized_data.append(standardized_record)
        
        logger.info(f"✅ Standardization complete: {len(standardized_data)} records processed")
        return standardized_data

    def transform_historical_data(self, historical_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """Transform historical data for all ETFs with technical indicators"""
        logger.info(f"Transforming historical data for {len(historical_data)} ETFs...")
        
        transformed_data = {}
        
        for etf_key, records in historical_data.items():
            if not records:
                transformed_data[etf_key] = []
                continue
            
            logger.info(f"Processing {len(records)} historical records for {etf_key}...")
            
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(records)
            
            # Ensure proper date handling
            df['record_date'] = pd.to_datetime(df['record_date'])
            df = df.sort_values('record_date')
            
            # Calculate technical indicators
            df = self._calculate_technical_indicators(df)
            
            # Calculate performance metrics
            df = self._calculate_performance_metrics(df)
            
            # Add volatility analysis
            df = self._calculate_volatility_metrics(df)
            
            # Add trend analysis
            df = self._calculate_trend_indicators(df)
            
            # Convert back to list of dictionaries
            transformed_records = []
            for _, row in df.iterrows():
                record = row.to_dict()
                
                # Handle NaN values
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif isinstance(value, np.integer):
                        record[key] = int(value)
                    elif isinstance(value, np.floating):
                        record[key] = float(value)
                
                # Add processing metadata
                record['processing_timestamp'] = datetime.now().isoformat()
                record['technical_indicators_version'] = '1.0.0'
                
                transformed_records.append(record)
            
            transformed_data[etf_key] = transformed_records
            logger.info(f"✅ {etf_key}: {len(transformed_records)} records with technical indicators")
        
        return transformed_data

    def _calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate comprehensive technical indicators"""
        
        # Moving averages
        for period in self.config.moving_average_periods:
            df[f'ma_{period}d'] = df['close_price'].rolling(window=period).mean()
            df[f'ma_{period}d_signal'] = np.where(df['close_price'] > df[f'ma_{period}d'], 'bullish', 'bearish')
        
        # Exponential moving averages
        df['ema_12'] = df['close_price'].ewm(span=12).mean()
        df['ema_26'] = df['close_price'].ewm(span=26).mean()
        
        # MACD
        df['macd'] = df['ema_12'] - df['ema_26']
        df['macd_signal'] = df['macd'].ewm(span=9).mean()
        df['macd_histogram'] = df['macd'] - df['macd_signal']
        
        # RSI
        delta = df['close_price'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        df['rsi_signal'] = np.where(df['rsi'] > 70, 'overbought', 
                                   np.where(df['rsi'] < 30, 'oversold', 'neutral'))
        
        # Bollinger Bands
        df['bb_middle'] = df['close_price'].rolling(window=20).mean()
        bb_std = df['close_price'].rolling(window=20).std()
        df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
        df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
        df['bb_position'] = (df['close_price'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])
        
        return df

    def _calculate_performance_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate performance and return metrics"""
        
        # Daily returns (if not already calculated)
        if 'daily_return' not in df.columns:
            df['daily_return'] = df['close_price'].pct_change()
        
        # Cumulative returns
        df['cumulative_return'] = (1 + df['daily_return']).cumprod() - 1
        
        # Rolling returns
        for period in [7, 30, 90, 252]:
            df[f'return_{period}d'] = df['close_price'].pct_change(periods=period)
        
        # Rolling Sharpe ratio (assuming 0% risk-free rate)
        for period in [30, 90, 252]:
            rolling_mean = df['daily_return'].rolling(window=period).mean()
            rolling_std = df['daily_return'].rolling(window=period).std()
            df[f'sharpe_ratio_{period}d'] = (rolling_mean / rolling_std) * np.sqrt(252)
        
        # Drawdown analysis
        rolling_max = df['close_price'].expanding().max()
        df['drawdown'] = (df['close_price'] - rolling_max) / rolling_max
        df['max_drawdown'] = df['drawdown'].expanding().min()
        
        return df

    def _calculate_volatility_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate volatility and risk metrics"""
        
        # Rolling volatility (annualized)
        for period in self.config.volatility_periods:
            df[f'volatility_{period}d'] = df['daily_return'].rolling(window=period).std() * np.sqrt(252)
        
        # GARCH-like volatility clustering indicator
        df['volatility_cluster'] = df['daily_return'].rolling(window=20).std().rolling(window=5).mean()
        
        # Value at Risk (95% confidence)
        for period in [30, 90]:
            df[f'var_95_{period}d'] = df['daily_return'].rolling(window=period).quantile(0.05)
        
        return df

    def _calculate_trend_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate trend and momentum indicators"""
        
        # Price momentum
        for period in [5, 10, 20]:
            df[f'momentum_{period}d'] = df['close_price'] / df['close_price'].shift(period) - 1
        
        # Trend strength (using linear regression slope)
        def calculate_trend_strength(series, window=20):
            x = np.arange(window)
            trends = []
            for i in range(len(series)):
                if i < window - 1:
                    trends.append(np.nan)
                else:
                    y = series.iloc[i-window+1:i+1].values
                    slope = np.polyfit(x, y, 1)[0]
                    trends.append(slope)
            return pd.Series(trends, index=series.index)
        
        df['trend_strength_20d'] = calculate_trend_strength(df['close_price'])
        
        # Support and resistance levels
        df['resistance_level'] = df['high_price'].rolling(window=20).max()
        df['support_level'] = df['low_price'].rolling(window=20).min()
        
        return df

    def _categorize_esg_score(self, score: float) -> str:
        """Categorize ESG scores"""
        if score >= 80:
            return 'excellent'
        elif score >= 60:
            return 'good'
        elif score >= 40:
            return 'average'
        elif score >= 20:
            return 'poor'
        else:
            return 'very_poor'

    def _categorize_expense_ratio(self, ratio: float) -> str:
        """Categorize expense ratios"""
        if ratio <= 0.10:
            return 'very_low'
        elif ratio <= 0.25:
            return 'low'
        elif ratio <= 0.50:
            return 'moderate'
        elif ratio <= 0.75:
            return 'high'
        else:
            return 'very_high'

    def _categorize_fund_size(self, assets: float) -> str:
        """Categorize fund size"""
        if assets >= 10000000000:  # $10B+
            return 'mega'
        elif assets >= 1000000000:  # $1B+
            return 'large'
        elif assets >= 100000000:  # $100M+
            return 'medium'
        elif assets >= 10000000:  # $10M+
            return 'small'
        else:
            return 'micro'

    def _enhance_asset_classification(self, asset_class: str, focus_area: str, fund_name: str) -> str:
        """Enhance asset class classification"""
        combined_text = f"{asset_class} {focus_area} {fund_name}".lower()
        
        if any(keyword in combined_text for keyword in ['green bond', 'climate bond']):
            return 'green_bonds'
        elif any(keyword in combined_text for keyword in ['esg', 'sustainable', 'responsible']):
            return 'esg_equity'
        elif any(keyword in combined_text for keyword in ['impact', 'social']):
            return 'impact_investing'
        elif any(keyword in combined_text for keyword in ['renewable', 'clean energy']):
            return 'clean_energy'
        else:
            return asset_class

    def _categorize_daily_performance(self, daily_return: float) -> str:
        """Categorize daily performance"""
        if daily_return > 0.02:
            return 'strong_positive'
        elif daily_return > 0.005:
            return 'positive'
        elif daily_return > -0.005:
            return 'neutral'
        elif daily_return > -0.02:
            return 'negative'
        else:
            return 'strong_negative'

    def _calculate_sustainability_metrics(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate enhanced sustainability metrics"""
        metrics = {}
        
        # ESG score analysis
        esg_score = record.get('esg_score')
        if esg_score:
            metrics['esg_score_percentile'] = min(esg_score / 100, 1.0)
            metrics['sustainability_grade'] = self._get_sustainability_grade(esg_score)
        
        # Carbon intensity analysis
        carbon_intensity = record.get('carbon_intensity')
        if carbon_intensity:
            metrics['carbon_intensity_category'] = self._categorize_carbon_intensity(carbon_intensity)
        
        # Green revenue analysis
        green_revenue = record.get('green_revenue_percentage')
        if green_revenue:
            metrics['green_revenue_category'] = self._categorize_green_revenue(green_revenue)
        
        return metrics

    def _get_sustainability_grade(self, esg_score: float) -> str:
        """Convert ESG score to letter grade"""
        if esg_score >= 90:
            return 'A+'
        elif esg_score >= 80:
            return 'A'
        elif esg_score >= 70:
            return 'B+'
        elif esg_score >= 60:
            return 'B'
        elif esg_score >= 50:
            return 'C+'
        elif esg_score >= 40:
            return 'C'
        else:
            return 'D'

    def _categorize_carbon_intensity(self, intensity: float) -> str:
        """Categorize carbon intensity"""
        if intensity <= 50:
            return 'very_low'
        elif intensity <= 100:
            return 'low'
        elif intensity <= 200:
            return 'moderate'
        elif intensity <= 400:
            return 'high'
        else:
            return 'very_high'

    def _categorize_green_revenue(self, percentage: float) -> str:
        """Categorize green revenue percentage"""
        if percentage >= 80:
            return 'predominantly_green'
        elif percentage >= 50:
            return 'majority_green'
        elif percentage >= 25:
            return 'significant_green'
        elif percentage >= 10:
            return 'some_green'
        else:
            return 'minimal_green'

    def _calculate_data_quality_score(self, record: Dict[str, Any]) -> float:
        """Calculate data quality score (0-1)"""
        score = 0.0
        max_score = 0.0
        
        # Required fields (40% weight)
        required_fields = ['ticker', 'fund_name', 'nav_price', 'asset_class']
        for field in required_fields:
            max_score += 0.1
            if record.get(field):
                score += 0.1
        
        # ESG data completeness (30% weight)
        esg_fields = ['esg_score', 'sustainability_score', 'carbon_intensity']
        for field in esg_fields:
            max_score += 0.1
            if record.get(field):
                score += 0.1
        
        # Fund metrics completeness (20% weight)
        fund_fields = ['total_assets', 'expense_ratio', 'dividend_yield']
        for field in fund_fields:
            max_score += 0.067
            if record.get(field):
                score += 0.067
        
        # Technical data completeness (10% weight)
        technical_fields = ['volume', 'daily_return']
        for field in technical_fields:
            max_score += 0.05
            if record.get(field):
                score += 0.05
        
        return score / max_score if max_score > 0 else 0.0

    def transform_all_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform all green bonds data (current + historical)"""
        start_time = datetime.now()
        logger.info("🚀 Starting comprehensive green bonds data transformation...")
        
        try:
            result = {
                'transformed_current_data': [],
                'transformed_historical_data': {},
                'validation_errors': [],
                'transformation_metadata': {}
            }
            
            # Transform current data
            if raw_data.get('current_etf_data'):
                validated_current, current_errors = self.validate_current_data(raw_data['current_etf_data'])
                result['transformed_current_data'] = self.standardize_current_data(validated_current)
                result['validation_errors'].extend(current_errors)
            
            # Transform historical data
            if raw_data.get('historical_data'):
                result['transformed_historical_data'] = self.transform_historical_data(raw_data['historical_data'])
            
            # Add transformation metadata
            end_time = datetime.now()
            duration = end_time - start_time
            
            result['transformation_metadata'] = {
                'transformation_timestamp': end_time.isoformat(),
                'transformation_duration_seconds': duration.total_seconds(),
                'records_processed': {
                    'current_data': len(result['transformed_current_data']),
                    'historical_data': sum(len(records) for records in result['transformed_historical_data'].values()),
                    'validation_errors': len(result['validation_errors'])
                },
                'transformer_version': '1.0.0_green_bonds',
                'config_used': {
                    'price_thresholds': [self.config.min_price_threshold, self.config.max_price_threshold],
                    'volatility_periods': self.config.volatility_periods,
                    'moving_average_periods': self.config.moving_average_periods
                }
            }
            
            logger.info("✅ Green bonds data transformation completed successfully!")
            return result
            
        except Exception as e:
            logger.error(f"❌ Green bonds transformation failed: {e}")
            raise

# Convenience function for standalone usage
def transform_green_bonds_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Standalone function to transform green bonds data"""
    transformer = GreenBondsTransformer()
    return transformer.transform_all_data(raw_data)

if __name__ == "__main__":
    # Test the transformer with sample data
    sample_data = {
        'current_etf_data': [
            {
                'ticker': 'BGRN',
                'fund_name': 'iShares Global Green Bond ETF',
                'nav_price': 45.32,
                'daily_return': 0.0123,
                'asset_class': 'green_bond',
                'fund_family': 'iShares',
                'esg_score': 85.5,
                'expense_ratio': 0.20,
                'total_assets': 1500000000,
                'volume': 125000
            }
        ],
        'historical_data': {
            'bgrn': [
                {
                    'record_date': '2025-06-01',
                    'close_price': 45.32,
                    'high_price': 45.50,
                    'low_price': 45.10,
                    'volume': 125000
                }
            ]
        }
    }
    
    transformer = GreenBondsTransformer()
    result = transformer.transform_all_data(sample_data)
    
    print("🌱 Green Bonds Transformer Test Results:")
    print(f"✅ Current records transformed: {len(result['transformed_current_data'])}")
    print(f"✅ Historical data transformed: {len(result['transformed_historical_data'])}")
    print(f"⚠️ Validation errors: {len(result['validation_errors'])}")