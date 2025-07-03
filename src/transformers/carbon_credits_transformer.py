"""
src/transformers/carbon_credits_transformer.py

Production-ready transformer for carbon credits and carbon offset data
Handles data cleaning, standardization, and market analysis for carbon markets
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
class CarbonCreditsTransformerConfig:
    """Configuration for carbon credits data transformation"""
    
    # Price validation thresholds
    min_price_per_tonne: float = 0.10  # $0.10 minimum price per tonne CO2e
    max_price_per_tonne: float = 500.0  # $500 maximum reasonable price
    
    # Volume validation
    min_volume_tonnes: float = 1.0  # Minimum 1 tonne transaction
    max_volume_tonnes: float = 10000000.0  # 10M tonnes max reasonable
    
    # Vintage year validation
    min_vintage_year: int = 2008  # Kyoto Protocol era start
    max_vintage_year: int = 2030  # Forward vintages
    
    # Price volatility thresholds
    max_daily_price_change: float = 0.50  # 50% max daily change
    
    # Standard types and methodologies
    standard_types = [
        'VCS', 'CDM', 'Gold Standard', 'CAR', 'ACR', 'VERRA', 
        'Plan Vivo', 'Social Carbon', 'REDD+', 'Panda Standard'
    ]
    
    project_types = [
        'Forestry and Land Use', 'Renewable Energy', 'Energy Efficiency',
        'Methane Avoidance', 'Fuel Switch', 'Waste Management',
        'Agriculture', 'Transport', 'Industrial Processes', 'Other'
    ]
    
    # Market segments
    compliance_markets = ['EU ETS', 'California Cap-and-Trade', 'RGGI', 'UK ETS', 'Koreans K-ETS']
    voluntary_markets = ['VCM', 'Voluntary Carbon Market', 'Private Offsetting']
    
    def __post_init__(self):
        self.current_year = datetime.now().year

class CarbonCreditsTransformer:
    """
    Production-ready transformer for carbon credits and offset data
    
    Features:
    - Comprehensive carbon credit validation
    - Market price analysis and benchmarking
    - Project type classification and standardization
    - Vintage analysis and premium calculation
    - Geographic region standardization
    - Quality score calculation based on standards
    - Market trend analysis
    """
    
    def __init__(self, config: Optional[CarbonCreditsTransformerConfig] = None):
        self.config = config or CarbonCreditsTransformerConfig()
        
        # Geographic region mappings
        self.region_mapping = {
            'africa': {
                'countries': ['kenya', 'south africa', 'nigeria', 'ghana', 'ethiopia', 'tanzania',
                            'uganda', 'malawi', 'zambia', 'zimbabwe', 'botswana', 'mozambique',
                            'madagascar', 'cameroon', 'democratic republic of congo', 'rwanda'],
                'region_code': 'AFR'
            },
            'asia_pacific': {
                'countries': ['china', 'india', 'indonesia', 'philippines', 'vietnam', 'thailand',
                            'malaysia', 'singapore', 'australia', 'new zealand', 'japan', 'south korea'],
                'region_code': 'APAC'
            },
            'latin_america': {
                'countries': ['brazil', 'mexico', 'colombia', 'peru', 'chile', 'argentina',
                            'costa rica', 'guatemala', 'ecuador', 'venezuela', 'bolivia'],
                'region_code': 'LATAM'
            },
            'north_america': {
                'countries': ['united states', 'canada', 'usa', 'us'],
                'region_code': 'NAM'
            },
            'europe': {
                'countries': ['germany', 'france', 'united kingdom', 'spain', 'italy', 'poland',
                            'netherlands', 'belgium', 'sweden', 'norway', 'denmark', 'finland'],
                'region_code': 'EUR'
            }
        }
        
        # Carbon project quality indicators
        self.quality_indicators = {
            'additionality': {
                'proven': 1.0,
                'likely': 0.8,
                'uncertain': 0.5,
                'questionable': 0.2
            },
            'permanence': {
                'high': 1.0,      # >100 years (geological storage)
                'medium': 0.8,    # 20-100 years (forestry with guarantees)
                'low': 0.5,       # <20 years (renewable energy)
                'buffer': 0.9     # Has buffer pool
            },
            'co_benefits': {
                'multiple': 1.0,  # Social + environmental + economic
                'some': 0.7,      # One or two types
                'minimal': 0.3,   # Limited additional benefits
                'none': 0.0       # No additional benefits
            }
        }
        
        # Standard quality ratings
        self.standard_quality_scores = {
            'Gold Standard': 0.95,
            'VCS': 0.85,
            'CDM': 0.80,
            'CAR': 0.90,
            'ACR': 0.88,
            'Plan Vivo': 0.85,
            'REDD+': 0.80,
            'Social Carbon': 0.75,
            'Panda Standard': 0.70,
            'Other': 0.50
        }
        
        logger.info("CarbonCreditsTransformer initialized")

    def validate_carbon_credits_data(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Validate and clean carbon credits data"""
        logger.info(f"Validating {len(data)} carbon credits records...")
        
        validated_data = []
        validation_errors = []
        
        for i, record in enumerate(data):
            errors = []
            
            # Required fields validation
            required_fields = ['project_name', 'price_per_tonne', 'standard_type', 'project_type']
            for field in required_fields:
                if not record.get(field):
                    errors.append(f"Missing required field: {field}")
            
            # Price validation
            price = record.get('price_per_tonne', 0)
            if not isinstance(price, (int, float)) or price <= 0:
                errors.append(f"Invalid price per tonne: {price}")
            elif price < self.config.min_price_per_tonne or price > self.config.max_price_per_tonne:
                errors.append(f"Price per tonne out of reasonable range: ${price}")
            
            # Volume validation
            volume = record.get('volume_tonnes')
            if volume:
                if volume < self.config.min_volume_tonnes or volume > self.config.max_volume_tonnes:
                    errors.append(f"Volume out of reasonable range: {volume} tonnes")
            
            # Vintage year validation
            vintage_year = record.get('vintage_year')
            if vintage_year:
                if vintage_year < self.config.min_vintage_year or vintage_year > self.config.max_vintage_year:
                    errors.append(f"Vintage year out of range: {vintage_year}")
            
            # Daily price change validation
            price_change = record.get('daily_price_change')
            if price_change and abs(price_change) > self.config.max_daily_price_change:
                errors.append(f"Extreme daily price change: {price_change:.2%}")
            
            # Standard type validation
            standard_type = record.get('standard_type', '')
            if standard_type and standard_type not in self.config.standard_types:
                errors.append(f"Unknown standard type: {standard_type}")
            
            if errors:
                validation_errors.extend([f"Record {i} ({record.get('project_name', 'Unknown')}): {error}" for error in errors])
                logger.warning(f"Validation errors for {record.get('project_name', 'Unknown')}: {errors}")
            else:
                validated_data.append(record)
        
        logger.info(f"✅ Validation complete: {len(validated_data)}/{len(data)} records passed")
        return validated_data, validation_errors

    def standardize_carbon_credits_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Standardize and enrich carbon credits data"""
        logger.info(f"Standardizing {len(data)} carbon credits records...")
        
        standardized_data = []
        
        for record in data:
            standardized_record = record.copy()
            
            # Standardize geographic information
            country = record.get('country', '').lower()
            region_info = self._get_region_info(country)
            standardized_record.update(region_info)
            
            # Standardize project type
            project_type = record.get('project_type', '')
            standardized_record['project_type_standardized'] = self._standardize_project_type(project_type)
            
            # Calculate vintage premium/discount
            vintage_year = record.get('vintage_year')
            if vintage_year:
                standardized_record['vintage_premium'] = self._calculate_vintage_premium(vintage_year)
                standardized_record['vintage_category'] = self._categorize_vintage(vintage_year)
            
            # Calculate quality score
            standardized_record['quality_score'] = self._calculate_quality_score(record)
            standardized_record['quality_tier'] = self._get_quality_tier(standardized_record['quality_score'])
            
            # Market classification
            standardized_record['market_type'] = self._classify_market_type(record)
            
            # Price analysis
            price = record.get('price_per_tonne', 0)
            standardized_record['price_category'] = self._categorize_price(price, standardized_record['project_type_standardized'])
            
            # Environmental impact metrics
            standardized_record.update(self._calculate_environmental_impact(record))
            
            # Risk assessment
            standardized_record.update(self._assess_project_risk(record))
            
            # Add co-benefits analysis
            standardized_record.update(self._analyze_co_benefits(record))
            
            # Add processing metadata
            standardized_record['processing_timestamp'] = datetime.now().isoformat()
            standardized_record['data_completeness_score'] = self._calculate_data_completeness(record)
            
            standardized_data.append(standardized_record)
        
        logger.info(f"✅ Standardization complete: {len(standardized_data)} records processed")
        return standardized_data

    def transform_carbon_market_data(self, market_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform carbon market price and trend data"""
        logger.info(f"Transforming {len(market_data)} carbon market records...")
        
        if not market_data:
            return []
        
        # Convert to DataFrame for time series analysis
        df = pd.DataFrame(market_data)
        
        # Ensure proper date handling
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
        
        # Calculate market indicators
        df = self._calculate_market_indicators(df)
        
        # Calculate volatility metrics
        df = self._calculate_carbon_volatility(df)
        
        # Add market sentiment indicators
        df = self._calculate_market_sentiment(df)
        
        # Convert back to list of dictionaries
        transformed_data = []
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
            
            transformed_data.append(record)
        
        logger.info(f"✅ Carbon market transformation complete: {len(transformed_data)} records")
        return transformed_data

    def _get_region_info(self, country: str) -> Dict[str, Any]:
        """Get standardized region information for country"""
        country_lower = country.lower()
        
        for region, info in self.region_mapping.items():
            if any(c in country_lower for c in info['countries']):
                return {
                    'region': region,
                    'region_code': info['region_code'],
                    'country_standardized': country.title()
                }
        
        return {
            'region': 'other',
            'region_code': 'OTH',
            'country_standardized': country.title()
        }

    def _standardize_project_type(self, project_type: str) -> str:
        """Standardize project type classification"""
        project_lower = project_type.lower()
        
        # Mapping rules for project types
        if any(keyword in project_lower for keyword in ['forest', 'afforestation', 'reforestation', 'redd', 'land use']):
            return 'Forestry and Land Use'
        elif any(keyword in project_lower for keyword in ['solar', 'wind', 'hydro', 'renewable', 'biomass', 'geothermal']):
            return 'Renewable Energy'
        elif any(keyword in project_lower for keyword in ['efficiency', 'energy saving', 'lighting', 'hvac']):
            return 'Energy Efficiency'
        elif any(keyword in project_lower for keyword in ['methane', 'landfill', 'livestock', 'biogas']):
            return 'Methane Avoidance'
        elif any(keyword in project_lower for keyword in ['fuel switch', 'fuel switching', 'coal to gas']):
            return 'Fuel Switch'
        elif any(keyword in project_lower for keyword in ['waste', 'recycling', 'composting']):
            return 'Waste Management'
        elif any(keyword in project_lower for keyword in ['agriculture', 'farming', 'soil', 'crop']):
            return 'Agriculture'
        elif any(keyword in project_lower for keyword in ['transport', 'vehicle', 'mobility', 'aviation']):
            return 'Transport'
        elif any(keyword in project_lower for keyword in ['industrial', 'manufacturing', 'cement', 'steel']):
            return 'Industrial Processes'
        else:
            return 'Other'

    def _calculate_vintage_premium(self, vintage_year: int) -> float:
        """Calculate vintage premium/discount based on age"""
        current_year = datetime.now().year
        age = current_year - vintage_year
        
        if age < 0:  # Future vintage
            return 0.05 + (abs(age) * 0.02)  # Premium for future vintages
        elif age <= 2:  # Recent vintages
            return 0.02  # Small premium for recent
        elif age <= 5:  # Standard vintages
            return 0.0   # No premium/discount
        elif age <= 10:  # Older vintages
            return -0.05 - (age - 5) * 0.01  # Increasing discount
        else:  # Very old vintages
            return -0.15  # Maximum discount

    def _categorize_vintage(self, vintage_year: int) -> str:
        """Categorize vintage by age"""
        current_year = datetime.now().year
        age = current_year - vintage_year
        
        if age < 0:
            return 'future'
        elif age <= 2:
            return 'recent'
        elif age <= 5:
            return 'standard'
        elif age <= 10:
            return 'aged'
        else:
            return 'legacy'

    def _calculate_quality_score(self, record: Dict[str, Any]) -> float:
        """Calculate comprehensive quality score for carbon credit"""
        score = 0.0
        
        # Standard quality (40% weight)
        standard = record.get('standard_type', 'Other')
        score += self.standard_quality_scores.get(standard, 0.5) * 0.4
        
        # Additionality (25% weight)
        additionality = record.get('additionality_assessment', 'uncertain')
        score += self.quality_indicators['additionality'].get(additionality, 0.5) * 0.25
        
        # Permanence (20% weight)
        permanence = record.get('permanence_rating', 'low')
        score += self.quality_indicators['permanence'].get(permanence, 0.5) * 0.2
        
        # Co-benefits (15% weight)
        co_benefits = record.get('co_benefits_level', 'minimal')
        score += self.quality_indicators['co_benefits'].get(co_benefits, 0.3) * 0.15
        
        return min(score, 1.0)

    def _get_quality_tier(self, quality_score: float) -> str:
        """Get quality tier based on score"""
        if quality_score >= 0.9:
            return 'premium'
        elif quality_score >= 0.75:
            return 'high'
        elif quality_score >= 0.6:
            return 'standard'
        elif quality_score >= 0.4:
            return 'basic'
        else:
            return 'low'

    def _classify_market_type(self, record: Dict[str, Any]) -> str:
        """Classify whether compliance or voluntary market"""
        registry = record.get('registry', '').upper()
        standard = record.get('standard_type', '').upper()
        
        if any(market in registry for market in ['EU ETS', 'RGGI', 'CAP-AND-TRADE']):
            return 'compliance'
        elif 'CDM' in standard:
            return 'compliance'
        else:
            return 'voluntary'

    def _categorize_price(self, price: float, project_type: str) -> str:
        """Categorize price relative to project type benchmarks"""
        # Benchmark prices by project type (rough market averages)
        benchmarks = {
            'Forestry and Land Use': 8.0,
            'Renewable Energy': 3.0,
            'Energy Efficiency': 4.0,
            'Methane Avoidance': 6.0,
            'Fuel Switch': 5.0,
            'Waste Management': 7.0,
            'Agriculture': 12.0,
            'Transport': 15.0,
            'Industrial Processes': 10.0,
            'Other': 5.0
        }
        
        benchmark = benchmarks.get(project_type, 5.0)
        ratio = price / benchmark
        
        if ratio >= 2.0:
            return 'premium'
        elif ratio >= 1.5:
            return 'high'
        elif ratio >= 0.8:
            return 'market'
        elif ratio >= 0.5:
            return 'discount'
        else:
            return 'deep_discount'

    def _calculate_environmental_impact(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate environmental impact metrics"""
        metrics = {}
        
        volume = record.get('volume_tonnes', 0)
        project_type = record.get('project_type', '')
        
        # CO2 equivalent calculations
        metrics['co2_equivalent_tonnes'] = volume
        
        # Estimate additional environmental benefits by project type
        if 'forest' in project_type.lower():
            metrics['estimated_trees_protected'] = volume * 40  # ~40 trees per tonne CO2
            metrics['biodiversity_impact'] = 'high'
        elif 'renewable' in project_type.lower():
            metrics['estimated_kwh_clean_energy'] = volume * 1000  # ~1000 kWh per tonne CO2
            metrics['air_quality_impact'] = 'medium'
        elif 'methane' in project_type.lower():
            metrics['methane_tonnes_avoided'] = volume * 0.04  # Methane is ~25x more potent
            metrics['local_pollution_impact'] = 'high'
        
        return metrics

    def _assess_project_risk(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Assess project risk factors"""
        risk_assessment = {}
        
        # Country risk (based on region)
        region = self._get_region_info(record.get('country', ''))['region']
        country_risk_scores = {
            'north_america': 'low',
            'europe': 'low',
            'asia_pacific': 'medium',
            'latin_america': 'medium',
            'africa': 'high',
            'other': 'high'
        }
        risk_assessment['country_risk'] = country_risk_scores.get(region, 'high')
        
        # Project type risk
        project_type = record.get('project_type', '')
        if 'forest' in project_type.lower():
            risk_assessment['permanence_risk'] = 'high'  # Fire, deforestation risk
        elif 'renewable' in project_type.lower():
            risk_assessment['permanence_risk'] = 'low'   # Already emitted reductions
        else:
            risk_assessment['permanence_risk'] = 'medium'
        
        # Vintage risk
        vintage_year = record.get('vintage_year')
        if vintage_year:
            age = datetime.now().year - vintage_year
            if age > 10:
                risk_assessment['vintage_risk'] = 'high'
            elif age > 5:
                risk_assessment['vintage_risk'] = 'medium'
            else:
                risk_assessment['vintage_risk'] = 'low'
        
        # Overall risk score
        risk_scores = {'low': 1, 'medium': 2, 'high': 3}
        total_risk = sum(risk_scores.get(risk, 2) for risk in risk_assessment.values())
        avg_risk = total_risk / len(risk_assessment)
        
        if avg_risk <= 1.5:
            risk_assessment['overall_risk'] = 'low'
        elif avg_risk <= 2.5:
            risk_assessment['overall_risk'] = 'medium'
        else:
            risk_assessment['overall_risk'] = 'high'
        
        return risk_assessment

    def _analyze_co_benefits(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze social and environmental co-benefits"""
        co_benefits = {}
        
        project_type = record.get('project_type', '').lower()
        region = self._get_region_info(record.get('country', ''))['region']
        
        # Social benefits
        if region in ['africa', 'latin_america', 'asia_pacific']:
            co_benefits['community_development'] = 'high'
            co_benefits['poverty_alleviation'] = 'medium'
        else:
            co_benefits['community_development'] = 'low'
            co_benefits['poverty_alleviation'] = 'low'
        
        # Environmental benefits
        if 'forest' in project_type:
            co_benefits['biodiversity_protection'] = 'high'
            co_benefits['water_cycle_protection'] = 'high'
            co_benefits['soil_conservation'] = 'high'
        elif 'renewable' in project_type:
            co_benefits['air_quality_improvement'] = 'high'
            co_benefits['energy_access'] = 'medium'
        elif 'agriculture' in project_type:
            co_benefits['food_security'] = 'medium'
            co_benefits['soil_health'] = 'high'
        
        # Economic benefits
        if region in ['africa', 'latin_america']:
            co_benefits['job_creation'] = 'high'
            co_benefits['economic_development'] = 'medium'
        
        return co_benefits

    def _calculate_data_completeness(self, record: Dict[str, Any]) -> float:
        """Calculate data completeness score"""
        required_fields = ['project_name', 'price_per_tonne', 'standard_type', 'project_type', 'country']
        optional_fields = ['vintage_year', 'volume_tonnes', 'registry', 'methodology', 'verification_date']
        
        required_score = sum(1 for field in required_fields if record.get(field)) / len(required_fields)
        optional_score = sum(1 for field in optional_fields if record.get(field)) / len(optional_fields)
        
        # Weight required fields more heavily
        return (required_score * 0.7) + (optional_score * 0.3)

    def _calculate_market_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate carbon market indicators"""
        if 'price_per_tonne' not in df.columns:
            return df
        
        # Price moving averages
        for window in [7, 30, 90]:
            df[f'price_ma_{window}d'] = df['price_per_tonne'].rolling(window=window).mean()
        
        # Price returns
        df['daily_return'] = df['price_per_tonne'].pct_change()
        df['weekly_return'] = df['price_per_tonne'].pct_change(periods=7)
        df['monthly_return'] = df['price_per_tonne'].pct_change(periods=30)
        
        # Price momentum
        df['momentum_14d'] = df['price_per_tonne'] / df['price_per_tonne'].shift(14) - 1
        
        return df

    def _calculate_carbon_volatility(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate carbon market volatility metrics"""
        if 'daily_return' not in df.columns:
            return df
        
        # Rolling volatility (annualized)
        for window in [30, 90, 252]:
            df[f'volatility_{window}d'] = df['daily_return'].rolling(window=window).std() * np.sqrt(252)
        
        # Price range indicators
        df['price_range_7d'] = df['price_per_tonne'].rolling(window=7).max() - df['price_per_tonne'].rolling(window=7).min()
        df['price_range_30d'] = df['price_per_tonne'].rolling(window=30).max() - df['price_per_tonne'].rolling(window=30).min()
        
        return df

    def _calculate_market_sentiment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate market sentiment indicators"""
        if 'price_per_tonne' not in df.columns:
            return df
        
        # Price trend indicators
        df['trend_7d'] = np.where(df['price_per_tonne'] > df['price_ma_7d'], 'bullish', 'bearish')
        df['trend_30d'] = np.where(df['price_per_tonne'] > df['price_ma_30d'], 'bullish', 'bearish')
        
        # Market pressure indicators
        df['market_pressure'] = np.where(df['daily_return'] > 0.05, 'buying_pressure',
                                       np.where(df['daily_return'] < -0.05, 'selling_pressure', 'neutral'))
        
        return df

    def transform_all_carbon_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform all carbon credits data"""
        start_time = datetime.now()
        logger.info("🚀 Starting comprehensive carbon credits data transformation...")
        
        try:
            result = {
                'transformed_credits_data': [],
                'transformed_market_data': [],
                'validation_errors': [],
                'transformation_metadata': {}
            }
            
            # Transform carbon credits data
            if raw_data.get('carbon_credits_data'):
                validated_credits, credits_errors = self.validate_carbon_credits_data(raw_data['carbon_credits_data'])
                result['transformed_credits_data'] = self.standardize_carbon_credits_data(validated_credits)
                result['validation_errors'].extend(credits_errors)
            
            # Transform market data
            if raw_data.get('market_data'):
                result['transformed_market_data'] = self.transform_carbon_market_data(raw_data['market_data'])
            
            # Add transformation metadata
            end_time = datetime.now()
            duration = end_time - start_time
            
            result['transformation_metadata'] = {
                'transformation_timestamp': end_time.isoformat(),
                'transformation_duration_seconds': duration.total_seconds(),
                'records_processed': {
                    'credits_data': len(result['transformed_credits_data']),
                    'market_data': len(result['transformed_market_data']),
                    'validation_errors': len(result['validation_errors'])
                },
                'transformer_version': '1.0.0_carbon_credits',
                'regions_processed': list(set(record.get('region', 'unknown') for record in result['transformed_credits_data'])),
                'project_types_processed': list(set(record.get('project_type_standardized', 'unknown') for record in result['transformed_credits_data']))
            }
            
            logger.info("✅ Carbon credits data transformation completed successfully!")
            return result
            
        except Exception as e:
            logger.error(f"❌ Carbon credits transformation failed: {e}")
            raise

# Convenience function for standalone usage
def transform_carbon_credits_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Standalone function to transform carbon credits data"""
    transformer = CarbonCreditsTransformer()
    return transformer.transform_all_carbon_data(raw_data)

if __name__ == "__main__":
    # Test the transformer with sample data
    sample_data = {
        'carbon_credits_data': [
            {
                'project_name': 'Kenya Forest Conservation Project',
                'price_per_tonne': 12.50,
                'standard_type': 'VCS',
                'project_type': 'Forestry and Land Use',
                'country': 'Kenya',
                'vintage_year': 2023,
                'volume_tonnes': 10000,
                'additionality_assessment': 'proven',
                'permanence_rating': 'medium',
                'co_benefits_level': 'multiple'
            }
        ],
        'market_data': [
            {
                'date': '2025-06-01',
                'price_per_tonne': 12.50,
                'volume_traded': 1000,
                'market_type': 'voluntary'
            }
        ]
    }
    
    transformer = CarbonCreditsTransformer()
    result = transformer.transform_all_carbon_data(sample_data)
    
    print("🌍 Carbon Credits Transformer Test Results:")
    print(f"✅ Credits records transformed: {len(result['transformed_credits_data'])}")
    print(f"✅ Market records transformed: {len(result['transformed_market_data'])}")
    print(f"⚠️ Validation errors: {len(result['validation_errors'])}")
    
    if result['transformed_credits_data']:
        sample_record = result['transformed_credits_data'][0]
        print(f"📊 Sample quality score: {sample_record.get('quality_score', 0):.2f}")
        print(f"🏆 Quality tier: {sample_record.get('quality_tier', 'unknown')}")
        print(f"🌍 Region: {sample_record.get('region', 'unknown')}")