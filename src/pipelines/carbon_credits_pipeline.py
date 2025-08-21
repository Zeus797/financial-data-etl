"""
scripts/simple_carbon_pipeline.py

Simplified Carbon Credits Pipeline for Static Platform Data
Handles platform-based carbon credit pricing with minimal overhead
"""

import pandas as pd
import logging
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from pathlib import Path
import json
import os

# Database imports
from src.models.base import db_manager
from src.models.universal_models import (
    DimEntity, DimFinancialInstrument, DimCurrency, 
    DimCountry, DimDate, FactCarbonOffset, get_date_key, 
    create_date_dimension_record
)
 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleCarbonPipeline:
    """
    Simplified pipeline for static carbon credits platform data
    
    Features:
    - Platform-based data loading (not individual projects)
    - Manual data entry or CSV import
    - Minimal processing overhead
    - Static data with periodic updates
    """
    
    def __init__(self):
        self.db_manager = db_manager
        
        # Static platform data structure
        self.platform_data = [
            {
                'platform': 'Terrapass',
                'min_investment': 5.00,
                'price_per_tonne': 12.99,
                'project_types': ['Renewable Energy', 'Forestry', 'Methane Capture'],
                'regions': ['North America', 'South America'],
                'certifications': ['Gold Standard', 'Verified Carbon Standard'],
                'last_updated': '2025-06-19 15:12:18'
            },
            {
                'platform': 'Carbonfund',
                'min_investment': 10.00,
                'price_per_tonne': 10.00,
                'project_types': ['Forestry', 'Renewable Energy'],
                'regions': ['North America', 'South America', 'Africa'],
                'certifications': ['American Carbon Registry', 'Verified Carbon Standard'],
                'last_updated': '2025-06-19 15:12:18'
            },
            {
                'platform': 'Pachama',
                'min_investment': 10.00,
                'price_per_tonne': 15.00,
                'project_types': ['Forestry', 'Reforestation'],
                'regions': ['South America', 'North America', 'Africa'],
                'certifications': ['Verified Carbon Standard', 'Climate Action Reserve'],
                'last_updated': '2025-06-19 15:12:18'
            },
            {
                'platform': 'Klima DAO',
                'min_investment': 15.00,
                'price_per_tonne': 18.75,
                'project_types': ['Forestry', 'Renewable Energy', 'Methane Capture'],
                'regions': ['Global (Blockchain-based)'],
                'certifications': ['Verra', 'Gold Standard'],
                'last_updated': '2025-06-19 15:12:18'
            },
            {
                'platform': 'Moss.Earth',
                'min_investment': 5.00,
                'price_per_tonne': 16.50,
                'project_types': ['Forestry', 'Biodiversity'],
                'regions': ['South America', 'Africa'],
                'certifications': ['Verified Carbon Standard'],
                'last_updated': '2025-06-19 15:12:18'
            }
        ]
        
        logger.info("SimpleCarbonPipeline initialized")

    def run_simple_pipeline(self, data_source: str = "static") -> Dict[str, Any]:
        """
        Run simplified pipeline with minimal overhead
        
        Args:
            data_source: "static", "csv", or "manual"
        """
        start_time = datetime.now()
        logger.info("🌍 Starting simplified carbon credits pipeline...")
        
        try:
            # Step 1: Get data based on source
            if data_source == "csv":
                data = self._load_from_csv()
            elif data_source == "manual":
                data = self._get_manual_data()
            else:
                data = self.platform_data
            
            # Step 2: Process and load data
            results = self._process_and_load(data)
            
            # Step 3: Generate summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            summary = {
                'pipeline_info': {
                    'pipeline_type': 'simplified_carbon_credits',
                    'execution_time': end_time.isoformat(),
                    'duration_seconds': duration,
                    'status': 'SUCCESS'
                },
                'data_summary': {
                    'platforms_processed': len(data),
                    'records_loaded': results['records_loaded'],
                    'data_source': data_source
                },
                'platform_details': {
                    'platforms': [item['platform'] for item in data],
                    'price_range': {
                        'min': min(item['price_per_tonne'] for item in data),
                        'max': max(item['price_per_tonne'] for item in data),
                        'avg': sum(item['price_per_tonne'] for item in data) / len(data)
                    }
                }
            }
            
            logger.info("✅ Simplified carbon credits pipeline completed successfully!")
            return summary
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            return {
                'pipeline_info': {
                    'pipeline_type': 'simplified_carbon_credits',
                    'status': 'FAILED',
                    'error': str(e)
                }
            }

    def _load_from_csv(self) -> List[Dict[str, Any]]:
        """Load data from CSV file"""
        csv_path = Path("data/carbon_credits/platform_data.csv")
        
        if not csv_path.exists():
            logger.warning(f"CSV file not found: {csv_path}")
            logger.info("Creating sample CSV file...")
            self._create_sample_csv(csv_path)
            
        df = pd.read_csv(csv_path)
        
        # Convert DataFrame to list of dictionaries
        data = []
        for _, row in df.iterrows():
            record = {
                'platform': row['Platform'],
                'min_investment': float(row['Min_Investment']),
                'price_per_tonne': float(row['Price_per_Tonne']),
                'project_types': row['Project_Types'].split(', ') if pd.notna(row['Project_Types']) else [],
                'regions': row['Regions'].split(', ') if pd.notna(row['Regions']) else [],
                'certifications': row['Certifications'].split(', ') if pd.notna(row['Certifications']) else [],
                'last_updated': row.get('Last_Updated', datetime.now().isoformat())
            }
            data.append(record)
        
        logger.info(f"📄 Loaded {len(data)} platforms from CSV")
        return data

    def _get_manual_data(self) -> List[Dict[str, Any]]:
        """Get data through manual entry (interactive)"""
        logger.info("📝 Manual data entry mode")
        
        data = []
        
        while True:
            print("\n" + "="*50)
            print("🌍 Carbon Credits Platform Entry")
            print("="*50)
            
            platform = input("Platform name (or 'done' to finish): ").strip()
            if platform.lower() == 'done':
                break
            
            try:
                min_investment = float(input("Minimum investment ($): "))
                price_per_tonne = float(input("Price per tonne ($/tCO2e): "))
                project_types = input("Project types (comma-separated): ").split(', ')
                regions = input("Regions (comma-separated): ").split(', ')
                certifications = input("Certifications (comma-separated): ").split(', ')
                
                record = {
                    'platform': platform,
                    'min_investment': min_investment,
                    'price_per_tonne': price_per_tonne,
                    'project_types': project_types,
                    'regions': regions,
                    'certifications': certifications,
                    'last_updated': datetime.now().isoformat()
                }
                
                data.append(record)
                print(f"✅ Added {platform}")
                
            except ValueError as e:
                print(f"❌ Invalid input: {e}")
                continue
        
        logger.info(f"📝 Collected {len(data)} platforms manually")
        return data

    def _process_and_load(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process and load data into database"""
        logger.info("💾 Processing and loading data...")
        
        # Ensure tables exist
        self.db_manager.create_tables()
        
        records_loaded = 0
        
        with self.db_manager.get_session() as session:
            
            # Process each platform
            for platform_data in data:
                try:
                    # Create or get platform entity
                    entity = self._ensure_platform_entity(session, platform_data)
                    
                    # Create or get financial instrument
                    instrument = self._ensure_platform_instrument(session, platform_data, entity)
                    
                    # Create or get date record
                    today = date.today()
                    date_key = get_date_key(today)
                    date_record = self._ensure_date_record(session, today, date_key)
                    
                    # Create or update fact record
                    fact_record = self._create_or_update_fact_record(
                        session, instrument, date_key, platform_data
                    )
                    
                    records_loaded += 1
                    logger.info(f"✅ Processed platform: {platform_data['platform']}")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to process {platform_data['platform']}: {e}")
                    continue
            
            session.commit()
        
        logger.info(f"💾 Loaded {records_loaded} platform records")
        return {'records_loaded': records_loaded}

    def _ensure_platform_entity(self, session, platform_data: Dict[str, Any]) -> DimEntity:
        """Ensure platform entity exists"""
        platform_name = platform_data['platform']
        
        entity = session.query(DimEntity).filter_by(
            entity_name=platform_name,
            entity_type='carbon_platform'
        ).first()
        
        if not entity:
            entity = DimEntity(
                entity_name=platform_name,
                entity_type='carbon_platform',
                country_code='US',  # Most platforms are US-based
                is_active=True
            )
            session.add(entity)
            session.flush()
        
        return entity

    def _ensure_platform_instrument(self, session, platform_data: Dict[str, Any], entity: DimEntity) -> DimFinancialInstrument:
        """Ensure platform instrument exists"""
        platform_name = platform_data['platform']
        instrument_code = f"CARBON_{platform_name.upper().replace(' ', '_')}"
        
        instrument = session.query(DimFinancialInstrument).filter_by(
            instrument_code=instrument_code
        ).first()
        
        if not instrument:
            instrument = DimFinancialInstrument(
                instrument_type='carbon_platform',
                instrument_name=f"{platform_name} Carbon Credits",
                instrument_code=instrument_code,
                display_name=f"{platform_name} Carbon Offsets",
                primary_currency_code='USD',
                issuer_entity_id=entity.entity_id,
                asset_class='carbon_credit',
                risk_level='medium',
                description=f"Carbon credits from {platform_name} platform",
                is_active=True
            )
            session.add(instrument)
            session.flush()
        
        return instrument

    def _ensure_date_record(self, session, date_obj: date, date_key: int) -> DimDate:
        """Ensure date record exists"""
        date_record = session.query(DimDate).filter_by(date_key=date_key).first()
        
        if not date_record:
            date_record = create_date_dimension_record(date_obj)
            session.add(date_record)
            session.flush()
        
        return date_record

    def _create_or_update_fact_record(self, session, instrument: DimFinancialInstrument, 
                                    date_key: int, platform_data: Dict[str, Any]) -> FactCarbonOffset:
        """Create or update fact record"""
        
        # Check if record exists for today
        existing = session.query(FactCarbonOffset).filter_by(
            instrument_id=instrument.instrument_id,
            date_key=date_key
        ).first()
        
        if existing:
            # Update existing record
            existing.price_per_tonne = platform_data['price_per_tonne']
            existing.project_type = platform_data['project_types'][0] if platform_data['project_types'] else 'Mixed'
            existing.project_location = platform_data['regions'][0] if platform_data['regions'] else 'Global'
            existing.standard = platform_data['certifications'][0] if platform_data['certifications'] else 'VCS'
            existing.co_benefits = ', '.join(platform_data['project_types'])
            existing.platform = platform_data['platform']
            existing.verification_date = date.today()
            return existing
        else:
            # Create new record
            fact_record = FactCarbonOffset(
                instrument_id=instrument.instrument_id,
                date_key=date_key,
                record_date=date.today(),
                
                # Pricing data
                price_per_tonne=platform_data['price_per_tonne'],
                volume_traded=1000.0,  # Default volume
                total_value=platform_data['price_per_tonne'] * 1000.0,
                
                # Project details
                project_type=platform_data['project_types'][0] if platform_data['project_types'] else 'Mixed',
                project_location=platform_data['regions'][0] if platform_data['regions'] else 'Global',
                project_vintage=2023,  # Default vintage
                methodology=platform_data['certifications'][0] if platform_data['certifications'] else 'VCS',
                
                # Certification
                standard=platform_data['certifications'][0] if platform_data['certifications'] else 'VCS',
                additionality_verified=True,
                permanence_rating='Medium',
                co_benefits=', '.join(platform_data['project_types']),
                
                # Supply data
                credits_issued=10000.0,
                credits_retired=0.0,
                credits_available=10000.0,
                retirement_rate=0.0,
                
                # Market data
                market_share=5.0,
                buyer_type='Corporate',
                geographic_demand=platform_data['regions'][0] if platform_data['regions'] else 'Global',
                
                # Risk factors
                reversal_risk=0.05,
                political_risk='Low',
                delivery_risk='Low',
                
                # Impact metrics
                co2_equivalent_tonnes=1000.0,
                monitoring_frequency='Annual',
                verification_date=date.today(),
                
                # Platform data
                platform=platform_data['platform'],
                transaction_fee=0.03,  # 3% fee
                settlement_period=7,  # 7 days
                
                # Metadata
                data_source='platform_data',
                quality_score=0.85
            )
            
            session.add(fact_record)
            return fact_record

    def _create_sample_csv(self, csv_path: Path):
        """Create sample CSV file"""
        os.makedirs(csv_path.parent, exist_ok=True)
        
        df = pd.DataFrame([
            {
                'Platform': 'Terrapass',
                'Min_Investment': 5.00,
                'Price_per_Tonne': 12.99,
                'Project_Types': 'Renewable Energy, Forestry, Methane Capture',
                'Regions': 'North America, South America',
                'Certifications': 'Gold Standard, Verified Carbon Standard',
                'Last_Updated': '2025-06-19 15:12:18'
            },
            {
                'Platform': 'Carbonfund',
                'Min_Investment': 10.00,
                'Price_per_Tonne': 10.00,
                'Project_Types': 'Forestry, Renewable Energy',
                'Regions': 'North America, South America, Africa',
                'Certifications': 'American Carbon Registry, Verified Carbon Standard',
                'Last_Updated': '2025-06-19 15:12:18'
            }
        ])
        
        df.to_csv(csv_path, index=False)
        logger.info(f"📄 Created sample CSV: {csv_path}")

    def export_current_data(self, output_path: str = "data/carbon_credits/exported_data.csv"):
        """Export current database data to CSV"""
        logger.info("📤 Exporting current data...")
        
        with self.db_manager.get_session() as session:
            # Query current data
            results = session.query(
                FactCarbonOffset.platform,
                FactCarbonOffset.price_per_tonne,
                FactCarbonOffset.project_type,
                FactCarbonOffset.project_location,
                FactCarbonOffset.standard,
                FactCarbonOffset.co_benefits,
                FactCarbonOffset.verification_date
            ).filter(
                FactCarbonOffset.record_date == date.today()
            ).all()
            
            if results:
                df = pd.DataFrame(results, columns=[
                    'Platform', 'Price_per_Tonne', 'Project_Type', 
                    'Project_Location', 'Standard', 'Co_Benefits', 'Last_Updated'
                ])
                
                os.makedirs(Path(output_path).parent, exist_ok=True)
                df.to_csv(output_path, index=False)
                logger.info(f"📤 Exported {len(results)} records to {output_path}")
            else:
                logger.warning("No data found to export")

    def get_platform_summary(self) -> Dict[str, Any]:
        """Get summary of platform data"""
        with self.db_manager.get_session() as session:
            results = session.query(
                FactCarbonOffset.platform,
                FactCarbonOffset.price_per_tonne
            ).filter(
                FactCarbonOffset.record_date == date.today()
            ).all()
            
            if results:
                prices = [r.price_per_tonne for r in results]
                return {
                    'total_platforms': len(results),
                    'price_range': {
                        'min': min(prices),
                        'max': max(prices),
                        'avg': sum(prices) / len(prices)
                    },
                    'platforms': [r.platform for r in results]
                }
            else:
                return {'total_platforms': 0, 'price_range': None, 'platforms': []}


def run_simple_carbon_pipeline(data_source: str = "static") -> Dict[str, Any]:
    """Convenience function to run simplified pipeline"""
    pipeline = SimpleCarbonPipeline()
    return pipeline.run_simple_pipeline(data_source)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Simplified Carbon Credits Pipeline")
    parser.add_argument("--source", choices=["static", "csv", "manual"], default="static",
                       help="Data source: static (hardcoded), csv (file), or manual (interactive)")
    parser.add_argument("--export", action="store_true", help="Export current data to CSV")
    parser.add_argument("--summary", action="store_true", help="Show platform summary")
    
    args = parser.parse_args()
    
    pipeline = SimpleCarbonPipeline()
    
    if args.export:
        pipeline.export_current_data()
    elif args.summary:
        summary = pipeline.get_platform_summary()
        print("\n🌍 Carbon Credits Platform Summary:")
        print(f"Total platforms: {summary['total_platforms']}")
        if summary['price_range']:
            print(f"Price range: ${summary['price_range']['min']:.2f} - ${summary['price_range']['max']:.2f}")
            print(f"Average price: ${summary['price_range']['avg']:.2f}")
        print(f"Platforms: {', '.join(summary['platforms'])}")
    else:
        results = pipeline.run_simple_pipeline(args.source)
        
        print("\n" + "="*60)
        print("🌍 SIMPLIFIED CARBON CREDITS PIPELINE RESULTS")
        print("="*60)
        print(f"Status: {results['pipeline_info']['status']}")
        print(f"Duration: {results['pipeline_info'].get('duration_seconds', 0):.2f} seconds")
        
        if 'data_summary' in results:
            print(f"Platforms processed: {results['data_summary']['platforms_processed']}")
            print(f"Records loaded: {results['data_summary']['records_loaded']}")
            print(f"Data source: {results['data_summary']['data_source']}")
            
        if 'platform_details' in results:
            print(f"Price range: ${results['platform_details']['price_range']['min']:.2f} - ${results['platform_details']['price_range']['max']:.2f}")
            print(f"Average price: ${results['platform_details']['price_range']['avg']:.2f}")
            print(f"Platforms: {', '.join(results['platform_details']['platforms'])}")
        
        print("="*60)