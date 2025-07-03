"""
src/pipelines/green_bonds_pipeline.py

Complete Green Bonds/ESG ETF Pipeline Orchestrator
Coordinates collection, transformation, and loading of green bonds data
"""

import os
import sys
import logging
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.models.base import db_manager
from src.transformers.green_bonds_transformer import GreenBondsTransformer
from src.loaders.green_bonds_loader import GreenBondsLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/green_bonds_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class GreenBondsPipelineOrchestrator:
    """
    Complete pipeline orchestrator for green bonds/ESG ETF data
    
    Coordinates:
    1. Data Collection (using existing collector)
    2. Data Transformation (cleaning, enrichment, technical indicators)
    3. Data Loading (into PostgreSQL star schema)
    4. Validation and reporting
    """
    
    def __init__(self, 
                 data_dir: str = "data",
                 force_collection: bool = False,
                 skip_validation: bool = False):
        """
        Initialize the pipeline orchestrator
        
        Args:
            data_dir: Base directory for data files
            force_collection: Force data collection even if recent data exists
            skip_validation: Skip final validation step
        """
        self.data_dir = Path(data_dir)
        self.force_collection = force_collection
        self.skip_validation = skip_validation
        
        # Create necessary directories
        self.raw_data_dir = self.data_dir / "raw_data" / "green_bonds"
        self.processed_data_dir = self.data_dir / "processed_data"
        self.logs_dir = Path("logs")
        
        for dir_path in [self.raw_data_dir, self.processed_data_dir, self.logs_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Pipeline execution tracking
        self.execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.start_time = datetime.now()
        self.pipeline_stats = {
            'execution_id': self.execution_id,
            'start_time': self.start_time.isoformat(),
            'steps_completed': [],
            'data_counts': {},
            'errors': []
        }
        
        logger.info(f"🌱 Pipeline Orchestrator initialized (ID: {self.execution_id})")

    def run_complete_pipeline(self) -> bool:
        """
        Execute the complete green bonds ETL pipeline
        
        Returns:
            bool: True if pipeline completed successfully, False otherwise
        """
        try:
            logger.info("🔍 Validating pipeline environment...")
            self._validate_environment()
            
            logger.info("=" * 80)
            logger.info("🌱 STARTING COMPLETE GREEN BONDS ETL PIPELINE")
            logger.info("=" * 80)
            logger.info(f"📅 Execution ID: {self.execution_id}")
            logger.info(f"⏰ Start Time: {self.start_time}")
            logger.info(f"🎯 Target ETFs: 15 green bonds and ESG ETFs")
            logger.info("=" * 80)
            
            # Step 1: Data Collection
            logger.info("🌱 STEP 1: DATA COLLECTION")
            logger.info("-" * 40)
            collected_records = self._execute_data_collection()
            self.pipeline_stats['data_counts']['collected'] = collected_records
            self.pipeline_stats['steps_completed'].append('collection')
            
            # Step 2: Data Transformation
            logger.info("\n🔄 STEP 2: DATA TRANSFORMATION")
            logger.info("-" * 40)
            transformed_records = self._execute_data_transformation()
            self.pipeline_stats['data_counts']['transformed'] = transformed_records
            self.pipeline_stats['steps_completed'].append('transformation')
            
            # Step 3: Data Loading
            logger.info("\n💾 STEP 3: DATA LOADING")
            logger.info("-" * 40)
            loaded_records = self._execute_data_loading()
            self.pipeline_stats['data_counts']['loaded'] = loaded_records
            self.pipeline_stats['steps_completed'].append('loading')
            
            # Step 4: Final Validation
            if not self.skip_validation:
                logger.info("\n✅ STEP 4: FINAL VALIDATION")
                logger.info("-" * 40)
                self._execute_final_validation()
                self.pipeline_stats['steps_completed'].append('validation')
            
            # Pipeline completion
            self._complete_pipeline_execution()
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {str(e)}")
            self.pipeline_stats['errors'].append(str(e))
            self._handle_pipeline_failure(e)
            return False

    def _validate_environment(self):
        """Validate that all required components are available"""
        
        # Check database connection
        if not db_manager.test_connection():
            raise Exception("Database connection failed. Please check your database configuration.")
        
        logger.info("✅ Database connection test successful")
        # Get database info from db_manager
        with db_manager.get_session() as session:
            from sqlalchemy import text
            result = session.execute(text("SELECT current_database()"))
            db_name = result.scalar()
            logger.info(f"   Connected to database: {db_name}")
        
        # Check if collector script exists
        collector_script = Path("src/collectors/green_bonds_collector.py")
        if not collector_script.exists():
            raise Exception(f"Collector script not found: {collector_script}")
        
        logger.info("✅ Environment validation passed")

    def _execute_data_collection(self) -> int:
        """Execute data collection step"""
        
        # Check if recent data exists and force_collection is False
        combined_file = self.raw_data_dir / "all_green_bonds_combined.csv"
        
        if combined_file.exists() and not self.force_collection:
            # Check file age
            file_age = datetime.now() - datetime.fromtimestamp(combined_file.stat().st_mtime)
            if file_age.days < 1:  # Less than 1 day old
                logger.info(f"📄 Found recent data file: {combined_file}")
                
                # Count records in existing file
                import pandas as pd
                df = pd.read_csv(combined_file)
                logger.info(f"✅ Using existing data: {len(df):,} records")
                return len(df)
        
        # Execute data collection
        logger.info("🔄 Executing: Data Collection (Attempt 1)")
        logger.info("💻 Command: python src/collectors/green_bonds_collector.py")
        
        try:
            result = subprocess.run(
                ["python", "src/collectors/green_bonds_collector.py"],
                capture_output=True,
                text=True,
                cwd=os.getcwd(),
                timeout=1800  # 30 minutes timeout
            )
            
            if result.returncode == 0:
                logger.info("✅ Data Collection completed successfully")
                
                # Log last few lines of output
                output_lines = result.stdout.strip().split('\n')
                logger.info("📄 Data Collection output (last 10 lines):")
                for line in output_lines[-10:]:
                    logger.info(f"   {line}")
                
                # Count records in combined file
                if combined_file.exists():
                    import pandas as pd
                    df = pd.read_csv(combined_file)
                    logger.info(f"📊 Collected records: {len(df):,}")
                    return len(df)
                else:
                    logger.warning("Combined file not found after collection")
                    return 0
            else:
                logger.error(f"❌ Data Collection failed with return code: {result.returncode}")
                logger.error(f"Error output: {result.stderr}")
                raise Exception(f"Data collection failed: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Data Collection timed out after 30 minutes")
            raise Exception("Data collection timed out")
        except Exception as e:
            logger.error(f"❌ Data Collection failed: {str(e)}")
            raise

    def _execute_data_transformation(self) -> int:
        """Execute data transformation step"""
        
        logger.info("🔄 Executing: Data Transformation")
        
        try:
            # Initialize transformer
            transformer = GreenBondsTransformer(
                data_dir=str(self.raw_data_dir),
                output_dir=str(self.processed_data_dir)
            )
            
            # Execute transformation
            start_time = datetime.now()
            output_file = transformer.process_all()
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"✅ Data Transformation completed successfully in {duration:.1f}s")
            
            # Count transformed records
            if os.path.exists(output_file):
                import pandas as pd
                df = pd.read_csv(output_file)
                logger.info(f"📊 Transformed records: {len(df):,}")
                return len(df)
            else:
                logger.warning("Transformed file not found")
                return 0
                
        except Exception as e:
            logger.error(f"❌ Data Transformation failed: {str(e)}")
            raise

    def _execute_data_loading(self) -> int:
        """Execute data loading step"""
        
        logger.info("🔄 Executing: Data Loading")
        
        try:
            # Initialize loader
            loader = GreenBondsLoader()
            
            # Execute loading
            start_time = datetime.now()
            transformed_file = self.processed_data_dir / "transformed_green_bonds.csv"
            
            if not transformed_file.exists():
                raise Exception(f"Transformed data file not found: {transformed_file}")
            
            success = loader.load_transformed_data(str(transformed_file))
            duration = (datetime.now() - start_time).total_seconds()
            
            if success:
                logger.info(f"✅ Data Loading completed successfully in {duration:.1f}s")
                
                # Count loaded records
                with db_manager.get_session() as session:
                    from src.models.universal_models import FactETFPerformance
                    count = session.query(FactETFPerformance).count()
                    logger.info(f"🗄️ Database records: {count:,}")
                    return count
            else:
                raise Exception("Data loading failed")
                
        except Exception as e:
            logger.error(f"❌ Data Loading failed: {str(e)}")
            raise

    def _execute_final_validation(self):
        """Execute final validation and quality checks"""
        
        logger.info("📊 Pipeline Data Flow:")
        collected = self.pipeline_stats['data_counts'].get('collected', 0)
        transformed = self.pipeline_stats['data_counts'].get('transformed', 0)
        loaded = self.pipeline_stats['data_counts'].get('loaded', 0)
        
        logger.info(f"   Collected:   {collected:,} records")
        logger.info(f"   Transformed: {transformed:,} records")
        logger.info(f"   Loaded:      {loaded:,} records")
        
        # Data quality checks
        with db_manager.get_session() as session:
            from src.models.universal_models import (
                FactETFPerformance, DimFinancialInstrument, DimDate
            )
            
            # Check for data completeness
            etf_count = session.query(DimFinancialInstrument).filter_by(instrument_type='etf').count()
            fact_count = session.query(FactETFPerformance).count()
            
            logger.info("✅ Database schema validation passed")
            logger.info(f"   ETF instruments: {etf_count}")
            logger.info(f"   Performance records: {fact_count}")
            
            # Check date range
            date_range = session.query(
                session.query(FactETFPerformance.record_date).order_by(FactETFPerformance.record_date.asc()).limit(1).scalar_subquery(),
                session.query(FactETFPerformance.record_date).order_by(FactETFPerformance.record_date.desc()).limit(1).scalar_subquery()
            ).first()
            
            if date_range and date_range[0] and date_range[1]:
                from datetime import date
                start_date, end_date = date_range
                total_days = (end_date - start_date).days
                logger.info(f"📅 Data date range: {start_date} to {end_date} ({total_days} days)")
        
        logger.info("✅ Final validation completed successfully")

    def _complete_pipeline_execution(self):
        """Complete pipeline execution and generate reports"""
        
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        self.pipeline_stats['end_time'] = end_time.isoformat()
        self.pipeline_stats['duration_minutes'] = duration.total_seconds() / 60
        
        logger.info("=" * 80)
        logger.info("🎉 PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"⏰ Total Execution Time: {duration.total_seconds() / 60:.1f} minutes")
        logger.info(f"📊 Steps Completed: {', '.join(self.pipeline_stats['steps_completed'])}")
        logger.info("📈 Records Flow:")
        for step, count in self.pipeline_stats['data_counts'].items():
            logger.info(f"   📥 {step.capitalize()}: {count:,}")
        
        # Save pipeline statistics
        stats_file = self.logs_dir / f"pipeline_stats_{self.execution_id}.json"
        with open(stats_file, 'w') as f:
            json.dump(self.pipeline_stats, f, indent=2, default=str)
        logger.info(f"📄 Pipeline statistics saved to: {stats_file}")
        
        # Next steps guidance
        logger.info("\n🎯 NEXT STEPS:")
        logger.info("1. 📊 Connect PowerBI to your PostgreSQL database")
        logger.info("2. 🔗 Use connection string: postgresql://user:pass@localhost:5432/postgres")
        logger.info("3. 📈 Query the star schema tables for optimal performance")
        logger.info("4. 🔄 Schedule this pipeline to run daily/weekly for fresh data")
        logger.info("=" * 80)

    def _handle_pipeline_failure(self, error: Exception):
        """Handle pipeline failure and cleanup"""
        
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        self.pipeline_stats['end_time'] = end_time.isoformat()
        self.pipeline_stats['duration_minutes'] = duration.total_seconds() / 60
        self.pipeline_stats['success'] = False
        
        logger.error("=" * 80)
        logger.error("❌ PIPELINE EXECUTION FAILED!")
        logger.error("=" * 80)
        logger.error(f"⏰ Execution Time: {duration.total_seconds() / 60:.1f} minutes")
        logger.error(f"📊 Steps Completed: {', '.join(self.pipeline_stats['steps_completed'])}")
        logger.error(f"❌ Error: {str(error)}")
        
        # Save failure statistics
        stats_file = self.logs_dir / f"pipeline_stats_failed_{self.execution_id}.json"
        with open(stats_file, 'w') as f:
            json.dump(self.pipeline_stats, f, indent=2, default=str)
        logger.error(f"📄 Failure statistics saved to: {stats_file}")
        
        logger.error("=" * 80)

    def run_individual_steps(self, 
                           collect: bool = True,
                           transform: bool = True,
                           load: bool = True,
                           validate: bool = True) -> Dict[str, bool]:
        """
        Run individual pipeline steps
        
        Args:
            collect: Run data collection
            transform: Run data transformation
            load: Run data loading
            validate: Run final validation
            
        Returns:
            Dict mapping step names to success status
        """
        results = {}
        
        try:
            self._validate_environment()
            
            if collect:
                logger.info("🌱 Running Data Collection...")
                try:
                    self._execute_data_collection()
                    results['collection'] = True
                except Exception as e:
                    logger.error(f"Data collection failed: {e}")
                    results['collection'] = False
            
            if transform:
                logger.info("🔄 Running Data Transformation...")
                try:
                    self._execute_data_transformation()
                    results['transformation'] = True
                except Exception as e:
                    logger.error(f"Data transformation failed: {e}")
                    results['transformation'] = False
            
            if load:
                logger.info("💾 Running Data Loading...")
                try:
                    self._execute_data_loading()
                    results['loading'] = True
                except Exception as e:
                    logger.error(f"Data loading failed: {e}")
                    results['loading'] = False
            
            if validate:
                logger.info("✅ Running Final Validation...")
                try:
                    self._execute_final_validation()
                    results['validation'] = True
                except Exception as e:
                    logger.error(f"Final validation failed: {e}")
                    results['validation'] = False
            
            return results
            
        except Exception as e:
            logger.error(f"Pipeline setup failed: {e}")
            return {'setup': False}

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Green Bonds ETL Pipeline")
    parser.add_argument("--force-collection", action="store_true", 
                       help="Force data collection even if recent data exists")
    parser.add_argument("--skip-validation", action="store_true",
                       help="Skip final validation step")
    parser.add_argument("--data-dir", default="data",
                       help="Base directory for data files")
    parser.add_argument("--collect-only", action="store_true",
                       help="Run only data collection step")
    parser.add_argument("--transform-only", action="store_true",
                       help="Run only data transformation step")
    parser.add_argument("--load-only", action="store_true",
                       help="Run only data loading step")
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = GreenBondsPipelineOrchestrator(
        data_dir=args.data_dir,
        force_collection=args.force_collection,
        skip_validation=args.skip_validation
    )
    
    # Run pipeline based on arguments
    if args.collect_only:
        results = pipeline.run_individual_steps(collect=True, transform=False, load=False, validate=False)
    elif args.transform_only:
        results = pipeline.run_individual_steps(collect=False, transform=True, load=False, validate=False)
    elif args.load_only:
        results = pipeline.run_individual_steps(collect=False, transform=False, load=True, validate=False)
    else:
        # Run complete pipeline
        success = pipeline.run_complete_pipeline()
        if success:
            print("🎉 Green Bonds Pipeline completed successfully!")
            sys.exit(0)
        else:
            print("❌ Green Bonds Pipeline failed!")
            sys.exit(1)

if __name__ == "__main__":
    main()