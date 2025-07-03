#!/usr/bin/env python3
"""
Market Index Pipeline Orchestrator
Runs the complete ETL pipeline: Collect → Transform → Load

Features:
- Full pipeline automation
- Step-by-step execution with validation
- Comprehensive logging and error handling
- Recovery from partial failures
- Performance monitoring
- Scheduling support
"""

import sys
import os
import time
import subprocess
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_execution.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MarketIndexPipelineOrchestrator:
    """
    Complete ETL pipeline orchestrator for market index data
    
    Pipeline Steps:
    1. Data Collection (Yahoo Finance → CSV files)
    2. Data Transformation (CSV → enriched CSV with analytics)
    3. Data Loading (CSV → PostgreSQL star schema)
    4. Validation and reporting
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._get_default_config()
        self.execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.start_time = None
        self.pipeline_stats = {
            'execution_id': self.execution_id,
            'steps_completed': [],
            'steps_failed': [],
            'total_records_collected': 0,
            'total_records_transformed': 0,
            'total_records_loaded': 0,
            'execution_time_seconds': 0,
            'status': 'INITIALIZED'
        }
        
        logger.info(f"🚀 Pipeline Orchestrator initialized (ID: {self.execution_id})")
        self._validate_environment()

    def _get_default_config(self) -> Dict:
        """Get default pipeline configuration"""
        return {
            'collect_historical_years': 5,
            'batch_size': 1000,
            'max_retries': 3,
            'retry_delay': 30,  # seconds
            'cleanup_temp_files': True,
            'validate_each_step': True,
            'parallel_processing': False,  # Set to True for faster execution
            'data_paths': {
                'raw_data': 'data/raw_data/market_indexes',
                'processed_data': 'data/processed_data',
                'logs': 'logs'
            },
            'target_indexes': [
                'S&P_500', 'Dow_Jones', 'NASDAQ_Composite', 'FTSE_100',
                'DAX_40', 'CAC_40', 'JSE_Top_40', 'Nikkei_225', 
                'Hang_Seng', 'EURO_STOXX_50'
            ]
        }

    def _validate_environment(self):
        """Validate that environment is ready for pipeline execution"""
        logger.info("🔍 Validating pipeline environment...")
        
        # Check Python dependencies
        required_modules = ['pandas', 'yfinance', 'sqlalchemy', 'psycopg2']
        missing_modules = []
        
        for module in required_modules:
            try:
                __import__(module)
            except ImportError:
                missing_modules.append(module)
        
        if missing_modules:
            raise EnvironmentError(f"Missing required modules: {missing_modules}")
        
        # Check database connection
        try:
            from src.models.base import db_manager
            if not db_manager.test_connection():
                raise ConnectionError("Database connection failed")
        except Exception as e:
            raise ConnectionError(f"Database validation failed: {e}")
        
        # Create necessary directories
        for path in self.config['data_paths'].values():
            os.makedirs(path, exist_ok=True)
        
        logger.info("✅ Environment validation passed")

    def run_complete_pipeline(self, force_recollect: bool = False) -> bool:
        """
        Execute the complete ETL pipeline
        
        Args:
            force_recollect: If True, always collect fresh data even if files exist
            
        Returns:
            bool: True if pipeline completed successfully
        """
        self.start_time = datetime.now()
        self.pipeline_stats['status'] = 'RUNNING'
        
        logger.info("="*80)
        logger.info("🚀 STARTING COMPLETE MARKET INDEX ETL PIPELINE")
        logger.info(f"📅 Execution ID: {self.execution_id}")
        logger.info(f"⏰ Start Time: {self.start_time}")
        logger.info(f"🎯 Target Indexes: {len(self.config['target_indexes'])}")
        logger.info("="*80)
        
        try:
            # Step 1: Data Collection
            if not self._execute_collection_step(force_recollect):
                return self._handle_pipeline_failure("Collection step failed")
            
            # Step 2: Data Transformation
            if not self._execute_transformation_step():
                return self._handle_pipeline_failure("Transformation step failed")
            
            # Step 3: Data Loading
            if not self._execute_loading_step():
                return self._handle_pipeline_failure("Loading step failed")
            
            # Step 4: Final Validation
            if not self._execute_validation_step():
                return self._handle_pipeline_failure("Validation step failed")
            
            return self._handle_pipeline_success()
            
        except Exception as e:
            return self._handle_pipeline_failure(f"Unexpected error: {str(e)}")

    def _execute_collection_step(self, force_recollect: bool = False) -> bool:
        """Execute data collection step"""
        logger.info("📥 STEP 1: DATA COLLECTION")
        logger.info("-" * 40)
        
        # Check if we need to collect data
        raw_data_path = self.config['data_paths']['raw_data']
        combined_file = os.path.join(raw_data_path, 'all_indexes_combined.csv')
        
        if os.path.exists(combined_file) and not force_recollect:
            logger.info(f"📄 Found existing data file: {combined_file}")
            
            # Validate existing data
            try:
                import pandas as pd
                df = pd.read_csv(combined_file)
                self.pipeline_stats['total_records_collected'] = len(df)
                logger.info(f"✅ Using existing data: {len(df):,} records")
                self.pipeline_stats['steps_completed'].append('collection')
                return True
            except Exception as e:
                logger.warning(f"⚠️ Existing data file corrupted: {e}")
                logger.info("🔄 Will collect fresh data...")
        
        # Execute collection
        return self._run_subprocess(
            'Data Collection',
            'python src/collectors/market_index_collector.py',
            expected_output_file=combined_file
        )

    def _execute_transformation_step(self) -> bool:
        """Execute data transformation step"""
        logger.info("\n🔄 STEP 2: DATA TRANSFORMATION")
        logger.info("-" * 40)
        
        expected_output = os.path.join(
            self.config['data_paths']['processed_data'], 
            'transformed_indexes.csv'
        )
        
        success = self._run_subprocess(
            'Data Transformation',
            'python src/transformers/market_index_transformer.py',
            expected_output_file=expected_output
        )
        
        if success:
            # Count transformed records
            try:
                import pandas as pd
                df = pd.read_csv(expected_output)
                self.pipeline_stats['total_records_transformed'] = len(df)
                logger.info(f"📊 Transformed records: {len(df):,}")
            except Exception as e:
                logger.warning(f"Could not count transformed records: {e}")
        
        return success

    def _execute_loading_step(self) -> bool:
        """Execute data loading step"""
        logger.info("\n💾 STEP 3: DATA LOADING")
        logger.info("-" * 40)
        
        success = self._run_subprocess(
            'Data Loading',
            'python scripts/simple_index_loader.py',
            check_database=True
        )
        
        if success:
            # Get database record count
            try:
                from src.models.base import db_manager
                from src.models.universal_models import FactMarketIndex
                
                with db_manager.get_session() as session:
                    count = session.query(FactMarketIndex).count()
                    self.pipeline_stats['total_records_loaded'] = count
                    logger.info(f"🗄️ Database records: {count:,}")
            except Exception as e:
                logger.warning(f"Could not count database records: {e}")
        
        return success

    def _execute_validation_step(self) -> bool:
        """Execute final validation step"""
        logger.info("\n✅ STEP 4: FINAL VALIDATION")
        logger.info("-" * 40)
        
        try:
            # Validate data consistency
            collected = self.pipeline_stats['total_records_collected']
            transformed = self.pipeline_stats['total_records_transformed']
            loaded = self.pipeline_stats['total_records_loaded']
            
            logger.info(f"📊 Pipeline Data Flow:")
            logger.info(f"   Collected:   {collected:,} records")
            logger.info(f"   Transformed: {transformed:,} records")
            logger.info(f"   Loaded:      {loaded:,} records")
            
            # Check for data loss
            if collected > 0 and transformed < collected * 0.95:
                logger.warning("⚠️ Significant data loss in transformation step")
            
            if transformed > 0 and loaded < transformed * 0.95:
                logger.warning("⚠️ Significant data loss in loading step")
            
            # Validate database schema
            self._validate_database_schema()
            
            # Validate date ranges
            self._validate_date_ranges()
            
            self.pipeline_stats['steps_completed'].append('validation')
            logger.info("✅ Final validation completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Validation step failed: {e}")
            self.pipeline_stats['steps_failed'].append('validation')
            return False

    def _run_subprocess(self, step_name: str, command: str, 
                       expected_output_file: str = None, 
                       check_database: bool = False) -> bool:
        """Run a subprocess with error handling and validation"""
        
        for attempt in range(self.config['max_retries']):
            try:
                logger.info(f"🔄 Executing: {step_name} (Attempt {attempt + 1})")
                logger.info(f"💻 Command: {command}")
                
                # Execute the command
                start_time = time.time()
                result = subprocess.run(
                    command.split(),
                    capture_output=True,
                    text=True,
                    timeout=3600  # 1 hour timeout
                )
                execution_time = time.time() - start_time
                
                # Check exit code
                if result.returncode != 0:
                    logger.error(f"❌ {step_name} failed with exit code {result.returncode}")
                    logger.error(f"Error output: {result.stderr}")
                    
                    if attempt < self.config['max_retries'] - 1:
                        logger.info(f"⏳ Retrying in {self.config['retry_delay']} seconds...")
                        time.sleep(self.config['retry_delay'])
                        continue
                    else:
                        self.pipeline_stats['steps_failed'].append(step_name.lower().replace(' ', '_'))
                        return False
                
                # Log success output
                if result.stdout:
                    logger.info(f"📄 {step_name} output (last 10 lines):")
                    for line in result.stdout.split('\n')[-10:]:
                        if line.strip():
                            logger.info(f"   {line}")
                
                # Validate output
                if expected_output_file and not os.path.exists(expected_output_file):
                    logger.error(f"❌ Expected output file not created: {expected_output_file}")
                    if attempt < self.config['max_retries'] - 1:
                        continue
                    else:
                        return False
                
                # Success
                logger.info(f"✅ {step_name} completed successfully in {execution_time:.1f}s")
                self.pipeline_stats['steps_completed'].append(step_name.lower().replace(' ', '_'))
                return True
                
            except subprocess.TimeoutExpired:
                logger.error(f"❌ {step_name} timed out after 1 hour")
                if attempt < self.config['max_retries'] - 1:
                    continue
                else:
                    return False
                    
            except Exception as e:
                logger.error(f"❌ {step_name} failed with error: {e}")
                if attempt < self.config['max_retries'] - 1:
                    time.sleep(self.config['retry_delay'])
                    continue
                else:
                    return False
        
        return False

    def _validate_database_schema(self):
        """Validate database schema and table structure"""
        try:
            from src.models.base import db_manager
            
            counts = db_manager.get_table_counts()
            
            expected_tables = ['currencies', 'countries', 'entities', 'instruments', 'dates']
            for table in expected_tables:
                if table not in counts:
                    raise ValueError(f"Missing expected table: {table}")
                
                if counts[table] == 0:
                    logger.warning(f"⚠️ Table {table} is empty")
            
            logger.info("✅ Database schema validation passed")
            
        except Exception as e:
            raise ValueError(f"Database schema validation failed: {e}")

    def _validate_date_ranges(self):
        """Validate that data covers expected date ranges"""
        try:
            from src.models.base import db_manager
            from src.models.universal_models import FactMarketIndex
            from sqlalchemy import func
            
            with db_manager.get_session() as session:
                date_range = session.query(
                    func.min(FactMarketIndex.record_date),
                    func.max(FactMarketIndex.record_date)
                ).first()
                
                if date_range and date_range[0] and date_range[1]:
                    min_date, max_date = date_range
                    date_span = (max_date - min_date).days
                    
                    logger.info(f"📅 Data date range: {min_date} to {max_date} ({date_span} days)")
                    
                    # Validate we have reasonable historical coverage
                    expected_min_days = self.config['collect_historical_years'] * 365 * 0.8  # 80% coverage
                    if date_span < expected_min_days:
                        logger.warning(f"⚠️ Date coverage may be insufficient: {date_span} days")
                else:
                    raise ValueError("No date range found in database")
                    
        except Exception as e:
            raise ValueError(f"Date range validation failed: {e}")

    def _handle_pipeline_success(self) -> bool:
        """Handle successful pipeline completion"""
        end_time = datetime.now()
        execution_time = (end_time - self.start_time).total_seconds()
        
        self.pipeline_stats['status'] = 'COMPLETED'
        self.pipeline_stats['execution_time_seconds'] = execution_time
        
        logger.info("="*80)
        logger.info("🎉 PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        logger.info(f"⏰ Total Execution Time: {execution_time/60:.1f} minutes")
        logger.info(f"📊 Steps Completed: {', '.join(self.pipeline_stats['steps_completed'])}")
        logger.info(f"📈 Records Flow:")
        logger.info(f"   📥 Collected:   {self.pipeline_stats['total_records_collected']:,}")
        logger.info(f"   🔄 Transformed: {self.pipeline_stats['total_records_transformed']:,}")
        logger.info(f"   💾 Loaded:      {self.pipeline_stats['total_records_loaded']:,}")
        
        # Save execution statistics
        self._save_pipeline_stats()
        
        logger.info("\n🎯 NEXT STEPS:")
        logger.info("1. 📊 Connect PowerBI to your PostgreSQL database")
        logger.info("2. 🔗 Use connection string: postgresql://user:pass@localhost:5432/postgres")
        logger.info("3. 📈 Query the star schema tables for optimal performance")
        logger.info("4. 🔄 Schedule this pipeline to run daily/weekly for fresh data")
        logger.info("="*80)
        
        return True

    def _handle_pipeline_failure(self, error_message: str) -> bool:
        """Handle pipeline failure"""
        end_time = datetime.now()
        execution_time = (end_time - self.start_time).total_seconds() if self.start_time else 0
        
        self.pipeline_stats['status'] = 'FAILED'
        self.pipeline_stats['execution_time_seconds'] = execution_time
        self.pipeline_stats['error_message'] = error_message
        
        logger.error("="*80)
        logger.error("❌ PIPELINE EXECUTION FAILED")
        logger.error("="*80)
        logger.error(f"💥 Error: {error_message}")
        logger.error(f"⏰ Execution Time: {execution_time/60:.1f} minutes")
        logger.error(f"✅ Steps Completed: {', '.join(self.pipeline_stats['steps_completed'])}")
        logger.error(f"❌ Steps Failed: {', '.join(self.pipeline_stats['steps_failed'])}")
        
        # Save failure statistics
        self._save_pipeline_stats()
        
        logger.error("\n🔧 TROUBLESHOOTING:")
        logger.error("1. Check the logs above for specific error details")
        logger.error("2. Verify database connection and credentials")
        logger.error("3. Ensure all dependencies are installed")
        logger.error("4. Check file permissions and disk space")
        logger.error("="*80)
        
        return False

    def _save_pipeline_stats(self):
        """Save pipeline execution statistics"""
        try:
            stats_file = f"logs/pipeline_stats_{self.execution_id}.json"
            os.makedirs('logs', exist_ok=True)
            
            with open(stats_file, 'w') as f:
                json.dump(self.pipeline_stats, f, indent=2, default=str)
            
            logger.info(f"📄 Pipeline statistics saved to: {stats_file}")
            
        except Exception as e:
            logger.warning(f"Could not save pipeline statistics: {e}")

    def run_individual_step(self, step_name: str, **kwargs) -> bool:
        """Run an individual pipeline step"""
        
        step_map = {
            'collect': lambda: self._execute_collection_step(kwargs.get('force', False)),
            'transform': self._execute_transformation_step,
            'load': self._execute_loading_step,
            'validate': self._execute_validation_step
        }
        
        if step_name not in step_map:
            logger.error(f"Unknown step: {step_name}")
            return False
        
        logger.info(f"🎯 Running individual step: {step_name}")
        return step_map[step_name]()

    def get_pipeline_status(self) -> Dict:
        """Get current pipeline status"""
        return self.pipeline_stats.copy()

# Convenience functions
def run_full_pipeline(force_recollect: bool = False) -> bool:
    """Run the complete ETL pipeline"""
    orchestrator = MarketIndexPipelineOrchestrator()
    return orchestrator.run_complete_pipeline(force_recollect=force_recollect)

def run_pipeline_step(step_name: str, **kwargs) -> bool:
    """Run an individual pipeline step"""
    orchestrator = MarketIndexPipelineOrchestrator()
    return orchestrator.run_individual_step(step_name, **kwargs)

# CLI Interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Market Index ETL Pipeline Orchestrator")
    parser.add_argument('--step', choices=['collect', 'transform', 'load', 'validate'], 
                       help='Run only a specific step')
    parser.add_argument('--force-recollect', action='store_true',
                       help='Force fresh data collection even if files exist')
    parser.add_argument('--config', help='Path to custom configuration file')
    
    args = parser.parse_args()
    
    try:
        if args.step:
            # Run individual step
            success = run_pipeline_step(args.step, force=args.force_recollect)
        else:
            # Run full pipeline
            success = run_full_pipeline(force_recollect=args.force_recollect)
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Pipeline execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)