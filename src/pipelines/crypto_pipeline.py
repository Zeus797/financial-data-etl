# src/pipelines/crypto_pipeline.py
"""
Cryptocurrency Pipeline Orchestrator
Coordinates collector → transformer → loader with comprehensive error handling
"""
import asyncio 
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import traceback

# Change relative imports to absolute imports
from src.collectors.crypto_collector import CryptoCollector, CryptoCollectorConfig
from src.transformers.crypto_transformer import CryptoTransformer, CryptoTransformationConfig
from src.loaders.crypto_loader import CryptoLoader
from src.models.base import test_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CryptoPipelineConfig:
    """Configuration for the complete cryptocurrency pipeline"""
    historical_days: int = 365 * 5  # 5 years
    max_retries: int = 5
    chunk_size: int = 90  # 90-day chunks to avoid rate limits
    retry_delay: int = 30
    enable_current_data: bool = True
    enable_historical_data: bool = True
    
    # Processing settings
    batch_size: int = 1000
    enable_quality_checks: bool = True
    
    # Error handling settings
    max_retries: int = 3
    retry_delay_seconds: int = 30
    continue_on_errors: bool = True
    
    # Performance settings
    timeout_seconds: int = 1800  # 30 minutes total timeout
    
    # Validation settings
    require_minimum_records: int = 5
    max_acceptable_errors: int = 2

class CryptoPipelineResult:
    """Results container for pipeline execution"""
    
    def __init__(self):
        self.success: bool = False
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration_seconds: float = 0.0
        
        # Component results
        self.collection_result = {}
        self.transformation_result = {}
        self.loading_result = {}
        
        # Statistics
        self.total_records_processed = 0
        self.total_records_loaded = 0
        self.total_errors = 0
        
        # Error tracking
        self.errors = []
        self.warnings = []
        
        # Performance metrics
        self.records_per_second: float = 0.0
        self.api_calls_made = 0
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'success': self.success,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration_seconds,
            'collection_result': self.collection_result,
            'transformation_result': self.transformation_result,
            'loading_result': self.loading_result,
            'total_records_processed': self.total_records_processed,
            'total_records_loaded': self.total_records_loaded,
            'total_errors': self.total_errors,
            'errors': self.errors,
            'warnings': self.warnings,
            'records_per_second': self.records_per_second,
            'api_calls_made': self.api_calls_made
        }

class CryptoPipeline:
    """
    Complete cryptocurrency pipeline orchestrator
    
    Responsibilities:
    - Coordinate all pipeline components
    - Handle errors and retries
    - Provide comprehensive logging
    - Generate execution reports
    - Manage performance monitoring
    """
    
    def __init__(self, config: Optional[CryptoPipelineConfig] = None):
        self.config = config or CryptoPipelineConfig()
        self.result = CryptoPipelineResult()
        
        # Initialize components with their configs
        self.collector_config = CryptoCollectorConfig()
        self.transformer_config = CryptoTransformationConfig()
        
        logger.info("CryptoPipeline orchestrator initialized")
    
    async def run_complete_pipeline(self) -> CryptoPipelineResult:
        """
        Execute the complete cryptocurrency pipeline
        
        Returns:
            CryptoPipelineResult with comprehensive execution details
        """
        self.result.start_time = datetime.now()
        logger.info("🚀 Starting complete cryptocurrency pipeline execution...")
        
        try:
            # Step 1: Prerequisites validation
            await self._validate_prerequisites()
            
            # Step 2: Data collection
            collected_data = await self._execute_collection_phase()
            
            # Step 3: Data transformation
            transformed_data = await self._execute_transformation_phase(collected_data)
            
            # Step 4: Data loading
            loading_results = await self._execute_loading_phase(transformed_data)
            
            # Step 5: Final validation and reporting
            await self._finalize_pipeline_execution(loading_results)
            
            self.result.success = True
            logger.info("✅ Cryptocurrency pipeline completed successfully!")
            
        except Exception as e:
            self.result.success = False
            error_msg = f"Pipeline execution failed: {str(e)}"
            self.result.errors.append(error_msg)
            logger.error(f"❌ {error_msg}")
            logger.error(f"Error details: {traceback.format_exc()}")
            
        finally:
            self.result.end_time = datetime.now()
            if self.result.start_time:
                duration = self.result.end_time - self.result.start_time
                self.result.duration_seconds = duration.total_seconds()
                if self.result.duration_seconds > 0:
                    self.result.records_per_second = self.result.total_records_processed / self.result.duration_seconds
        
        return self.result
    
    async def _validate_prerequisites(self):
        """Validate all prerequisites before starting pipeline"""
        logger.info("🔍 Validating pipeline prerequisites...")
        
        # Database connectivity
        if not test_connection():
            raise Exception("Database connection failed. Check your database configuration.")
        
        # Component initialization
        try:
            # Test transformer initialization
            transformer = CryptoTransformer(self.transformer_config)
            
            # Test loader initialization  
            loader = CryptoLoader()
            
            # Validate instrument mappings exist
            mappings = loader.get_instrument_mappings()
            if len(mappings) < self.config.require_minimum_records:
                raise Exception(
                    f"Insufficient cryptocurrency instruments configured. "
                    f"Found {len(mappings)}, require minimum {self.config.require_minimum_records}. "
                    f"Please run the universal database setup first."
                )
            
            logger.info(f"✅ Prerequisites validated: {len(mappings)} cryptocurrency instruments available")
            
        except Exception as e:
            raise Exception(f"Component initialization failed: {e}")
    
    async def _execute_collection_phase(self) -> Dict[str, Any]:
        """Execute data collection with error handling and retries"""
        logger.info("📊 Executing data collection phase...")
        
        for attempt in range(self.config.max_retries):
            try:
                async with CryptoCollector(self.collector_config) as collector:
                    if self.config.enable_current_data and self.config.enable_historical_data:
                        # Collect all data
                        collected_data = await collector.collect_all_data(self.config.historical_days)
                    elif self.config.enable_current_data:
                        # Only current data
                        market_data = await collector.collect_market_data()
                        collected_data = {
                            'market_data': market_data,
                            'historical_data': {},
                            'collection_metadata': {
                                'collection_timestamp': datetime.now().isoformat(),
                                'data_source': 'coingecko_api'
                            }
                        }
                    else:
                        raise Exception("At least current data collection must be enabled")
                
                # Validate collection results
                self._validate_collection_results(collected_data)
                
                # Store collection results
                self.result.collection_result = collected_data.get('collection_metadata', {})
                self.result.api_calls_made = len(collected_data.get('market_data', [])) + len(collected_data.get('historical_data', {}))
                
                logger.info(f"✅ Data collection completed successfully on attempt {attempt + 1}")
                return collected_data
                
            except Exception as e:
                error_msg = f"Collection attempt {attempt + 1} failed: {str(e)}"
                logger.warning(error_msg)
                self.result.warnings.append(error_msg)
                
                if attempt == self.config.max_retries - 1:
                    # Final attempt failed
                    raise Exception(f"Data collection failed after {self.config.max_retries} attempts: {e}")
                
                # Wait before retry
                logger.info(f"Retrying in {self.config.retry_delay_seconds} seconds...")
                await asyncio.sleep(self.config.retry_delay_seconds)
    
    def _validate_collection_results(self, collected_data: Dict[str, Any]):
        """Validate collection results meet minimum requirements"""
        market_data = collected_data.get('market_data', [])
        historical_data = collected_data.get('historical_data', {})
        
        if len(market_data) < self.config.require_minimum_records:
            raise Exception(
                f"Insufficient market data collected: {len(market_data)} records, "
                f"minimum required: {self.config.require_minimum_records}"
            )
        
        if self.config.enable_historical_data and len(historical_data) == 0:
            raise Exception("Historical data collection enabled but no historical data collected")
        
        logger.info(f"Collection validation passed: {len(market_data)} market records, {len(historical_data)} historical datasets")
    
    async def _execute_transformation_phase(self, collected_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data transformation with quality validation"""
        logger.info("🔧 Executing data transformation phase...")
        
        try:
            transformer = CryptoTransformer(self.transformer_config)
            transformed_data = transformer.transform_complete_dataset(collected_data)
            
            # Validate transformation results
            #self._validate_transformation_results(transformed_data)
            
            # Store transformation results
            self.result.transformation_result = transformed_data.get('transformation_metadata', {})
            self.result.total_records_processed = (
                len(transformed_data.get('market_data', [])) +
                len(transformed_data.get('historical_data', []))
            )
            
            # Handle quality issues
            quality_report = transformed_data.get('quality_report', {})
            quality_issues = quality_report.get('issues', [])
            quality_warnings = quality_report.get('warnings', [])
            
            if quality_issues:
                self.result.errors.extend(quality_issues)
                if len(quality_issues) > self.config.max_acceptable_errors:
                    raise Exception(f"Too many data quality issues: {len(quality_issues)} errors found")
            
            if quality_warnings:
                self.result.warnings.extend(quality_warnings)
            
            logger.info(f"✅ Data transformation completed: {self.result.total_records_processed} records processed")
            return transformed_data
            
        except Exception as e:
            raise Exception(f"Data transformation failed: {e}")
    
    # UPDATE THIS METHOD in your src/pipelines/crypto_pipeline.py

    def _validate_transformation_results(self, transformed_data: Dict[str, Any]):
        """UPDATED: Validate transformation results with new data structure"""
        market_data = transformed_data.get('market_data', [])
        historical_data = transformed_data.get('historical_data', [])  # Now a list, not dict!
        quality_report = transformed_data.get('quality_report', {})
    
        if len(market_data) == 0:
            raise Exception("No market data after transformation")
    
        # Check quality report for critical issues
        critical_issues = [
            issue for issue in quality_report.get('issues', [])
            if 'missing' in issue.lower() or 'invalid' in issue.lower()
        ]
    
        if len(critical_issues) > self.config.max_acceptable_errors:
            raise Exception(f"Critical data quality issues found: {critical_issues}")
    
        logger.info(f"Transformation validation passed: {len(market_data)} market records, {len(historical_data)} historical records transformed")

    # ALSO UPDATE THIS METHOD in your src/pipelines/crypto_pipeline.py

    async def _execute_transformation_phase(self, collected_data: Dict[str, Any]) -> Dict[str, Any]:
        """UPDATED: Execute data transformation with quality validation"""
        logger.info("🔧 Executing data transformation phase...")
    
        try:
            transformer = CryptoTransformer(self.transformer_config)
            transformed_data = transformer.transform_complete_dataset(collected_data)
        
            # Validate transformation results
            self._validate_transformation_results(transformed_data)
        
            # Store transformation results
            self.result.transformation_result = transformed_data.get('transformation_metadata', {})
        
            # UPDATED: Calculate total records processed with new structure
            market_count = len(transformed_data.get('market_data', []))
            historical_count = len(transformed_data.get('historical_data', []))  # Direct length on list
            self.result.total_records_processed = market_count + historical_count
        
            # Handle quality issues
            quality_report = transformed_data.get('quality_report', {})
            quality_issues = quality_report.get('issues', [])
            quality_warnings = quality_report.get('warnings', [])
        
            if quality_issues:
                self.result.errors.extend(quality_issues)
                if len(quality_issues) > self.config.max_acceptable_errors:
                    raise Exception(f"Too many data quality issues: {len(quality_issues)} errors found")
        
            if quality_warnings:
                self.result.warnings.extend(quality_warnings)
        
            logger.info(f"✅ Data transformation completed: {self.result.total_records_processed} records processed")
            return transformed_data
        
        except Exception as e:
            raise Exception(f"Data transformation failed: {e}")
    
    async def _execute_loading_phase(self, transformed_data: dict) -> dict:
        """Load transformed data into the database and return loading results."""
        logger.info("💾 Executing data loading phase...")
        try:
            loader = CryptoLoader()
            market_data = transformed_data.get('market_data', [])
            historical_data = transformed_data.get('historical_data', [])

            market_stats = loader.load_market_data(market_data)
            historical_stats = loader.load_historical_data(historical_data)
            validation_report = loader.validate_data_integrity()
            loading_statistics = loader.get_summary_stats()

            # Store loading results
            self.result.loading_result = {
                'market_data_stats': market_stats,
                'historical_data_stats': historical_stats,
                'validation_report': validation_report,
                'loading_statistics': loading_statistics,
            }
            self.result.total_records_loaded = (
                market_stats.get('inserted', 0) + market_stats.get('updated', 0) +
                historical_stats.get('inserted', 0) + historical_stats.get('updated', 0)
            )
            self.result.total_errors += (
                market_stats.get('errors', 0) + historical_stats.get('errors', 0)
            )
            logger.info(f"✅ Data loading completed: {self.result.total_records_loaded} records loaded")
            return self.result.loading_result

        except Exception as e:
            raise Exception(f"Data loading failed: {e}")
    
    def _validate_loading_results(self, loading_results: Dict[str, Any]):
        """Validate loading results"""
        market_stats = loading_results.get('market_data_stats', {})
        historical_stats = loading_results.get('historical_data_stats', {})
        validation_report = loading_results.get('validation_report', {})
        
        total_processed = (
            market_stats.get('total_processed', 0) +
            historical_stats.get('total_processed', 0)
        )
        
        if total_processed == 0:
            raise Exception("No records were processed during loading")
        
        # Check for critical validation issues
        validation_issues = validation_report.get('issues', [])
        critical_validation_issues = [
            issue for issue in validation_issues
            if 'orphaned' in issue.lower() or 'duplicate' in issue.lower()
        ]
        
        if critical_validation_issues:
            self.result.warnings.extend(critical_validation_issues)
        
        logger.info(f"Loading validation passed: {total_processed} records processed")
    
    async def _finalize_pipeline_execution(self, loading_results: Dict[str, Any]):
        """Finalize pipeline execution with comprehensive reporting"""
        logger.info("📋 Finalizing pipeline execution...")
        
        # Final data integrity check
        validation_report = loading_results.get('validation_report', {})
        loading_statistics = loading_results.get('loading_statistics', {})
        
        # Log final statistics
        logger.info("📊 Final Pipeline Statistics:")
        logger.info(f"   Total Records Processed: {self.result.total_records_processed}")
        logger.info(f"   Total Records Loaded: {self.result.total_records_loaded}")
        logger.info(f"   Total API Calls Made: {self.result.api_calls_made}")
        logger.info(f"   Total Errors: {self.result.total_errors}")
        logger.info(f"   Total Warnings: {len(self.result.warnings)}")
        
        # Log database statistics
        if loading_statistics:
            logger.info(f"   Database Total Records: {loading_statistics.get('total_price_records', 0)}")
            logger.info(f"   Data Date Range: {loading_statistics.get('earliest_date')} to {loading_statistics.get('latest_date')}")
            logger.info(f"   Data Coverage: {loading_statistics.get('total_instruments', 0)} instruments")
    
    def _calculate_final_metrics(self):
        """Calculate final performance metrics"""
        self.result.end_time = datetime.now()
        
        if self.result.start_time:
            duration = self.result.end_time - self.result.start_time
            self.result.duration_seconds = duration.total_seconds()
            
            # Calculate records per second
            if self.result.duration_seconds > 0:
                self.result.records_per_second = self.result.total_records_processed / self.result.duration_seconds
        
        logger.info(f"Pipeline execution completed in {self.result.duration_seconds:.2f} seconds")
        logger.info(f"Processing rate: {self.result.records_per_second:.2f} records/second")

# Convenience function for simple pipeline execution
async def run_crypto_pipeline(config: Optional[CryptoPipelineConfig] = None) -> CryptoPipelineResult:
    """
    Run the complete cryptocurrency pipeline
    
    Args:
        config: Optional pipeline configuration
        
    Returns:
        CryptoPipelineResult with execution details
    """
    pipeline = CryptoPipeline(config)
    return await pipeline.run_complete_pipeline()

# Example usage and testing
if __name__ == "__main__":
    async def main():
        print("🔄 Testing complete cryptocurrency pipeline...")
        
        # Test configuration
        test_config = CryptoPipelineConfig(
            historical_days=1826,  # 5 years
            require_minimum_records=3,  # Reduced for testing
            max_acceptable_errors=5  # More lenient for testing
        )
        
        try:
            result = await run_crypto_pipeline(test_config)
            
            print("\n" + "="*60)
            print("📊 CRYPTOCURRENCY PIPELINE EXECUTION REPORT")
            print("="*60)
            
            print(f"✅ Status: {'SUCCESS' if result.success else 'FAILED'}")
            print(f"⏱️  Execution Time: {result.duration_seconds:.2f} seconds")
            print(f"📈 Processing Rate: {result.records_per_second:.2f} records/second")
            print(f"🔢 Records Processed: {result.total_records_processed}")
            print(f"💾 Records Loaded: {result.total_records_loaded}")
            print(f"📞 API Calls Made: {result.api_calls_made}")
            
            if result.errors:
                print(f"\n❌ Errors ({len(result.errors)}):")
                for error in result.errors[:5]:  # Show first 5 errors
                    print(f"   - {error}")
            
            if result.warnings:
                print(f"\n⚠️  Warnings ({len(result.warnings)}):")
                for warning in result.warnings[:3]:  # Show first 3 warnings
                    print(f"   - {warning}")
            
            print(f"\n💡 Pipeline Result:")
            if result.success:
                print("   ✅ Cryptocurrency data successfully collected, transformed, and loaded")
                print("   ✅ Ready for PowerBI API connection")
                print("   ✅ Manual CSV download process replaced")
            else:
                print("   ❌ Pipeline execution failed")
                print("   🔧 Check errors above and database configuration")
            
        except Exception as e:
            print(f"\n❌ Pipeline execution failed: {e}")
            return 1
        
        return 0
    
    # Run the test
    import sys
    exit_code = asyncio.run(main())
    sys.exit(exit_code)