# scripts/run_forex_pipeline.py
"""
Enhanced Forex Data Pipeline Runner
"""
import logging
import time
import sys
from pathlib import Path
from typing import List, Tuple, Dict, Any
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.collectors.forex_collector import get_exchange_rate_data
from src.transformers.cleaner import clean_forex_data, add_technical_indicators
from src.loaders.csv_loader import save_forex_data_to_csv, save_summary_report
from src.loaders.forex_db_loader import ForexDatabaseLoader
from src.models.base import SessionLocal, engine
from src.models.models import Base

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('forex_pipeline.log')
    ]
)
logger = logging.getLogger('pipeline')


def run_forex_pipeline(
    currency_pairs: List[Tuple[str, str]], 
    years: int = 15, 
    interval: str = "1mo", 
    save_csv: bool = True, 
    save_db: bool = True,
    add_tech_indicators: bool = True,
    timeout: int = 30
) -> Dict[str, Any]:
    """Run the forex data pipeline using Yahoo Finance"""
    
    logger.info(f"Starting Forex Data Pipeline (Yahoo Finance)")
    logger.info(f"Parameters: {years} years")
    logger.info(f"Processing {len(currency_pairs)} currency pairs")
    
    results = {}
    source_stats = {"success": 0, "failed": 0}
    
    for base, quote in currency_pairs:
        pair_name = f"{base}/{quote}"
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing {pair_name}")
        
        try:
            start_time = time.time()
            
            # Collect data from Google Finance
            logger.info("Collecting data from Google Finance...")
            raw_data = get_exchange_rate_data(
                base, 
                quote, 
                years=years,
                timeout=timeout
            )
            
            source_stats["success"] += 1
            logger.info(f"✓ Collected {len(raw_data)} records")
            
            if len(raw_data) == 0:
                logger.warning(f"No data found for {pair_name}")
                results[pair_name] = {
                    "status": "Failed",
                    "error": "No data available from yfinance"
                }
                continue
            
            # Step 2: Transform the data
            logger.info("Step 2: Transforming and cleaning data...")
            clean_data = clean_forex_data(raw_data)
            
            # Add additional technical indicators if requested
            if add_tech_indicators:
                logger.info("Step 2b: Adding technical indicators...")
                clean_data = add_technical_indicators(clean_data)
            
            logger.info(f"✓ Transformed data, now has {len(clean_data)} rows with {len(clean_data.columns)} columns")
            
            # Step 3: Save to CSV if enabled
            if save_csv:
                logger.info("Step 3: Saving to CSV...")
                file_path = save_forex_data_to_csv(clean_data, base, quote)
                logger.info(f"✓ Saved to: {file_path}")
            
            # Step 4: Save to database if enabled
            if save_db:
                logger.info("Step 4: Saving to database...")
                with ForexDatabaseLoader() as loader:
                    db_result = loader.load_dataframe(clean_data, base, quote)
                    
                    if db_result['success']:
                        logger.info(f"✓ Saved {db_result['records_processed']} records to database")
                        logger.info(f"  Latest rate: {db_result.get('latest_rate', 'N/A')}")
                    else:
                        logger.error(f"✗ Database save failed: {db_result.get('error')}")
            
            # Record success
            execution_time = time.time() - start_time
            results[pair_name] = {
                "status": "Success",
                "rows": len(clean_data),
                "execution_time": f"{execution_time:.2f} seconds",
                "columns": list(clean_data.columns),
                "date_range": f"{clean_data['date'].min()} to {clean_data['date'].max()}"
            }
            
            logger.info(f"✓ Completed {pair_name} in {execution_time:.2f} seconds")
            
        except Exception as e:
            source_stats["failed"] += 1
            logger.error(f"✗ Error processing {pair_name}: {str(e)}")
            results[pair_name] = {
                "status": "Failed",
                "error": str(e)
            }
    
    # Update summary logging
    logger.info("\n" + "="*50)
    logger.info("PIPELINE SUMMARY")
    logger.info("="*50)
    logger.info(f"Successful pairs: {source_stats['success']}")
    logger.info(f"Failed pairs: {source_stats['failed']}")
    
    return results


def main():
    """Main function with default African currency pairs"""
    # Define currency pairs with fallbacks
    primary_pairs = [
        ("USD", "KES"),  # Kenya Shilling
        ("USD", "ZAR"),  # South African Rand (most liquid African currency)
        ("USD", "NGN"),  # Nigerian Naira
        ("USD", "EGP"),  # Egyptian Pound
        ("USD", "TZS"),  # Tanzanian Shilling
        ("USD", "GHS"),  # Ghanaian Cedi
        ("USD", "DZD"),  # Algerian Dinar
        ("USD", "MAD"),  # Moroccan Dirham
        ("USD", "ETB"),  # Ethiopian Birr
        ("EUR", "KES"),  # Euro to Kenya Shilling
    ]
    
    # Add fallback pairs for cross-rates calculation
    fallback_pairs = [
        ("EUR", "USD"),  # For cross-rate calculations
        ("GBP", "USD"),  # Major pairs as fallbacks
        ("USD", "ZAR"),  # ZAR is most liquid African currency
    ]
    
    # Create logs directory
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    # Update log file path
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f'forex_pipeline_{current_time}.log'
    
    # Add file handler with timestamp
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    
    try:
        # Run the pipeline with longer timeout for African pairs
        results = run_forex_pipeline(
            currency_pairs=primary_pairs,
            years=15,
            interval="1mo",
            save_csv=True,
            save_db=True,
            add_tech_indicators=True,
            timeout=30  # Increased timeout for less liquid pairs
        )
        
        # Log results to separate summary file
        summary_file = log_dir / f'summary_{current_time}.txt'
        with open(summary_file, 'w') as f:
            f.write(f"Forex Pipeline Summary - {current_time}\n")
            f.write("="*50 + "\n")
            success_count = sum(1 for r in results.values() if r.get('status') == 'Success')
            f.write(f"\nTotal Pairs: {len(results)}\n")
            f.write(f"Successful: {success_count}\n")
            f.write(f"Failed: {len(results) - success_count}\n\n")
            
            for pair, result in results.items():
                f.write(f"{pair}: {result['status']}\n")
                if result['status'] == 'Failed':
                    f.write(f"  Error: {result.get('error', 'Unknown error')}\n")
        
        logger.info(f"Summary written to: {summary_file}")
        
        # Return error if critical pairs failed
        critical_pairs = ["USD/ZAR", "USD/KES", "USD/NGN"]
        critical_failures = [p for p in critical_pairs if results.get(p, {}).get('status') != 'Success']
        
        if critical_failures:
            logger.error(f"Critical pairs failed: {critical_failures}")
            return 1
            
        return 0 if success_count > len(primary_pairs) * 0.7 else 1  # Require 70% success
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)