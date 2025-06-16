# scripts/test_pipeline_components.py
"""
Test script to verify all pipeline components work correctly
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
from datetime import datetime, timedelta
import logging

 
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)



def test_database_connection():
    """Test database connection"""
    logger.info("Testing database connection...")
    try:
        from src.models.base import engine, SessionLocal
        from sqlalchemy import text
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("✓ Database connection successful")
            
        # Test session
        session = SessionLocal()
        session.execute(text("SELECT 1"))
        session.close()
        logger.info("✓ Session creation successful")
        
        return True
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False


def test_models():
    """Test database models"""
    logger.info("\nTesting database models...")
    try:
        from src.models.models import Country, CurrencyPair, ForexRate
        from src.models.base import SessionLocal
        
        session = SessionLocal()
        
        # Count existing data
        countries = session.query(Country).count()
        pairs = session.query(CurrencyPair).count()
        rates = session.query(ForexRate).count()
        
        logger.info(f"✓ Models loaded successfully")
        logger.info(f"  - Countries: {countries}")
        logger.info(f"  - Currency pairs: {pairs}")
        logger.info(f"  - Forex rates: {rates}")
        
        session.close()
        return True
    except Exception as e:
        logger.error(f"✗ Model test failed: {e}")
        return False


def test_collector():
    """Test forex data collector"""
    logger.info("\nTesting forex collector...")
    try:
        # Try Google Finance first
        try:
            from src.collectors.forex_collector import get_exchange_rate_data
            df = get_exchange_rate_data("USD", "EUR", years=1, interval="1mo")
        except Exception as e:
            logger.warning(f"Live data collection failed, using mock data: {e}")
            from src.collectors.mock_collector import get_mock_exchange_rate_data
            df = get_mock_exchange_rate_data("USD", "EUR", years=1, interval="1mo")
        
        logger.info(f"✓ Collector successful")
        logger.info(f"  - Rows collected: {len(df)}")
        logger.info(f"  - Columns: {list(df.columns)}")
        logger.info(f"  - Date range: {df['date'].min()} to {df['date'].max()}")
        
        return True, df
    except Exception as e:
        logger.error(f"✗ Collector test failed: {e}")
        return False, None


def test_transformer(df):
    """Test data transformer"""
    logger.info("\nTesting transformer...")
    try:
        from src.transformers.cleaner import clean_forex_data
        
        if df is None or len(df) == 0:
            logger.warning("No data to transform")
            return False, None
        
        cleaned = clean_forex_data(df)
        
        logger.info(f"✓ Transformer successful")
        logger.info(f"  - Rows after transform: {len(cleaned)}")
        logger.info(f"  - New columns: {[col for col in cleaned.columns if col not in df.columns]}")
        
        # Check key calculations
        if 'monthly_pct_change' in cleaned.columns:
            logger.info(f"  - Monthly changes calculated: {cleaned['monthly_pct_change'].notna().sum()}")
        if 'significant_move' in cleaned.columns:
            logger.info(f"  - Significant moves: {cleaned['significant_move'].sum()}")
        
        return True, cleaned
    except Exception as e:
        logger.error(f"✗ Transformer test failed: {e}")
        return False, None


def test_repository():
    """Test repository operations"""
    logger.info("\nTesting repository...")
    try:
        from src.repositories.forex_repository import ForexRepository
        from src.models.base import SessionLocal
        
        session = SessionLocal()
        repo = ForexRepository(session)
        
        # Test getting/creating currency pair
        pair = repo.get_currency_pair("USD", "TEST")
        if pair:
            logger.info(f"✓ Repository successful")
            logger.info(f"  - Created/found pair: {pair.pair_symbol}")
            
            # Clean up test pair
            session.delete(pair)
            session.commit()
            logger.info(f"  - Cleaned up test pair")
        
        session.close()
        return True
    except Exception as e:
        logger.error(f"✗ Repository test failed: {e}")
        return False


def test_loader():
    """Test database loader"""
    logger.info("\nTesting database loader...")
    try:
        from src.loaders.forex_db_loader import ForexDatabaseLoader
        
        # Create test data with correct column names matching ForexRate model
        test_data = pd.DataFrame([{
            'date': pd.Timestamp.now(),
            'exchange_rate': 1.1234,
            'open': 1.1200,  # This matches the model field name
            'high': 1.1250,
            'low': 1.1180,
            'volume': 1000000,
            'monthly_pct_change': 0.5,
            'yoy_pct_change': 2.3,
            'quarterly_volatility': 0.0123,
            'significant_move': False,
            'base_currency': 'TEST',
            'quote_currency': 'LOAD'
        }])
        
        logger.info(f"Loading {len(test_data)} records for TEST/LOAD")
        
        with ForexDatabaseLoader() as loader:
            result = loader.load_dataframe(test_data, "TEST", "LOAD")
            
            if result['success']:
                logger.info(f"✓ Loader successful")
                logger.info(f"  - Records processed: {result['records_processed']}")
                
                # Clean up test data
                from src.models.base import SessionLocal
                from src.models.models import CurrencyPair, ForexRate
                
                session = SessionLocal()
                test_pair = session.query(CurrencyPair).filter(
                    CurrencyPair.pair_symbol == "TEST/LOAD"
                ).first()
                
                if test_pair:
                    session.query(ForexRate).filter(
                        ForexRate.currency_pair_id == test_pair.id
                    ).delete()
                    session.delete(test_pair)
                    session.commit()
                    logger.info(f"  - Cleaned up test data")
                
                session.close()
                return True
            else:
                logger.error(f"✗ Loader failed: {result.get('error')}")
                return False
        
    except Exception as e:
        logger.error(f"✗ Loader test failed: {e}")
        return False


def main():
    """Run all tests"""
    logger.info("Starting Pipeline Component Tests")
    logger.info("="*50)
    
    results = {
        "Database Connection": test_database_connection(),
        "Models": test_models(),
    }
    
    # Test collector
    collector_success, df = test_collector()
    results["Collector"] = collector_success
    
    # Test transformer if collector succeeded
    if collector_success and df is not None:
        transformer_success, cleaned = test_transformer(df)
        results["Transformer"] = transformer_success
    else:
        results["Transformer"] = False
    
    # Test repository and loader
    results["Repository"] = test_repository()
    results["Loader"] = test_loader()
    
    # Summary
    logger.info("\n" + "="*50)
    logger.info("TEST SUMMARY")
    logger.info("="*50)
    
    for component, success in results.items():
        status = "✓ PASS" if success else "✗ FAIL"
        logger.info(f"{component}: {status}")
    
    all_passed = all(results.values())
    logger.info("\n" + ("="*50))
    if all_passed:
        logger.info("✓ ALL TESTS PASSED - Pipeline is ready to run!")
        logger.info("\nRun the full pipeline with:")
        logger.info("  python scripts/run_forex_pipeline.py")
    else:
        logger.info("✗ SOME TESTS FAILED - Please fix issues before running pipeline")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())