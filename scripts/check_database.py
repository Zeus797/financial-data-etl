import sys
from pathlib import Path

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

from src.models.base import SessionLocal
from src.models.models import CurrencyPair, ForexRate
from datetime import datetime
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_database_content():
    """Check the content of the forex database"""
    logger.info("\n=== Database Content Check ===")
    
    try:
        with SessionLocal() as session:
            # Check currency pairs
            pairs = session.query(CurrencyPair).all()
            logger.info(f"\nCurrency Pairs in Database: {len(pairs)}")
            
            for pair in pairs:
                rates_count = session.query(ForexRate).filter(
                    ForexRate.currency_pair_id == pair.id
                ).count()
                logger.info(f"- {pair.pair_symbol}: {rates_count} rates")
                
                # Get latest rate
                latest_rate = session.query(ForexRate).filter(
                    ForexRate.currency_pair_id == pair.id
                ).order_by(ForexRate.date.desc()).first()
                
                if latest_rate:
                    logger.info(f"  Latest rate ({latest_rate.date}): {latest_rate.exchange_rate}")
    
    except Exception as e:
        logger.error(f"Error checking database: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    success = check_database_content()
    sys.exit(0 if success else 1)