import sys
from pathlib import Path

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

from src.models.base import SessionLocal
from src.models.models import CurrencyPair, ForexRate
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_pipeline():
    """Verify entire pipeline from database to API"""
    
    # 1. Check Database
    with SessionLocal() as db:
        pairs = db.query(CurrencyPair).all()
        logger.info(f"Database has {len(pairs)} currency pairs")
        
        for pair in pairs:
            rates = db.query(ForexRate).filter(
                ForexRate.currency_pair_id == pair.id
            ).count()
            logger.info(f"{pair.pair_symbol}: {rates} rates")
            
    # 2. Verify API matches database
    response = requests.get("http://localhost:8000/api/v1/forex/pairs")
    api_pairs = response.json()
    logger.info(f"API returns {len(api_pairs)} currency pairs")
    
    # 3. Compare counts
    logger.info("\nVerification:")
    logger.info(f"Database pairs: {len(pairs)}")
    logger.info(f"API pairs: {len(api_pairs)}")
    logger.info("Pipeline status: ✅ Working" if len(pairs) == len(api_pairs) else "Pipeline status: ❌ Mismatch")

if __name__ == "__main__":
    verify_pipeline()