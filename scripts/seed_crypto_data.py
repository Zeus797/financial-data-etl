import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy.orm import Session
from src.models.base import engine
from src.models.universal_models import DimCurrency, DimCountry, DimEntity, DimFinancialInstrument
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def seed_crypto_data():
    """Seed initial cryptocurrency data"""
    with Session(engine) as session:
        # 1. Add major cryptocurrencies
        cryptos = [
            {'code': 'BTC', 'name': 'Bitcoin', 'coingecko_id': 'bitcoin'},
            {'code': 'ETH', 'name': 'Ethereum', 'coingecko_id': 'ethereum'},
            {'code': 'USDT', 'name': 'Tether', 'coingecko_id': 'tether'},
            {'code': 'USDC', 'name': 'USD Coin', 'coingecko_id': 'usd-coin'},
            {'code': 'BNB', 'name': 'BNB', 'coingecko_id': 'binancecoin'}
        ]
        
        # Ensure USD exists for pairs
        usd = session.query(DimCurrency).filter_by(currency_code='USD').first()
        if not usd:
            usa = session.query(DimCountry).filter_by(country_code='US').first()
            if not usa:
                usa = DimCountry(country_code='US', country_name='United States', region='North America')
                session.add(usa)
                session.flush()
            
            usd = DimCurrency(currency_code='USD', currency_name='US Dollar', country_code='US')
            session.add(usd)
            session.flush()

        # Add cryptocurrencies to dimension tables
        for crypto in cryptos:
            # Add as currency
            crypto_curr = DimCurrency(
                currency_code=crypto['code'],
                currency_name=crypto['name'],
                country_code='US'  # Default to US for cryptocurrencies
            )
            session.add(crypto_curr)
            session.flush()
            
            # Add as financial instrument
            instrument = DimFinancialInstrument(
                instrument_type='cryptocurrency',
                instrument_name=crypto['name'],
                primary_currency_code=crypto['code'],
                secondary_currency_code='USD',
                asset_class='cryptocurrency',
                coingecko_id=crypto['coingecko_id'],
                is_active=True,
                is_stablecoin=crypto['code'] in ['USDT', 'USDC']
            )
            session.add(instrument)
        
        session.commit()
        logger.info("✅ Cryptocurrency data seeded successfully")

if __name__ == "__main__":
    seed_crypto_data()