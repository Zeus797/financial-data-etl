#!/usr/bin/env python3
"""
scripts/setup_crypto_database.py

Fixed setup script to initialize database for cryptocurrency pipeline
- Creates all tables from universal schema
- Populates dimension tables with required reference data in correct order
- Sets up cryptocurrency instruments
"""

import sys
from pathlib import Path
from datetime import datetime, date

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from models.base import get_session, db_manager
from models.universal_models import (
    Base, DimCurrency, DimCountry, DimEntity, 
    DimFinancialInstrument, DimDate, FactCryptocurrencyPrice,
    get_date_key, create_date_dimension_record
)

def main():
    """Main setup function"""
    print("🚀 Setting up cryptocurrency database...")
    print("=" * 60)
    
    # Step 1: Test database connection
    print("1️⃣ Testing database connection...")
    if not db_manager.test_connection():
        print("❌ Database connection failed!")
        print("\n💡 Check your .env file contains:")
        print("   DB_HOST=localhost")
        print("   DB_PORT=5432")
        print("   DB_NAME=postgres")
        print("   DB_USER=your_username")
        print("   DB_PASSWORD=your_password")
        return False
    print("✅ Database connection successful")
    
    # Step 2: Create all tables
    print("\n2️⃣ Creating database tables...")
    try:
        Base.metadata.create_all(bind=db_manager.engine)
        print("✅ All tables created successfully")
    except Exception as e:
        print(f"❌ Failed to create tables: {e}")
        return False
    
    # Step 3: Setup dimension data in correct order (countries first!)
    print("\n3️⃣ Setting up dimension tables...")
    setup_countries()      # MUST be first due to foreign key constraints
    setup_currencies()     # Now can reference countries
    setup_entities()       # Can reference countries
    setup_crypto_instruments()  # Can reference entities and currencies
    
    # Step 4: Verify setup
    print("\n4️⃣ Verifying database setup...")
    if verify_setup():
        print("\n🎉 Database setup completed successfully!")
        print("✅ Your cryptocurrency pipeline is ready to use")
        return True
    else:
        print("\n⚠️ Setup completed with warnings")
        return False

def setup_countries():
    """Setup country dimension FIRST (before currencies that reference it)"""
    print("🌍 Setting up countries...")
    
    countries_data = [
        ('US', 'United States', 'North America'),
        ('KE', 'Kenya', 'Africa'),
        ('DE', 'Germany', 'Europe'),
        ('GB', 'United Kingdom', 'Europe'),
        ('JP', 'Japan', 'Asia'),
        ('CA', 'Canada', 'North America'),
        ('AU', 'Australia', 'Oceania'),
        ('SG', 'Singapore', 'Asia')
    ]
    
    with get_session() as session:
        added_count = 0
        for code, name, region in countries_data:
            existing = session.query(DimCountry).filter_by(country_code=code).first()
            if not existing:
                country = DimCountry(
                    country_code=code,
                    country_name=name,
                    region=region
                )
                session.add(country)
                added_count += 1
        
        session.commit()
        print(f"   ✅ Added {added_count} countries")

def setup_currencies():
    """Setup currency dimension with crypto and fiat currencies"""
    print("💰 Setting up currencies...")
    
    currencies_data = [
        # Fiat currencies
        ('USD', 'US Dollar', 'US'),
        ('KES', 'Kenyan Shilling', 'KE'),
        ('EUR', 'Euro', 'DE'),
        ('GBP', 'British Pound', 'GB'),
        ('JPY', 'Japanese Yen', 'JP'),
        # Crypto currencies (for reference)
        ('BTC', 'Bitcoin', 'US'),
        ('ETH', 'Ethereum', 'US'),
        ('BNB', 'Binance Coin', 'US'),
        ('SOL', 'Solana', 'US'),
        ('ADA', 'Cardano', 'US'),
        ('USDT', 'Tether', 'US'),
        ('USDC', 'USD Coin', 'US'),
        ('BUSD', 'Binance USD', 'US'),
        ('DAI', 'Dai', 'US'),
        ('TUSD', 'TrueUSD', 'US')
    ]
    
    with get_session() as session:
        added_count = 0
        for code, name, country_code in currencies_data:
            existing = session.query(DimCurrency).filter_by(currency_code=code).first()
            if not existing:
                currency = DimCurrency(
                    currency_code=code,
                    currency_name=name,
                    country_code=country_code
                )
                session.add(currency)
                added_count += 1
        
        session.commit()
        print(f"   ✅ Added {added_count} currencies")

def setup_entities():
    """Setup entity dimension with crypto exchanges and platforms"""
    print("🏢 Setting up entities...")
    
    entities_data = [
        # Crypto exchanges
        ('Binance', 'exchange', 'BINANCE', 'US', 'https://binance.com', 'regulated'),
        ('Coinbase', 'exchange', 'COINBASE', 'US', 'https://coinbase.com', 'regulated'),
        ('CoinGecko', 'platform', 'COINGECKO', 'SG', 'https://coingecko.com', 'unregulated'),
        ('Yahoo Finance', 'platform', 'YAHOO', 'US', 'https://finance.yahoo.com', 'regulated'),
        ('CoinAPI', 'platform', 'COINAPI', 'US', 'https://coinapi.io', 'regulated'),
        # Crypto projects/foundations
        ('Bitcoin Foundation', 'issuer', 'BTC_FOUND', 'US', 'https://bitcoin.org', 'unregulated'),
        ('Ethereum Foundation', 'issuer', 'ETH_FOUND', 'US', 'https://ethereum.org', 'unregulated'),
        ('Binance Organization', 'issuer', 'BNB_ORG', 'US', 'https://binance.org', 'regulated'),
        ('Solana Foundation', 'issuer', 'SOL_FOUND', 'US', 'https://solana.com', 'unregulated'),
        ('Cardano Foundation', 'issuer', 'ADA_FOUND', 'US', 'https://cardano.org', 'unregulated'),
        ('Tether Limited', 'issuer', 'TETHER', 'US', 'https://tether.to', 'regulated'),
        ('Centre Consortium', 'issuer', 'CENTRE', 'US', 'https://centre.io', 'regulated'),
        ('MakerDAO', 'issuer', 'MAKERDAO', 'US', 'https://makerdao.com', 'unregulated'),
        ('TrustToken', 'issuer', 'TRUSTTOKEN', 'US', 'https://trusttoken.com', 'regulated')
    ]
    
    with get_session() as session:
        added_count = 0
        for name, entity_type, code, country, website, status in entities_data:
            existing = session.query(DimEntity).filter_by(entity_code=code).first()
            if not existing:
                entity = DimEntity(
                    entity_name=name,
                    entity_type=entity_type,
                    entity_code=code,
                    country_code=country,
                    website_url=website,
                    regulatory_status=status,
                    is_active=True
                )
                session.add(entity)
                added_count += 1
        
        session.commit()
        print(f"   ✅ Added {added_count} entities")

def setup_crypto_instruments():
    """Setup cryptocurrency instruments that match your collector's target_coins"""
    print("🪙 Setting up cryptocurrency instruments...")
    
    # Get entity IDs for issuers
    with get_session() as session:
        entities = {
            entity.entity_code: entity.entity_id 
            for entity in session.query(DimEntity).all()
        }
    
    crypto_instruments_data = [
        # (type, name, code, display_name, currency, coingecko_id, is_stablecoin, asset_class, issuer_code)
        ('cryptocurrency', 'Bitcoin', 'BTC', 'Bitcoin', 'USD', 'bitcoin', False, 'alternative', 'BTC_FOUND'),
        ('cryptocurrency', 'Ethereum', 'ETH', 'Ethereum', 'USD', 'ethereum', False, 'alternative', 'ETH_FOUND'),
        ('cryptocurrency', 'Binance Coin', 'BNB', 'Binance Coin', 'USD', 'binancecoin', False, 'alternative', 'BNB_ORG'),
        ('cryptocurrency', 'Solana', 'SOL', 'Solana', 'USD', 'solana', False, 'alternative', 'SOL_FOUND'),
        ('cryptocurrency', 'Cardano', 'ADA', 'Cardano', 'USD', 'cardano', False, 'alternative', 'ADA_FOUND'),
        ('stablecoin', 'Tether', 'USDT', 'Tether USD', 'USD', 'tether', True, 'currency', 'TETHER'),
        ('stablecoin', 'USD Coin', 'USDC', 'USD Coin', 'USD', 'usd-coin', True, 'currency', 'CENTRE'),
        ('stablecoin', 'Binance USD', 'BUSD', 'Binance USD', 'USD', 'binance-usd', True, 'currency', 'BNB_ORG'),
        ('stablecoin', 'Dai', 'DAI', 'Dai Stablecoin', 'USD', 'dai', True, 'currency', 'MAKERDAO'),
        ('stablecoin', 'TrueUSD', 'TUSD', 'TrueUSD', 'USD', 'true-usd', True, 'currency', 'TRUSTTOKEN')
    ]
    
    with get_session() as session:
        added_count = 0
        
        for instrument_type, name, code, display_name, currency, coingecko_id, is_stablecoin, asset_class, issuer_code in crypto_instruments_data:
            # Check if already exists
            existing = session.query(DimFinancialInstrument).filter_by(
                coingecko_id=coingecko_id
            ).first()
            
            if not existing:
                issuer_id = entities.get(issuer_code)
                
                instrument = DimFinancialInstrument(
                    instrument_type=instrument_type,
                    instrument_name=name,
                    instrument_code=code,
                    display_name=display_name,
                    primary_currency_code=currency,
                    asset_class=asset_class,
                    coingecko_id=coingecko_id,
                    is_stablecoin=is_stablecoin,
                    issuer_entity_id=issuer_id,
                    risk_level='high' if not is_stablecoin else 'low',
                    is_active=True,
                    inception_date=date(2020, 1, 1),  # Approximate for crypto
                    description=f"{name} - {instrument_type.replace('_', ' ').title()}"
                )
                session.add(instrument)
                added_count += 1
        
        session.commit()
        print(f"   ✅ Added {added_count} cryptocurrency instruments")

def verify_setup():
    """Verify that database setup is complete and compatible with loader"""
    print("🔍 Verifying database setup...")
    
    with get_session() as session:
        # Check table counts
        counts = {
            'currencies': session.query(DimCurrency).count(),
            'countries': session.query(DimCountry).count(),
            'entities': session.query(DimEntity).count(),
            'instruments': session.query(DimFinancialInstrument).count(),
            'crypto_instruments': session.query(DimFinancialInstrument).filter(
                DimFinancialInstrument.instrument_type.in_(['cryptocurrency', 'stablecoin'])
            ).count()
        }
        
        print("📊 Table counts:")
        for table, count in counts.items():
            print(f"   {table}: {count}")
        
        # Verify crypto instruments match collector expectations
        expected_coins = [
            'bitcoin', 'ethereum', 'binancecoin', 'solana', 'cardano',
            'tether', 'usd-coin', 'binance-usd', 'dai', 'true-usd'
        ]
        
        crypto_instruments = session.query(DimFinancialInstrument).filter(
            DimFinancialInstrument.coingecko_id.in_(expected_coins)
        ).all()
        
        print(f"\n🪙 Cryptocurrency instrument mappings:")
        found_coins = []
        for instrument in crypto_instruments:
            print(f"   {instrument.coingecko_id} → {instrument.display_name} (ID: {instrument.instrument_id})")
            found_coins.append(instrument.coingecko_id)
        
        missing_coins = set(expected_coins) - set(found_coins)
        if missing_coins:
            print(f"\n⚠️ Missing instruments: {missing_coins}")
            return False
        else:
            print(f"\n✅ All {len(expected_coins)} required cryptocurrency instruments configured")
            return True

def test_loader_compatibility():
    """Test that the loader can work with the database setup"""
    print("\n🧪 Testing loader compatibility...")
    
    try:
        # Import loader and test instrument mappings
        from loaders.crypto_loader import CryptoLoader
        
        loader = CryptoLoader()
        mappings = loader.get_instrument_mappings()
        
        print(f"✅ Loader found {len(mappings)} instrument mappings")
        
        # Check for all expected cryptocurrencies
        expected_coins = [
            'bitcoin', 'ethereum', 'binancecoin', 'solana', 'cardano',
            'tether', 'usd-coin', 'binance-usd', 'dai', 'true-usd'
        ]
        
        missing = [coin for coin in expected_coins if coin not in mappings]
        
        if missing:
            print(f"⚠️ Loader cannot find: {missing}")
            return False
        else:
            print("✅ Loader can find all required cryptocurrency instruments")
            return True
            
    except ImportError as e:
        print(f"⚠️ Could not import loader: {e}")
        print("   This is OK if you haven't created the loader yet")
        return True
    except Exception as e:
        print(f"❌ Loader compatibility test failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    
    if success:
        # Optional: Test loader compatibility if available
        test_loader_compatibility()
        
        print("\n" + "=" * 60)
        print("🎯 NEXT STEPS:")
        print("1. Run your crypto collector to gather data")
        print("2. Transform the data with your transformer")
        print("3. Load the data with your loader") 
        print("4. Connect PowerBI to your PostgreSQL database")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("❌ Setup failed - please fix the issues above and try again")
        print("=" * 60)