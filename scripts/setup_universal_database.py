# scripts/setup_universal_database.py
"""
Universal database setup for ALL asset classes
Creates shared dimensions that enable cross-asset analysis in PowerBI
"""
import sys
import os
from pathlib import Path
from datetime import datetime, date, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.models.base import db_manager, get_session
from src.models.universal_models import (
    Base, DimCurrency, DimCountry, DimEntity, DimFinancialInstrument, 
    DimDate, FactCryptocurrencyPrice, FactTreasurySecurity, 
    FactMoneyMarketFund, FactForexRate, FactMarketIndex,
    create_date_dimension_record
)

def setup_universal_database():
    """Complete database setup for ALL asset classes"""
    
    print("🔧 Setting up Universal Financial Data Warehouse...")
    print("   This creates shared dimensions for cross-asset analysis in PowerBI")
    
    # Test connection first
    print("1. Testing database connection...")
    if not db_manager.test_connection():
        print("❌ Database connection failed!")
        print("   Please check your .env file with database credentials")
        return False
    
    print("✅ Database connection successful!")
    
    # Create tables
    print("2. Creating universal database tables...")
    try:
        # Update the engine reference in Base
        Base.metadata.bind = db_manager.engine
        Base.metadata.create_all(db_manager.engine)
        print("✅ All database tables created successfully!")
    except Exception as e:
        print(f"❌ Failed to create tables: {e}")
        return False
    
    # Populate shared dimensions
    print("3. Populating shared dimension data...")
    try:
        populate_shared_dimensions()
        print("✅ Shared dimension data populated successfully!")
    except Exception as e:
        print(f"❌ Failed to populate dimension data: {e}")
        return False
    
    # Create date dimension
    print("4. Creating date dimension...")
    try:
        populate_date_dimension()
        print("✅ Date dimension populated successfully!")
    except Exception as e:
        print(f"❌ Failed to create date dimension: {e}")
        return False
    
    print("\n🎉 Universal database setup completed successfully!")
    print("\n📊 What was created:")
    print("   ✅ Shared dimension tables for ALL asset classes")
    print("   ✅ Universal currency table (fiat + crypto)")
    print("   ✅ Universal entity table (banks + exchanges + governments)")
    print("   ✅ Universal instrument table (crypto + forex + treasury + money market)")
    print("   ✅ Universal date dimension for PowerBI time intelligence")
    print("   ✅ Fact tables for all asset types")
    
    print("\n🔗 Cross-Asset Analysis Enabled:")
    print("   • Compare crypto vs treasury vs money market performance")
    print("   • Analyze all assets by country/region")
    print("   • Single date slicer works across ALL asset types")
    print("   • Universal entity analysis (bank products vs crypto)")
    
    print("\n🎯 Ready for asset-by-asset data loading:")
    print("   1. Cryptocurrency: python scripts/run_crypto_pipeline.py")
    print("   2. Forex: Enhanced forex pipeline")
    print("   3. Treasury: Automated PDF processing")
    print("   4. Money Market: Web scraping")
    
    return True

def populate_shared_dimensions():
    """Populate shared dimension tables with comprehensive data"""
    
    with get_session() as session:
        # Check if data already exists
        existing_currencies = session.query(DimCurrency).count()
        if existing_currencies > 0:
            print("   Shared dimension data already exists, skipping...")
            return
        
        # 1. CURRENCIES (Fiat + Crypto + Commodities)
        print("   Adding currencies (fiat + crypto)...")
        currencies = [
            # Major Fiat Currencies
            DimCurrency(currency_code='USD', currency_name='US Dollar', currency_symbol='$', 
                       currency_type='fiat', country_code='USA'),
            DimCurrency(currency_code='EUR', currency_name='Euro', currency_symbol='€', 
                       currency_type='fiat', country_code='EUR'),
            DimCurrency(currency_code='GBP', currency_name='British Pound', currency_symbol='£', 
                       currency_type='fiat', country_code='GBR'),
            
            # African Currencies
            DimCurrency(currency_code='KES', currency_name='Kenyan Shilling', currency_symbol='KSh', 
                       currency_type='fiat', country_code='KEN'),
            DimCurrency(currency_code='ZAR', currency_name='South African Rand', currency_symbol='R', 
                       currency_type='fiat', country_code='ZAF'),
            DimCurrency(currency_code='NGN', currency_name='Nigerian Naira', currency_symbol='₦', 
                       currency_type='fiat', country_code='NGA'),
            DimCurrency(currency_code='EGP', currency_name='Egyptian Pound', currency_symbol='E£', 
                       currency_type='fiat', country_code='EGY'),
            DimCurrency(currency_code='TZS', currency_name='Tanzanian Shilling', currency_symbol='TSh', 
                       currency_type='fiat', country_code='TZA'),
            DimCurrency(currency_code='GHS', currency_name='Ghanaian Cedi', currency_symbol='₵', 
                       currency_type='fiat', country_code='GHA'),
            DimCurrency(currency_code='DZD', currency_name='Algerian Dinar', currency_symbol='د.ج', 
                       currency_type='fiat', country_code='DZA'),
            DimCurrency(currency_code='MAD', currency_name='Moroccan Dirham', currency_symbol='د.م.', 
                       currency_type='fiat', country_code='MAR'),
            DimCurrency(currency_code='ETB', currency_name='Ethiopian Birr', currency_symbol='Br', 
                       currency_type='fiat', country_code='ETH'),
            
            # Cryptocurrencies
            DimCurrency(currency_code='BTC', currency_name='Bitcoin', currency_symbol='₿', 
                       currency_type='cryptocurrency', country_code=None),
            DimCurrency(currency_code='ETH', currency_name='Ethereum', currency_symbol='Ξ', 
                       currency_type='cryptocurrency', country_code=None),
            DimCurrency(currency_code='BNB', currency_name='Binance Coin', currency_symbol='BNB', 
                       currency_type='cryptocurrency', country_code=None),
            DimCurrency(currency_code='SOL', currency_name='Solana', currency_symbol='SOL', 
                       currency_type='cryptocurrency', country_code=None),
            DimCurrency(currency_code='ADA', currency_name='Cardano', currency_symbol='ADA', 
                       currency_type='cryptocurrency', country_code=None),
            
            # Stablecoins
            DimCurrency(currency_code='USDT', currency_name='Tether', currency_symbol='USDT', 
                       currency_type='cryptocurrency', country_code=None),
            DimCurrency(currency_code='USDC', currency_name='USD Coin', currency_symbol='USDC', 
                       currency_type='cryptocurrency', country_code=None),
            DimCurrency(currency_code='BUSD', currency_name='Binance USD', currency_symbol='BUSD', 
                       currency_type='cryptocurrency', country_code=None),
            DimCurrency(currency_code='DAI', currency_name='Dai', currency_symbol='DAI', 
                       currency_type='cryptocurrency', country_code=None),
            DimCurrency(currency_code='TUSD', currency_name='TrueUSD', currency_symbol='TUSD', 
                       currency_type='cryptocurrency', country_code=None),
        ]
        
        for currency in currencies:
            session.add(currency)
        session.commit()
        
        # 2. COUNTRIES (African focus + major economies)
        print("   Adding countries and regions...")
        countries = [
            # Major African Countries
            DimCountry(country_code='KEN', country_name='Kenya', region='East Africa', continent='Africa',
                      latitude=-1.2921, longitude=36.8219, primary_currency_code='KES'),
            DimCountry(country_code='ZAF', country_name='South Africa', region='Southern Africa', continent='Africa',
                      latitude=-30.5595, longitude=22.9375, primary_currency_code='ZAR'),
            DimCountry(country_code='NGA', country_name='Nigeria', region='West Africa', continent='Africa',
                      latitude=9.0820, longitude=8.6753, primary_currency_code='NGN'),
            DimCountry(country_code='EGY', country_name='Egypt', region='North Africa', continent='Africa',
                      latitude=26.0975, longitude=31.2357, primary_currency_code='EGP'),
            DimCountry(country_code='TZA', country_name='Tanzania', region='East Africa', continent='Africa',
                      latitude=-6.3690, longitude=34.8888, primary_currency_code='TZS'),
            DimCountry(country_code='GHA', country_name='Ghana', region='West Africa', continent='Africa',
                      latitude=7.9465, longitude=-1.0232, primary_currency_code='GHS'),
            DimCountry(country_code='DZA', country_name='Algeria', region='North Africa', continent='Africa',
                      latitude=28.0339, longitude=1.6596, primary_currency_code='DZD'),
            DimCountry(country_code='MAR', country_name='Morocco', region='North Africa', continent='Africa',
                      latitude=31.7917, longitude=-7.0926, primary_currency_code='MAD'),
            DimCountry(country_code='ETH', country_name='Ethiopia', region='East Africa', continent='Africa',
                      latitude=9.1450, longitude=40.4897, primary_currency_code='ETB'),
            
            # Major Global Economies
            DimCountry(country_code='USA', country_name='United States', region='North America', continent='North America',
                      latitude=39.8283, longitude=-98.5795, primary_currency_code='USD'),
            DimCountry(country_code='EUR', country_name='Eurozone', region='Europe', continent='Europe',
                      latitude=50.1109, longitude=8.6821, primary_currency_code='EUR'),
            DimCountry(country_code='GBR', country_name='United Kingdom', region='Europe', continent='Europe',
                      latitude=55.3781, longitude=-3.4360, primary_currency_code='GBP'),
        ]
        
        for country in countries:
            session.add(country)
        session.commit()
        
        # 3. ENTITIES (Banks, Exchanges, Governments, Platforms)
        print("   Adding entities (banks, exchanges, governments, platforms)...")
        entities = [
            # Kenyan Financial Institutions
            DimEntity(entity_name='Central Bank of Kenya', entity_type='government', entity_code='CBK',
                     country_code='KEN', website_url='https://www.centralbank.go.ke', 
                     regulatory_status='government'),
            DimEntity(entity_name='Equity Bank Kenya', entity_type='bank', entity_code='EQUITY',
                     country_code='KEN', website_url='https://www.equitybank.co.ke', 
                     regulatory_status='regulated'),
            DimEntity(entity_name='KCB Bank Kenya', entity_type='bank', entity_code='KCB',
                     country_code='KEN', website_url='https://www.kcbgroup.com', 
                     regulatory_status='regulated'),
            DimEntity(entity_name='Cooperative Bank of Kenya', entity_type='bank', entity_code='COOP',
                     country_code='KEN', website_url='https://www.co-opbank.co.ke', 
                     regulatory_status='regulated'),
            DimEntity(entity_name='ABSA Bank Kenya', entity_type='bank', entity_code='ABSA',
                     country_code='KEN', website_url='https://www.absa.co.ke', 
                     regulatory_status='regulated'),
            DimEntity(entity_name='Standard Chartered Bank Kenya', entity_type='bank', entity_code='STANCHART',
                     country_code='KEN', website_url='https://www.sc.com/ke', 
                     regulatory_status='regulated'),
            DimEntity(entity_name='Nairobi Securities Exchange', entity_type='exchange', entity_code='NSE',
                     country_code='KEN', website_url='https://www.nse.co.ke', 
                     regulatory_status='regulated'),
            
            # Fund Managers
            DimEntity(entity_name='Equity Investment Bank', entity_type='fund_manager', entity_code='EIB',
                     country_code='KEN', website_url='https://www.equityinvestment.co.ke', 
                     regulatory_status='regulated'),
            DimEntity(entity_name='CIC Asset Management', entity_type='fund_manager', entity_code='CIC',
                     country_code='KEN', website_url='https://www.cic.co.ke', 
                     regulatory_status='regulated'),
            DimEntity(entity_name='Zimele Asset Management', entity_type='fund_manager', entity_code='ZIMELE',
                     country_code='KEN', website_url='https://www.zimele.co.ke', 
                     regulatory_status='regulated'),
            
            # Cryptocurrency Platforms
            DimEntity(entity_name='CoinGecko', entity_type='platform', entity_code='GECKO',
                     country_code=None, website_url='https://www.coingecko.com', 
                     regulatory_status='unregulated'),
            DimEntity(entity_name='Bitcoin Foundation', entity_type='issuer', entity_code='BTC_FOUND',
                     country_code=None, website_url='https://bitcoinfoundation.org', 
                     regulatory_status='unregulated'),
            DimEntity(entity_name='Ethereum Foundation', entity_type='issuer', entity_code='ETH_FOUND',
                     country_code=None, website_url='https://ethereum.foundation', 
                     regulatory_status='unregulated'),
            DimEntity(entity_name='Binance', entity_type='exchange', entity_code='BINANCE',
                     country_code=None, website_url='https://www.binance.com', 
                     regulatory_status='licensed'),
            DimEntity(entity_name='Tether Limited', entity_type='issuer', entity_code='TETHER',
                     country_code=None, website_url='https://tether.to', 
                     regulatory_status='regulated'),
            DimEntity(entity_name='Centre Consortium', entity_type='issuer', entity_code='CENTRE',
                     country_code=None, website_url='https://www.centre.io', 
                     regulatory_status='regulated'),
            
            # International Entities
            DimEntity(entity_name='Federal Reserve', entity_type='government', entity_code='FED',
                     country_code='USA', website_url='https://www.federalreserve.gov', 
                     regulatory_status='government'),
            DimEntity(entity_name='European Central Bank', entity_type='government', entity_code='ECB',
                     country_code='EUR', website_url='https://www.ecb.europa.eu', 
                     regulatory_status='government'),
        ]
        
        for entity in entities:
            session.add(entity)
        session.commit()
        
        # 4. FINANCIAL INSTRUMENTS (All Asset Classes)
        print("   Adding financial instruments for all asset classes...")
        
        # Get entity IDs for relationships
        cbk = session.query(DimEntity).filter_by(entity_code='CBK').first()
        equity_bank = session.query(DimEntity).filter_by(entity_code='EQUITY').first()
        kcb_bank = session.query(DimEntity).filter_by(entity_code='KCB').first()
        btc_foundation = session.query(DimEntity).filter_by(entity_code='BTC_FOUND').first()
        eth_foundation = session.query(DimEntity).filter_by(entity_code='ETH_FOUND').first()
        binance = session.query(DimEntity).filter_by(entity_code='BINANCE').first()
        tether = session.query(DimEntity).filter_by(entity_code='TETHER').first()
        centre = session.query(DimEntity).filter_by(entity_code='CENTRE').first()
        
        instruments = [
            # CRYPTOCURRENCIES
            DimFinancialInstrument(
                instrument_type='cryptocurrency', instrument_name='Bitcoin', instrument_code='BTC',
                display_name='Bitcoin (BTC)', primary_currency_code='BTC', issuer_entity_id=btc_foundation.entity_id if btc_foundation else None,
                asset_class='alternative', risk_level='high', coingecko_id='bitcoin', is_stablecoin=False,
                description='The first and largest cryptocurrency by market cap', inception_date=date(2009, 1, 3)
            ),
            DimFinancialInstrument(
                instrument_type='cryptocurrency', instrument_name='Ethereum', instrument_code='ETH',
                display_name='Ethereum (ETH)', primary_currency_code='ETH', issuer_entity_id=eth_foundation.entity_id if eth_foundation else None,
                asset_class='alternative', risk_level='high', coingecko_id='ethereum', is_stablecoin=False,
                description='Smart contract platform and cryptocurrency', inception_date=date(2015, 7, 30)
            ),
            DimFinancialInstrument(
                instrument_type='cryptocurrency', instrument_name='Binance Coin', instrument_code='BNB',
                display_name='Binance Coin (BNB)', primary_currency_code='BNB', issuer_entity_id=binance.entity_id if binance else None,
                asset_class='alternative', risk_level='high', coingecko_id='binancecoin', is_stablecoin=False,
                description='Binance exchange native token', inception_date=date(2017, 7, 8)
            ),
            DimFinancialInstrument(
                instrument_type='cryptocurrency', instrument_name='Solana', instrument_code='SOL',
                display_name='Solana (SOL)', primary_currency_code='SOL', 
                asset_class='alternative', risk_level='high', coingecko_id='solana', is_stablecoin=False,
                description='High-performance blockchain platform', inception_date=date(2020, 3, 16)
            ),
            DimFinancialInstrument(
                instrument_type='cryptocurrency', instrument_name='Cardano', instrument_code='ADA',
                display_name='Cardano (ADA)', primary_currency_code='ADA',
                asset_class='alternative', risk_level='high', coingecko_id='cardano', is_stablecoin=False,
                description='Research-driven blockchain platform', inception_date=date(2017, 9, 29)
            ),
            
            # STABLECOINS
            DimFinancialInstrument(
                instrument_type='stablecoin', instrument_name='Tether', instrument_code='USDT',
                display_name='Tether (USDT)', primary_currency_code='USDT', issuer_entity_id=tether.entity_id if tether else None,
                asset_class='currency', risk_level='low', coingecko_id='tether', is_stablecoin=True,
                description='USD-pegged stablecoin', inception_date=date(2014, 10, 6)
            ),
            DimFinancialInstrument(
                instrument_type='stablecoin', instrument_name='USD Coin', instrument_code='USDC',
                display_name='USD Coin (USDC)', primary_currency_code='USDC', issuer_entity_id=centre.entity_id if centre else None,
                asset_class='currency', risk_level='low', coingecko_id='usd-coin', is_stablecoin=True,
                description='Regulated USD-pegged stablecoin', inception_date=date(2018, 9, 26)
            ),
            DimFinancialInstrument(
                instrument_type='stablecoin', instrument_name='Binance USD', instrument_code='BUSD',
                display_name='Binance USD (BUSD)', primary_currency_code='BUSD', issuer_entity_id=binance.entity_id if binance else None,
                asset_class='currency', risk_level='low', coingecko_id='binance-usd', is_stablecoin=True,
                description='Binance-issued USD-pegged stablecoin', inception_date=date(2019, 9, 5)
            ),
            DimFinancialInstrument(
                instrument_type='stablecoin', instrument_name='Dai', instrument_code='DAI',
                display_name='Dai (DAI)', primary_currency_code='DAI',
                asset_class='currency', risk_level='low', coingecko_id='dai', is_stablecoin=True,
                description='Decentralized USD-pegged stablecoin', inception_date=date(2017, 12, 18)
            ),
            
            # FOREX PAIRS (Major African pairs)
            DimFinancialInstrument(
                instrument_type='forex_pair', instrument_name='USD/KES', instrument_code='USDKES',
                display_name='US Dollar / Kenyan Shilling', primary_currency_code='USD', secondary_currency_code='KES',
                asset_class='currency', risk_level='medium', bloomberg_ticker='USDKES',
                description='US Dollar to Kenyan Shilling exchange rate'
            ),
            DimFinancialInstrument(
                instrument_type='forex_pair', instrument_name='EUR/KES', instrument_code='EURKES',
                display_name='Euro / Kenyan Shilling', primary_currency_code='EUR', secondary_currency_code='KES',
                asset_class='currency', risk_level='medium', bloomberg_ticker='EURKES',
                description='Euro to Kenyan Shilling exchange rate'
            ),
            DimFinancialInstrument(
                instrument_type='forex_pair', instrument_name='USD/ZAR', instrument_code='USDZAR',
                display_name='US Dollar / South African Rand', primary_currency_code='USD', secondary_currency_code='ZAR',
                asset_class='currency', risk_level='medium', bloomberg_ticker='USDZAR',
                description='US Dollar to South African Rand exchange rate'
            ),
            DimFinancialInstrument(
                instrument_type='forex_pair', instrument_name='USD/NGN', instrument_code='USDNGN',
                display_name='US Dollar / Nigerian Naira', primary_currency_code='USD', secondary_currency_code='NGN',
                asset_class='currency', risk_level='medium', bloomberg_ticker='USDNGN',
                description='US Dollar to Nigerian Naira exchange rate'
            ),
            
            # TREASURY SECURITIES (Kenya focus)
            DimFinancialInstrument(
                instrument_type='treasury_bond', instrument_name='Kenya 2-Year Government Bond', instrument_code='KE_2Y',
                display_name='Kenya 2-Year Bond', primary_currency_code='KES', issuer_entity_id=cbk.entity_id if cbk else None,
                asset_class='fixed_income', risk_level='low', 
                description='2-year Kenya government bond', maturity_date=date(2027, 6, 30)
            ),
            DimFinancialInstrument(
                instrument_type='treasury_bond', instrument_name='Kenya 5-Year Government Bond', instrument_code='KE_5Y',
                display_name='Kenya 5-Year Bond', primary_currency_code='KES', issuer_entity_id=cbk.entity_id if cbk else None,
                asset_class='fixed_income', risk_level='low',
                description='5-year Kenya government bond', maturity_date=date(2030, 6, 30)
            ),
            DimFinancialInstrument(
                instrument_type='treasury_bond', instrument_name='Kenya 10-Year Government Bond', instrument_code='KE_10Y',
                display_name='Kenya 10-Year Bond', primary_currency_code='KES', issuer_entity_id=cbk.entity_id if cbk else None,
                asset_class='fixed_income', risk_level='low',
                description='10-year Kenya government bond', maturity_date=date(2035, 6, 30)
            ),
            DimFinancialInstrument(
                instrument_type='treasury_bill', instrument_name='Kenya 91-Day Treasury Bill', instrument_code='KE_91D',
                display_name='Kenya 91-Day T-Bill', primary_currency_code='KES', issuer_entity_id=cbk.entity_id if cbk else None,
                asset_class='fixed_income', risk_level='low',
                description='91-day Kenya treasury bill', minimum_investment=100000
            ),
            DimFinancialInstrument(
                instrument_type='treasury_bill', instrument_name='Kenya 182-Day Treasury Bill', instrument_code='KE_182D',
                display_name='Kenya 182-Day T-Bill', primary_currency_code='KES', issuer_entity_id=cbk.entity_id if cbk else None,
                asset_class='fixed_income', risk_level='low',
                description='182-day Kenya treasury bill', minimum_investment=100000
            ),
            DimFinancialInstrument(
                instrument_type='treasury_bill', instrument_name='Kenya 364-Day Treasury Bill', instrument_code='KE_364D',
                display_name='Kenya 364-Day T-Bill', primary_currency_code='KES', issuer_entity_id=cbk.entity_id if cbk else None,
                asset_class='fixed_income', risk_level='low',
                description='364-day Kenya treasury bill', minimum_investment=100000
            ),
            
            # MONEY MARKET FUNDS
            DimFinancialInstrument(
                instrument_type='money_market_fund', instrument_name='Equity Money Market Fund (KES)', instrument_code='EMMF_KES',
                display_name='Equity MMF (KES)', primary_currency_code='KES', 
                issuer_entity_id=equity_bank.entity_id if equity_bank else None, manager_entity_id=equity_bank.entity_id if equity_bank else None,
                asset_class='fixed_income', risk_level='low',
                description='KES-denominated money market fund', minimum_investment=1000
            ),
            DimFinancialInstrument(
                instrument_type='money_market_fund', instrument_name='Equity Money Market Fund (USD)', instrument_code='EMMF_USD',
                display_name='Equity MMF (USD)', primary_currency_code='USD',
                issuer_entity_id=equity_bank.entity_id if equity_bank else None, manager_entity_id=equity_bank.entity_id if equity_bank else None,
                asset_class='fixed_income', risk_level='low',
                description='USD-denominated money market fund', minimum_investment=100
            ),
            DimFinancialInstrument(
                instrument_type='money_market_fund', instrument_name='KCB Money Market Fund (KES)', instrument_code='KCBMF_KES',
                display_name='KCB MMF (KES)', primary_currency_code='KES',
                issuer_entity_id=kcb_bank.entity_id if kcb_bank else None, manager_entity_id=kcb_bank.entity_id if kcb_bank else None,
                asset_class='fixed_income', risk_level='low',
                description='KES-denominated KCB money market fund', minimum_investment=1000
            ),
        ]
        
        for instrument in instruments:
            session.add(instrument)
        session.commit()
        
        print(f"   ✅ Added {len(currencies)} currencies")
        print(f"   ✅ Added {len(countries)} countries")
        print(f"   ✅ Added {len(entities)} entities")
        print(f"   ✅ Added {len(instruments)} instruments")

def populate_date_dimension(years_back=2, years_forward=3):
    """Populate date dimension for time intelligence"""
    
    with get_session() as session:
        # Check if dates already exist
        existing_dates = session.query(DimDate).count()
        if existing_dates > 0:
            print("   Date dimension already populated, skipping...")
            return
        
        print(f"   Creating date dimension ({years_back} years back, {years_forward} years forward)...")
        
        start_date = date.today() - timedelta(days=365 * years_back)
        end_date = date.today() + timedelta(days=365 * years_forward)
        
        current_date = start_date
        dates_added = 0
        
        while current_date <= end_date:
            try:
                date_record = create_date_dimension_record(current_date)
                session.add(date_record)
                dates_added += 1
                
                # Commit in batches for better performance
                if dates_added % 100 == 0:
                    session.commit()
                
            except Exception as e:
                print(f"   Warning: Failed to add date {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        session.commit()
        print(f"   ✅ Added {dates_added} date records")

def verify_universal_setup():
    """Verify universal database setup"""
    
    print("\n🔍 Verifying universal database setup...")
    
    with get_session() as session:
        # Check table counts
        currency_count = session.query(DimCurrency).count()
        country_count = session.query(DimCountry).count()
        entity_count = session.query(DimEntity).count()
        instrument_count = session.query(DimFinancialInstrument).count()
        date_count = session.query(DimDate).count()
        
        print(f"   📊 Database Contents:")
        print(f"      - Currencies: {currency_count} (fiat + crypto)")
        print(f"      - Countries: {country_count} (African focus)")
        print(f"      - Entities: {entity_count} (banks + exchanges + governments)")
        print(f"      - Instruments: {instrument_count} (all asset classes)")
        print(f"      - Dates: {date_count} (time intelligence)")
        
        # Test cross-asset relationships
        print(f"   🔗 Cross-Asset Analysis Ready:")
        
        # Instruments by asset class
        crypto_count = session.query(DimFinancialInstrument).filter_by(asset_class='alternative').count()
        forex_count = session.query(DimFinancialInstrument).filter(
            DimFinancialInstrument.instrument_type.like('%forex%')).count()
        treasury_count = session.query(DimFinancialInstrument).filter(
            DimFinancialInstrument.instrument_type.like('%treasury%')).count()
        mmf_count = session.query(DimFinancialInstrument).filter_by(instrument_type='money_market_fund').count()
        
        print(f"      - Cryptocurrencies: {crypto_count}")
        print(f"      - Forex Pairs: {forex_count}")
        print(f"      - Treasury Securities: {treasury_count}")
        print(f"      - Money Market Funds: {mmf_count}")
        
        # Geographic distribution
        kenyan_entities = session.query(DimEntity).filter_by(country_code='KEN').count()
        african_countries = session.query(DimCountry).filter_by(continent='Africa').count()
        
        print(f"      - Kenyan Entities: {kenyan_entities}")
        print(f"      - African Countries: {african_countries}")
        
        # Sample joined query
        print(f"   🧪 Sample Cross-Asset Query:")
        sample_instruments = session.query(
            DimFinancialInstrument.display_name,
            DimFinancialInstrument.instrument_type,
            DimCurrency.currency_name,
            DimEntity.entity_name
        ).outerjoin(DimCurrency, DimFinancialInstrument.primary_currency_code == DimCurrency.currency_code
        ).outerjoin(DimEntity, DimFinancialInstrument.issuer_entity_id == DimEntity.entity_id
        ).limit(5).all()
        
        for display_name, instrument_type, currency_name, entity_name in sample_instruments:
            print(f"      - {display_name} ({instrument_type}) in {currency_name}")
    
    print("✅ Universal database verification completed!")

def main():
    """Main setup function"""
    
    print("🚀 Universal Financial Data Warehouse Setup")
    print("=" * 60)
    print("This creates shared dimensions for cross-asset analysis in PowerBI")
    print("Supports: Crypto + Forex + Treasury + Money Market + Fixed Deposits + ETFs")
    
    # Run setup
    success = setup_universal_database()
    
    if success:
        # Verify setup
        verify_universal_setup()
        
        print("\n" + "=" * 60)
        print("🎉 UNIVERSAL SETUP COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
        print("\n💡 Cross-Asset Analysis Benefits:")
        print("   ✅ Single date slicer works across ALL asset types")
        print("   ✅ Compare crypto vs treasury vs money market performance")
        print("   ✅ Analyze all assets by country/region")
        print("   ✅ Universal entity analysis (bank products vs crypto)")
        print("   ✅ Consistent time intelligence across all reports")
        
        print("\n🎯 Next Steps - Asset-by-Asset Data Loading:")
        print("   1. Cryptocurrency: python scripts/run_crypto_pipeline.py")
        print("   2. Forex: Enhanced forex pipeline with shared dimensions")
        print("   3. Treasury: Automated PDF processing")
        print("   4. Money Market: Web scraping automation")
        print("   5. Fixed Deposits: Bank rate monitoring")
        
        print("\n📊 PowerBI Benefits:")
        print("   • Build unified dashboards across all asset classes")
        print("   • Create comprehensive investment overview reports")
        print("   • Enable sophisticated cross-asset correlation analysis")
        print("   • Support advanced geographic and entity analysis")
        
    else:
        print("\n❌ Setup failed. Please fix the issues above and try again.")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)