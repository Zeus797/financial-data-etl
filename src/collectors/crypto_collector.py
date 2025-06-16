"""
Cryptocurrency data collector - Pure data collection from CoinGecko API
Separation of concerns: Only responsible for gathering raw data
"""
import asyncio
import aiohttp
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import time
import os
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CryptoCollectorConfig:
    """Configuration for cryptocurrency data collection"""
    api_key: Optional[str] = None
    
    # Multiple data sources for reliability
    coingecko_url: str = "https://api.coingecko.com/api/v3"
    coinapi_url: str = "https://rest.coinapi.io/v1"
    yahoo_finance_enabled: bool = True
    
    @property
    def base_url(self) -> str:
        """Primary API endpoint"""
        return self.coingecko_url
    
    # Source priority (try in this order)
    source_priority: List[str] = None
    
    rate_limit_delay: float = 2.0
    max_retries: int = 5
    timeout: int = 45
    
    # Target cryptocurrencies with multiple source mappings
    target_coins = {
        "bitcoin": {
            "coingecko_id": "bitcoin",
            "yahoo_symbol": "BTC-USD",
            "coinapi_symbol": "BTC"
        },
        "ethereum": {
            "coingecko_id": "ethereum", 
            "yahoo_symbol": "ETH-USD",
            "coinapi_symbol": "ETH"
        },
        "binancecoin": {
            "coingecko_id": "binancecoin",
            "yahoo_symbol": "BNB-USD", 
            "coinapi_symbol": "BNB"
        },
        "solana": {
            "coingecko_id": "solana",
            "yahoo_symbol": "SOL-USD",
            "coinapi_symbol": "SOL"
        },
        "cardano": {
            "coingecko_id": "cardano",
            "yahoo_symbol": "ADA-USD",
            "coinapi_symbol": "ADA"
        },
        "tether": {
            "coingecko_id": "tether",
            "yahoo_symbol": "USDT-USD",
            "coinapi_symbol": "USDT"
        },
        "usd-coin": {
            "coingecko_id": "usd-coin",
            "yahoo_symbol": "USDC-USD", 
            "coinapi_symbol": "USDC"
        },
        "binance-usd": {
            "coingecko_id": "binance-usd",
            "yahoo_symbol": "BUSD-USD",
            "coinapi_symbol": "BUSD"
        },
        "dai": {
            "coingecko_id": "dai",
            "yahoo_symbol": "DAI-USD",
            "coinapi_symbol": "DAI"
        },
        "true-usd": {
            "coingecko_id": "true-usd",
            "yahoo_symbol": "TUSD-USD",
            "coinapi_symbol": "TUSD"
        }
    }
    
    # Stablecoins for special tracking  
    stablecoins = ["tether", "usd-coin", "binance-usd", "dai", "true-usd"]
    
    def __post_init__(self):
        if self.source_priority is None:
            self.source_priority = ["coingecko", "yahoo", "coinapi"]

class CryptoCollector:
    """
    Pure cryptocurrency data collector
    
    Responsibilities:
    - Collect data from CoinGecko API
    - Handle rate limiting and retries
    - Return standardized data format
    
    NOT responsible for:
    - Data transformation or calculations
    - Database operations
    - Data validation beyond basic API response checks
    """
    
    def __init__(self, config: Optional[CryptoCollectorConfig] = None):
        self.config = config or CryptoCollectorConfig()
        self.config.api_key = self.config.api_key or os.getenv('COINGECKO_API_KEY')
        self.coinapi_key = os.getenv('COINAPI_KEY')
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Rate limiting per source
        self.last_request_time = {}
        
        # Try importing yfinance for Yahoo Finance fallback
        self.yfinance_available = False
        try:
            import yfinance
            self.yfinance_available = True
        except ImportError:
            logger.warning("yfinance not available - Yahoo Finance source disabled")
            if "yahoo" in self.config.source_priority:
                self.config.source_priority.remove("yahoo")
        
        logger.info(f"CryptoCollector initialized for {len(self.config.target_coins)} coins")
        logger.info(f"Available sources: {self.config.source_priority}")
    
    def _get_headers(self, source: str = "coingecko") -> Dict[str, str]:
        """Get API headers for specific source"""
        headers = {
            'User-Agent': 'Financial-Data-Pipeline/1.0',
            'Accept': 'application/json'
        }
        
        if source == "coingecko" and self.config.api_key:
            headers['x-cg-demo-api-key'] = self.config.api_key
        elif source == "coinapi" and self.coinapi_key:
            headers['X-CoinAPI-Key'] = self.coinapi_key
        
        return headers
    
    async def _rate_limit(self, source: str = "default"):
        """Implement per-source rate limiting"""
        if source not in self.last_request_time:
            self.last_request_time[source] = 0
            
        elapsed = time.time() - self.last_request_time[source]
        if elapsed < self.config.rate_limit_delay:
            await asyncio.sleep(self.config.rate_limit_delay - elapsed)
        self.last_request_time[source] = time.time()
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.timeout)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def _make_request(self, url: str, params: Dict[str, Any] = None, source: str = "coingecko") -> Dict[str, Any]:
        """Make HTTP request with retry logic and source-specific handling"""
        if params is None:
            params = {}
            
        await self._rate_limit(source)
        
        headers = self._get_headers(source)
        
        for attempt in range(self.config.max_retries):
            try:
                if not self.session:
                    self.session = aiohttp.ClientSession(
                        timeout=aiohttp.ClientTimeout(total=self.config.timeout)
                    )
                
                async with self.session.get(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:  # Rate limited
                        wait_time = min(60, 5 * (2 ** attempt))
                        logger.warning(f"{source} rate limited, waiting {wait_time}s before retry")
                        await asyncio.sleep(wait_time)
                        continue
                    elif response.status == 404:
                        logger.error(f"{source} resource not found: {url}")
                        raise Exception(f"Resource not found: {url}")
                    else:
                        error_text = await response.text()
                        logger.error(f"{source} HTTP {response.status}: {error_text[:200]}")
                        response.raise_for_status()
                        
            except asyncio.TimeoutError:
                wait_time = min(30, 2 ** attempt)
                logger.warning(f"{source} timeout on attempt {attempt + 1}, waiting {wait_time}s")
                if attempt == self.config.max_retries - 1:
                    raise
                await asyncio.sleep(wait_time)
                
            except aiohttp.ClientError as e:
                wait_time = min(30, 2 ** attempt)
                logger.warning(f"{source} request failed on attempt {attempt + 1}: {e}, waiting {wait_time}s")
                if attempt == self.config.max_retries - 1:
                    raise
                await asyncio.sleep(wait_time)
        
        raise Exception(f"Failed to complete {source} request after {self.config.max_retries} attempts")
    
    async def collect_from_yahoo_finance(self, coin_key: str) -> Optional[Dict[str, Any]]:
        """Collect data from Yahoo Finance as fallback"""
        if not self.yfinance_available:
            return None
            
        try:
            import yfinance as yf
            
            coin_config = self.config.target_coins[coin_key]
            yahoo_symbol = coin_config.get('yahoo_symbol')
            
            if not yahoo_symbol:
                return None
            
            logger.info(f"Fetching {coin_key} from Yahoo Finance ({yahoo_symbol})...")
            
            ticker = yf.Ticker(yahoo_symbol)
            
            # Get current info
            info = ticker.info
            
            # Get recent price data (last 2 days to get current price)
            hist = ticker.history(period="2d", interval="1d")
            
            if hist.empty:
                return None
            
            latest = hist.iloc[-1]
            
            return {
                'coingecko_id': coin_key,
                'symbol': coin_config.get('coinapi_symbol', coin_key.upper()),
                'name': info.get('longName', coin_key.title()),
                'current_price': float(latest['Close']),
                'market_cap': info.get('marketCap'),
                'total_volume': float(latest['Volume']) if 'Volume' in latest else None,
                'high_24h': float(latest['High']),
                'low_24h': float(latest['Low']),
                'price_change_24h': float(latest['Close'] - latest['Open']),
                'last_updated': datetime.now().isoformat(),
                'collection_timestamp': datetime.now().isoformat(),
                'data_source': 'yahoo_finance'
            }
            
        except Exception as e:
            logger.warning(f"Yahoo Finance failed for {coin_key}: {e}")
            return None
    
    async def collect_from_coinapi(self, coin_key: str) -> Optional[Dict[str, Any]]:
        """Collect data from CoinAPI as alternative source"""
        if not self.coinapi_key:
            return None
            
        try:
            coin_config = self.config.target_coins[coin_key]
            symbol = coin_config.get('coinapi_symbol', coin_key.upper())
            
            url = f"{self.config.coinapi_url}/exchangerate/{symbol}/USD"
            
            response_data = await self._make_request(url, source="coinapi")
            
            return {
                'coingecko_id': coin_key,
                'symbol': symbol,
                'name': coin_key.title(),
                'current_price': response_data.get('rate'),
                'last_updated': response_data.get('time'),
                'collection_timestamp': datetime.now().isoformat(),
                'data_source': 'coinapi'
            }
            
        except Exception as e:
            logger.warning(f"CoinAPI failed for {coin_key}: {e}")
            return None
    
    async def collect_current_prices(self) -> List[Dict[str, Any]]:
        """
        Collect current cryptocurrency prices
        
        Returns:
            List of raw price records from API
        """
        logger.info("Collecting current cryptocurrency prices...")
        
        # Use base_url property
        url = f"{self.config.base_url}/simple/price"
        params = {
            'ids': ','.join(self.config.target_coins),
            'vs_currencies': 'usd',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true',
            'include_last_updated_at': 'true'
        }
        
        try:
            response_data = await self._make_request(url, params)
            
            current_prices = []
            for coin_id, data in response_data.items():
                price_record = {
                    'coingecko_id': coin_id,
                    'price_usd': data.get('usd'),
                    'market_cap_usd': data.get('usd_market_cap'),
                    'volume_24h_usd': data.get('usd_24h_vol'),
                    'price_change_24h_percent': data.get('usd_24h_change'),
                    'last_updated_timestamp': data.get('last_updated_at'),
                    'collection_timestamp': datetime.now().isoformat(),
                    'data_source': 'coingecko_simple_price'
                }
                current_prices.append(price_record)
            
            logger.info(f"Collected current prices for {len(current_prices)} cryptocurrencies")
            return current_prices
            
        except Exception as e:
            logger.error(f"Failed to collect current prices: {e}")
            raise
    
    async def collect_market_data(self) -> List[Dict[str, Any]]:
        """
        Collect comprehensive market data including rankings
        
        Returns:
            List of detailed market records from API
        """
        logger.info("Collecting comprehensive market data...")
        
        # Use base_url property
        url = f"{self.config.base_url}/coins/markets"
        params = {
            'vs_currency': 'usd',
            'ids': ','.join(self.config.target_coins.keys()),
            'order': 'market_cap_desc',
            'per_page': len(self.config.target_coins),
            'page': 1,
            'sparkline': 'false',
            'price_change_percentage': '24h,7d,30d'
        }
        
        try:
            response_data = await self._make_request(url, params)
            
            market_data = []
            for coin in response_data:
                market_record = {
                    'coingecko_id': coin.get('id'),
                    'symbol': coin.get('symbol'),
                    'name': coin.get('name'),
                    'current_price': coin.get('current_price'),
                    'market_cap': coin.get('market_cap'),
                    'market_cap_rank': coin.get('market_cap_rank'),
                    'fully_diluted_valuation': coin.get('fully_diluted_valuation'),
                    'total_volume': coin.get('total_volume'),
                    'high_24h': coin.get('high_24h'),
                    'low_24h': coin.get('low_24h'),
                    'price_change_24h': coin.get('price_change_24h'),
                    'price_change_percentage_24h': coin.get('price_change_percentage_24h'),
                    'market_cap_change_24h': coin.get('market_cap_change_24h'),
                    'market_cap_change_percentage_24h': coin.get('market_cap_change_percentage_24h'),
                    'circulating_supply': coin.get('circulating_supply'),
                    'total_supply': coin.get('total_supply'),
                    'max_supply': coin.get('max_supply'),
                    'ath': coin.get('ath'),
                    'ath_change_percentage': coin.get('ath_change_percentage'),
                    'ath_date': coin.get('ath_date'),
                    'atl': coin.get('atl'),
                    'atl_change_percentage': coin.get('atl_change_percentage'),
                    'atl_date': coin.get('atl_date'),
                    'price_change_percentage_7d': coin.get('price_change_percentage_7d_in_currency'),
                    'price_change_percentage_30d': coin.get('price_change_percentage_30d_in_currency'),
                    'last_updated': coin.get('last_updated'),
                    'collection_timestamp': datetime.now().isoformat(),
                    'data_source': 'coingecko_markets'
                }
                market_data.append(market_record)
            
            logger.info(f"Collected market data for {len(market_data)} cryptocurrencies")
            return market_data
            
        except Exception as e:
            logger.error(f"Failed to collect market data: {e}")
            raise
    
    async def collect_historical_data(self, coin_id: str, days: int = 365 * 5) -> List[Dict[str, Any]]:
        """
        Collect historical price data with chunking for longer periods
        
        Args:
            coin_id: CoinGecko coin identifier
            days: Number of days of historical data (default 5 years)
        """
        logger.info(f"Collecting {days} days of historical data for {coin_id}...")
        
        # Calculate chunks to avoid rate limits
        chunk_size = 90  # CoinGecko works best with 90-day chunks
        chunks = [(i, min(i + chunk_size, days)) 
                 for i in range(0, days, chunk_size)]
        
        all_historical_data = []
        
        for chunk_start, chunk_end in chunks:
            try:
                # Calculate dates for this chunk
                end_date = datetime.now() - timedelta(days=chunk_start)
                
                url = f"{self.config.base_url}/coins/{coin_id}/market_chart"
                params = {
                    'vs_currency': 'usd',
                    'days': str(chunk_end - chunk_start),
                    'interval': 'daily'
                }
                
                logger.info(f"Collecting chunk {chunk_start}-{chunk_end} days for {coin_id}...")
                response_data = await self._make_request(url, params)
                
                # Process chunk data
                prices = response_data.get('prices', [])
                market_caps = response_data.get('market_caps', [])
                volumes = response_data.get('total_volumes', [])
                
                for i, (timestamp, price) in enumerate(prices):
                    record_date = datetime.fromtimestamp(timestamp / 1000).date()
                    
                    historical_record = {
                        'coingecko_id': coin_id,
                        'record_date': record_date.isoformat(),
                        'timestamp': timestamp,
                        'price_usd': price,
                        'market_cap_usd': market_caps[i][1] if i < len(market_caps) else None,
                        'volume_24h_usd': volumes[i][1] if i < len(volumes) else None,
                        'collection_timestamp': datetime.now().isoformat(),
                        'data_source': 'coingecko_historical'
                    }
                    all_historical_data.append(historical_record)
                
                # Rate limiting delay between chunks
                await asyncio.sleep(2.0)
                
            except Exception as e:
                logger.error(f"Failed to collect chunk {chunk_start}-{chunk_end} for {coin_id}: {e}")
                # Continue with next chunk instead of failing completely
                await asyncio.sleep(5.0)  # Longer delay after error
                continue
        
        logger.info(f"Collected {len(all_historical_data)} historical records for {coin_id}")
        return all_historical_data
    
    async def collect_all_historical_data(self, days: int = 30) -> Dict[str, List[Dict[str, Any]]]:
        """
        Collect historical data for all target cryptocurrencies
        
        Args:
            days: Number of days of historical data
            
        Returns:
            Dictionary mapping coin_id to historical records
        """
        logger.info(f"Collecting {days} days of historical data for all cryptocurrencies...")
        
        all_historical = {}
        total_requests = len(self.config.target_coins)
        
        for i, coin_id in enumerate(self.config.target_coins.keys()):
            try:
                logger.info(f"Processing {coin_id} ({i+1}/{total_requests})...")
                historical_data = await self.collect_historical_data(coin_id, days)
                all_historical[coin_id] = historical_data
                
                # Add extra delay between coins to avoid rate limiting
                if i < total_requests - 1:
                    await asyncio.sleep(3.0)
                    
            except Exception as e:
                logger.error(f"Failed to collect historical data for {coin_id}: {e}")
                all_historical[coin_id] = []
                await asyncio.sleep(2.0)
        
        total_records = sum(len(records) for records in all_historical.values())
        logger.info(f"Collected {total_records} total historical records")
        
        return all_historical
    
    async def collect_all_data(self, historical_days: int = 30) -> Dict[str, Any]:
        """
        Collect all cryptocurrency data in one coordinated operation
        
        Args:
            historical_days: Number of days of historical data to collect
            
        Returns:
            Dictionary containing all collected data
        """
        logger.info("Starting comprehensive cryptocurrency data collection...")
        
        try:
            # Collect current market data (most comprehensive)
            market_data = await self.collect_market_data()
            
            # Collect historical data for technical analysis
            historical_data = await self.collect_all_historical_data(historical_days)
            
            result = {
                'market_data': market_data,
                'historical_data': historical_data,
                'collection_metadata': {
                    'collection_timestamp': datetime.now().isoformat(),
                    'historical_days': historical_days,
                    'total_coins': len(self.config.target_coins),
                    'market_records': len(market_data),
                    'historical_records': sum(len(records) for records in historical_data.values()),
                    'data_source': 'coingecko_api',
                    'collector_version': '1.0.0'
                }
            }
            
            logger.info("✅ Cryptocurrency data collection completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to collect cryptocurrency data: {e}")
            raise

# Convenience function for standalone usage
async def collect_crypto_data(historical_days: int = 30) -> Dict[str, Any]:
    """Standalone function to collect cryptocurrency data"""
    async with CryptoCollector() as collector:
        return await collector.collect_all_data(historical_days)

# Example usage and testing
if __name__ == "__main__":
    async def main():
        print("🔄 Testing cryptocurrency data collection...")
        
        try:
            # Test data collection
            async with CryptoCollector() as collector:
                # Test current prices
                current_prices = await collector.collect_current_prices()
                print(f"✅ Collected {len(current_prices)} current prices")
                
                # Test market data
                market_data = await collector.collect_market_data()
                print(f"✅ Collected market data for {len(market_data)} cryptocurrencies")
                
                # Test historical data for Bitcoin only
                btc_historical = await collector.collect_historical_data('bitcoin', days=7)
                print(f"✅ Collected {len(btc_historical)} historical records for Bitcoin")
                
                # Show sample data structure
                if market_data:
                    sample = market_data[0]
                    print(f"\n📊 Sample market data structure:")
                    print(f"   Coin: {sample['name']} ({sample['symbol']})")
                    print(f"   Price: ${sample['current_price']:,.2f}")
                    print(f"   Market Cap: ${sample['market_cap']:,.0f}")
                    print(f"   24h Change: {sample['price_change_percentage_24h']:.2f}%")
                
        except Exception as e:
            print(f"❌ Error during testing: {e}")
            return False
        
        print("\n✅ All collection tests completed successfully!")
        return True
    
    # Run the test
    asyncio.run(main())