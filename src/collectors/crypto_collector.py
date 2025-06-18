# src/collectors/crypto_collector.py
"""
Production-ready cryptocurrency data collector with task-specific source priority
Optimized for real-world Yahoo Finance rate limiting and maximum reliability

Task-Specific Source Priority:
- Current Prices: Binance → CoinGecko → Yahoo → CoinAPI (frequent updates)
- Historical Data: Yahoo → Binance → CoinGecko → CoinAPI (bulk collection)  
- Market Data: CoinGecko → Binance → Yahoo → CoinAPI (comprehensive metrics)
"""
import asyncio
import aiohttp
import time
import os
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CryptoCollectorConfig:
    """Production-ready configuration for cryptocurrency data collection"""
    api_key: Optional[str] = None
    
    # API endpoints
    yahoo_finance_enabled: bool = True
    binance_url: str = "https://api.binance.com/api/v3"
    coingecko_url: str = "https://api.coingecko.com/api/v3"
    coinapi_url: str = "https://rest.coinapi.io/v1"
    
    # Default settings
    rate_limit_delay: float = 2.0
    max_retries: int = 5
    timeout: int = 45
    
    # Enhanced target coins with all source mappings
    target_coins = {
        "bitcoin": {
            "coingecko_id": "bitcoin",
            "yahoo_symbol": "BTC-USD",
            "binance_symbol": "BTCUSDT",
            "coinapi_symbol": "BTC"
        },
        "ethereum": {
            "coingecko_id": "ethereum", 
            "yahoo_symbol": "ETH-USD",
            "binance_symbol": "ETHUSDT",
            "coinapi_symbol": "ETH"
        },
        "binancecoin": {
            "coingecko_id": "binancecoin",
            "yahoo_symbol": "BNB-USD",
            "binance_symbol": "BNBUSDT",
            "coinapi_symbol": "BNB"
        },
        "solana": {
            "coingecko_id": "solana",
            "yahoo_symbol": "SOL-USD",
            "binance_symbol": "SOLUSDT",
            "coinapi_symbol": "SOL"
        },
        "cardano": {
            "coingecko_id": "cardano",
            "yahoo_symbol": "ADA-USD",
            "binance_symbol": "ADAUSDT",
            "coinapi_symbol": "ADA"
        },
        "tether": {
            "coingecko_id": "tether",
            "yahoo_symbol": "USDT-USD",
            "binance_symbol": "USDCUSDT",  # USDT via USDC pair
            "coinapi_symbol": "USDT"
        },
        "usd-coin": {
            "coingecko_id": "usd-coin",
            "yahoo_symbol": "USDC-USD",
            "binance_symbol": "USDCUSDT",
            "coinapi_symbol": "USDC"
        },
        "binance-usd": {
            "coingecko_id": "binance-usd",
            "yahoo_symbol": "BUSD-USD",
            "binance_symbol": "BUSDUSDT",
            "coinapi_symbol": "BUSD"
        },
        "dai": {
            "coingecko_id": "dai",
            "yahoo_symbol": "DAI-USD",
            "binance_symbol": "DAIUSDT",
            "coinapi_symbol": "DAI"
        },
        "true-usd": {
            "coingecko_id": "true-usd",
            "yahoo_symbol": "TUSD-USD",
            "binance_symbol": "TUSDUSDT",
            "coinapi_symbol": "TUSD"
        }
    }
    
    # Stablecoin identification
    stablecoins = ["tether", "usd-coin", "binance-usd", "dai", "true-usd"]
    
    def __post_init__(self):
        """Set task-specific source priorities and rate limits"""
        
        # Task-specific source priorities (optimized for real-world performance)
        self.source_priority_by_task = {
            'current_prices': ['binance', 'coingecko', 'yahoo', 'coinapi'],     # Binance best for frequent updates
            'historical_data': ['yahoo', 'binance', 'coingecko', 'coinapi'],    # Yahoo best for bulk collection
            'market_data': ['coingecko', 'binance', 'yahoo', 'coinapi']         # CoinGecko most comprehensive
        }
        
        # Source-specific rate limits (based on real performance testing)
        self.rate_limits = {
            "yahoo": 1.5,        # Conservative for current prices, generous for historical
            "binance": 0.3,      # Very generous rate limits  
            "coingecko": 8.0,    # Conservative to avoid blocks
            "coinapi": 2.5       # Respectful of free tier
        }
        
        # Yahoo Finance specific settings (handles rate limiting better)
        self.yahoo_settings = {
            'historical_delay': 1.0,     # Delay between historical requests
            'current_delay': 3.0,        # Longer delay for current price requests
            'max_burst_requests': 3,      # Max requests before longer delay
            'burst_reset_time': 300       # 5 minutes reset
        }

    @property
    def base_url(self) -> str:
        """Primary API endpoint (for compatibility)"""
        return self.coingecko_url

    def get_source_priority(self, task: str) -> List[str]:
        """Get optimal source priority for specific task"""
        return self.source_priority_by_task.get(task, ['yahoo', 'binance', 'coingecko', 'coinapi'])

class CryptoCollector:
    """
    Production-ready cryptocurrency data collector with task-specific optimization
    
    Intelligent Source Selection:
    - Current Prices: Prioritizes reliable, frequent-update sources
    - Historical Data: Prioritizes fast bulk collection sources  
    - Market Data: Prioritizes comprehensive data sources
    
    Features:
    - Smart Yahoo Finance rate limit handling
    - Automatic fallback between sources
    - Task-optimized source selection
    - Production-grade error handling
    """
    
    def __init__(self, config: Optional[CryptoCollectorConfig] = None):
        self.config = config or CryptoCollectorConfig()
        self.config.api_key = self.config.api_key or os.getenv('COINGECKO_API_KEY')
        self.coinapi_key = os.getenv('COINAPI_KEY')
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Rate limiting per source
        self.last_request_time = {}
        
        # Yahoo Finance burst tracking
        self.yahoo_burst_count = 0
        self.yahoo_burst_start = 0
        
        # Check Yahoo Finance availability
        self.yfinance_available = False
        try:
            import yfinance
            self.yfinance_available = True
        except ImportError:
            logger.warning("yfinance not available - Yahoo Finance source disabled")
            # Remove yahoo from all task priorities
            for task in self.config.source_priority_by_task:
                if "yahoo" in self.config.source_priority_by_task[task]:
                    self.config.source_priority_by_task[task].remove("yahoo")
        
        logger.info(f"CryptoCollector initialized for {len(self.config.target_coins)} coins")
        logger.info(f"Task-specific priorities configured:")
        for task, priority in self.config.source_priority_by_task.items():
            logger.info(f"  {task}: {' → '.join(priority)}")

    def _get_headers(self, source: str = "coingecko") -> Dict[str, str]:
        """Get API headers for specific source"""
        headers = {
            'User-Agent': 'Financial-Data-Pipeline/2.0',
            'Accept': 'application/json'
        }
        
        if source == "coingecko" and self.config.api_key:
            headers['x-cg-demo-api-key'] = self.config.api_key
        elif source == "coinapi" and self.coinapi_key:
            headers['X-CoinAPI-Key'] = self.coinapi_key
        elif source == "binance":
            # Binance public API doesn't need auth headers
            pass
        
        return headers

    async def _rate_limit_yahoo_smart(self, request_type: str = "historical"):
        """Smart Yahoo Finance rate limiting based on request type and burst tracking"""
        if not self.yfinance_available:
            return
        
        current_time = time.time()
        
        # Reset burst counter if enough time has passed
        if current_time - self.yahoo_burst_start > self.config.yahoo_settings['burst_reset_time']:
            self.yahoo_burst_count = 0
            self.yahoo_burst_start = current_time
        
        # Determine delay based on request type and burst count
        if request_type == "current" or self.yahoo_burst_count >= self.config.yahoo_settings['max_burst_requests']:
            delay = self.config.yahoo_settings['current_delay']  # Longer delay for current prices or after burst
        else:
            delay = self.config.yahoo_settings['historical_delay']  # Shorter delay for historical data
        
        # Apply rate limiting
        last_request = self.last_request_time.get('yahoo', 0)
        elapsed = current_time - last_request
        
        if elapsed < delay:
            wait_time = delay - elapsed
            await asyncio.sleep(wait_time)
        
        # Update tracking
        self.last_request_time['yahoo'] = time.time()
        self.yahoo_burst_count += 1
        
        if self.yahoo_burst_count == 1:
            self.yahoo_burst_start = current_time

    async def _rate_limit(self, source: str = "default"):
        """Implement optimized per-source rate limiting"""
        if source == "yahoo":
            # Use smart Yahoo rate limiting
            await self._rate_limit_yahoo_smart()
            return
        
        if source not in self.last_request_time:
            self.last_request_time[source] = 0
        
        # Use source-specific rate limits
        delay_needed = self.config.rate_limits.get(source, self.config.rate_limit_delay)
        elapsed = time.time() - self.last_request_time[source]
        
        if elapsed < delay_needed:
            wait_time = delay_needed - elapsed
            await asyncio.sleep(wait_time)
        
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
                        wait_time = min(60, 8 * (2 ** attempt))  # More conservative backoff
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
                wait_time = min(30, 3 ** attempt)
                logger.warning(f"{source} timeout on attempt {attempt + 1}, waiting {wait_time}s")
                if attempt == self.config.max_retries - 1:
                    raise
                await asyncio.sleep(wait_time)
                
            except aiohttp.ClientError as e:
                wait_time = min(30, 3 ** attempt)
                logger.warning(f"{source} request failed on attempt {attempt + 1}: {e}, waiting {wait_time}s")
                if attempt == self.config.max_retries - 1:
                    raise
                await asyncio.sleep(wait_time)
        
        raise Exception(f"Failed to complete {source} request after {self.config.max_retries} attempts")

    # YAHOO FINANCE - OPTIMIZED FOR HISTORICAL DATA
    async def collect_yahoo_historical(self, coin_key: str, days: int = 365 * 5) -> Optional[List[Dict[str, Any]]]:
        """Collect historical data from Yahoo Finance (optimized for bulk collection)"""
        if not self.yfinance_available:
            return None
            
        try:
            import yfinance as yf
            
            coin_config = self.config.target_coins[coin_key]
            yahoo_symbol = coin_config.get('yahoo_symbol')
            
            if not yahoo_symbol:
                return None
            
            logger.info(f"🚀 Fetching {coin_key} from Yahoo Finance ({yahoo_symbol}) - {days} days instantly...")
            
            # Smart rate limiting for historical requests
            await self._rate_limit_yahoo_smart("historical")
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            ticker = yf.Ticker(yahoo_symbol)
            
            # Single call for ALL historical data - Yahoo's strength
            hist = ticker.history(start=start_date, end=end_date, interval="1d")
            
            if hist.empty:
                logger.warning(f"No Yahoo Finance historical data for {coin_key}")
                return None
            
            historical_data = []
            for date_index, row in hist.iterrows():
                historical_data.append({
                    'coingecko_id': coin_key,
                    'record_date': date_index.date().isoformat(),
                    'timestamp': int(date_index.timestamp() * 1000),
                    'price_usd': float(row['Close']),
                    'high_24h': float(row['High']),
                    'low_24h': float(row['Low']),
                    'volume_24h_usd': float(row['Volume']) if 'Volume' in row else None,
                    'collection_timestamp': datetime.now().isoformat(),
                    'data_source': 'yahoo_finance'
                })
            
            logger.info(f"✅ Yahoo Finance: {len(historical_data)} records collected instantly!")
            return historical_data
            
        except Exception as e:
            logger.warning(f"Yahoo Finance historical failed for {coin_key}: {e}")
            return None

    async def collect_yahoo_current_prices(self) -> List[Dict[str, Any]]:
        """Get current prices from Yahoo Finance (with conservative rate limiting)"""
        if not self.yfinance_available:
            return []
        
        try:
            import yfinance as yf
            
            current_prices = []
            
            # More conservative approach for current prices
            for coin_key, coin_config in self.config.target_coins.items():
                yahoo_symbol = coin_config.get('yahoo_symbol')
                if not yahoo_symbol:
                    continue
                
                try:
                    # Smart rate limiting for current price requests
                    await self._rate_limit_yahoo_smart("current")
                    
                    ticker = yf.Ticker(yahoo_symbol)
                    
                    # Get recent price (avoid .info() which is more rate limited)
                    hist = ticker.history(period="2d", interval="1d")
                    if not hist.empty:
                        latest = hist.iloc[-1]
                        
                        current_prices.append({
                            'coingecko_id': coin_key,
                            'symbol': coin_config.get('coinapi_symbol', coin_key.upper()),
                            'name': coin_key.title(),
                            'current_price': float(latest['Close']),
                            'total_volume': float(latest['Volume']) if 'Volume' in latest else None,
                            'high_24h': float(latest['High']),
                            'low_24h': float(latest['Low']),
                            'price_change_24h': float(latest['Close'] - latest['Open']),
                            'last_updated': datetime.now().isoformat(),
                            'collection_timestamp': datetime.now().isoformat(),
                            'data_source': 'yahoo_finance'
                        })
                        
                except Exception as e:
                    logger.warning(f"Yahoo Finance current price failed for {coin_key}: {e}")
                    continue
            
            logger.info(f"✅ Yahoo Finance: {len(current_prices)} current prices collected")
            return current_prices
            
        except Exception as e:
            logger.error(f"Yahoo Finance current prices failed: {e}")
            return []

    # BINANCE - OPTIMIZED FOR CURRENT PRICES AND RELIABLE FALLBACK
    async def collect_binance_historical(self, coin_key: str, days: int = 365 * 5) -> Optional[List[Dict[str, Any]]]:
        """Collect historical data from Binance (efficient chunking)"""
        coin_config = self.config.target_coins.get(coin_key)
        if not coin_config or not coin_config.get('binance_symbol'):
            return None
        
        symbol = coin_config['binance_symbol']
        logger.info(f"📊 Fetching {coin_key} from Binance ({symbol}) - chunked efficiently...")
        
        # Calculate time range
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        
        all_data = []
        current_start = start_time
        
        # Binance allows 1000 candles per request
        while current_start < end_time:
            url = f"{self.config.binance_url}/klines"
            params = {
                'symbol': symbol,
                'interval': '1d',
                'startTime': current_start,
                'endTime': end_time,
                'limit': 1000
            }
            
            try:
                response_data = await self._make_request(url, params, source="binance")
                
                if response_data:
                    # Process Binance kline data
                    for kline in response_data:
                        timestamp = kline[0]
                        record_date = datetime.fromtimestamp(timestamp / 1000).date()
                        
                        all_data.append({
                            'coingecko_id': coin_key,
                            'record_date': record_date.isoformat(),
                            'timestamp': timestamp,
                            'price_usd': float(kline[4]),  # Close price
                            'high_24h': float(kline[2]),   # High
                            'low_24h': float(kline[3]),    # Low
                            'volume_24h_usd': float(kline[7]),  # Quote asset volume
                            'collection_timestamp': datetime.now().isoformat(),
                            'data_source': 'binance'
                        })
                    
                    # Move to next chunk
                    if response_data:
                        current_start = int(response_data[-1][0]) + (24 * 60 * 60 * 1000)
                    else:
                        break
                else:
                    break
                    
            except Exception as e:
                logger.warning(f"Binance request failed for {symbol}: {e}")
                break
        
        if all_data:
            logger.info(f"✅ Binance: {len(all_data)} records collected efficiently")
            return all_data
        
        return None

    async def collect_binance_current_prices(self) -> List[Dict[str, Any]]:
        """Get current prices from Binance (optimized for frequent updates)"""
        try:
            url = f"{self.config.binance_url}/ticker/24hr"
            response_data = await self._make_request(url, source="binance")
            
            current_prices = []
            
            # Create mapping of Binance symbols to our coin keys
            binance_to_coin = {}
            for coin_key, config in self.config.target_coins.items():
                binance_symbol = config.get('binance_symbol')
                if binance_symbol:
                    binance_to_coin[binance_symbol] = coin_key
            
            for ticker in response_data:
                coin_key = binance_to_coin.get(ticker['symbol'])
                if coin_key:
                    current_prices.append({
                        'coingecko_id': coin_key,
                        'symbol': self.config.target_coins[coin_key].get('coinapi_symbol', coin_key.upper()),
                        'name': coin_key.title(),
                        'current_price': float(ticker['lastPrice']),
                        'total_volume': float(ticker['quoteVolume']),
                        'price_change_24h_percent': float(ticker['priceChangePercent']),
                        'high_24h': float(ticker['highPrice']),
                        'low_24h': float(ticker['lowPrice']),
                        'collection_timestamp': datetime.now().isoformat(),
                        'data_source': 'binance'
                    })
            
            logger.info(f"✅ Binance: {len(current_prices)} current prices collected")
            return current_prices
            
        except Exception as e:
            logger.error(f"Binance current prices failed: {e}")
            return []

    # COINGECKO - OPTIMIZED FOR COMPREHENSIVE MARKET DATA
    async def collect_coingecko_historical(self, coin_id: str, days: int = 365 * 5) -> List[Dict[str, Any]]:
        """Collect historical data from CoinGecko (conservative chunking)"""
        logger.info(f"🐸 Using CoinGecko for {coin_id} (conservative rate limiting)...")
        
        # Conservative chunking for CoinGecko
        chunk_size = 90
        chunks = [(i, min(i + chunk_size, days)) 
                 for i in range(0, days, chunk_size)]
        
        all_historical_data = []
        
        for chunk_start, chunk_end in chunks:
            try:
                url = f"{self.config.coingecko_url}/coins/{coin_id}/market_chart"
                params = {
                    'vs_currency': 'usd',
                    'days': str(chunk_end - chunk_start),
                    'interval': 'daily'
                }
                
                logger.info(f"  CoinGecko chunk {chunk_start}-{chunk_end} days...")
                response_data = await self._make_request(url, params, source="coingecko")
                
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
                        'data_source': 'coingecko'
                    }
                    all_historical_data.append(historical_record)
                
                # Conservative delay between chunks
                await asyncio.sleep(3.0)
                
            except Exception as e:
                logger.error(f"CoinGecko chunk {chunk_start}-{chunk_end} failed for {coin_id}: {e}")
                await asyncio.sleep(8.0)  # Longer delay after error
                continue
        
        logger.info(f"✅ CoinGecko: {len(all_historical_data)} records collected (slowly)")
        return all_historical_data

    async def collect_coingecko_current_prices(self) -> List[Dict[str, Any]]:
        """Collect current prices from CoinGecko (simple batch API)"""
        logger.info("Collecting current prices from CoinGecko...")
        
        url = f"{self.config.coingecko_url}/simple/price"
        params = {
            'ids': ','.join(self.config.target_coins.keys()),
            'vs_currencies': 'usd',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true',
            'include_last_updated_at': 'true'
        }
        
        try:
            response_data = await self._make_request(url, params, source="coingecko")
            
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
                    'data_source': 'coingecko'
                }
                current_prices.append(price_record)
            
            logger.info(f"✅ CoinGecko: {len(current_prices)} current prices collected")
            return current_prices
            
        except Exception as e:
            logger.error(f"CoinGecko current prices failed: {e}")
            return []

    async def collect_coingecko_market_data(self) -> List[Dict[str, Any]]:
        """Collect comprehensive market data from CoinGecko (its specialty)"""
        logger.info("🏪 Collecting comprehensive market data from CoinGecko...")
        
        url = f"{self.config.coingecko_url}/coins/markets"
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
            response_data = await self._make_request(url, params, source="coingecko")
            
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
            
            logger.info(f"✅ Market data: {len(market_data)} coins from CoinGecko markets")
            return market_data
            
        except Exception as e:
            logger.warning(f"CoinGecko markets failed: {e}")
            raise

    # COINAPI - YOUR PAID BACKUP
    async def collect_coinapi_historical(self, coin_key: str, days: int = 365 * 5) -> Optional[List[Dict[str, Any]]]:
        """Collect historical data from your CoinAPI key"""
        if not self.coinapi_key:
            return None
            
        coin_config = self.config.target_coins.get(coin_key)
        if not coin_config:
            return None
            
        symbol = coin_config.get('coinapi_symbol', coin_key.upper())
        logger.info(f"💎 Using your CoinAPI key for {coin_key} ({symbol})...")
        
        try:
            url = f"{self.config.coinapi_url}/exchangerate/{symbol}/USD/history"
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            params = {
                'period_id': '1DAY',
                'time_start': start_date.isoformat(),
                'time_end': end_date.isoformat(),
                'limit': 10000  # Adjust based on your plan
            }
            
            response_data = await self._make_request(url, params, source="coinapi")
            
            historical_data = []
            for record in response_data:
                historical_data.append({
                    'coingecko_id': coin_key,
                    'record_date': record['time_period_start'][:10],  # Extract date
                    'timestamp': int(datetime.fromisoformat(record['time_period_start'].replace('Z', '+00:00')).timestamp() * 1000),
                    'price_usd': record.get('rate_close', record.get('rate_open')),
                    'high_24h': record.get('rate_high'),
                    'low_24h': record.get('rate_low'),
                    'collection_timestamp': datetime.now().isoformat(),
                    'data_source': 'coinapi'
                })
            
            logger.info(f"✅ CoinAPI: {len(historical_data)} records from your paid key")
            return historical_data
            
        except Exception as e:
            logger.warning(f"CoinAPI failed for {coin_key}: {e}")
            return None

    async def collect_coinapi_current_prices(self) -> List[Dict[str, Any]]:
        """Get current prices from CoinAPI"""
        if not self.coinapi_key:
            return []
        
        try:
            current_prices = []
            
            for coin_key, coin_config in self.config.target_coins.items():
                symbol = coin_config.get('coinapi_symbol')
                if not symbol:
                    continue
                
                url = f"{self.config.coinapi_url}/exchangerate/{symbol}/USD"
                
                try:
                    response_data = await self._make_request(url, source="coinapi")
                    
                    current_prices.append({
                        'coingecko_id': coin_key,
                        'symbol': symbol,
                        'name': coin_key.title(),
                        'current_price': response_data.get('rate'),
                        'last_updated': response_data.get('time'),
                        'collection_timestamp': datetime.now().isoformat(),
                        'data_source': 'coinapi'
                    })
                    
                except Exception as e:
                    logger.warning(f"CoinAPI failed for {coin_key}: {e}")
                    continue
            
            logger.info(f"✅ CoinAPI: {len(current_prices)} current prices from your key")
            return current_prices
            
        except Exception as e:
            logger.error(f"CoinAPI current prices failed: {e}")
            return []

    # SMART COLLECTION WITH TASK-SPECIFIC SOURCE PRIORITY
    async def collect_historical_data_smart(self, coin_id: str, days: int = 365 * 5) -> List[Dict[str, Any]]:
        """Smart historical collection with task-optimized source priority"""
        
        sources = self.config.get_source_priority('historical_data')
        logger.info(f"Collecting historical data for {coin_id} using priority: {' → '.join(sources)}")
        
        for source in sources:
            try:
                if source == "yahoo" and self.yfinance_available:
                    data = await self.collect_yahoo_historical(coin_id, days)
                elif source == "binance":
                    data = await self.collect_binance_historical(coin_id, days)
                elif source == "coingecko":
                    data = await self.collect_coingecko_historical(coin_id, days)
                elif source == "coinapi":
                    data = await self.collect_coinapi_historical(coin_id, days)
                else:
                    continue
                
                if data:
                    return data
                    
            except Exception as e:
                logger.warning(f"Historical data failed from {source} for {coin_id}: {e}")
                continue
        
        logger.error(f"All sources failed for historical data: {coin_id}")
        return []

    async def collect_current_prices_smart(self) -> List[Dict[str, Any]]:
        """Smart current price collection with task-optimized source priority"""
        
        sources = self.config.get_source_priority('current_prices')
        logger.info(f"Collecting current prices using priority: {' → '.join(sources)}")
        
        for source in sources:
            try:
                if source == "binance":
                    data = await self.collect_binance_current_prices()
                elif source == "coingecko":
                    data = await self.collect_coingecko_current_prices()
                elif source == "yahoo" and self.yfinance_available:
                    data = await self.collect_yahoo_current_prices()
                elif source == "coinapi":
                    data = await self.collect_coinapi_current_prices()
                else:
                    continue
                
                if data:
                    logger.info(f"✅ Current prices collected from {source}")
                    return data
                    
            except Exception as e:
                logger.warning(f"Current prices failed from {source}: {e}")
                continue
        
        logger.error("All sources failed for current prices")
        return []

    async def collect_market_data_smart(self) -> List[Dict[str, Any]]:
        """Smart market data collection with task-optimized source priority"""
        
        sources = self.config.get_source_priority('market_data')
        logger.info(f"Collecting market data using priority: {' → '.join(sources)}")
        
        for source in sources:
            try:
                if source == "coingecko":
                    # CoinGecko's specialty - comprehensive market data
                    data = await self.collect_coingecko_market_data()
                elif source == "binance":
                    # Fallback to current prices if markets endpoint fails
                    data = await self.collect_binance_current_prices()
                elif source == "yahoo" and self.yfinance_available:
                    data = await self.collect_yahoo_current_prices()
                elif source == "coinapi":
                    data = await self.collect_coinapi_current_prices()
                else:
                    continue
                
                if data:
                    source_used = data[0].get('data_source', source)
                    logger.info(f"✅ Market data collected from {source_used}")
                    return data
                    
            except Exception as e:
                logger.warning(f"Market data failed from {source}: {e}")
                continue
        
        logger.error("All sources failed for market data")
        return []

    async def collect_all_historical_data_smart(self, days: int = 365 * 5) -> Dict[str, List[Dict[str, Any]]]:
        """
        Collect historical data for all cryptocurrencies using task-optimized source priority
        
        Args:
            days: Number of days of historical data
            
        Returns:
            Dictionary mapping coin_id to historical records
        """
        logger.info(f"🚀 Collecting {days} days of historical data using task-optimized source priority...")
        
        all_historical = {}
        total_coins = len(self.config.target_coins)
        
        for i, coin_id in enumerate(self.config.target_coins.keys()):
            logger.info(f"Processing {coin_id} ({i+1}/{total_coins})...")
            
            try:
                historical_data = await self.collect_historical_data_smart(coin_id, days)
                all_historical[coin_id] = historical_data
                
                # Show which source was used
                if historical_data:
                    source_used = historical_data[0].get('data_source', 'unknown')
                    logger.info(f"  ✅ {coin_id}: {len(historical_data)} records from {source_used}")
                else:
                    logger.warning(f"  ❌ {coin_id}: No data collected from any source")
                
                # Small delay between coins to be respectful
                if i < total_coins - 1:
                    await asyncio.sleep(0.8)
                    
            except Exception as e:
                logger.error(f"Failed to collect historical data for {coin_id}: {e}")
                all_historical[coin_id] = []
                await asyncio.sleep(2.0)
        
        total_records = sum(len(records) for records in all_historical.values())
        successful_coins = len([coin for coin, records in all_historical.items() if records])
        
        logger.info(f"✅ Historical collection complete: {successful_coins}/{total_coins} coins, {total_records} total records")
        
        return all_historical

    async def collect_all_data_smart(self, historical_days: int = 365 * 5) -> Dict[str, Any]:
        """
        Collect all cryptocurrency data using task-specific source optimization
        
        Args:
            historical_days: Number of days of historical data to collect
            
        Returns:
            Dictionary containing all collected data with metadata
        """
        start_time = datetime.now()
        logger.info("🚀 Starting SMART cryptocurrency data collection with task-specific optimization...")
        
        # Log the strategy being used
        for task, sources in self.config.source_priority_by_task.items():
            logger.info(f"  {task}: {' → '.join(sources)}")
        
        try:
            # Collect comprehensive market data (CoinGecko optimized)
            market_data = await self.collect_market_data_smart()
            
            # Collect historical data using task-optimized priority (Yahoo optimized)
            historical_data = await self.collect_all_historical_data_smart(historical_days)
            
            # Calculate collection statistics
            successful_coins = len([coin for coin, records in historical_data.items() if records])
            total_historical_records = sum(len(records) for records in historical_data.values())
            
            # Determine which sources were actually used
            sources_used = set()
            for records in historical_data.values():
                if records:
                    sources_used.add(records[0].get('data_source', 'unknown'))
            
            if market_data:
                sources_used.add(market_data[0].get('data_source', 'unknown'))
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            result = {
                'market_data': market_data,
                'historical_data': historical_data,
                'collection_metadata': {
                    'collection_timestamp': end_time.isoformat(),
                    'collection_duration_seconds': duration.total_seconds(),
                    'historical_days_requested': historical_days,
                    'total_coins_targeted': len(self.config.target_coins),
                    'successful_coins': successful_coins,
                    'market_records': len(market_data),
                    'total_historical_records': total_historical_records,
                    'task_priorities_used': self.config.source_priority_by_task,
                    'sources_actually_used': list(sources_used),
                    'data_source_performance': self._calculate_source_performance(historical_data, market_data),
                    'collector_version': '2.1.0_task_optimized'
                }
            }
            
            # Log performance summary
            self._log_performance_summary(result['collection_metadata'])
            
            logger.info("✅ SMART cryptocurrency data collection completed successfully!")
            return result
            
        except Exception as e:
            logger.error(f"❌ SMART collection failed: {e}")
            raise

    def _calculate_source_performance(self, historical_data: Dict[str, List[Dict[str, Any]]], market_data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Calculate performance metrics for each data source used"""
        source_stats = {}
        
        # Analyze historical data sources
        for coin_id, records in historical_data.items():
            if not records:
                continue
                
            source = records[0].get('data_source', 'unknown')
            
            if source not in source_stats:
                source_stats[source] = {
                    'historical_coins': 0,
                    'historical_records': 0,
                    'market_data_coins': 0,
                    'total_records': 0
                }
            
            source_stats[source]['historical_coins'] += 1
            source_stats[source]['historical_records'] += len(records)
            source_stats[source]['total_records'] += len(records)
        
        # Analyze market data source
        if market_data:
            market_source = market_data[0].get('data_source', 'unknown')
            if market_source not in source_stats:
                source_stats[market_source] = {
                    'historical_coins': 0,
                    'historical_records': 0,
                    'market_data_coins': 0,
                    'total_records': 0
                }
            
            source_stats[market_source]['market_data_coins'] = len(market_data)
            source_stats[market_source]['total_records'] += len(market_data)
        
        # Calculate averages
        for source, stats in source_stats.items():
            if stats['historical_coins'] > 0:
                stats['avg_historical_records_per_coin'] = stats['historical_records'] / stats['historical_coins']
            else:
                stats['avg_historical_records_per_coin'] = 0
        
        return source_stats

    def _log_performance_summary(self, metadata: Dict[str, Any]):
        """Log a comprehensive performance summary"""
        duration = metadata['collection_duration_seconds']
        successful_coins = metadata['successful_coins']
        total_coins = metadata['total_coins_targeted']
        total_records = metadata['total_historical_records']
        market_records = metadata['market_records']
        
        logger.info("=" * 90)
        logger.info("🎯 TASK-OPTIMIZED COLLECTION PERFORMANCE SUMMARY")
        logger.info("=" * 90)
        logger.info(f"⏱️  Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        logger.info(f"🎯 Success Rate: {successful_coins}/{total_coins} coins ({(successful_coins/total_coins)*100:.1f}%)")
        logger.info(f"📊 Historical Records: {total_records:,}")
        logger.info(f"📈 Market Records: {market_records}")
        logger.info(f"⚡ Speed: {(total_records + market_records)/duration:.1f} records/second")
        
        logger.info(f"\n🔄 TASK-SPECIFIC SOURCE USAGE:")
        source_performance = metadata.get('data_source_performance', {})
        for source, stats in source_performance.items():
            parts = []
            if stats['historical_coins'] > 0:
                parts.append(f"{stats['historical_coins']} coins historical")
            if stats['market_data_coins'] > 0:
                parts.append(f"{stats['market_data_coins']} coins market data")
            
            logger.info(f"  {source}: {', '.join(parts)} ({stats['total_records']:,} total records)")
        
        logger.info(f"\n🚀 TASK OPTIMIZATION IMPACT:")
        task_priorities = metadata.get('task_priorities_used', {})
        for task, sources in task_priorities.items():
            logger.info(f"  {task}: {' → '.join(sources)}")
        
        if 'yahoo_finance' in source_performance:
            yahoo_coins = source_performance['yahoo_finance']['historical_coins']
            if yahoo_coins > 0:
                logger.info(f"  📈 Yahoo Finance: {yahoo_coins} coins collected INSTANTLY (5 years each)")
        
        if 'binance' in source_performance:
            binance_stats = source_performance['binance']
            if binance_stats['historical_coins'] > 0 or binance_stats['market_data_coins'] > 0:
                logger.info(f"  🚀 Binance: Reliable fallback with excellent performance")
        
        if 'coingecko_markets' in source_performance:
            cg_stats = source_performance['coingecko_markets']
            if cg_stats['market_data_coins'] > 0:
                logger.info(f"  🐸 CoinGecko: Comprehensive market data for {cg_stats['market_data_coins']} coins")
        
        logger.info("=" * 90)

    # Legacy methods for backward compatibility
    async def collect_current_prices(self) -> List[Dict[str, Any]]:
        """Legacy method - redirects to smart collection"""
        return await self.collect_current_prices_smart()

    async def collect_market_data(self) -> List[Dict[str, Any]]:
        """Legacy method - redirects to smart collection"""
        return await self.collect_market_data_smart()

    async def collect_historical_data(self, coin_id: str, days: int = 365) -> List[Dict[str, Any]]:
        """Legacy method - redirects to smart collection"""
        return await self.collect_historical_data_smart(coin_id, days)

    async def collect_all_historical_data(self, days: int = 30) -> Dict[str, List[Dict[str, Any]]]:
        """Legacy method - redirects to smart collection"""
        return await self.collect_all_historical_data_smart(days)

    async def collect_all_data(self, historical_days: int = 30) -> Dict[str, Any]:
        """Legacy method - redirects to smart collection"""
        return await self.collect_all_data_smart(historical_days)

# Convenience functions for standalone usage
async def collect_crypto_data_smart(historical_days: int = 365 * 5) -> Dict[str, Any]:
    """Standalone function to collect cryptocurrency data with task-specific optimization"""
    async with CryptoCollector() as collector:
        return await collector.collect_all_data_smart(historical_days)

async def collect_crypto_data(historical_days: int = 30) -> Dict[str, Any]:
    """Legacy standalone function - redirects to smart collection"""
    return await collect_crypto_data_smart(historical_days)

# Example usage and testing
if __name__ == "__main__":
    async def main():
        print("🚀 Testing PRODUCTION-READY cryptocurrency data collection...")
        print("📊 Task-Specific Source Priority:")
        print("   Current Prices: Binance → CoinGecko → Yahoo → CoinAPI")
        print("   Historical Data: Yahoo → Binance → CoinGecko → CoinAPI")
        print("   Market Data: CoinGecko → Binance → Yahoo → CoinAPI")
        
        try:
            # Test production-ready data collection
            async with CryptoCollector() as collector:
                print(f"\n🔧 Configuration:")
                print(f"   Yahoo Finance: {'✅' if collector.yfinance_available else '❌'}")
                print(f"   CoinAPI key: {'✅' if collector.coinapi_key else '❌'}")
                
                print(f"\n📊 Task-Specific Priorities:")
                for task, sources in collector.config.source_priority_by_task.items():
                    print(f"   {task}: {' → '.join(sources)}")
                
                # Test current prices with task-optimized source selection
                print(f"\n📈 Testing current prices (Binance-first priority)...")
                current_prices = await collector.collect_current_prices_smart()
                if current_prices:
                    source_used = current_prices[0].get('data_source', 'unknown')
                    print(f"✅ Collected {len(current_prices)} current prices from {source_used}")
                
                # Test market data with CoinGecko-first priority
                print(f"\n🏪 Testing comprehensive market data (CoinGecko-first priority)...")
                market_data = await collector.collect_market_data_smart()
                if market_data:
                    source_used = market_data[0].get('data_source', 'unknown')
                    print(f"✅ Collected market data for {len(market_data)} cryptocurrencies from {source_used}")
                
                # Test historical data with Yahoo-first priority
                print(f"\n📊 Testing 5 years of Bitcoin historical data (Yahoo-first priority)...")
                btc_historical = await collector.collect_historical_data_smart('bitcoin', days=365 * 5)
                if btc_historical:
                    source_used = btc_historical[0].get('data_source', 'unknown')
                    print(f"✅ Collected {len(btc_historical)} historical records for Bitcoin from {source_used}")
                    
                    # Show time range
                    if len(btc_historical) > 1:
                        earliest = min(btc_historical, key=lambda x: x['record_date'])
                        latest = max(btc_historical, key=lambda x: x['record_date'])
                        print(f"   📅 Date range: {earliest['record_date']} to {latest['record_date']}")
                        
                        # Show source-specific performance
                        if source_used == 'yahoo_finance':
                            print(f"   ⚡ INSTANT collection from Yahoo Finance!")
                        elif source_used == 'binance':
                            print(f"   🚀 EFFICIENT collection from Binance!")
                        elif source_used == 'coingecko':
                            print(f"   🐸 RELIABLE collection from CoinGecko!")
                
                # Show sample data structure
                if market_data:
                    sample = market_data[0]
                    print(f"\n📊 Sample market data structure:")
                    print(f"   Coin: {sample.get('name', 'N/A')} ({sample.get('symbol', 'N/A')})")
                    price = sample.get('current_price')
                    if price:
                        print(f"   Price: ${price:,.2f}")
                    market_cap = sample.get('market_cap')
                    if market_cap:
                        print(f"   Market Cap: ${market_cap:,.0f}")
                    change_24h = sample.get('price_change_percentage_24h')
                    if change_24h is not None:
                        print(f"   24h Change: {change_24h:.2f}%")
                    print(f"   Data Source: {sample.get('data_source', 'unknown')}")
                
        except Exception as e:
            print(f"❌ Error during testing: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        print(f"\n✅ All PRODUCTION-READY collection tests completed successfully!")
        print(f"🎯 Your crypto automation is now optimized for real-world conditions!")
        print(f"🏭 Ready for production deployment with task-specific source optimization!")
        return True
    
    # Run the test
    asyncio.run(main())