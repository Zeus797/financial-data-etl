import yfinance as yf
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional
import time

# Configure logger
logger = logging.getLogger(__name__)

def get_exchange_rate_data(
    base_currency: str, 
    quote_currency: str, 
    years: int = 1, 
    interval: str = "1mo",
    timeout: int = 30
) -> pd.DataFrame:
    """Get exchange rate data from Yahoo Finance"""
    try:
        # Format currency pair for Yahoo Finance
        pair = f"{base_currency}{quote_currency}=X"
        logger.info(f"Fetching data for {pair} from Yahoo Finance...")
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years*365)
        
        # Add retry logic for rate limits
        max_retries = 3
        retry_delay = timeout // 3
        last_error = None
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    logger.info(f"Retry attempt {attempt + 1} for {pair}...")
                    time.sleep(retry_delay)
                
                # Fetch data from Yahoo Finance
                df = yf.download(
                    pair,
                    start=start_date,
                    end=end_date,
                    interval=interval,
                    progress=False,
                    timeout=timeout
                )
                
                if df.empty:
                    raise ValueError(f"No data available for {pair}")
                
                # Process the data
                df = df.reset_index()
                df = df.rename(columns={
                    'Date': 'date',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'exchange_rate',
                    'Volume': 'volume'
                })
                
                # Add metadata
                df['base_currency'] = base_currency
                df['quote_currency'] = quote_currency
                df['pair'] = f"{base_currency}/{quote_currency}"
                df['source'] = 'yahoo_finance'
                
                logger.info(f"Successfully collected {len(df)} records for {pair}")
                return df
                
            except Exception as e:
                last_error = str(e)
                if attempt == max_retries - 1:
                    break
        
        raise Exception(f"Failed to collect data after {max_retries} attempts. Last error: {last_error}")
        
    except Exception as e:
        logger.error(f"Error collecting data for {pair}: {str(e)}")
        raise Exception(f"Failed to collect forex data: {str(e)}")