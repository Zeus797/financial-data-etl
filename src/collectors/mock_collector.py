import pandas as pd
from datetime import datetime, timedelta
import numpy as np

def get_mock_exchange_rate_data(base_currency: str, quote_currency: str, years: int = 1, interval: str = "1mo"):
    """Generate mock forex data for testing"""
    
    # Generate dates (using 'ME' instead of deprecated 'M')
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)
    dates = pd.date_range(start=start_date, end=end_date, freq='ME')
    
    # Generate mock data
    data = {
        'date': dates,
        'exchange_rate': np.random.uniform(1.0, 1.5, len(dates)),
        'open': np.random.uniform(1.0, 1.5, len(dates)),
        'high': np.random.uniform(1.0, 1.5, len(dates)),
        'low': np.random.uniform(1.0, 1.5, len(dates)),
        'volume': np.random.uniform(1000000, 5000000, len(dates)),
    }
    
    df = pd.DataFrame(data)
    
    # Add currency info
    df['base_currency'] = base_currency
    df['quote_currency'] = quote_currency
    df['pair'] = f"{base_currency}/{quote_currency}"
    
    return df