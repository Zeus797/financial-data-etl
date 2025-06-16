# src/transformers/cleaner.py
"""
Enhanced transformer for forex data with additional technical indicators
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def clean_forex_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform forex data"""
    
    # Make a copy to avoid modifying original
    df = df.copy()
    
    # Ensure date is datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Add date components
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    
    # Calculate monthly percentage change
    df['monthly_pct_change'] = df['exchange_rate'].pct_change() * 100
    
    # Calculate year-over-year change
    df['yoy_pct_change'] = df.groupby(['month'])['exchange_rate'].pct_change(periods=12) * 100
    
    # Calculate quarterly volatility (rolling 3-month std dev)
    df['quarterly_volatility'] = df['monthly_pct_change'].rolling(window=3).std()
    
    # Mark significant moves (more than 2 std dev from mean)
    mean_change = df['monthly_pct_change'].mean()
    std_change = df['monthly_pct_change'].std()
    df['significant_move'] = abs(df['monthly_pct_change'] - mean_change) > (2 * std_change)
    
    # Fill NaN values with 0 for calculated fields
    fill_columns = ['monthly_pct_change', 'yoy_pct_change', 'quarterly_volatility']
    df[fill_columns] = df[fill_columns].fillna(0)
    
    return df


def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add technical indicators to forex data"""
    try:
        # Create a copy to avoid modifying original
        df_tech = df.copy()
        
        # Calculate SMA
        df_tech['sma_20'] = df_tech['exchange_rate'].rolling(window=20).mean()
        
        # Calculate Bollinger Bands
        rolling_std = df_tech['exchange_rate'].rolling(window=20).std()
        df_tech['bb_upper'] = df_tech['sma_20'] + (rolling_std * 2)
        df_tech['bb_lower'] = df_tech['sma_20'] - (rolling_std * 2)
        
        # Calculate RSI
        delta = df_tech['exchange_rate'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df_tech['rsi'] = 100 - (100 / (1 + rs))
        
        # Calculate MACD
        exp1 = df_tech['exchange_rate'].ewm(span=12, adjust=False).mean()
        exp2 = df_tech['exchange_rate'].ewm(span=26, adjust=False).mean()
        df_tech['macd'] = exp1 - exp2
        df_tech['macd_signal'] = df_tech['macd'].ewm(span=9, adjust=False).mean()
        
        # Add volatility
        df_tech['volatility'] = df_tech['exchange_rate'].rolling(window=20).std()
        
        # Fill NaN values with 0
        df_tech = df_tech.fillna(0)
        
        return df_tech
        
    except Exception as e:
        logger.error(f"Error adding technical indicators: {str(e)}")
        raise