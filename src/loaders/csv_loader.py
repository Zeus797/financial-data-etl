# src/loaders/csv_loader.py
"""
CSV loader for saving forex data to CSV files
"""
import os
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def save_forex_data_to_csv(df: pd.DataFrame, base_currency: str, 
                          quote_currency: str, output_dir: str = "data/output") -> str:
    """
    Save forex data to CSV file
    
    Args:
        df: DataFrame with forex data
        base_currency: Base currency code
        quote_currency: Quote currency code
        output_dir: Directory to save CSV files
        
    Returns:
        Path to saved CSV file
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{base_currency}_{quote_currency}_forex_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    
    # Save to CSV
    df.to_csv(filepath, index=False)
    logger.info(f"Saved {len(df)} records to {filepath}")
    
    return filepath


def load_forex_data_from_csv(filepath: str) -> pd.DataFrame:
    """
    Load forex data from CSV file
    
    Args:
        filepath: Path to CSV file
        
    Returns:
        DataFrame with forex data
    """
    # Read CSV with proper date parsing
    df = pd.read_csv(filepath, parse_dates=['date'])
    logger.info(f"Loaded {len(df)} records from {filepath}")
    
    return df


def save_summary_report(results: dict, output_dir: str = "data/output") -> str:
    """
    Save pipeline execution summary to CSV
    
    Args:
        results: Dictionary with pipeline results
        output_dir: Directory to save report
        
    Returns:
        Path to saved report
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Convert results to DataFrame
    summary_data = []
    for pair, result in results.items():
        summary_data.append({
            'currency_pair': pair,
            'status': result.get('status'),
            'rows': result.get('rows', 0),
            'execution_time': result.get('execution_time', 'N/A')
        })
    
    df = pd.DataFrame(summary_data)
    
    # Save to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"pipeline_summary_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    
    df.to_csv(filepath, index=False)
    logger.info(f"Saved pipeline summary to {filepath}")
    
    return filepath