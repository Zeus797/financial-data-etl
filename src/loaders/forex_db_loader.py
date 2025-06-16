# src/loaders/forex_db_loader.py
"""
Database loader for forex data
"""
import logging
import pandas as pd
from typing import Optional, Dict, Any
from contextlib import contextmanager
from sqlalchemy.orm import Session

from src.models.base import SessionLocal
from src.models.models import CurrencyPair, ForexRate
from src.repositories.forex_repository import ForexRepository

logger = logging.getLogger(__name__)


class ForexDatabaseLoader:
    """Handles loading forex data into the database"""
    
    def __init__(self, session: Optional[Session] = None):
        """
        Initialize the loader with optional session
        
        Args:
            session: SQLAlchemy session. If None, creates new session.
        """
        self.session = session or SessionLocal()
        self.repository = ForexRepository(self.session)
        self._owns_session = session is None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._owns_session:
            self.session.close()
    
    def load_dataframe(self, df: pd.DataFrame, base_currency: str, 
                      quote_currency: str) -> dict:
        """Load DataFrame into database"""
        logger.info(f"Loading {len(df)} records for {base_currency}/{quote_currency}")
        
        try:
            # Get or create currency pair
            pair = self.repository.get_currency_pair(base_currency, quote_currency)
            records_processed = 0
            
            for _, row in df.iterrows():
                try:
                    # Create forex rate record with correct field names
                    rate = ForexRate(
                        currency_pair_id=pair.id,
                        date=row['date'],
                        exchange_rate=row['exchange_rate'],
                        open=row['open'],  # Make sure this matches the model
                        high=row['high'],
                        low=row['low'],
                        volume=row['volume'],
                        monthly_pct_change=row.get('monthly_pct_change', 0),
                        yoy_pct_change=row.get('yoy_pct_change', 0),
                        quarterly_volatility=row.get('quarterly_volatility', 0),
                        significant_move=row.get('significant_move', False)
                    )
                    
                    self.session.add(rate)
                    records_processed += 1
                    
                except Exception as e:
                    logger.error(f"Error processing row for date {row['date']}: {str(e)}")
                    continue
            
            self.session.commit()
            return {
                "success": True,
                "records_processed": records_processed
            }
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error loading data: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def load_from_csv(self, csv_path: str, base_currency: str, 
                     quote_currency: str) -> Dict[str, Any]:
        """
        Load forex data from CSV file into database
        
        Args:
            csv_path: Path to CSV file
            base_currency: Base currency code
            quote_currency: Quote currency code
            
        Returns:
            Dictionary with loading statistics
        """
        try:
            logger.info(f"Loading data from CSV: {csv_path}")
            
            # Read CSV
            df = pd.read_csv(csv_path, parse_dates=['date'])
            
            # Load into database
            return self.load_dataframe(df, base_currency, quote_currency)
            
        except Exception as e:
            logger.error(f"Error loading from CSV: {e}")
            return {
                "success": False,
                "error": str(e),
                "file": csv_path
            }
    
    def bulk_load(self, currency_pairs: list, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Bulk load multiple currency pairs
        
        Args:
            currency_pairs: List of (base, quote) tuples
            data_dict: Dictionary mapping pair strings to DataFrames
            
        Returns:
            Dictionary with results for each pair
        """
        results = {}
        
        for base, quote in currency_pairs:
            pair_key = f"{base}/{quote}"
            
            if pair_key not in data_dict:
                logger.warning(f"No data found for {pair_key}")
                results[pair_key] = {"success": False, "error": "No data provided"}
                continue
            
            df = data_dict[pair_key]
            result = self.load_dataframe(df, base, quote)
            results[pair_key] = result
        
        return results
    
    def update_or_insert(self, base_currency: str, quote_currency: str,
                        date_val: pd.Timestamp, rate_data: dict) -> bool:
        """
        Update or insert a single forex rate
        
        Args:
            base_currency: Base currency code
            quote_currency: Quote currency code
            date_val: Date of the rate
            rate_data: Dictionary with rate data
            
        Returns:
            True if successful
        """
        try:
            # Create DataFrame with single row
            df = pd.DataFrame([{
                'date': date_val,
                'exchange_rate': rate_data.get('close', rate_data.get('rate')),
                'open': rate_data.get('open'),
                'high': rate_data.get('high'),
                'low': rate_data.get('low'),
                'volume': rate_data.get('volume'),
                'monthly_pct_change': rate_data.get('monthly_pct_change'),
                'yoy_pct_change': rate_data.get('yoy_pct_change'),
                'quarterly_volatility': rate_data.get('quarterly_volatility'),
                'significant_move': rate_data.get('significant_move', False)
            }])
            
            # Save using repository
            self.repository.save_forex_data(base_currency, quote_currency, df)
            return True
            
        except Exception as e:
            logger.error(f"Error updating/inserting rate: {e}")
            self.session.rollback()
            return False


# Standalone functions for simple usage
def load_forex_data_to_db(df: pd.DataFrame, base_currency: str, 
                         quote_currency: str) -> Dict[str, Any]:
    """
    Simple function to load forex data to database
    
    Args:
        df: DataFrame with forex data
        base_currency: Base currency code
        quote_currency: Quote currency code
        
    Returns:
        Dictionary with loading results
    """
    with ForexDatabaseLoader() as loader:
        return loader.load_dataframe(df, base_currency, quote_currency)


def load_forex_csv_to_db(csv_path: str, base_currency: str, 
                        quote_currency: str) -> Dict[str, Any]:
    """
    Simple function to load forex data from CSV to database
    
    Args:
        csv_path: Path to CSV file
        base_currency: Base currency code
        quote_currency: Quote currency code
        
    Returns:
        Dictionary with loading results
    """
    with ForexDatabaseLoader() as loader:
        return loader.load_from_csv(csv_path, base_currency, quote_currency)