# src/repositories/forex_repository.py
"""
Repository for forex data database operations
"""
import logging
from typing import Optional, List
from datetime import date
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import and_
from decimal import Decimal

from src.models.models import ForexRate, CurrencyPair, Country

logger = logging.getLogger(__name__)


class ForexRepository:
    """Repository for managing forex data in the database"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def get_currency_pair(self, base_currency: str, quote_currency: str) -> Optional[CurrencyPair]:
        """Get currency pair from database or create if not exists"""
        pair_symbol = f"{base_currency}/{quote_currency}"
        
        # Try to find existing pair
        pair = self.session.query(CurrencyPair).filter(
            CurrencyPair.pair_symbol == pair_symbol
        ).first()
        
        if not pair:
            # Create new pair with required pair_name
            pair = CurrencyPair(
                pair_symbol=pair_symbol,
                base_currency=base_currency,
                quote_currency=quote_currency,
                pair_name=f"{base_currency} to {quote_currency}",  # Add descriptive pair_name
                active=True
            )
            try:
                self.session.add(pair)
                self.session.commit()
                logger.info(f"Created new currency pair: {pair_symbol}")
            except Exception as e:
                logger.error(f"Error creating currency pair: {e}")
                self.session.rollback()
                raise
    
        return pair
    
    def save_forex_data(self, base_currency: str, quote_currency: str, 
                       df: pd.DataFrame) -> int:
        """
        Save forex data from DataFrame to database
        
        Args:
            base_currency: Base currency code
            quote_currency: Quote currency code
            df: DataFrame with forex data
            
        Returns:
            Number of records inserted/updated
        """
        # Get or create currency pair
        currency_pair = self.get_currency_pair(base_currency, quote_currency)
        
        if currency_pair is None:
            raise ValueError(f"Failed to get/create currency pair {base_currency}/{quote_currency}")
        
        records_processed = 0
        
        for _, row in df.iterrows():
            try:
                # Check if record exists for this date
                existing = self.session.query(ForexRate).filter(
                    and_(
                        ForexRate.currency_pair_id == currency_pair.id,
                        ForexRate.date == row['date'].date()
                    )
                ).first()
                
                if existing:
                    # Update existing record
                    self._update_forex_rate(existing, row)
                    logger.debug(f"Updated rate for {currency_pair.pair_symbol} on {row['date']}")
                else:
                    # Create new record
                    forex_rate = self._create_forex_rate(currency_pair.id, row)
                    self.session.add(forex_rate)
                    logger.debug(f"Inserted rate for {currency_pair.pair_symbol} on {row['date']}")
                
                records_processed += 1
                
                # Commit in batches
                if records_processed % 100 == 0:
                    self.session.commit()
                    logger.info(f"Processed {records_processed} records...")
                    
            except Exception as e:
                logger.error(f"Error processing row for date {row['date']}: {e}")
                self.session.rollback()
                raise
        
        # Final commit
        self.session.commit()
        logger.info(f"Completed processing {records_processed} records for {currency_pair.pair_symbol}")
        
        return records_processed
    
    def _create_forex_rate(self, currency_pair_id: int, row: pd.Series) -> ForexRate:
        """Create ForexRate object from DataFrame row"""
        # Map DataFrame columns to database columns
        forex_rate = ForexRate(
            currency_pair_id=currency_pair_id,
            date=row['date'].date(),
            open_rate=Decimal(str(row.get('open', row['exchange_rate']))),
            high_rate=Decimal(str(row.get('high', row['exchange_rate']))),
            low_rate=Decimal(str(row.get('low', row['exchange_rate']))),
            close_rate=Decimal(str(row['exchange_rate'])),
            volume=int(row['volume']) if pd.notna(row.get('volume')) else None,
            
            # Calculate daily change (we'll use monthly change since it's monthly data)
            daily_change=None,  # Not applicable for monthly data
            daily_change_percent=None,  # Not applicable for monthly data
            
            # Technical indicators - map what we have
            ma_20=None,  # Not calculated in transformer
            ma_50=None,  # Not calculated in transformer
            ma_200=None,  # Not calculated in transformer
            volatility_20=Decimal(str(row['quarterly_volatility'])) if pd.notna(row.get('quarterly_volatility')) else None,
            
            # Period changes
            monthly_change=Decimal(str(row['monthly_pct_change'])) if pd.notna(row.get('monthly_pct_change')) else None,
            yearly_change=Decimal(str(row['yoy_pct_change'])) if pd.notna(row.get('yoy_pct_change')) else None,
            
            # Significant move flag
            is_significant_move=bool(row.get('significant_move', False))
        )
        
        return forex_rate
    
    def _update_forex_rate(self, existing: ForexRate, row: pd.Series):
        """Update existing ForexRate with new data"""
        existing.open_rate = Decimal(str(row.get('open', row['exchange_rate'])))
        existing.high_rate = Decimal(str(row.get('high', row['exchange_rate'])))
        existing.low_rate = Decimal(str(row.get('low', row['exchange_rate'])))
        existing.close_rate = Decimal(str(row['exchange_rate']))
        existing.volume = int(row['volume']) if pd.notna(row.get('volume')) else None
        
        # Update technical indicators
        if pd.notna(row.get('quarterly_volatility')):
            existing.volatility_20 = Decimal(str(row['quarterly_volatility']))
        if pd.notna(row.get('monthly_pct_change')):
            existing.monthly_change = Decimal(str(row['monthly_pct_change']))
        if pd.notna(row.get('yoy_pct_change')):
            existing.yearly_change = Decimal(str(row['yoy_pct_change']))
        
        existing.is_significant_move = bool(row.get('significant_move', False))
    
    def get_latest_rate(self, base_currency: str, quote_currency: str) -> Optional[ForexRate]:
        """Get the latest exchange rate for a currency pair"""
        pair = self.get_currency_pair(base_currency, quote_currency)
        if not pair:
            return None
        
        return self.session.query(ForexRate).filter(
            ForexRate.currency_pair_id == pair.id
        ).order_by(ForexRate.date.desc()).first()
    
    def get_rates_between_dates(self, base_currency: str, quote_currency: str,
                                start_date: date, end_date: date) -> List[ForexRate]:
        """Get exchange rates between two dates"""
        pair = self.get_currency_pair(base_currency, quote_currency)
        if not pair:
            return []
        
        return self.session.query(ForexRate).filter(
            and_(
                ForexRate.currency_pair_id == pair.id,
                ForexRate.date >= start_date,
                ForexRate.date <= end_date
            )
        ).order_by(ForexRate.date).all()
    
    def delete_rates_for_pair(self, base_currency: str, quote_currency: str) -> int:
        """Delete all rates for a currency pair (useful for re-importing)"""
        pair = self.get_currency_pair(base_currency, quote_currency)
        if not pair:
            return 0
        
        count = self.session.query(ForexRate).filter(
            ForexRate.currency_pair_id == pair.id
        ).delete()
        
        self.session.commit()
        return count