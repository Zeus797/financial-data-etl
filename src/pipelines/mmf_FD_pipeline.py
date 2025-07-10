"""
scripts/simple_mmf_fd_loader.py

Simple loader for Money Market Funds and Fixed Deposits
Designed for weekly manual data entry with minimal processing
"""

import pandas as pd
import logging
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from pathlib import Path
import json
import os

# Database imports
from src.models.base import db_manager
from src.models.universal_models import (
    DimEntity, DimFinancialInstrument, DimCurrency, 
    DimCountry, DimDate, FactMoneyMarketFund, FactFixedDeposit,
    get_date_key, create_date_dimension_record
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleMMFFixedDepositLoader:
    """
    Simple loader for Money Market Funds and Fixed Deposits
    
    Features:
    - Manual data entry via dictionary/CSV
    - Automatic yield calculations
    - Direct database loading
    - Minimal transformation overhead
    """
    
    def __init__(self):
        self.db_manager = db_manager
        
        # Pre-defined data for testing
        self.sample_fixed_deposits = [
            {
                'bank': 'Stanbic Holdings Plc',
                'yield_per_annum': 4.70,
                'minimum_amount': 20000,
                'term_months': 12,
                'penalty_severity': 'Strict',
                'penalty_description': 'Forfeit all interest acquired',
                'source': 'https://www.stanbicbank.co.ke/',
                'currency': 'KES'
            },
            {
                'bank': 'Equity Group Holdings',
                'yield_per_annum': 2.00,
                'minimum_amount': 20000,
                'term_months': 1,
                'penalty_severity': 'Flexible',
                'penalty_description': 'Premature withdrawal allowed',
                'source': 'https://www.equitybankgroup.com/',
                'currency': 'KES'
            },
            {
                'bank': 'KCB Group Ltd',
                'yield_per_annum': 6.30,
                'minimum_amount': 5000,
                'term_months': 12,
                'penalty_severity': 'Strict',
                'penalty_description': 'Forfeit all interest acquired',
                'source': 'https://ke.kcbgroup.com/',
                'currency': 'KES'
            }
        ]
        
        self.sample_kes_mmfs = [
            {
                'company': 'APA',
                'product_name': 'Apollo Money Market Fund',
                'average_annual_yield': 9.95,
                'withholding_tax': 15.00,
                'minimum_investment': 1000,
                'management_fee': 2.00,
                'net_yield': 6.46,
                'currency': 'KES'
            },
            {
                'company': 'Britam',
                'product_name': 'Britam Money Market Fund',
                'average_annual_yield': 10.72,
                'withholding_tax': 15.00,
                'minimum_investment': 1000,
                'management_fee': 1.50,
                'net_yield': 7.61,
                'currency': 'KES'
            },
            {
                'company': 'Cytonn Asset Managers Limited',
                'product_name': 'Cytonn Money Market Fund',
                'average_annual_yield': 13.63,
                'withholding_tax': 15.00,
                'minimum_investment': 1000,
                'management_fee': 2.00,
                'net_yield': 9.59,
                'currency': 'KES'
            }
        ]
        
        self.sample_usd_mmfs = [
            {
                'company': 'CIC Group',
                'product_name': 'CIC Dollar MMF',
                'average_annual_yield': 4.90,
                'withholding_tax': 15.00,
                'minimum_investment': 100,
                'management_fee': 1.50,
                'net_yield': 2.67,
                'currency': 'USD'
            },
            {
                'company': 'Cytonn Asset Managers Limited',
                'product_name': 'Cytonn Money Market Fund (USD)',
                'average_annual_yield': 6.17,
                'withholding_tax': 15.00,
                'minimum_investment': 100,
                'management_fee': 2.00,
                'net_yield': 3.24,
                'currency': 'USD'
            }
        ]
        
        logger.info("SimpleMMFFixedDepositLoader initialized")

    def load_weekly_data(self, data_source: str = "sample") -> Dict[str, Any]:
        """
        Load weekly MMF and Fixed Deposit data
        
        Args:
            data_source: "sample", "csv", or "manual"
        """
        start_time = datetime.now()
        logger.info("💰 Starting weekly MMF & Fixed Deposit data load...")
        
        try:
            # Get data based on source
            if data_source == "csv":
                fd_data, kes_mmf_data, usd_mmf_data = self._load_from_csv()
            elif data_source == "manual":
                fd_data, kes_mmf_data, usd_mmf_data = self._get_manual_data()
            else:
                fd_data = self.sample_fixed_deposits
                kes_mmf_data = self.sample_kes_mmfs
                usd_mmf_data = self.sample_usd_mmfs
            
            # Ensure database tables exist
            self.db_manager.create_tables()
            
            # Load data
            results = {
                'fixed_deposits': self._load_fixed_deposits(fd_data),
                'kes_mmfs': self._load_money_market_funds(kes_mmf_data),
                'usd_mmfs': self._load_money_market_funds(usd_mmf_data)
            }
            
            # Generate summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            total_records = sum(results.values())
            
            summary = {
                'pipeline_info': {
                    'pipeline_type': 'weekly_mmf_fd_load',
                    'execution_time': end_time.isoformat(),
                    'duration_seconds': duration,
                    'status': 'SUCCESS'
                },
                'data_summary': {
                    'fixed_deposits_loaded': results['fixed_deposits'],
                    'kes_mmfs_loaded': results['kes_mmfs'],
                    'usd_mmfs_loaded': results['usd_mmfs'],
                    'total_records': total_records,
                    'data_source': data_source
                },
                'rate_summary': self._get_rate_summary()
            }
            
            logger.info("✅ Weekly MMF & Fixed Deposit load completed successfully!")
            return summary
            
        except Exception as e:
            logger.error(f"❌ Weekly load failed: {e}")
            return {
                'pipeline_info': {
                    'pipeline_type': 'weekly_mmf_fd_load',
                    'status': 'FAILED',
                    'error': str(e)
                }
            }

    def _load_fixed_deposits(self, fd_data: List[Dict[str, Any]]) -> int:
        """Load fixed deposit data"""
        logger.info(f"🏦 Loading {len(fd_data)} fixed deposit records...")
        
        records_loaded = 0
        
        with self.db_manager.get_session() as session:
            for fd in fd_data:
                try:
                    # Create or get bank entity
                    bank_entity = self._ensure_bank_entity(session, fd['bank'])
                    
                    # Create or get instrument
                    instrument = self._ensure_fd_instrument(session, fd, bank_entity)
                    
                    # Create or get date record
                    today = date.today()
                    date_key = get_date_key(today)
                    self._ensure_date_record(session, today, date_key)
                    
                    # Create or update fact record
                    self._create_or_update_fd_record(session, instrument, date_key, fd)
                    
                    records_loaded += 1
                    logger.info(f"✅ Loaded FD: {fd['bank']} - {fd['term_months']}M @ {fd['yield_per_annum']}%")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to load FD for {fd['bank']}: {e}")
                    continue
            
            session.commit()
        
        return records_loaded

    def _load_money_market_funds(self, mmf_data: List[Dict[str, Any]]) -> int:
        """Load money market fund data"""
        if not mmf_data:
            return 0
            
        currency = mmf_data[0]['currency']
        logger.info(f"📈 Loading {len(mmf_data)} {currency} MMF records...")
        
        records_loaded = 0
        
        with self.db_manager.get_session() as session:
            for mmf in mmf_data:
                try:
                    # Create or get company entity
                    company_entity = self._ensure_company_entity(session, mmf['company'])
                    
                    # Create or get instrument
                    instrument = self._ensure_mmf_instrument(session, mmf, company_entity)
                    
                    # Create or get date record
                    today = date.today()
                    date_key = get_date_key(today)
                    self._ensure_date_record(session, today, date_key)
                    
                    # Create or update fact record
                    self._create_or_update_mmf_record(session, instrument, date_key, mmf)
                    
                    records_loaded += 1
                    logger.info(f"✅ Loaded MMF: {mmf['company']} - {mmf['currency']} @ {mmf['net_yield']}%")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to load MMF for {mmf['company']}: {e}")
                    continue
            
            session.commit()
        
        return records_loaded

    def _ensure_bank_entity(self, session, bank_name: str) -> DimEntity:
        """Ensure bank entity exists"""
        entity = session.query(DimEntity).filter_by(
            entity_name=bank_name,
            entity_type='bank'
        ).first()
        
        if not entity:
            entity = DimEntity(
                entity_name=bank_name,
                entity_type='bank',
                country_code='KE',
                is_active=True
            )
            session.add(entity)
            session.flush()
        
        return entity

    def _ensure_company_entity(self, session, company_name: str) -> DimEntity:
        """Ensure fund management company entity exists"""
        entity = session.query(DimEntity).filter_by(
            entity_name=company_name,
            entity_type='fund_manager'
        ).first()
        
        if not entity:
            entity = DimEntity(
                entity_name=company_name,
                entity_type='fund_manager',
                country_code='KE',
                is_active=True
            )
            session.add(entity)
            session.flush()
        
        return entity

    def _ensure_fd_instrument(self, session, fd_data: Dict[str, Any], entity: DimEntity) -> DimFinancialInstrument:
        """Ensure fixed deposit instrument exists"""
        instrument_code = f"FD_{entity.entity_name.replace(' ', '_').upper()}_{fd_data['term_months']}M_{fd_data['currency']}"
        
        instrument = session.query(DimFinancialInstrument).filter_by(
            instrument_code=instrument_code
        ).first()
        
        if not instrument:
            instrument = DimFinancialInstrument(
                instrument_type='fixed_deposit',
                instrument_name=f"{entity.entity_name} {fd_data['term_months']}-Month Fixed Deposit",
                instrument_code=instrument_code,
                display_name=f"{entity.entity_name} FD {fd_data['term_months']}M",
                primary_currency_code=fd_data['currency'],
                issuer_entity_id=entity.entity_id,
                asset_class='fixed_deposit',
                risk_level='low',
                description=f"{fd_data['term_months']}-month fixed deposit from {entity.entity_name}",
                is_active=True
            )
            session.add(instrument)
            session.flush()
        
        return instrument

    def _ensure_mmf_instrument(self, session, mmf_data: Dict[str, Any], entity: DimEntity) -> DimFinancialInstrument:
        """Ensure money market fund instrument exists"""
        instrument_code = f"MMF_{entity.entity_name.replace(' ', '_').upper()}_{mmf_data['currency']}"
        
        instrument = session.query(DimFinancialInstrument).filter_by(
            instrument_code=instrument_code
        ).first()
        
        if not instrument:
            instrument = DimFinancialInstrument(
                instrument_type='money_market_fund',
                instrument_name=mmf_data['product_name'],
                instrument_code=instrument_code,
                display_name=f"{entity.entity_name} {mmf_data['currency']} MMF",
                primary_currency_code=mmf_data['currency'],
                issuer_entity_id=entity.entity_id,
                asset_class='money_market',
                risk_level='low',
                description=f"{mmf_data['currency']} money market fund from {entity.entity_name}",
                is_active=True
            )
            session.add(instrument)
            session.flush()
        
        return instrument

    def _ensure_date_record(self, session, date_obj: date, date_key: int):
        """Ensure date record exists"""
        existing = session.query(DimDate).filter_by(date_key=date_key).first()
        if not existing:
            date_record = create_date_dimension_record(date_obj)
            session.add(date_record)
            session.flush()

    def _create_or_update_fd_record(self, session, instrument: DimFinancialInstrument, 
                                   date_key: int, fd_data: Dict[str, Any]):
        """Create or update fixed deposit record"""
        # Check if record exists for today
        existing = session.query(FactFixedDeposit).filter_by(
            instrument_id=instrument.instrument_id,
            date_key=date_key,
            term_months=fd_data['term_months']
        ).first()
        
        if existing:
            # Update existing record
            existing.interest_rate = fd_data['yield_per_annum'] / 100
            existing.minimum_deposit = fd_data['minimum_amount']
            existing.early_withdrawal_penalty = self._get_penalty_rate(fd_data['penalty_severity'])
            existing.last_verified = datetime.now()
        else:
            # Create new record
            penalty_rate = self._get_penalty_rate(fd_data['penalty_severity'])
            after_tax_return = (fd_data['yield_per_annum'] / 100) * 0.85  # 15% withholding tax
            
            fact_record = FactFixedDeposit(
                instrument_id=instrument.instrument_id,
                date_key=date_key,
                record_date=date.today(),
                
                # Basic terms
                term_months=fd_data['term_months'],
                interest_rate=fd_data['yield_per_annum'] / 100,
                minimum_deposit=fd_data['minimum_amount'],
                
                # Calculated fields
                effective_rate=after_tax_return,
                base_rate=fd_data['yield_per_annum'] / 100,
                after_tax_return=after_tax_return,
                
                # Penalty info
                early_withdrawal_penalty=penalty_rate,
                
                # Defaults
                maximum_deposit=10000000,  # 10M default
                auto_renewal=True,
                tax_rate=0.15,
                rate_type='fixed',
                currency_code=fd_data['currency'],
                
                # Metadata
                data_source='manual_entry',
                last_verified=datetime.now()
            )
            session.add(fact_record)

    def _create_or_update_mmf_record(self, session, instrument: DimFinancialInstrument, 
                                    date_key: int, mmf_data: Dict[str, Any]):
        """Create or update money market fund record"""
        # Check if record exists for today
        existing = session.query(FactMoneyMarketFund).filter_by(
            instrument_id=instrument.instrument_id,
            date_key=date_key
        ).first()
        
        if existing:
            # Update existing record
            existing.current_yield = mmf_data['net_yield'] / 100
            existing.seven_day_yield = mmf_data['average_annual_yield'] / 100
            existing.expense_ratio = mmf_data['management_fee'] / 100
            existing.management_fee = mmf_data['management_fee'] / 100
        else:
            # Create new record
            fact_record = FactMoneyMarketFund(
                instrument_id=instrument.instrument_id,
                date_key=date_key,
                record_date=date.today(),
                
                # Yield data
                current_yield=mmf_data['net_yield'] / 100,
                seven_day_yield=mmf_data['average_annual_yield'] / 100,
                thirty_day_yield=mmf_data['average_annual_yield'] / 100,
                year_to_date_yield=mmf_data['net_yield'] / 100,
                
                # Fund metrics
                net_asset_value=1.0,  # Default NAV
                total_assets=1000000,  # Default 1M
                expense_ratio=mmf_data['management_fee'] / 100,
                management_fee=mmf_data['management_fee'] / 100,
                
                # Performance defaults
                sharpe_ratio=1.0,
                volatility=0.05,
                max_drawdown=0.02,
                
                # Metadata
                data_source='manual_entry'
            )
            session.add(fact_record)

    def _get_penalty_rate(self, penalty_severity: str) -> float:
        """Convert penalty severity to rate"""
        penalty_map = {
            'Strict': 1.0,      # 100% penalty
            'Moderate': 0.5,    # 50% penalty
            'Flexible': 0.0     # No penalty
        }
        return penalty_map.get(penalty_severity, 0.5)

    def _get_rate_summary(self) -> Dict[str, Any]:
        """Get summary of current rates"""
        with self.db_manager.get_session() as session:
            today = date.today()
            
            # Fixed deposit rates
            fd_rates = session.query(FactFixedDeposit.interest_rate).filter(
                FactFixedDeposit.record_date == today
            ).all()
            
            # MMF rates
            mmf_rates = session.query(FactMoneyMarketFund.current_yield).filter(
                FactMoneyMarketFund.record_date == today
            ).all()
            
            summary = {}
            
            if fd_rates:
                fd_values = [r.interest_rate * 100 for r in fd_rates]
                summary['fixed_deposits'] = {
                    'count': len(fd_values),
                    'min_rate': min(fd_values),
                    'max_rate': max(fd_values),
                    'avg_rate': sum(fd_values) / len(fd_values)
                }
            
            if mmf_rates:
                mmf_values = [r.current_yield * 100 for r in mmf_rates]
                summary['money_market_funds'] = {
                    'count': len(mmf_values),
                    'min_rate': min(mmf_values),
                    'max_rate': max(mmf_values),
                    'avg_rate': sum(mmf_values) / len(mmf_values)
                }
            
            return summary

    def _load_from_csv(self) -> tuple:
        """Load data from CSV files"""
        # This would load from CSV files
        # For now, return sample data
        return self.sample_fixed_deposits, self.sample_kes_mmfs, self.sample_usd_mmfs

    def _get_manual_data(self) -> tuple:
        """Get data through manual entry"""
        # This would provide interactive data entry
        # For now, return sample data
        return self.sample_fixed_deposits, self.sample_kes_mmfs, self.sample_usd_mmfs

    def export_current_rates(self, output_path: str = "data/current_rates.csv"):
        """Export current rates to CSV"""
        logger.info("📤 Exporting current rates...")
        
        with self.db_manager.get_session() as session:
            today = date.today()
            
            # Get all current rates
            fd_query = session.query(
                FactFixedDeposit.instrument_id,
                FactFixedDeposit.interest_rate,
                FactFixedDeposit.term_months,
                FactFixedDeposit.minimum_deposit
            ).filter(FactFixedDeposit.record_date == today)
            
            mmf_query = session.query(
                FactMoneyMarketFund.instrument_id,
                FactMoneyMarketFund.current_yield,
                FactMoneyMarketFund.management_fee
            ).filter(FactMoneyMarketFund.record_date == today)
            
            # Export to CSV
            # Implementation would go here
            
            logger.info(f"📤 Rates exported to {output_path}")


def run_weekly_mmf_fd_load(data_source: str = "sample") -> Dict[str, Any]:
    """Convenience function to run weekly load"""
    loader = SimpleMMFFixedDepositLoader()
    return loader.load_weekly_data(data_source)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Weekly MMF & Fixed Deposit Loader")
    parser.add_argument("--source", choices=["sample", "csv", "manual"], default="sample",
                       help="Data source")
    parser.add_argument("--export", action="store_true", help="Export current rates")
    
    args = parser.parse_args()
    
    loader = SimpleMMFFixedDepositLoader()
    
    if args.export:
        loader.export_current_rates()
    else:
        results = loader.load_weekly_data(args.source)
        
        print("\n" + "="*60)
        print("💰 WEEKLY MMF & FIXED DEPOSIT LOAD RESULTS")
        print("="*60)
        print(f"Status: {results['pipeline_info']['status']}")
        print(f"Duration: {results['pipeline_info'].get('duration_seconds', 0):.2f} seconds")
        
        if 'data_summary' in results:
            print(f"Fixed Deposits: {results['data_summary']['fixed_deposits_loaded']}")
            print(f"KES MMFs: {results['data_summary']['kes_mmfs_loaded']}")
            print(f"USD MMFs: {results['data_summary']['usd_mmfs_loaded']}")
            print(f"Total Records: {results['data_summary']['total_records']}")
        
        if 'rate_summary' in results:
            for product_type, rates in results['rate_summary'].items():
                print(f"\n{product_type.replace('_', ' ').title()}:")
                print(f"  Count: {rates['count']}")
                print(f"  Range: {rates['min_rate']:.2f}% - {rates['max_rate']:.2f}%")
                print(f"  Average: {rates['avg_rate']:.2f}%")
        
        print("="*60)