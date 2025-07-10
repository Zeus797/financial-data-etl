#!/usr/bin/env python3
"""
NSE Treasury Securities Pipeline
Complete pipeline for downloading, processing, and storing Kenya treasury bond data
"""

import os
import sys
import asyncio
import logging
import pandas as pd
import requests
import aiohttp
import pdfplumber
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import json
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/nse_pipeline.log') if Path('logs').exists() else logging.NullHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class BondData:
    """Data structure for treasury bond information"""
    date: str
    isin: str
    issue_number: str
    issue_date: str
    maturity_date: str
    tenor: str
    coupon_rate: float
    current_price: float
    ytm: float
    bond_type: str
    years_to_maturity: float
    tax_rate: float = 0.1
    after_tax_ytm: Optional[float] = None
    price_premium: Optional[float] = None

    def __post_init__(self):
        """Calculate derived fields"""
        if self.after_tax_ytm is None:
            self.after_tax_ytm = self.ytm * (1 - self.tax_rate)
        
        if self.price_premium is None:
            self.price_premium = ((self.current_price - 100) / 100) * 100

class NSEBondProcessor:
    """NSE Bond PDF processor"""
    
    def __init__(self):
        self.base_url = "https://www.nse.co.ke/wp-content/uploads/"
        self.download_folder = Path("downloads")
        self.download_folder.mkdir(exist_ok=True)
        
        # Patterns for data extraction
        self.isin_pattern = r'KE\d{10}'
        self.date_patterns = [
            r'(\d{1,2}-[A-Z]{3}-\d{4})',
            r'(\d{1,2}/\d{1,2}/\d{4})',
            r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})'
        ]
    
    async def get_latest_pdf_url(self, target_date: Optional[str] = None) -> Optional[str]:
        """Get URL for latest bond prices PDF"""
        if target_date is None:
            target_date = datetime.now().strftime("%d-%b-%Y").upper()
        
        # Try different filename formats
        filename_formats = [
            f"BondPrices_{target_date}.pdf",
            f"BondPrices_{target_date.replace('-', '')}.pdf",
            f"bond_prices_{target_date.lower()}.pdf"
        ]
        
        for filename in filename_formats:
            url = f"{self.base_url}{filename}"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.head(url) as response:
                        if response.status == 200:
                            logger.info(f"Found PDF: {url}")
                            return url
            except Exception as e:
                logger.debug(f"Failed to check {url}: {e}")
        
        # Try yesterday's file
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y").upper()
        for filename in filename_formats:
            filename = filename.replace(target_date, yesterday)
            url = f"{self.base_url}{filename}"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.head(url) as response:
                        if response.status == 200:
                            logger.info(f"Found yesterday's PDF: {url}")
                            return url
            except Exception:
                continue
        
        logger.warning(f"No PDF found for {target_date}")
        return None
    
    async def download_pdf(self, url: str) -> Optional[Path]:
        """Download PDF from URL"""
        try:
            filename = url.split('/')[-1]
            filepath = self.download_folder / filename
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.read()
                        with open(filepath, 'wb') as f:
                            f.write(content)
                        logger.info(f"Downloaded: {filepath}")
                        return filepath
            
            logger.error(f"Failed to download: HTTP {response.status}")
            return None
            
        except Exception as e:
            logger.error(f"Download error: {e}")
            return None
    
    def standardize_date(self, date_str: str) -> str:
        """Convert various date formats to YYYY-MM-DD"""
        formats = ["%d-%b-%Y", "%d/%m/%Y", "%d %B %Y", "%d %b %Y"]
        
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str.upper(), fmt)
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        return date_str
    
    def safe_float(self, value: str) -> float:
        """Safely convert string to float"""
        if not value or value.strip() == '':
            return 0.0
        
        try:
            clean_value = re.sub(r'[^\d.-]', '', str(value))
            return float(clean_value) if clean_value else 0.0
        except (ValueError, TypeError):
            return 0.0
    
    def parse_pdf(self, filepath: Path) -> List[BondData]:
        """Extract bond data from PDF"""
        bonds = []
        
        try:
            with pdfplumber.open(filepath) as pdf:
                full_text = ""
                
                # Extract text from all pages
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        full_text += page_text + "\n"
                
                # Extract date from filename or content
                pdf_date = self.extract_date_from_content(filepath.name, full_text)
                
                # Parse bonds from text
                bonds = self.parse_bonds_from_text(full_text, pdf_date)
                
            logger.info(f"Extracted {len(bonds)} bonds from PDF")
            return bonds
            
        except Exception as e:
            logger.error(f"PDF parsing error: {e}")
            return []
    
    def extract_date_from_content(self, filename: str, content: str) -> str:
        """Extract date from filename or PDF content"""
        # Try filename first
        for pattern in self.date_patterns:
            match = re.search(pattern, filename)
            if match:
                return self.standardize_date(match.group(1))
        
        # Try content
        for pattern in self.date_patterns:
            match = re.search(pattern, content)
            if match:
                return self.standardize_date(match.group(1))
        
        return datetime.now().strftime("%Y-%m-%d")
    
    def parse_bonds_from_text(self, text: str, pdf_date: str) -> List[BondData]:
        """Parse bond data from PDF text"""
        bonds = []
        lines = text.split('\n')
        
        current_bond = {}
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Look for ISIN
            isin_match = re.search(self.isin_pattern, line)
            if isin_match:
                # Save previous bond
                if current_bond:
                    bond = self.create_bond_from_data(current_bond, pdf_date)
                    if bond:
                        bonds.append(bond)
                
                # Start new bond
                current_bond = {'isin': isin_match.group()}
                self.extract_data_from_line(line, current_bond)
            
            # Extract other data from current line
            elif current_bond:
                self.extract_data_from_line(line, current_bond)
        
        # Don't forget last bond
        if current_bond:
            bond = self.create_bond_from_data(current_bond, pdf_date)
            if bond:
                bonds.append(bond)
        
        return bonds
    
    def extract_data_from_line(self, line: str, bond_data: Dict):
        """Extract bond data from a line of text"""
        # Look for issue numbers
        issue_match = re.search(r'FXD\d+/\d+/\d+', line)
        if issue_match:
            bond_data['issue_number'] = issue_match.group()
        
        # Look for dates
        for pattern in self.date_patterns:
            dates = re.findall(pattern, line)
            if dates:
                if 'issue_date' not in bond_data:
                    bond_data['issue_date'] = self.standardize_date(dates[0])
                elif 'maturity_date' not in bond_data:
                    bond_data['maturity_date'] = self.standardize_date(dates[0])
        
        # Look for percentages and numbers
        percentages = re.findall(r'(\d+\.?\d*)\s*%', line)
        numbers = re.findall(r'\b(\d+\.?\d*)\b', line)
        
        # Assign percentages to appropriate fields
        for pct in percentages:
            value = self.safe_float(pct)
            if 5 <= value <= 25 and 'coupon_rate' not in bond_data:
                bond_data['coupon_rate'] = value
            elif 5 <= value <= 25 and 'ytm' not in bond_data:
                bond_data['ytm'] = value
        
        # Assign numbers to price if in reasonable range
        for num in numbers:
            value = self.safe_float(num)
            if 80 <= value <= 120 and 'current_price' not in bond_data:
                bond_data['current_price'] = value
    
    def create_bond_from_data(self, bond_dict: Dict, pdf_date: str) -> Optional[BondData]:
        """Create BondData object from extracted data"""
        try:
            # Ensure required fields
            if 'isin' not in bond_dict:
                return None
            
            # Fill defaults
            bond_dict.setdefault('issue_number', 'Unknown')
            bond_dict.setdefault('issue_date', pdf_date)
            bond_dict.setdefault('maturity_date', pdf_date)
            bond_dict.setdefault('tenor', 'Unknown')
            bond_dict.setdefault('coupon_rate', 0.0)
            bond_dict.setdefault('current_price', 100.0)
            bond_dict.setdefault('ytm', 0.0)
            
            # Calculate years to maturity
            try:
                current_date = datetime.strptime(pdf_date, "%Y-%m-%d")
                maturity_date = datetime.strptime(bond_dict['maturity_date'], "%Y-%m-%d")
                years_to_maturity = (maturity_date - current_date).days / 365.25
            except:
                years_to_maturity = 0.0
            
            return BondData(
                date=pdf_date,
                isin=bond_dict['isin'],
                issue_number=bond_dict['issue_number'],
                issue_date=bond_dict['issue_date'],
                maturity_date=bond_dict['maturity_date'],
                tenor=bond_dict['tenor'],
                coupon_rate=bond_dict['coupon_rate'],
                current_price=bond_dict['current_price'],
                ytm=bond_dict['ytm'],
                bond_type='Treasury Bond',
                years_to_maturity=years_to_maturity
            )
            
        except Exception as e:
            logger.error(f"Error creating bond data: {e}")
            return None
    
    def to_dataframe(self, bonds: List[BondData]) -> pd.DataFrame:
        """Convert bonds to DataFrame"""
        if not bonds:
            return pd.DataFrame()
        
        data = []
        for bond in bonds:
            data.append({
                'Date': bond.date,
                'ISIN': bond.isin,
                'IssueNumber': bond.issue_number,
                'IssueDate': bond.issue_date,
                'MaturityDate': bond.maturity_date,
                'Tenor': bond.tenor,
                'CouponRate': bond.coupon_rate,
                'CurrentPrice': bond.current_price,
                'YTM': bond.ytm,
                'BondType': bond.bond_type,
                'YearsToMaturity': bond.years_to_maturity,
                'TaxRate': bond.tax_rate,
                'AfterTaxYTM': bond.after_tax_ytm,
                'PricePremium': bond.price_premium
            })
        
        return pd.DataFrame(data)
    
    async def process_latest_bonds(self, target_date: Optional[str] = None) -> pd.DataFrame:
        """Complete workflow: download, parse, return bond data"""
        try:
            # Get PDF URL
            pdf_url = await self.get_latest_pdf_url(target_date)
            if not pdf_url:
                logger.warning("No PDF URL found")
                return pd.DataFrame()
            
            # Download PDF
            filepath = await self.download_pdf(pdf_url)
            if not filepath:
                logger.error("Failed to download PDF")
                return pd.DataFrame()
            
            # Parse PDF
            bonds = self.parse_pdf(filepath)
            if not bonds:
                logger.warning("No bonds parsed from PDF")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = self.to_dataframe(bonds)
            logger.info(f"Successfully processed {len(df)} bonds")
            return df
            
        except Exception as e:
            logger.error(f"Error in process_latest_bonds: {e}")
            return pd.DataFrame()

class MockDatabase:
    """Mock database for demonstration purposes"""
    
    def __init__(self):
        self.data_file = Path("treasury_data.json")
        self.bonds = self.load_data()
    
    def load_data(self) -> List[Dict]:
        """Load existing data"""
        if self.data_file.exists():
            try:
                with open(self.data_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Error loading data: {e}")
        return []
    
    def save_data(self):
        """Save data to file"""
        try:
            with open(self.data_file, 'w') as f:
                json.dump(self.bonds, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Error saving data: {e}")
    
    def insert_bonds(self, df: pd.DataFrame) -> Dict[str, int]:
        """Insert bond data"""
        stats = {'inserted': 0, 'updated': 0, 'skipped': 0}
        
        for _, row in df.iterrows():
            bond_dict = row.to_dict()
            
            # Check if bond already exists
            existing_index = None
            for i, existing in enumerate(self.bonds):
                if (existing.get('ISIN') == bond_dict['ISIN'] and 
                    existing.get('Date') == bond_dict['Date']):
                    existing_index = i
                    break
            
            if existing_index is not None:
                # Update existing
                self.bonds[existing_index] = bond_dict
                stats['updated'] += 1
            else:
                # Insert new
                self.bonds.append(bond_dict)
                stats['inserted'] += 1
        
        self.save_data()
        return stats
    
    def get_summary(self) -> Dict[str, Any]:
        """Get data summary"""
        if not self.bonds:
            return {}
        
        df = pd.DataFrame(self.bonds)
        
        return {
            'total_bonds': len(df),
            'unique_isins': df['ISIN'].nunique(),
            'latest_date': df['Date'].max(),
            'earliest_date': df['Date'].min(),
            'avg_ytm': df['YTM'].mean(),
            'avg_coupon_rate': df['CouponRate'].mean(),
            'avg_current_price': df['CurrentPrice'].mean()
        }
    
    def get_latest_data(self, limit: int = 100) -> pd.DataFrame:
        """Get latest bond data"""
        if not self.bonds:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.bonds)
        df = df.sort_values(['Date', 'ISIN'], ascending=[False, True])
        return df.head(limit)

class TreasuryPipeline:
    """Main treasury pipeline orchestrator"""
    
    def __init__(self):
        self.processor = NSEBondProcessor()
        self.database = MockDatabase()
    
    async def run_collection(self, target_date: Optional[str] = None) -> Dict[str, Any]:
        """Run complete collection pipeline"""
        start_time = time.time()
        
        try:
            logger.info("Starting treasury bond collection...")
            
            # Process bonds
            df = await self.processor.process_latest_bonds(target_date)
            
            if df.empty:
                return {
                    'success': False,
                    'message': 'No bond data collected',
                    'processed_bonds': 0,
                    'processing_time': time.time() - start_time
                }
            
            # Validate data
            validation_issues = self.validate_data(df)
            
            # Store in database
            stats = self.database.insert_bonds(df)
            
            processing_time = time.time() - start_time
            
            result = {
                'success': True,
                'message': f'Successfully processed {len(df)} bonds',
                'processed_bonds': len(df),
                'database_stats': stats,
                'validation_issues': validation_issues,
                'processing_time': round(processing_time, 2)
            }
            
            logger.info(f"Collection completed: {result['message']}")
            return result
            
        except Exception as e:
            logger.error(f"Collection failed: {e}")
            return {
                'success': False,
                'message': f'Collection error: {str(e)}',
                'processed_bonds': 0,
                'processing_time': time.time() - start_time
            }
    
    def validate_data(self, df: pd.DataFrame) -> List[str]:
        """Validate bond data quality"""
        issues = []
        
        # Check for missing ISINs
        missing_isins = df[df['ISIN'].isna()].shape[0]
        if missing_isins > 0:
            issues.append(f"Missing ISINs: {missing_isins} records")
        
        # Check ISIN format
        invalid_isins = df[~df['ISIN'].str.match(r'KE\d{10}', na=False)].shape[0]
        if invalid_isins > 0:
            issues.append(f"Invalid ISIN format: {invalid_isins} records")
        
        # Check price ranges
        invalid_prices = df[(df['CurrentPrice'] < 50) | (df['CurrentPrice'] > 150)].shape[0]
        if invalid_prices > 0:
            issues.append(f"Invalid prices: {invalid_prices} records")
        
        # Check yield ranges
        invalid_yields = df[(df['YTM'] < 0.01) | (df['YTM'] > 0.30)].shape[0]
        if invalid_yields > 0:
            issues.append(f"Invalid yields: {invalid_yields} records")
        
        return issues
    
    def get_status(self) -> Dict[str, Any]:
        """Get pipeline status"""
        summary = self.database.get_summary()
        
        status = {
            'pipeline_status': 'Ready',
            'last_run': datetime.now().isoformat(),
            'data_summary': summary
        }
        
        return status
    
    def export_data(self, filepath: str, limit: int = 1000) -> bool:
        """Export data to file"""
        try:
            df = self.database.get_latest_data(limit)
            
            if df.empty:
                logger.warning("No data to export")
                return False
            
            if filepath.endswith('.csv'):
                df.to_csv(filepath, index=False)
            elif filepath.endswith('.json'):
                df.to_json(filepath, orient='records', indent=2)
            else:
                logger.error("Unsupported file format. Use .csv or .json")
                return False
            
            logger.info(f"Data exported to {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return False

async def main():
    """Main function with CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='NSE Treasury Securities Pipeline')
    parser.add_argument('--run', action='store_true', help='Run bond collection')
    parser.add_argument('--date', type=str, help='Target date (DD-MMM-YYYY)')
    parser.add_argument('--status', action='store_true', help='Show pipeline status')
    parser.add_argument('--export', type=str, help='Export data to file (CSV/JSON)')
    parser.add_argument('--test', action='store_true', help='Run system test')
    parser.add_argument('--summary', action='store_true', help='Show data summary')
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = TreasuryPipeline()
    
    if args.test:
        print("Running system test...")
        
        # Test imports
        try:
            import pdfplumber, aiohttp, pandas
            print("✓ All required packages available")
        except ImportError as e:
            print(f"✗ Missing package: {e}")
            return
        
        # Test directories
        if Path('downloads').exists():
            print("✓ Downloads directory exists")
        else:
            print("⚠ Downloads directory missing (will be created)")
        
        if Path('logs').exists():
            print("✓ Logs directory exists")
        else:
            print("⚠ Logs directory missing (will be created)")
        
        print("🎉 System test passed!")
    
    elif args.run:
        print("Running treasury bond collection...")
        result = await pipeline.run_collection(args.date)
        
        print(f"\nResult: {'✓ Success' if result['success'] else '✗ Failed'}")
        print(f"Message: {result['message']}")
        print(f"Bonds processed: {result['processed_bonds']}")
        print(f"Processing time: {result['processing_time']:.2f}s")
        
        if result.get('validation_issues'):
            print(f"Validation issues: {result['validation_issues']}")
        
        if result.get('database_stats'):
            stats = result['database_stats']
            print(f"Database: {stats['inserted']} inserted, {stats['updated']} updated")
    
    elif args.status:
        status = pipeline.get_status()
        print("\nPipeline Status:")
        print(f"Status: {status['pipeline_status']}")
        print(f"Last run: {status['last_run']}")
        
        if status['data_summary']:
            summary = status['data_summary']
            print(f"\nData Summary:")
            print(f"Total bonds: {summary.get('total_bonds', 0)}")
            print(f"Unique ISINs: {summary.get('unique_isins', 0)}")
            print(f"Latest date: {summary.get('latest_date', 'N/A')}")
            print(f"Average YTM: {summary.get('avg_ytm', 0):.2f}%")
    
    elif args.summary:
        summary = pipeline.database.get_summary()
        if summary:
            print("\nTreasury Data Summary:")
            print("=" * 30)
            for key, value in summary.items():
                if isinstance(value, float):
                    print(f"{key}: {value:.2f}")
                else:
                    print(f"{key}: {value}")
        else:
            print("No data available")
    
    elif args.export:
        success = pipeline.export_data(args.export)
        if success:
            print(f"✓ Data exported to {args.export}")
        else:
            print(f"✗ Export failed")
    
    else:
        parser.print_help()
        print("\nExample usage:")
        print("  python NSE_Pipeline.py --test          # Test system")
        print("  python NSE_Pipeline.py --run           # Run collection")
        print("  python NSE_Pipeline.py --run --date 03-JUL-2025")
        print("  python NSE_Pipeline.py --status        # Show status")
        print("  python NSE_Pipeline.py --summary       # Show data summary")
        print("  python NSE_Pipeline.py --export data.csv  # Export data")

if __name__ == "__main__":
    asyncio.run(main())