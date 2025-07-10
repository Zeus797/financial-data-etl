#!/usr/bin/env python3
"""
NSE Bond Data PDF Reader and Transformer
Automatically downloads, reads, and transforms NSE bond price PDFs
"""

import os
import re
import requests
import pandas as pd
import pdfplumber
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urljoin
import logging
from pathlib import Path
import asyncio
import aiohttp
from decimal import Decimal, InvalidOperation

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class BondData:
    """Data class for bond information"""
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
    tax_rate: float = 0.1  # Default 10% tax rate
    after_tax_ytm: Optional[float] = None
    price_premium: Optional[float] = None

    def __post_init__(self):
        """Calculate derived fields"""
        if self.after_tax_ytm is None:
            self.after_tax_ytm = self.ytm * (1 - self.tax_rate)
        
        if self.price_premium is None:
            # Price premium = (Current Price - 100) / 100 * 100
            self.price_premium = ((self.current_price - 100) / 100) * 100

class NSEBondPDFProcessor:
    """NSE Bond PDF processor with enhanced parsing capabilities"""
    
    def __init__(self, base_url: str = "https://www.nse.co.ke/wp-content/uploads/"):
        self.base_url = base_url
        self.download_folder = Path("downloads")
        self.download_folder.mkdir(exist_ok=True)
        
        # PDF parsing patterns
        self.date_patterns = [
            r'(\d{1,2}-[A-Z]{3}-\d{4})',  # 03-JUL-2025
            r'(\d{1,2}/\d{1,2}/\d{4})',   # 03/07/2025
            r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})',  # 03 July 2025
        ]
        
        self.isin_pattern = r'KE\d{10}'
        self.issue_number_pattern = r'FXD\d+/\d+/\d+'
        
        # Bond type classification
        self.bond_types = {
            'FXD': 'Treasury Bond',
            'IFB': 'Infrastructure Bond',
            'TBL': 'Treasury Bill',
            'TBD': 'Treasury Bond'
        }

    async def get_latest_pdf_url(self, target_date: Optional[str] = None) -> Optional[str]:
        """
        Get the URL for the latest bond prices PDF
        
        Args:
            target_date: Specific date to look for (format: DD-MMM-YYYY)
        
        Returns:
            URL of the PDF file
        """
        if target_date is None:
            target_date = datetime.now().strftime("%d-%b-%Y").upper()
        
        # Try different filename formats
        filename_formats = [
            f"BondPrices_{target_date}.pdf",
            f"BondPrices_{target_date.replace('-', '')}.pdf",
            f"bond_prices_{target_date.lower()}.pdf",
            f"NSE_BondPrices_{target_date}.pdf"
        ]
        
        for filename in filename_formats:
            url = urljoin(self.base_url, filename)
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.head(url) as response:
                        if response.status == 200:
                            logger.info(f"Found PDF at: {url}")
                            return url
            except Exception as e:
                logger.debug(f"Failed to check {url}: {e}")
                continue
        
        # If today's file not found, try yesterday
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y").upper()
        for filename in filename_formats:
            filename = filename.replace(target_date, yesterday)
            url = urljoin(self.base_url, filename)
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.head(url) as response:
                        if response.status == 200:
                            logger.info(f"Found yesterday's PDF at: {url}")
                            return url
            except Exception as e:
                logger.debug(f"Failed to check {url}: {e}")
                continue
        
        logger.warning(f"No PDF found for {target_date} or {yesterday}")
        return None

    async def download_pdf(self, url: str) -> Optional[Path]:
        """
        Download PDF from URL
        
        Args:
            url: PDF URL
            
        Returns:
            Path to downloaded file
        """
        try:
            filename = url.split('/')[-1]
            filepath = self.download_folder / filename
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.read()
                        with open(filepath, 'wb') as f:
                            f.write(content)
                        logger.info(f"Downloaded PDF to: {filepath}")
                        return filepath
                    else:
                        logger.error(f"Failed to download PDF: HTTP {response.status}")
                        return None
        except Exception as e:
            logger.error(f"Error downloading PDF: {e}")
            return None

    def extract_date_from_filename_or_content(self, filename: str, content: str) -> str:
        """Extract date from filename or PDF content"""
        # Try filename first
        for pattern in self.date_patterns:
            match = re.search(pattern, filename)
            if match:
                return self.standardize_date(match.group(1))
        
        # Try PDF content
        for pattern in self.date_patterns:
            match = re.search(pattern, content)
            if match:
                return self.standardize_date(match.group(1))
        
        # Fallback to today
        return datetime.now().strftime("%Y-%m-%d")

    def standardize_date(self, date_str: str) -> str:
        """Convert various date formats to YYYY-MM-DD"""
        try:
            # Try different parsing formats
            formats = [
                "%d-%b-%Y",  # 03-JUL-2025
                "%d/%m/%Y",  # 03/07/2025
                "%d %B %Y",  # 03 July 2025
                "%d %b %Y",  # 03 Jul 2025
            ]
            
            for fmt in formats:
                try:
                    parsed_date = datetime.strptime(date_str.upper(), fmt)
                    return parsed_date.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            
            # If all fails, return as-is
            return date_str
        except Exception as e:
            logger.warning(f"Date standardization failed for {date_str}: {e}")
            return date_str

    def classify_bond_type(self, issue_number: str, isin: str) -> str:
        """Classify bond type based on issue number or ISIN"""
        if not issue_number:
            return "Unknown"
        
        # Check issue number prefix
        for prefix, bond_type in self.bond_types.items():
            if issue_number.startswith(prefix):
                return bond_type
        
        # Default classification
        return "Treasury Bond"

    def calculate_years_to_maturity(self, current_date: str, maturity_date: str) -> float:
        """Calculate years to maturity"""
        try:
            current = datetime.strptime(current_date, "%Y-%m-%d")
            maturity = datetime.strptime(maturity_date, "%Y-%m-%d")
            days_diff = (maturity - current).days
            return round(days_diff / 365.25, 2)
        except Exception as e:
            logger.warning(f"Years to maturity calculation failed: {e}")
            return 0.0

    def safe_float_conversion(self, value: str) -> float:
        """Safely convert string to float"""
        if not value or value.strip() == '':
            return 0.0
        
        try:
            # Remove common non-numeric characters
            clean_value = re.sub(r'[^\d.-]', '', str(value))
            return float(clean_value)
        except (ValueError, InvalidOperation):
            logger.warning(f"Failed to convert '{value}' to float")
            return 0.0

    def parse_bond_table(self, text: str, pdf_date: str) -> List[BondData]:
        """
        Parse bond data from PDF text using enhanced pattern matching
        
        Args:
            text: Extracted PDF text
            pdf_date: Date from PDF filename/content
            
        Returns:
            List of BondData objects
        """
        bonds = []
        lines = text.split('\n')
        
        # Look for table data patterns
        current_bond = {}
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Skip header lines
            if any(header in line.upper() for header in ['ISIN', 'ISSUE', 'MATURITY', 'COUPON', 'PRICE']):
                continue
            
            # Look for ISIN pattern - indicates start of new bond
            isin_match = re.search(self.isin_pattern, line)
            if isin_match:
                # Save previous bond if exists
                if current_bond:
                    bonds.append(self.create_bond_data(current_bond, pdf_date))
                
                # Start new bond
                current_bond = {'isin': isin_match.group()}
                
                # Extract other data from the same line
                parts = line.split()
                self.extract_bond_data_from_line(parts, current_bond)
            
            # Look for issue number pattern
            issue_match = re.search(self.issue_number_pattern, line)
            if issue_match and current_bond:
                current_bond['issue_number'] = issue_match.group()
            
            # Look for date patterns
            for pattern in self.date_patterns:
                date_matches = re.findall(pattern, line)
                if date_matches and current_bond:
                    if 'issue_date' not in current_bond:
                        current_bond['issue_date'] = self.standardize_date(date_matches[0])
                    elif 'maturity_date' not in current_bond:
                        current_bond['maturity_date'] = self.standardize_date(date_matches[0])
            
            # Look for numeric data (coupon, price, ytm)
            numbers = re.findall(r'\d+\.?\d*', line)
            if numbers and current_bond:
                self.extract_numeric_data(numbers, current_bond)
        
        # Don't forget the last bond
        if current_bond:
            bonds.append(self.create_bond_data(current_bond, pdf_date))
        
        return bonds

    def extract_bond_data_from_line(self, parts: List[str], bond_data: Dict):
        """Extract bond data from line parts"""
        for part in parts:
            # Look for tenor patterns
            if re.match(r'\d+\s*(Y|YEAR|YRS?)', part.upper()):
                bond_data['tenor'] = part
            
            # Look for percentage values
            if '%' in part:
                num_value = self.safe_float_conversion(part.replace('%', ''))
                if 'coupon_rate' not in bond_data:
                    bond_data['coupon_rate'] = num_value
                elif 'ytm' not in bond_data:
                    bond_data['ytm'] = num_value

    def extract_numeric_data(self, numbers: List[str], bond_data: Dict):
        """Extract numeric data from numbers list"""
        for num_str in numbers:
            num_value = self.safe_float_conversion(num_str)
            
            # Logic to assign numbers to appropriate fields
            if 80 <= num_value <= 120 and 'current_price' not in bond_data:
                bond_data['current_price'] = num_value
            elif 5 <= num_value <= 25 and 'coupon_rate' not in bond_data:
                bond_data['coupon_rate'] = num_value
            elif 5 <= num_value <= 25 and 'ytm' not in bond_data:
                bond_data['ytm'] = num_value

    def create_bond_data(self, bond_dict: Dict, pdf_date: str) -> BondData:
        """Create BondData object from dictionary"""
        # Fill in missing data with defaults
        bond_dict.setdefault('issue_number', 'Unknown')
        bond_dict.setdefault('issue_date', pdf_date)
        bond_dict.setdefault('maturity_date', pdf_date)
        bond_dict.setdefault('tenor', 'Unknown')
        bond_dict.setdefault('coupon_rate', 0.0)
        bond_dict.setdefault('current_price', 100.0)
        bond_dict.setdefault('ytm', 0.0)
        
        # Calculate derived fields
        bond_type = self.classify_bond_type(bond_dict['issue_number'], bond_dict['isin'])
        years_to_maturity = self.calculate_years_to_maturity(pdf_date, bond_dict['maturity_date'])
        
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
            bond_type=bond_type,
            years_to_maturity=years_to_maturity
        )

    def parse_pdf(self, filepath: Path) -> List[BondData]:
        """
        Parse PDF file and extract bond data
        
        Args:
            filepath: Path to PDF file
            
        Returns:
            List of BondData objects
        """
        try:
            with pdfplumber.open(filepath) as pdf:
                full_text = ""
                
                # Extract text from all pages
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        full_text += page_text + "\n"
                
                # Try to extract tables if text parsing fails
                if not full_text.strip():
                    for page in pdf.pages:
                        tables = page.extract_tables()
                        for table in tables:
                            full_text += "\n".join(["\t".join(row) for row in table]) + "\n"
                
                # Extract date from filename or content
                pdf_date = self.extract_date_from_filename_or_content(
                    filepath.name, full_text
                )
                
                # Parse bond data
                bonds = self.parse_bond_table(full_text, pdf_date)
                
                logger.info(f"Parsed {len(bonds)} bonds from PDF")
                return bonds
                
        except Exception as e:
            logger.error(f"Error parsing PDF {filepath}: {e}")
            return []

    def to_dataframe(self, bonds: List[BondData]) -> pd.DataFrame:
        """Convert bond data to pandas DataFrame"""
        if not bonds:
            return pd.DataFrame()
        
        # Convert to list of dictionaries
        bond_dicts = []
        for bond in bonds:
            bond_dict = {
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
            }
            bond_dicts.append(bond_dict)
        
        return pd.DataFrame(bond_dicts)

    async def process_latest_bonds(self, target_date: Optional[str] = None) -> pd.DataFrame:
        """
        Complete workflow: download, parse, and return bond data
        
        Args:
            target_date: Specific date to process (format: DD-MMM-YYYY)
            
        Returns:
            DataFrame with bond data
        """
        try:
            # Get PDF URL
            pdf_url = await self.get_latest_pdf_url(target_date)
            if not pdf_url:
                logger.error("No PDF URL found")
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
            
            # Clean up downloaded file (optional)
            # filepath.unlink()
            
            logger.info(f"Successfully processed {len(df)} bonds")
            return df
            
        except Exception as e:
            logger.error(f"Error in process_latest_bonds: {e}")
            return pd.DataFrame()

    def validate_bond_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Validate bond data and return cleaned DataFrame with issues list
        
        Args:
            df: DataFrame with bond data
            
        Returns:
            Tuple of (cleaned_df, issues_list)
        """
        issues = []
        
        if df.empty:
            issues.append("No data to validate")
            return df, issues
        
        # Validation rules
        required_columns = ['ISIN', 'CouponRate', 'CurrentPrice', 'YTM']
        
        for col in required_columns:
            if col not in df.columns:
                issues.append(f"Missing required column: {col}")
        
        # Data quality checks
        if 'ISIN' in df.columns:
            invalid_isins = df[~df['ISIN'].str.match(r'KE\d{10}', na=False)]
            if not invalid_isins.empty:
                issues.append(f"Found {len(invalid_isins)} invalid ISINs")
        
        if 'CouponRate' in df.columns:
            invalid_coupons = df[(df['CouponRate'] < 0) | (df['CouponRate'] > 30)]
            if not invalid_coupons.empty:
                issues.append(f"Found {len(invalid_coupons)} bonds with invalid coupon rates")
        
        if 'CurrentPrice' in df.columns:
            invalid_prices = df[(df['CurrentPrice'] < 50) | (df['CurrentPrice'] > 150)]
            if not invalid_prices.empty:
                issues.append(f"Found {len(invalid_prices)} bonds with unusual prices")
        
        # Remove completely invalid rows
        valid_df = df.dropna(subset=['ISIN'])
        
        if len(valid_df) < len(df):
            issues.append(f"Removed {len(df) - len(valid_df)} rows with missing ISIN")
        
        return valid_df, issues

# Example usage and testing
async def main():
    """Example usage of the NSE Bond PDF Processor"""
    processor = NSEBondPDFProcessor()
    
    # Process latest bonds
    df = await processor.process_latest_bonds()
    
    if not df.empty:
        print(f"Successfully processed {len(df)} bonds")
        print("\nSample data:")
        print(df.head())
        
        # Validate data
        validated_df, issues = processor.validate_bond_data(df)
        
        if issues:
            print("\nValidation issues:")
            for issue in issues:
                print(f"- {issue}")
        
        # Save to CSV
        output_file = f"nse_bonds_{datetime.now().strftime('%Y%m%d')}.csv"
        validated_df.to_csv(output_file, index=False)
        print(f"\nData saved to: {output_file}")
    else:
        print("No bond data processed")

if __name__ == "__main__":
    asyncio.run(main())