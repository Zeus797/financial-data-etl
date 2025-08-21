#!/usr/bin/env python3
"""
NSE Treasury Securities Pipeline - Fixed with GET requests
The key fix: Use GET instead of HEAD for URL checking
"""

import os
import sys
import asyncio
import logging
import pandas as pd
import numpy as np
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
from PIL import Image
import pytesseract
from pdf2image import convert_from_path  # pip install pdf2image
from paddleocr import PaddleOCR
from transformers import DonutProcessor, VisionEncoderDecoderModel
from PIL import Image
 
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

# Initialize PaddleOCR with proper error handling
try:
    ocr_engine = PaddleOCR(use_textline_orientation=True, lang='en')
    logger.info("PaddleOCR initialized successfully")
except Exception as e:
    logger.warning(f"PaddleOCR initialization failed: {e}")
    ocr_engine = None

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
    """NSE Bond PDF processor - FIXED VERSION"""
    
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
    
    async def find_available_pdf(self, start_date: Optional[str] = None) -> Optional[str]:
        """FIXED: Enhanced method using GET instead of HEAD requests"""
        
        if start_date:
            try:
                # Try parsing different date formats
                if '-' in start_date and len(start_date.split('-')) == 3:
                    if start_date.count('-') == 2 and len(start_date.split('-')[0]) <= 2:
                        # DD-MMM-YYYY format
                        search_date = datetime.strptime(start_date, "%d-%b-%Y")
                    else:
                        # YYYY-MM-DD format
                        search_date = datetime.strptime(start_date, "%Y-%m-%d")
                else:
                    search_date = datetime.strptime(start_date, "%d-%b-%Y")
            except ValueError:
                logger.warning(f"Invalid date format: {start_date}, using today")
                search_date = datetime.now()
        else:
            search_date = datetime.now()
        
        logger.info(f"Searching for PDFs starting from: {search_date.strftime('%d-%b-%Y')}")
        
        # Try multiple date formats and go back up to 15 business days
        for days_back in range(20):  # Try 20 days back to be thorough
            test_date = search_date - timedelta(days=days_back)
            
            # Skip weekends for business data
            if test_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
                logger.debug(f"Skipping weekend: {test_date.strftime('%d-%b-%Y')}")
                continue
            
            date_str = test_date.strftime("%d-%b-%Y").upper()
            logger.info(f"Checking for PDF on: {date_str}")
            
            # Try different filename formats
            filename_formats = [
                f"BondPrices_{date_str}.pdf",
                f"BondPrices_{date_str.replace('-', '')}.pdf",
                f"bond_prices_{date_str.lower()}.pdf",
                f"BondPrices-{date_str}.pdf",
                f"NSE_BondPrices_{date_str}.pdf",
                f"Treasury_BondPrices_{date_str}.pdf"
            ]
            
            logger.debug(f"Testing formats: {filename_formats}")
            
            for filename in filename_formats:
                url = f"{self.base_url}{filename}"
                logger.debug(f"Testing URL: {url}")
                
                try:
                    # KEY FIX: Use GET instead of HEAD - only read first few bytes
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url, timeout=15) as response:
                            if response.status == 200:
                                # Check if it's actually a PDF by reading just a bit
                                content_start = await response.content.read(1024)  # Read first 1KB
                                if content_start.startswith(b'%PDF'):
                                    logger.info(f"✓ Found valid PDF for {date_str}: {url}")
                                    return url
                                else:
                                    logger.debug(f"Not a PDF file: {url}")
                            else:
                                logger.debug(f"HTTP {response.status} for {url}")
                except Exception as e:
                    logger.debug(f"Error checking {url}: {e}")
                    continue
            
            logger.debug(f"No PDF found for {date_str}")
        
        logger.error("No PDF files found in the last 20 days")
        return None
    
    async def get_latest_pdf_url(self, target_date: Optional[str] = None) -> Optional[str]:
        """Wrapper method for backward compatibility"""
        return await self.find_available_pdf(target_date)
    
    async def download_pdf(self, url: str) -> Optional[Path]:
        """Download PDF from URL - ENHANCED VERSION"""
        try:
            filename = url.split('/')[-1]
            filepath = self.download_folder / filename
            
            # Skip download if file already exists and is recent
            if filepath.exists():
                file_age = datetime.now().timestamp() - filepath.stat().st_mtime
                if file_age < 3600:  # Less than 1 hour old
                    logger.info(f"Using existing file: {filepath}")
                    return filepath
            
            logger.info(f"Downloading: {url}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=60) as response:  # Longer timeout
                    if response.status == 200:
                        content = await response.read()
                        
                        # Validate it's actually a PDF
                        if not content.startswith(b'%PDF'):
                            logger.error("Downloaded file is not a valid PDF")
                            return None
                        
                        with open(filepath, 'wb') as f:
                            f.write(content)
                        
                        logger.info(f"Downloaded: {filepath} ({len(content)} bytes)")
                        return filepath
                    else:
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
        
    def ocr_extract_text(self, filepath: Path) -> str:
        """Extract text from image-based PDF using Tesseract OCR"""
        try:
            pages = convert_from_path(str(filepath), dpi=300)
            full_text = ""

            for i, page in enumerate(pages):
                gray = page.convert('L')
                bw = gray.point(lambda x: 0 if x < 200 else 255, '1')
                text = pytesseract.image_to_string(bw, config="--psm 6")
                full_text += text + "\n"
                logger.info(f"Tesseract extracted {len(text)} chars from page {i+1}")
            
            return full_text
        except Exception as e:
            logger.error(f"Tesseract OCR failed: {e}")
            return ""
    
    def extract_with_paddleocr(self, filepath: Path) -> str:
        """Extract text using PaddleOCR with enhanced error handling"""
        if ocr_engine is None:
            logger.error("PaddleOCR not available")
            return ""
            
        try:
            images = convert_from_path(str(filepath), dpi=300)
            full_text = ""
            
            for page_num, img in enumerate(images):
                logger.info(f"Processing page {page_num + 1} with PaddleOCR...")
                result = ocr_engine.predict(np.array(img))
                
                page_text = ""
                if result and result[0]:
                    for line in result[0]:
                        if len(line) >= 2 and line[1] and len(line[1]) >= 1:
                            text = line[1][0]
                            confidence = line[1][1] if len(line[1]) >= 2 else 0.0
                            
                            # Include text with reasonable confidence
                            if confidence > 0.3:
                                page_text += text + "\n"
                
                full_text += f"=== PAGE {page_num + 1} ===\n{page_text}\n"
                logger.info(f"PaddleOCR extracted {len(page_text)} chars from page {page_num + 1}")
            
            return full_text
        except Exception as e:
            logger.error(f"PaddleOCR extraction failed: {e}")
            return ""
    
    def parse_pdf(self, filepath: Path) -> List[BondData]:
        """Extract bond data from PDF with multiple OCR fallbacks"""
        bonds = []
        
        try:
            logger.info(f"Parsing PDF: {filepath}")
            
            # Method 1: Try pdfplumber first
            with pdfplumber.open(filepath) as pdf:
                full_text = ""

                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        full_text += page_text + "\n"
            
            logger.info(f"pdfplumber extracted {len(full_text)} characters")
            
            # Method 2: If pdfplumber fails or gets poor results, try PaddleOCR
            if len(full_text.strip()) < 100:
                logger.warning("pdfplumber extraction poor, trying PaddleOCR...")
                full_text = self.extract_with_paddleocr(filepath)
                
                # Method 3: If PaddleOCR also fails, try Tesseract as final fallback
                if len(full_text.strip()) < 100:
                    logger.warning("PaddleOCR also poor, trying Tesseract OCR...")
                    full_text = self.ocr_extract_text(filepath)
            
            # Save extracted text for debugging
            debug_file = f"extracted_text_{filepath.stem}.txt"
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(full_text)
            logger.info(f"Saved extracted text to: {debug_file}")
            
            # Show sample of extracted text
            logger.info("Sample extracted text (first 500 chars):")
            logger.info("-" * 50)
            logger.info(full_text[:500])
            logger.info("-" * 50)
            
            if not full_text.strip():
                logger.error("No text could be extracted from PDF using any method")
                return []
            
            # Extract date from filename or content
            pdf_date = self.extract_date_from_content(filepath.name, full_text)
            logger.info(f"Extracted date: {pdf_date}")
            
            # Parse bonds from text
            bonds = self.parse_bonds_from_text(full_text, pdf_date)
            
            logger.info(f"Successfully extracted {len(bonds)} bonds from PDF")
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
        """Parse bond data from PDF text with enhanced detection"""
        bonds = []
        lines = text.split('\n')
        
        logger.info(f"Parsing {len(lines)} lines of text...")
        
        current_bond = {}
        
        for line_num, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            # Look for ISIN
            isin_match = re.search(self.isin_pattern, line)
            if isin_match:
                # Save previous bond
                if current_bond and 'isin' in current_bond:
                    bond = self.create_bond_from_data(current_bond, pdf_date)
                    if bond:
                        bonds.append(bond)
                        logger.info(f"Found bond: {bond.isin} - {bond.coupon_rate}% - {bond.current_price}")
                
                # Start new bond
                current_bond = {'isin': isin_match.group()}
                logger.info(f"Line {line_num}: Found ISIN {isin_match.group()}")
                self.extract_data_from_line(line, current_bond)
            
            # Extract other data from current line
            elif current_bond and 'isin' in current_bond:
                self.extract_data_from_line(line, current_bond)
        
        # Don't forget last bond
        if current_bond and 'isin' in current_bond:
            bond = self.create_bond_from_data(current_bond, pdf_date)
            if bond:
                bonds.append(bond)
                logger.info(f"Found final bond: {bond.isin} - {bond.coupon_rate}% - {bond.current_price}")
        
        logger.info(f"Total bonds parsed: {len(bonds)}")
        return bonds
    
    def extract_data_from_line(self, line: str, bond_data: Dict):
        """Extract bond data from a line of text"""
        # Look for issue numbers
        issue_match = re.search(r'FXD\d+/\d+/\d+', line)
        if issue_match and 'issue_number' not in bond_data:
            bond_data['issue_number'] = issue_match.group()
            logger.debug(f"Found issue number: {issue_match.group()}")
        
        # Look for dates
        for pattern in self.date_patterns:
            dates = re.findall(pattern, line)
            if dates:
                if 'issue_date' not in bond_data:
                    bond_data['issue_date'] = self.standardize_date(dates[0])
                    logger.debug(f"Found issue date: {dates[0]}")
                elif 'maturity_date' not in bond_data:
                    bond_data['maturity_date'] = self.standardize_date(dates[0])
                    logger.debug(f"Found maturity date: {dates[0]}")
        
        # Look for percentages and numbers
        percentages = re.findall(r'(\d+\.?\d*)\s*%', line)
        numbers = re.findall(r'\b(\d+\.?\d*)\b', line)
        
        # Assign percentages to appropriate fields
        for pct in percentages:
            value = self.safe_float(pct)
            if 1 <= value <= 30 and 'coupon_rate' not in bond_data:
                bond_data['coupon_rate'] = value
                logger.debug(f"Found coupon rate: {value}%")
            elif 1 <= value <= 30 and 'ytm' not in bond_data:
                bond_data['ytm'] = value
                logger.debug(f"Found YTM: {value}%")
        
        # Assign numbers to price if in reasonable range
        for num in numbers:
            value = self.safe_float(num)
            if 70 <= value <= 130 and 'current_price' not in bond_data:
                bond_data['current_price'] = value
                logger.debug(f"Found price: {value}")
    
    def create_bond_from_data(self, bond_dict: Dict, pdf_date: str) -> Optional[BondData]:
        """Create BondData object from extracted data"""
        try:
            # Ensure required fields
            if 'isin' not in bond_dict:
                return None
            
            # Fill defaults
            bond_dict.setdefault('issue_number', f"Unknown-{bond_dict['isin'][-4:]}")
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
        """Complete workflow: find, download, parse, return bond data"""
        try:
            logger.info("Starting bond processing workflow...")
            
            # Use enhanced PDF search with GET requests
            pdf_url = await self.find_available_pdf(target_date)
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
            
            # Show sample of processed data
            if not df.empty:
                logger.info("Sample processed data:")
                logger.info(df[['ISIN', 'CouponRate', 'CurrentPrice', 'YTM']].head().to_string())
            
            return df
            
        except Exception as e:
            logger.error(f"Error in process_latest_bonds: {e}")
            return pd.DataFrame()

# Include all your other classes (MockDatabase, TreasuryPipeline, main) unchanged...

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
            logger.info(f"Saved {len(self.bonds)} bonds to {self.data_file}")
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

async def main():
    """Main function with CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='NSE Treasury Securities Pipeline - FIXED VERSION')
    parser.add_argument('--run', action='store_true', help='Run bond collection')
    parser.add_argument('--date', type=str, help='Target date (DD-MMM-YYYY)')
    parser.add_argument('--test', action='store_true', help='Run system test')
    parser.add_argument('--search', action='store_true', help='Search for available PDFs')
    
    args = parser.parse_args()
    
    # Create directories
    Path('downloads').mkdir(exist_ok=True)
    Path('logs').mkdir(exist_ok=True)
    
    if args.test:
        print("🧪 Testing system...")
        if ocr_engine is not None:
            print("✓ PaddleOCR available")
        else:
            print("⚠ PaddleOCR not available")
        print("✓ Downloads directory ready")
        print("✓ Logs directory ready")
        print("🎉 System test passed!")
    
    elif args.search:
        print("🔍 Searching for available PDFs...")
        processor = NSEBondProcessor()
        pdf_url = await processor.find_available_pdf(args.date)
        if pdf_url:
            print(f"✓ Found PDF: {pdf_url}")
        else:
            print("✗ No PDFs found")
    
    elif args.run:
        print("🚀 Running treasury bond collection...")
        pipeline = TreasuryPipeline()
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
        
        # Show sample data if successful
        if result['success'] and result['processed_bonds'] > 0:
            print("\n📊 Sample bond data:")
            df = pipeline.database.get_latest_data(5)
            if not df.empty:
                print(df[['ISIN', 'CouponRate', 'CurrentPrice', 'YTM']].to_string())
            
            # Save to CSV for easy viewing
            output_file = f"bonds_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            pipeline.database.get_latest_data().to_csv(output_file, index=False)
            print(f"💾 Data saved to: {output_file}")
    
    else:
        parser.print_help()
        print("\nExample usage:")
        print("  python NSE_Pipeline_Fixed.py --test")
        print("  python NSE_Pipeline_Fixed.py --search")
        print("  python NSE_Pipeline_Fixed.py --search --date 10-JUL-2025")
        print("  python NSE_Pipeline_Fixed.py --run")
        print("  python NSE_Pipeline_Fixed.py --run --date 10-JUL-2025")

if __name__ == "__main__":
    asyncio.run(main())