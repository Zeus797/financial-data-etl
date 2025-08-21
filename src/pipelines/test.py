#!/usr/bin/env python3
"""
Quick test to download and process the specific PDF
"""

import asyncio
import aiohttp
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_pdf_download():
    """Test downloading the specific PDF"""
    
    url = "https://www.nse.co.ke/wp-content/uploads/BondPrices_10-JUL-2025.pdf"
    download_folder = Path("downloads")
    download_folder.mkdir(exist_ok=True)
    
    print(f"Testing download from: {url}")
    
    try:
        async with aiohttp.ClientSession() as session:
            # First try HEAD request to check if file exists
            print("1. Checking if file exists...")
            async with session.head(url, timeout=15) as response:
                print(f"   HEAD response: {response.status}")
                if response.status == 200:
                    content_length = response.headers.get('content-length', 'unknown')
                    print(f"   File size: {content_length} bytes")
                else:
                    print(f"   Error: HTTP {response.status}")
                    return None
            
            # Now try GET request to download
            print("2. Downloading file...")
            async with session.get(url, timeout=30) as response:
                print(f"   GET response: {response.status}")
                if response.status == 200:
                    content = await response.read()
                    
                    filename = "BondPrices_10-JUL-2025.pdf"
                    filepath = download_folder / filename
                    
                    with open(filepath, 'wb') as f:
                        f.write(content)
                    
                    print(f"   Downloaded: {filepath} ({len(content)} bytes)")
                    return filepath
                else:
                    print(f"   Download failed: HTTP {response.status}")
                    return None
                    
    except Exception as e:
        print(f"Error: {e}")
        return None

async def main():
    """Test the download"""
    filepath = await test_pdf_download()
    
    if filepath:
        print(f"\n✓ Success! File saved to: {filepath}")
        
        # Quick test if we can open it
        try:
            import pdfplumber
            with pdfplumber.open(filepath) as pdf:
                print(f"   PDF has {len(pdf.pages)} pages")
                
                # Try to extract some text from first page
                if pdf.pages:
                    text = pdf.pages[0].extract_text()
                    if text:
                        print(f"   Sample text (first 200 chars): {text[:200]}")
                    else:
                        print("   No text extracted - may need OCR")
        except Exception as e:
            print(f"   PDF analysis error: {e}")
    else:
        print("\n✗ Download failed")

if __name__ == "__main__":
    asyncio.run(main())