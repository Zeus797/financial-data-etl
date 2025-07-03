#!/usr/bin/env python3
"""
Simple script to load market index data into PostgreSQL database
Run this after your collector and transformer have completed successfully
"""

import sys
import os
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.append(str(src_path))

from src.loaders.market_indexes_loader import load_market_index_data
from src.models.base import db_manager

def main():
    """Load market index data into database"""
    
    print("🚀 MARKET INDEX DATA LOADER")
    print("=" * 50)
    
    # Test database connection first
    print("🔌 Testing database connection...")
    if not db_manager.test_connection():
        print("❌ Database connection failed!")
        print("\n🔧 Troubleshooting:")
        print("1. Check PostgreSQL is running")
        print("2. Verify your .env file has correct database credentials")
        print("3. Ensure database exists and user has permissions")
        return False
    
    print("✅ Database connection successful!")
    
    # Check if transformed data file exists
    csv_path = "data/processed_data/transformed_indexes.csv"
    if not os.path.exists(csv_path):
        print(f"❌ Transformed data file not found: {csv_path}")
        print("\n🔧 Please run the transformer first:")
        print("   python src/transformers/market_index_transformer.py")
        return False
    
    print(f"📄 Found transformed data file: {csv_path}")
    
    # Load the data
    print("\n💾 Starting data load...")
    success = load_market_index_data(csv_path)
    
    if success:
        print("\n🎉 SUCCESS! Market index data loaded into database")
        
        # Show final statistics
        print("\n📊 Database Statistics:")
        counts = db_manager.get_table_counts()
        for table, count in counts.items():
            print(f"   {table}: {count:,} records")
            
        print("\n🎯 Next Steps:")
        print("1. Connect PowerBI to your PostgreSQL database")
        print("2. Use the star schema tables for optimal performance")
        print("3. Your ETL pipeline is now complete!")
        
    else:
        print("\n❌ FAILED to load market index data")
        print("Check the logs above for detailed error information")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)