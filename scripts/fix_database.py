import logging
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_database_schema():
    """Add missing pair_symbol column to currency_pairs table"""
    
    # Database configuration
    DATABASE_URL = "postgresql://postgres:Postgres%407394@localhost:5432/postgres"
    
    try:
        # Create engine
        engine = create_engine(DATABASE_URL)
        
        # Test connection
        with engine.connect() as conn:
            logger.info("✓ Connected to database successfully")
            
            # Add the missing column
            conn.execute(text("ALTER TABLE currency_pairs ADD COLUMN IF NOT EXISTS pair_symbol VARCHAR(10);"))
            conn.commit()
            logger.info("✓ Successfully added pair_symbol column")
            
            # Verify the column was added
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'currency_pairs' AND column_name = 'pair_symbol'
            """))
            
            if result.fetchone():
                logger.info("✓ Verified: pair_symbol column exists")
            else:
                logger.warning("⚠ Warning: Could not verify column creation")
            
        engine.dispose()
        logger.info("✓ Database connection closed")
        
    except Exception as e:
        logger.error(f"✗ Error: {e}")
        return False
        
    return True

if __name__ == "__main__":
    print("=" * 50)
    print("DATABASE SCHEMA FIX")
    print("=" * 50)
    
    success = fix_database_schema()
    
    print("=" * 50)
    if success:
        print("✓ SUCCESS: Database schema fixed!")
        print("You can now run your pipeline tests.")
    else:
        print("✗ FAILED: Could not fix database schema.")
        print("Try the direct PostgreSQL approach instead.")
    print("=" * 50)