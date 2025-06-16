from src.models.base import db_manager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_database():
    """Initialize database with all required tables"""
    try:
        # Create all tables
        db_manager.create_tables()
        
        # Verify tables were created
        counts = db_manager.get_table_counts()
        
        logger.info("✅ Database initialization complete")
        logger.info("\n📊 Table Status:")
        for table, count in counts.items():
            logger.info(f"   {table}: {count} records")
            
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {str(e)}")
        raise

if __name__ == "__main__":
    logger.info("🔄 Initializing database tables...")
    initialize_database()