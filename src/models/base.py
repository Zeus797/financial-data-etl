"""
Database connection and session management for the Financial Data Pipeline
Enhanced for universal cross-asset schema
"""

from sqlalchemy import create_engine, event, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from typing import Generator  
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin')

# Create SQLAlchemy engine and session factory
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def test_connection():
    """Test database connection and log configuration"""
    logger.info("Database connection configured")
    logger.info(f"  Host: {DB_HOST}:{DB_PORT}")
    logger.info(f"  Database: {DB_NAME}")
    logger.info(f"  User: {DB_USER}")
    logger.info(f"  Password: {'*' * len(DB_PASSWORD)}")
    
    try:
        with engine.connect() as conn:
            # Fix: Use proper SQL syntax and text() for raw SQL
            conn.execute(text("SELECT 1"))
            logger.info("DatabaseManager initialized with universal schema support")
            return True
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return False

@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Database error: {str(e)}")
        raise
    finally:
        session.close()

# Initialize database connection when module is loaded
test_connection()

# Enhanced: Database Manager Class for better organization
class DatabaseManager:
    """Enhanced database manager for universal schema operations"""
    def __init__(self):
        self.engine = engine
        self.SessionLocal = SessionLocal
        logger.info("DatabaseManager initialized with universal schema support")

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions with automatic cleanup

        Usage:
            with db_manager.get_session() as session:
                # Use session here
                pass
        """

        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error: {str(e)}")
            raise
        finally:
            session.close()


    def create_tables(self):
        """Create all tables defined in the universal modeld"""
        try:
            #Import here to avoid circular imports
            from .universal_models import Base as UniversalBase
            UniversalBase.metadata.create_all(bind=self.engine)
            logger.info("Universal database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise

    def drop_tables(self):
        """Drop all tables (use with caution!)"""
        try:
            from .universal_models import Base as UniversalBase
            UniversalBase.metadata.drop_all(bind=self.engine)
            logger.info("Universal database tables dropped successfully")
        except Exception as e:
            logger.error(f"Error dropping tables: {e}")
            raise

    def test_connection(self) -> bool:
        """Test database connectivity without exposing credentials"""
        try:
            with self.get_session() as session:
                result = session.execute(text("SELECT current_database(), current_user, version()")).fetchone()
                if result:
                    logger.info(f"✅ Database connection test successful")
                    logger.info(f"   Connected to database: {result[0]}")
                    logger.info(f"   Connected as user: {result[1]}")
                    logger.info(f"   PostgreSQL version: :{result[2].split(',')[0]}")
                    return True
                else:
                    logger.error("Database query returned no results")
                    return False
        except Exception as e:
            logger.error(f"❌ Database connection test failed")
            logger.error(f"   Error: {str(e)}")
            logger.error("   Please verify your .env file contains correct database credentials")
            return False

    def get_table_counts(self) -> dict:
        """Get record counts for all dimension tables"""
        try:
            from .universal_models import(
                DimCurrency, DimCountry, DimEntity,
                DimFinancialInstrument, DimDate
            )

            with self.get_session() as session:
                counts = {
                    'currencies': session.query(DimCurrency).count(),
                    'countries': session.query(DimCountry).count(),
                    'entities': session.query(DimEntity).count(),
                    'instruments': session.query(DimFinancialInstrument).count(),
                    'dates': session.query(DimDate).count()
                }
                return counts
        except Exception as e:
            logger.error(f"Error getting table counts: {e}")
            return {}
        
# Global database manager instance
db_manager = DatabaseManager()

# ENHANCED: Keep your original function for backwards compatibility

def get_db():
    """Get database session (Original function for FastAPI dependency)"""
    return db_manager.get_session()

def create_tables():
    """Create all universal database tables"""
    return db_manager.create_tables()

def test_connection():
    """Test database connection"""
    return db_manager.get_table_counts()


# Connection event listeners for bette error handling
@event.listens_for(engine, "connect")
def set_postgresql_settins(dbapi_connection, connection_record):
    """Set PostgreSQL connection settings"""
    with dbapi_connection.cursor() as cursor:
        # Set timezone to UTC for consistency
        cursor.execute("SET timezone to 'UTC'")
        # Set statement timeout (optional)
        cursor.execute("SET statement_timeout = '300s'")


@event.listens_for(engine, "checkout")
def check_connection(dbapi_connection, connection_recors, connection_proxy):
    """Check connection health on checkout"""
    try:
        # Ping the connection
        with dbapi_connection.cursor() as cursor:
            cursor.execute("SELECT 1")
    except Exception as e:
        logger.warning(f"Connection check failed, invalidating: {e}")
        # Invalidate the connection
        connection_proxy._invalidate()
        raise

# Example usage and testing functions
if __name__ == "__main__":
    print("Testing enhanced database configuration...")

    try:
        # Test connection
        if test_connection():
            print("✅ Database connection test successful")

            # Show table counts
            counts = db_manager.get_table_counts()
            if counts:
                print("📊 Current table counts:")
                for table, count in counts.items():
                    print(f"   {table}: {count} records")

            # Test session usage
            print("Testing session management...")
            with db_manager.get_session() as session:
                result = session.execute(text("SELECT current_timestamp")).fetchone()
                if result:
                    print(f"✅ Current database time: {result[0]}")
                else:
                    print("❌ Could not fetch current database time")

        else:
            print("❌ Database connection failed!")
            print("\n🔧 Troubleshooting steps:")
            print("1. Check your .env file exists in project root")
            print("2. Verify .env contains all required variables:")
            print("   DB_USER=your_username")
            print("   DB_PASSWORD=your_password")
            print("   DB_HOST=localhost")
            print("   DB_PORT=5432") 
            print("   DB_NAME=postgres")
            print("3. Ensure PostgreSQL is running")
            print("4. Test connection manually: psql -h localhost -U your_username -d postgres")
            
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        print("\n💡 This usually means your .env file is missing or incomplete")
        print("   Create a .env file in your project root with database credentials")
