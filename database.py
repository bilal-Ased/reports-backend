import logging
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logger = logging.getLogger(__name__)

# MySQL connection string - using your existing configuration
DATABASE_URL = "mysql+pymysql://bilal:Bilal%402025@127.0.0.1:3306/kati_reports"

# Create engine with your existing configuration
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Verify connections before use
    pool_recycle=300,    # Recycle connections every 5 minutes
    pool_size=10,        # Connection pool size
    max_overflow=20,     # Maximum overflow connections
    echo=False           # Set to True for SQL query logging
)

# Create SessionLocal class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create Base class for models
Base = declarative_base()

def get_database_session():
    """
    Dependency function to get database session.
    Yields a database session and ensures it's closed after use.
    """
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"Database session error: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def create_tables():
    """
    Create all database tables defined in models.
    """
    try:
        # Import models to ensure they're registered with Base
        from models import Company, TicketRequest, RequestLog, EmailLog, SystemConfig
        
        # Create all tables
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
        return True
    except Exception as e:
        logger.error(f"Error creating database tables: {e}")
        raise

def test_connection():
    """
    Test database connection.
    Returns True if connection is successful, False otherwise.
    """
    try:
        with engine.connect() as connection:
            # Execute a simple query to test connection
            result = connection.execute(text("SELECT 1"))
            result.fetchone()
            logger.info("Database connection test successful")
            return True
    except SQLAlchemyError as e:
        logger.error(f"Database connection test failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during database connection test: {e}")
        return False

def get_db_info():
    """
    Get database information for debugging.
    """
    try:
        with engine.connect() as connection:
            # Get database version
            version_result = connection.execute(text("SELECT VERSION()"))
            version = version_result.fetchone()[0]
            
            # Get current database name
            db_result = connection.execute(text("SELECT DATABASE()"))
            current_db = db_result.fetchone()[0]
            
            return {
                "database_url": DATABASE_URL.replace(DB_PASSWORD, "***") if DB_PASSWORD else DATABASE_URL,
                "database_version": version,
                "current_database": current_db,
                "connection_pool_size": engine.pool.size(),
                "checked_out_connections": engine.pool.checkedout()
            }
    except Exception as e:
        logger.error(f"Error getting database info: {e}")
        return {"error": str(e)}

def close_db_connections():
    """
    Close all database connections (useful for cleanup).
    """
    try:
        engine.dispose()
        logger.info("Database connections closed")
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")

# Alternative session management for direct use (not FastAPI dependency)
def get_db_session():
    """
    Get a database session for direct use (not as FastAPI dependency).
    Remember to close the session manually.
    """
    return SessionLocal()

# Context manager for database sessions
class DatabaseSession:
    """
    Context manager for database sessions.
    Usage:
        with DatabaseSession() as db:
            # Use db session
            pass
    """
    def __init__(self):
        self.db = None
    
    def __enter__(self):
        self.db = SessionLocal()
        return self.db
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.db.rollback()
        self.db.close()

# Database health check function
def check_database_health():
    """
    Comprehensive database health check.
    """
    health_status = {
        "status": "healthy",
        "checks": {
            "connection": False,
            "tables_exist": False,
            "can_write": False
        },
        "info": {},
        "errors": []
    }
    
    try:
        # Test connection
        if test_connection():
            health_status["checks"]["connection"] = True
            health_status["info"] = get_db_info()
        else:
            health_status["status"] = "unhealthy"
            health_status["errors"].append("Database connection failed")
        
        # Check if tables exist
        try:
            from models import Company
            with engine.connect() as connection:
                result = connection.execute(text("SHOW TABLES LIKE 'companies'"))
                if result.fetchone():
                    health_status["checks"]["tables_exist"] = True
                else:
                    health_status["errors"].append("Required tables do not exist")
        except Exception as e:
            health_status["errors"].append(f"Table check failed: {str(e)}")
        
        # Test write capability
        try:
            with DatabaseSession() as db:
                # Try to query a table (this tests read capability)
                from models import Company
                db.query(Company).count()
                health_status["checks"]["can_write"] = True
        except Exception as e:
            health_status["errors"].append(f"Database write test failed: {str(e)}")
    
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["errors"].append(f"Health check failed: {str(e)}")
    
    if health_status["errors"]:
        health_status["status"] = "unhealthy"
    
    return health_status