import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

# ----------------------------------------------------
# DATABASE CONFIG
# ----------------------------------------------------
# change these values to your real postgres credentials
DATABASE_URL = "postgresql+psycopg://koyeb-adm:npg_Kv3jR5phErgG@ep-still-fog-a2h2urki.eu-central-1.pg.koyeb.app/koyebdb"


engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=10,
    max_overflow=20,
    echo=False
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ----------------------------------------------------
# SESSION UTILS
# ----------------------------------------------------
def get_database_session():
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"Database session error: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def get_db_session():
    return SessionLocal()

# ----------------------------------------------------
# CREATE TABLES
# ----------------------------------------------------
def create_tables():
    try:
        from models import Company, TicketRequest, RequestLog, EmailLog, SystemConfig
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Database tables created successfully")
    except Exception as e:
        logger.error(f"❌ Error creating database tables: {e}")
        raise

# ----------------------------------------------------
# HEALTH / TEST
# ----------------------------------------------------
def test_connection():
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("✅ Database connection OK")
        return True
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False

def check_tables():
    """
    Example check for the `companies` table existence
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT to_regclass('public.companies')"))
            exists = result.fetchone()[0] is not None
            return exists
    except Exception as e:
        logger.error(f"Table check failed: {e}")
        return False

def get_db_info():
    try:
        with engine.connect() as connection:
            version = connection.execute(text("SELECT version()")).fetchone()[0]
            current_db = connection.execute(text("SELECT current_database()")).fetchone()[0]
            return {
                "version": version,
                "current_db": current_db
            }
    except Exception as e:
        return {"error": str(e)}

def check_database_health():
    status = {
        "connection": test_connection(),
        "tables_exist": check_tables(),
        "info": get_db_info()
    }
    return status

# ----------------------------------------------------
# CLEANUP
# ----------------------------------------------------
def close_db_connections():
    try:
        engine.dispose()
        logger.info("✅ DB connections closed")
    except Exception as e:
        logger.error(f"❌ Error closing connections: {e}")
