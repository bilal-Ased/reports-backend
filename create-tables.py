from database import engine, Base
import models
import pymysql
import traceback

def create_tables():
    try:
        # Test direct connection to MySQL first
        print("Testing direct MySQL connection...")
        conn = pymysql.connect(
            host='127.0.0.1',
            user='bilal',
            password='Bilal@2025',
            database='banking_crm',
            port=3306
        )
        print("Direct MySQL connection successful!")
        conn.close()
        
        # Now try SQLAlchemy
        print("Creating database tables...")
        Base.metadata.create_all(bind=engine)
        print("Tables created successfully!")
    except Exception as e:
        print(f"Error creating tables: {e}")
        print(traceback.format_exc())

if __name__ == "__main__":
    create_tables()