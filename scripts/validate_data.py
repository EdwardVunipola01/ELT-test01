import os
import sys
import pandas as pd
import sqlalchemy as sa
from datetime import datetime, timedelta

# Load environment variables from GitHub secrets
DB_SERVER = os.getenv("DB_SERVER_SECRET")
DB_USER = os.getenv("DB_USER_SECRET")
DB_PASSWORD = os.getenv("DB_PASSWORD_SECRET")
DB_NAME = os.getenv("DB_NAME02_SECRET")
DB_PORT = 1433

# Build connection string
connection_string = (
    f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER},{DB_PORT}/{DB_NAME}"
    "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)

    def fail(message):
        """Print error and exit CI pipeline."""
        print(f"❌ VALIDATION FAILED: {message}")
        sys.exit(1)

    def success(message):
        """Print success message."""
        print(f"✅ {message}")

    def validate_database_connection(engine):
        """Check if database is online and reachable."""
        try:
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
                success("Database connection successful.")
        except Exception as e:
            fail(f"Database connection failed: {e}")

    def validate_data_freshness(engine, table_name, date_column):
        """Check if table contains recent data."""
        query = sa.text(f"""
            SELECT MAX({date_column}) AS last_date
            FROM {table_name}
        """)
        df = pd.read_sql(query, engine)
        last_date = df.iloc[0]["last_date"]

        if last_date is None:
            fail(f"No data found in {table_name}.")
    
        # Example: require data within last 48 hours
        if last_date < datetime.now() - timedelta(hours=48):
            fail(f"{table_name} is stale. Last data date: {last_date}")
    
            success(f"{table_name} contains recent data ({last_date}).")

    def validate_row_count(engine, table_name, min_count=1):
        """Check that the table contains at least min_count rows."""
        query = sa.text(f"SELECT COUNT(*) AS count FROM {table_name}")
        df = pd.read_sql(query, engine)
        count = df.iloc[0]["count"]

        if count < min_count:
            fail(f"{table_name} contains too few rows: {count}")
    
            success(f"{table_name} contains {count} rows.")

    def validate_no_duplicates(engine, table_name, column_name):
        """Check primary key or business key has no duplicates."""
        query = sa.text(f"""
            SELECT {column_name}, COUNT(*) AS c
            FROM {table_name}
            GROUP BY {column_name}
            HAVING COUNT(*) > 1
        """)
        df = pd.read_sql(query, engine)

        if not df.empty:
            fail(f"Duplicate values found in {column_name} of {table_name}")
    
            success(f"No duplicates in {column_name} of {table_name}.")

    def validate_no_nulls(engine, table_name, column_name):
        """Ensure required field is never null."""
        query = sa.text(f"""
            SELECT COUNT(*) AS nulls
            FROM {table_name}
            WHERE {column_name} IS NULL
        """)
        df = pd.read_sql(query, engine)

        if df.iloc[0]["nulls"] > 0:
            fail(f"Null values found in {column_name} of {table_name}.")
    
            success(f"No nulls in {column_name} of {table_name}.")

    def main():
        engine = sa.create_engine(connection_string)

        # 1. Check DB connection
        validate_database_connection(engine)

        # 2. Freshness check (modify table/column to fit your ELT data)
        validate_data_freshness(engine, "Product", "Date")

        # 3. Row count sanity check
        validate_row_count(engine, "Product", min_count=10)

        # 4. Primary key duplicate check
        validate_no_duplicates(engine, "Product", "SaleID")

        # 5. Required column null check
        validate_no_nulls(engine, "Product", "Branch")

        print("✅ ALL DATA VALIDATION CHECKS PASSED")
        sys.exit(0)

    if __name__ == "__main__":
        main()
