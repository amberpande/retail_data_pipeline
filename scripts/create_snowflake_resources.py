from log_config import setup_logger
import snowflake.connector as sc
from scripts.config import SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA


logger = setup_logger(__name__, 'logs/create_snowflake_resources.log')

def create_snowflake_resources():
    logger.info("Starting creation of Snowflake resources...")
    try:
        conn = sc.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT
        )
        cursor = conn.cursor()

        # Create warehouse
        logger.info(f"Creating warehouse: {SNOWFLAKE_WAREHOUSE}")
        cursor.execute(f"CREATE WAREHOUSE IF NOT EXISTS {SNOWFLAKE_WAREHOUSE} WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE")

        # Create database
        logger.info(f"Creating database: {SNOWFLAKE_DATABASE}")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")

        # Use the database
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")

        # Create schema
        logger.info(f"Creating schema: {SNOWFLAKE_SCHEMA}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")

        # Use the schema
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

        # Create date dimension table
        logger.info("Creating date_dim table")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS date_dim (
            date_key DATE PRIMARY KEY,
            year INT,
            month INT,
            day INT
        )
        """)

        # Create product dimension table
        logger.info("Creating product_dim table")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_dim (
            product_key STRING PRIMARY KEY,
            product_name STRING
        )
        """)

        # Create sales fact table
        logger.info("Creating sales_fact table")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_fact (
            date_key DATE,
            product_key STRING,
            total_units_sold INT,
            total_revenue FLOAT,
            PRIMARY KEY (date_key, product_key),
            FOREIGN KEY (date_key) REFERENCES date_dim(date_key),
            FOREIGN KEY (product_key) REFERENCES product_dim(product_key)
        )
        """)

        # Create processed_files table
        logger.info("Creating processed_files table")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            filename STRING PRIMARY KEY,
            processed_at TIMESTAMP_NTZ
        )
        """)

        conn.close()
        logger.info("Snowflake resources created successfully.")
    except Exception as e:
        logger.exception(f"Error creating Snowflake resources: {str(e)}")
        raise