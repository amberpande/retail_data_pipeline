from log_config import setup_logger
import snowflake.connector
import boto3
from io import StringIO
import csv
from datetime import datetime
from scripts.create_snowflake_resources import create_snowflake_resources
from scripts.config import (
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, AWS_S3_BUCKET_NAME, AWS_ACCESS_KEY,
    AWS_SECRET_KEY, AWS_REGION,
)

logger = setup_logger(__name__, 'logs/load_to_snowflake.log')

def get_latest_s3_filename():
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY,
                             region_name=AWS_REGION)

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=AWS_S3_BUCKET_NAME, Prefix='processed_data/')

    latest_file = None
    latest_date = None

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                if latest_date is None or obj['LastModified'] > latest_date:
                    latest_date = obj['LastModified']
                    latest_file = obj['Key']

    if latest_file:
        logger.info(f"Latest file found: {latest_file}")
    else:
        logger.error(f"No files found in S3 bucket {AWS_S3_BUCKET_NAME}/processed_data")

    return latest_file

def load_to_snowflake(s3_filename):
    logger.info(f"Starting data load to Snowflake from S3 file: {s3_filename}")
    try:
        create_snowflake_resources()

        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        cursor = conn.cursor()

        # Check if file has been processed before
        cursor.execute("SELECT filename FROM processed_files WHERE filename = %s", (s3_filename,))
        if cursor.fetchone():
            logger.info(f"File {s3_filename} already processed. Skipping.")
            return

        # Create a temporary stage
        stage_name = f"temp_stage_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        cursor.execute(f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")

        # Create temporary table
        cursor.execute("""
        CREATE OR REPLACE TEMPORARY TABLE temp_sales (
            date_str VARCHAR,
            year FLOAT,
            month FLOAT,
            day FLOAT,
            product_key VARCHAR,
            total_units_sold FLOAT,
            total_revenue FLOAT
        )
        """)

        # Load data into temporary table using COPY command
        cursor.execute(f"""
        COPY INTO temp_sales
        FROM 's3://{AWS_S3_BUCKET_NAME}/{s3_filename}'
        CREDENTIALS = (AWS_KEY_ID='{AWS_ACCESS_KEY}' AWS_SECRET_KEY='{AWS_SECRET_KEY}')
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        """)

        # Upsert into date_dim
        cursor.execute("""
        MERGE INTO date_dim AS target
        USING (
            SELECT DISTINCT TO_DATE(date_str, 'YYYY-MM-DD') AS date_key, 
                   CAST(year AS INT) AS year, 
                   CAST(month AS INT) AS month, 
                   CAST(day AS INT) AS day
            FROM temp_sales
        ) AS source
        ON target.date_key = source.date_key
        WHEN MATCHED THEN UPDATE SET
            year = source.year,
            month = source.month,
            day = source.day
        WHEN NOT MATCHED THEN INSERT (date_key, year, month, day)
        VALUES (source.date_key, source.year, source.month, source.day)
        """)

        # Upsert into product_dim
        cursor.execute("""
        MERGE INTO product_dim AS target
        USING (
            SELECT DISTINCT product_key, CONCAT('Product ', SUBSTR(product_key, 2, 1)) AS product_name
            FROM temp_sales
        ) AS source
        ON target.product_key = source.product_key
        WHEN MATCHED THEN UPDATE SET
            product_name = source.product_name
        WHEN NOT MATCHED THEN INSERT (product_key, product_name)
        VALUES (source.product_key, source.product_name)
        """)

        # Upsert into sales_fact
        cursor.execute("""
        MERGE INTO sales_fact AS target
        USING (
            SELECT TO_DATE(date_str, 'YYYY-MM-DD') AS date_key, 
                   product_key, 
                   CAST(total_units_sold AS INT) AS total_units_sold, 
                   total_revenue
            FROM temp_sales
        ) AS source
        ON target.date_key = source.date_key AND target.product_key = source.product_key
        WHEN MATCHED THEN UPDATE SET
            total_units_sold = source.total_units_sold,
            total_revenue = source.total_revenue
        WHEN NOT MATCHED THEN INSERT (date_key, product_key, total_units_sold, total_revenue)
        VALUES (source.date_key, source.product_key, source.total_units_sold, source.total_revenue)
        """)

        # Mark file as processed
        cursor.execute("INSERT INTO processed_files (filename, processed_at) VALUES (%s, CURRENT_TIMESTAMP())", (s3_filename,))

        conn.commit()
        conn.close()

        logger.info(f"Data from {s3_filename} loaded successfully into Snowflake.")
    except Exception as e:
        logger.exception(f"Error during Snowflake data load: {str(e)}")
        raise

if __name__ == "__main__":
    latest_s3_filename = get_latest_s3_filename()
    if latest_s3_filename:
        logger.info(f"Latest S3 filename obtained: {latest_s3_filename}")
        load_to_snowflake(latest_s3_filename)
    else:
        logger.error("No valid S3 file to process.")
