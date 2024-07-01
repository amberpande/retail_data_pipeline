from log_config import setup_logger
from scripts.extract_data import extract_data
from scripts.transform_data import transform_data
from scripts.load_to_s3 import load_to_s3
from scripts.load_to_snowflake import load_to_snowflake

logger = setup_logger(__name__, 'logs/etl_process.log')

def etl_task():
    try:
        logger.info("Starting ETL process...")
        
        logger.info("Extracting data...")
        df = extract_data()
        
        logger.info("Transforming data...")
        df_transformed = transform_data(df)
        
        logger.info("Loading data to S3...")
        s3_filename = load_to_s3(df_transformed)
        
        logger.info(f"Loading data from S3 file '{s3_filename}' to Snowflake...")
        load_to_snowflake(s3_filename)

        logger.info("ETL process completed successfully.")
    except Exception as e:
        logger.exception(f"Error in ETL process: {str(e)}")
        raise

if __name__ == "__main__":
    etl_task()