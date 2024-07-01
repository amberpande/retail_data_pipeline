from log_config import setup_logger
from pyspark.sql import SparkSession
import boto3
from scripts.config import AWS_S3_BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION

logger = setup_logger(__name__, 'logs/extract_data.log')

def download_s3_file(bucket_name, file_key, download_path):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    
    try:
        s3_client.download_file(bucket_name, file_key, download_path)
        logger.info(f"File downloaded from S3: s3://{bucket_name}/{file_key}")
    except Exception as e:
        logger.error(f"Error downloading file from S3: {str(e)}")
        raise

def extract_data():
    logger.info("Starting data extraction...")
    try:
        # Define the S3 file key
        file_key = 'data/statsfinal.csv'
        download_path = 'data/statsfinal.csv'
        
        # Download the CSV file from S3
        download_s3_file(AWS_S3_BUCKET_NAME, file_key, download_path)
        
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("RetailDataETL") \
            .getOrCreate()

        # Load CSV file into DataFrame
        df = spark.read.csv(download_path, header=True, inferSchema=True)
        
        logger.info(f"Data extracted successfully from s3://{AWS_S3_BUCKET_NAME}/{file_key}. Row count: {df.count()}")
        return df
    except Exception as e:
        logger.exception(f"Error during data extraction: {str(e)}")
        raise