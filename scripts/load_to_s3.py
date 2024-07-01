from log_config import setup_logger
import boto3
import hashlib
from io import StringIO
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan
from pyspark.sql.types import IntegerType, FloatType, DateType
from datetime import datetime
from scripts.config import AWS_S3_BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION

logger = setup_logger(__name__, 'logs/load_to_s3.log')

def calculate_hash(csv_content):
    return hashlib.md5(csv_content.encode('utf-8')).hexdigest()

def clean_and_validate_data(df: DataFrame) -> DataFrame:
    # Convert 'Date' to date type and drop rows with invalid dates
    df = df.withColumn("Date", when(col("Date").cast(DateType()).isNotNull(), col("Date")).cast(DateType()))

    # Handle potential floating-point values and convert to integer types
    df = df.withColumn("Year", when((col("Year").cast(FloatType()).isNotNull()), col("Year").cast(FloatType())).cast(IntegerType()))
    df = df.withColumn("Month", when((col("Month").cast(FloatType()).isNotNull()), col("Month").cast(FloatType())).cast(IntegerType()))
    df = df.withColumn("Day", when((col("Day").cast(FloatType()).isNotNull()), col("Day").cast(FloatType())).cast(IntegerType()))
    df = df.withColumn("Total_Units_Sold", when((col("Total_Units_Sold").cast(FloatType()).isNotNull()), col("Total_Units_Sold").cast(FloatType())).cast(IntegerType()))
    df = df.withColumn("Total_Revenue", when((col("Total_Revenue").cast(FloatType()).isNotNull()), col("Total_Revenue").cast(FloatType())))

    # Drop rows with any null values
    df = df.dropna()

    return df

def load_to_s3(df: DataFrame):
    logger.info("Starting data load to S3...")
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )

        # Clean and validate data
        cleaned_df = clean_and_validate_data(df)

        # Convert cleaned DataFrame to CSV
        csv_buffer = StringIO()
        cleaned_df.toPandas().to_csv(csv_buffer, index=False)
        cleaned_csv_content = csv_buffer.getvalue()

        # Calculate hash for the cleaned CSV content
        current_hash = calculate_hash(cleaned_csv_content)
        logger.info(f"Calculated hash for the cleaned CSV content: {current_hash}")

        # Check if the hash already exists in the metadata of S3 objects
        response = s3_client.list_objects_v2(Bucket=AWS_S3_BUCKET_NAME, Prefix='processed_data/')
        if 'Contents' in response:
            for obj in response['Contents']:
                obj_metadata = s3_client.head_object(Bucket=AWS_S3_BUCKET_NAME, Key=obj['Key'])
                if 'Metadata' in obj_metadata and obj_metadata['Metadata'].get('hash') == current_hash:
                    logger.info(f"CSV file with hash {current_hash} already exists. Skipping upload.")
                    return None

        # Generate a timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'processed_data/output_{timestamp}.csv'

        # Upload to S3 with metadata including the hash
        s3_client.put_object(
            Bucket=AWS_S3_BUCKET_NAME,
            Key=filename,
            Body=cleaned_csv_content,
            Metadata={
                'ProcessedDate': datetime.now().isoformat(),
                'RecordCount': str(cleaned_df.count()),
                'PipelineVersion': '1.0',
                'hash': current_hash
            }
        )
        
        logger.info(f"File {filename} uploaded successfully to S3.")
        return filename
    except Exception as e:
        logger.exception(f"Error during S3 upload: {str(e)}")
        raise