from log_config import setup_logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, to_date

logger = setup_logger(__name__, 'logs/transform_data.log')

def transform_data(df: DataFrame) -> DataFrame:
    logger.info("Starting data transformation...")
    try:
        # Log initial DataFrame info
        logger.info(f"Initial DataFrame: {df.count()} rows, {len(df.columns)} columns")

        # Convert Date column to date type
        logger.info("Converting Date column to date type...")
        df = df.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy"))
        logger.info("Date column converted successfully")

        # Extract date parts
        logger.info("Extracting year, month, and day from Date...")
        df = df.withColumn("year", year(col("Date"))) \
               .withColumn("month", month(col("Date"))) \
               .withColumn("day", dayofmonth(col("Date")))
        logger.info("Date parts extracted successfully")

        # Explode the sales data into multiple rows for each product
        logger.info("Exploding sales data for each product...")
        sales_data = df.selectExpr(
            "Date as date",
            "year",
            "month",
            "day",
            "'P1' as product_id", "`Q-P1` as total_units_sold", "`S-P1` as total_revenue"
        ).union(df.selectExpr(
            "Date as date",
            "year",
            "month",
            "day",
            "'P2' as product_id", "`Q-P2` as total_units_sold", "`S-P2` as total_revenue"
        )).union(df.selectExpr(
            "Date as date",
            "year",
            "month",
            "day",
            "'P3' as product_id", "`Q-P3` as total_units_sold", "`S-P3` as total_revenue"
        )).union(df.selectExpr(
            "Date as date",
            "year",
            "month",
            "day",
            "'P4' as product_id", "`Q-P4` as total_units_sold", "`S-P4` as total_revenue"
        ))

        # Log final DataFrame info
        logger.info(f"Transformed DataFrame: {sales_data.count()} rows, {len(sales_data.columns)} columns")
        logger.info("Data transformation completed successfully")

        return sales_data

    except Exception as e:
        logger.exception(f"Error during data transformation: {str(e)}")
        raise