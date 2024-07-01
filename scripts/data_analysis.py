from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import snowflake.connector
import matplotlib.pyplot as plt
from log_config import setup_logger
from scripts.config import (
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
)
import os

logger = setup_logger(__name__, 'logs/data_analysis.log')

def create_spark_session():
    return SparkSession.builder.appName("SalesDataAnalysis").getOrCreate()

def load_data(spark, conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM sales_fact")
        
        # Define the schema for the Spark DataFrame
        schema = StructType([
            StructField("DATE_KEY", DateType(), True),
            StructField("PRODUCT_KEY", StringType(), True),
            StructField("TOTAL_UNITS_SOLD", IntegerType(), True),
            StructField("TOTAL_REVENUE", DoubleType(), True)
        ])
        
        # Fetch data and create Spark DataFrame
        data = cur.fetchall()
        df = spark.createDataFrame(data, schema)
        
        logger.info(f"Successfully loaded data. Count: {df.count()}")
        df.printSchema()
        df.show(5, truncate=False)
        return df
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise
    finally:
        if cur:
            cur.close()

def analyze_data(df):
    logger.info("Starting data analysis...")

    # Total sales by product
    logger.info("Calculating total sales by product...")
    total_sales = df.groupBy("PRODUCT_KEY").agg(F.sum("TOTAL_REVENUE").alias("total_sales")).orderBy(F.desc("total_sales"))
    total_sales.show()

    # Monthly sales trend
    logger.info("Calculating monthly sales trend...")
    monthly_sales = df.groupBy(F.date_trunc("month", "DATE_KEY").alias("month")) \
                      .agg(F.sum("TOTAL_REVENUE").alias("total_sales")) \
                      .orderBy("month")
    monthly_sales.show()

    # Top selling products
    logger.info("Identifying top selling products...")
    top_products = df.groupBy("PRODUCT_KEY").agg(F.sum("TOTAL_UNITS_SOLD").alias("total_units")).orderBy(F.desc("total_units")).limit(5)
    top_products.show()

    # Average revenue per sale
    logger.info("Calculating average revenue per sale...")
    avg_revenue = df.agg(F.avg("TOTAL_REVENUE").alias("avg_revenue"))
    avg_revenue.show()

    return total_sales, monthly_sales, top_products, avg_revenue

import os
import matplotlib.pyplot as plt

def visualize_data(total_sales, monthly_sales):
    logger.info("Creating visualizations...")

    # Create visualizations folder if it doesn't exist
    vis_folder = "visualizations"
    if not os.path.exists(vis_folder):
        os.makedirs(vis_folder)
        logger.info(f"Created {vis_folder} directory")

    # Bar plot for total sales by product
    total_sales_data = total_sales.collect()
    plt.figure(figsize=(10, 6))
    plt.bar([row['PRODUCT_KEY'] for row in total_sales_data], [row['total_sales'] for row in total_sales_data])
    plt.title('Total Sales by Product')
    plt.xlabel('Product Key')
    plt.ylabel('Total Sales')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(vis_folder, 'total_sales_by_product.png'))
    plt.close()

    # Line plot for monthly sales trend
    monthly_sales_data = monthly_sales.collect()
    plt.figure(figsize=(12, 6))
    plt.plot([row['month'] for row in monthly_sales_data], [row['total_sales'] for row in monthly_sales_data])
    plt.title('Monthly Sales Trend')
    plt.xlabel('Month')
    plt.ylabel('Total Sales')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(vis_folder, 'monthly_sales_trend.png'))
    plt.close()

    logger.info(f"Visualizations saved in the '{vis_folder}' folder.")

def main():
    spark = None
    conn = None
    try:
        spark = create_spark_session()
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        df = load_data(spark, conn)
        total_sales, monthly_sales, top_products, avg_revenue = analyze_data(df)
        visualize_data(total_sales, monthly_sales)
        logger.info("Data analysis completed successfully.")
    except Exception as e:
        logger.exception(f"Error in data analysis: {str(e)}")
    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.")
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()