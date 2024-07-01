from scripts.etl_process import etl_task
from scripts.create_snowflake_resources import create_snowflake_resources

if __name__ == "__main__":
    create_snowflake_resources()
    etl_task()
