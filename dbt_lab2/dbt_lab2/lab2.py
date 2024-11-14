from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
import logging
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.hooks.base import BaseHook


# Default settings for the DAG
default_args = {
    'owner': 'keerthi',
    'email': ['keerthikaloganathan@sjsu.edu'],
    'retries': 1,
}
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = '/opt/airflow/dbt'

# Fetch connection details
conn = BaseHook.get_connection('snowflake_conn')

with DAG(
    dag_id='stock_price_analysis_lab2',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ETL', 'dbt', 'Analytics'],
    schedule_interval='@daily',
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_SCHEMA": conn.schema,
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }}
) as dag:
    
    @task
    def fetch_data(symbols=['AAPL', 'TSLA']):
        """Fetch daily stock prices from Alpha Vantage API for specified symbols."""
        try:
            api_key = Variable.get('alpha_vantage_api_key')
            logging.info("Retrieving data using Alpha Vantage API.")
            stock_data = []
            for symbol in symbols:
                url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                if "Time Series (Daily)" not in data:
                    logging.error(f"Data not found for symbol: {symbol}")
                    continue
                for date, price_data in data["Time Series (Daily)"].items():
                    price_data["date"] = date
                    price_data["symbol"] = symbol
                    stock_data.append(price_data)
                logging.info(f"Data successfully retrieved for {symbol}")
            return stock_data
        except Exception as e:
            logging.error(f"Data fetch error: {str(e)}")
            raise

    create_warehouse_db_schema = SnowflakeOperator(
        task_id='create_warehouse_db_schema',
        snowflake_conn_id='snowflake_conn',
        sql="""
        -- Initialize warehouse, database, and schema if not present
        CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
        WITH WAREHOUSE_SIZE = 'SMALL'
        AUTO_SUSPEND = 300
        AUTO_RESUME = TRUE
        INITIALLY_SUSPENDED = TRUE;

        CREATE DATABASE IF NOT EXISTS STOCK_PRICE_DB;
        CREATE SCHEMA IF NOT EXISTS STOCK_PRICE_DB.RAW_DATA;
        """,
        warehouse='COMPUTE_WH'
    )

    @task
    def insert_data_into_snowflake(stock_data):
        """Insert or update fetched stock data into Snowflake."""
        try:
            snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
            connection = snowflake_hook.get_conn()
            with connection.cursor() as cursor:
                cursor.execute("USE DATABASE STOCK_PRICE_DB;")
                cursor.execute("USE SCHEMA RAW_DATA;")
                
                # Ensure main table exists
                create_table_query = """
                CREATE TABLE IF NOT EXISTS VANTAGE_API (
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume INT,
                    date DATE,
                    symbol VARCHAR(20),
                    PRIMARY KEY (date, symbol)
                );
                """
                cursor.execute(create_table_query)
                
                # Create a temporary table for staging data
                cursor.execute("""
                CREATE OR REPLACE TEMPORARY TABLE VANTAGE_API_STAGE LIKE VANTAGE_API;
                """)
                
                # Structure data for bulk insertion
                insert_records = [
                    (
                        float(record['1. open']),
                        float(record['2. high']),
                        float(record['3. low']),
                        float(record['4. close']),
                        int(record['5. volume']),
                        record['date'],
                        record['symbol']
                    )
                    for record in stock_data
                ]
                
                # Insert data into temporary table
                insert_query = """
                INSERT INTO VANTAGE_API_STAGE (open, high, low, close, volume, date, symbol)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """
                cursor.executemany(insert_query, insert_records)
                logging.info("Inserted data into staging table.")

                # Use MERGE to sync data to main table
                merge_query = """
                MERGE INTO VANTAGE_API AS target
                USING VANTAGE_API_STAGE AS source
                ON target.date = source.date AND target.symbol = source.symbol
                WHEN MATCHED THEN
                    UPDATE SET
                        open = source.open,
                        high = source.high,
                        low = source.low,
                        close = source.close,
                        volume = source.volume
                WHEN NOT MATCHED THEN
                    INSERT (open, high, low, close, volume, date, symbol)
                    VALUES (source.open, source.high, source.low, source.close, source.volume, source.date, source.symbol);
                """
                cursor.execute(merge_query)
                logging.info("Merged data into main table.")

                # Remove the staging table
                cursor.execute("DROP TABLE IF EXISTS VANTAGE_API_STAGE;")
                logging.info("Staging table removed.")
        except Exception as e:
            logging.error(f"Snowflake insertion error: {str(e)}")
            raise
        finally:
            connection.close()

    # Define DBT tasks
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    # Task execution order
    fetched_data = fetch_data()
    create_warehouse_db_schema >> insert_data_into_snowflake(fetched_data) >> dbt_run >> dbt_test >> dbt_snapshot
