"""
Goal of this File:
- Create a local DuckDB database with tables for yellow, green trips, and vehicle_emissions.
- Download 2024 monthly parquet files for yellow and green taxis (programmatically).
- Insert datasets into DuckDB and print raw row counts (pre-cleaning).
- Log each operation and handle errors robustly.
"""

import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def setup_database():

    try:
        con = duckdb.connect(database='taxi_data.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        return con
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def create_tables(con):

    try:
        # create yellow taxi table
        con.execute("""
            CREATE OR REPLACE TABLE yellow_trips (
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count INTEGER,
                trip_distance DOUBLE
            )
        """)
        logger.info("Created yellow_trips table")

        con.execute("""
            CREATE OR REPLACE TABLE green_trips (
                lpep_pickup_datetime TIMESTAMP,
                lpep_dropoff_datetime TIMESTAMP,
                passenger_count INTEGER,
                trip_distance DOUBLE
            )
        """)
        logger.info("Created green_trips table")

        con.execute("""
            CREATE OR REPLACE TABLE vehicle_emissions AS SELECT * FROM read_csv_auto('vehicle_emissions.csv');
        """)
        logger.info("Created vehicle_emissions table")

    except Exception as e:
        logging.error(f"Failed to create tables: {e}")
        raise

def load_data_files(con):

  taxi_types = ['yellow','green']
  years = range(2015, 2025)

  for taxi in taxi_types:
    logger.info(f"Loading data for {taxi} taxis")
    pickup_col = 'tpep_pickup_datetime' if taxi == 'yellow' else 'lpep_pickup_datetime'
    dropoff_col = 'tpep_dropoff_datetime' if taxi == 'yellow' else 'lpep_dropoff_datetime'

    for year in years:

      # load data for all 12 months of 2024
      for month in range(1, 13):
        data = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month:02d}.parquet"

        try:
          loadlog = f"""
            INSERT INTO {taxi}_trips ({pickup_col}, {dropoff_col}, passenger_count, trip_distance)
            SELECT "{pickup_col}", "{dropoff_col}", "passenger_count", "trip_distance" FROM read_parquet('{data}')
          """

          con.execute(loadlog)
          logger.info(f"The data has been loaded from {data}")

        except Exception as e:
          logger.warning(f"Data cannot be loaded from {data}. Error: {e}")
        time.sleep(9) # so the server can rest

    logger.info(f"Finished data loading for {taxi} taxis")

def data_summary(con):

   logger.info("Summarizing the data")
   for table_name in ['yellow_trips', 'green_trips', 'vehicle_emissions']:
       count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

       output = f"There are {count} entries in {table_name}."
       logger.info(output)
       print(output)

def main():

    try:
        logging.info("Starting data loading process")

        con = setup_database()
        create_tables(con)
        load_data_files(con)
        data_summary(con)

        con.close()
        logging.info("Data loading completed successfully!")

    except Exception as e:
        logging.error(f"Data loading failed: {e}")
        raise

if __name__ == "__main__":
   main()
