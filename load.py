"""
Goal of this File:
- Create a local DuckDB database with tables for yellow, green trips, and vehicle_emissions.
- Download 2024 monthly parquet files for yellow and green taxis (programmatically).
- Insert datasets into DuckDB and print raw row counts (pre-cleaning).
- Log each operation and handle errors robustly.
"""

import duckdb
import pandas as pd
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def setup_database():
    try:
        conn = duckdb.connect(database='taxi_data.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def load_parquet_files():

    con = None

    try:
        os.makedirs('data', exist_ok=True)
        os.makedirs('logs', exist_ok=True)

        yellow_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        green_url  = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet"

        download_file(yellow_url, "data/yellow_tripdata_2024-01.parquet")
        download_file(green_url, "data/green_tripdata_2024-01.parquet")

        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        con.execute("DROP TABLE IF EXISTS yellow_trips")
        con.execute("""
            CREATE TABLE yellow_trips (
                VendorID INTEGER,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count INTEGER,
                trip_distance DOUBLE,
                RatecodeID INTEGER,
                store_and_fwd_flag VARCHAR,
                PULocationID INTEGER,
                DOLocationID INTEGER,
                payment_type INTEGER,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                improvement_surcharge DOUBLE,
                total_amount DOUBLE,
                congestion_surcharge DOUBLE,
                Airport_fee DOUBLE
            )
        """)
        logger.info("Created yellow_trips table")

        con.execute("DROP TABLE IF EXISTS green_trips")
        con.execute("""
            CREATE TABLE green_trips (
                VendorID INTEGER,
                lpep_pickup_datetime TIMESTAMP,
                lpep_dropoff_datetime TIMESTAMP,
                store_and_fwd_flag VARCHAR,
                RatecodeID INTEGER,
                PULocationID INTEGER,
                DOLocationID INTEGER,
                passenger_count INTEGER,
                trip_distance DOUBLE,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                ehail_fee DOUBLE,
                improvement_surcharge DOUBLE,
                total_amount DOUBLE,
                payment_type INTEGER,
                trip_type INTEGER,
                congestion_surcharge DOUBLE
            )
        """)
        logger.info("Created green_trips table")

        con.execute("DROP TABLE IF EXISTS vehicle_emissions")
        con.execute("""
            CREATE TABLE vehicle_emissions (
                vehicle_type VARCHAR,
                fuel_type VARCHAR,
                mpg_city INTEGER,
                mpg_highway INTEGER,
                co2_grams_per_mile INTEGER,
                vehicle_year_avg INTEGER
            )
        """)
        logger.info("Created vehicle_emissions table")

        if os.path.exists("vehicle_emissions.csv"):
            emissions_df = pd.read_csv("vehicle_emissions.csv")
            con.execute("DELETE FROM vehicle_emissions")
            con.register("emissions_temp", emissions_df)
            con.execute("INSERT INTO vehicle_emissions SELECT * FROM emissions_temp")
            emissions_count = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
            logger.info(f"Loaded {emissions_count} emission records")
        else:
            emissions_count = 0
            logger.warning("vehicle_emissions.csv not found")

        yellow_file = download_file("yellow_tripdata_2024-01.parquet")
        green_file = download_file("green_tripdata_2024-01.parquet")

        yellow_files = list_matching_files(["data/yellow_tripdata_2024-*.parquet"])
        green_files = list_matching_files(["data/green_tripdata_2024-*.parquet"])

        if yellow_files:
            con.execute("DELETE FROM yellow_trips")
            con.execute("""
                INSERT INTO yellow_trips
                SELECT * FROM read_parquet(?)
            """, [yellow_files])
            yellow_count = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
            logger.info(f"Loaded {yellow_count:,} yellow taxi trips from {len(yellow_files)} file(s)")
        else:
            yellow_count = 0
            logger.warning("No yellow taxi Parquet files found")

        if green_files:
            con.execute("DELETE FROM green_trips")
            con.execute("""
                INSERT INTO green_trips
                SELECT * FROM read_parquet(?)
            """, [green_files])
            green_count = con.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]
            logger.info(f"Loaded {green_count:,} green taxi trips from {len(green_files)} file(s)")
        else:
            green_count = 0
            logger.warning("No green taxi Parquet files found")

        print("=== Data Load Summary ===")
        print(f"Yellow Taxi Trips: {yellow_count:,}")
        print(f"Green Taxi Trips:  {green_count:,}")
        print(f"Emissions Records: {emissions_count:,}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    finally:
        if con:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    load_parquet_files()
