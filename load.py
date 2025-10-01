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

def download_taxi_data(year=2024, taxi_type='yellow', month=1):
    
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url = f"{base_url}/{filename}"
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        os.makedirs('data/temp', exist_ok=True)
        filepath = f"data/temp/{filename}"
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(filepath, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {filename}") as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        
        logging.info(f"Downloaded {filename}")
        return filepath
    
    except Exception as e:
        logging.error(f"Failed to download {filename}: {e}")
        return None

def create_tables(conn):
    
    try:
        # create yellow taxi table
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

    except Exception as e:
        logging.error(f"Failed to create tables: {e}")
        raise

def load_emissions_data(conn):
    
    try:
        emissions_df = pd.read_csv('data/vehicle_emissions.csv')
        conn.execute("DELETE FROM vehicle_emissions")
        conn.register('emissions_temp', emissions_df)
        conn.execute("INSERT INTO vehicle_emissions SELECT * FROM emissions_temp")
        
        count = conn.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
        logging.info(f"Loaded {count} emission records")
        return count
        
    except Exception as e:
        logging.error(f"Failed to load emissions data: {e}")
        raise

def load_parquet_files():

    yellow_count = 0
    green_count = 0
    
    # load data for all 12 months of 2024
    for month in range(1, 13):
        try:
            # load yellow taxi data
            yellow_file = download_taxi_data(2024, 'yellow', month)
            if yellow_file and os.path.exists(yellow_file):
                conn.execute(f"""
                    INSERT INTO yellow_trips 
                    SELECT * FROM read_parquet('{yellow_file}')
                """)
                logging.info(f"Loaded Yellow taxi data for month {month}")
            
            # load green taxi data  
            green_file = download_taxi_data(2024, 'green', month)
            if green_file and os.path.exists(green_file):
                conn.execute(f"""
                    INSERT INTO green_trips 
                    SELECT * FROM read_parquet('{green_file}')
                """)
                logging.info(f"Loaded Green taxi data for month {month}")
                
        except Exception as e:
            logging.error(f"Failed to load data for month {month}: {e}")
            continue
    
    # get final counts for both yellow and green
    yellow_count = conn.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
    green_count = conn.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]
    
    return yellow_count, green_count

def main():

    try:
        logging.info("Starting data loading process")
        
        conn = setup_database()
        create_tables(conn)
        emissions_count = load_emissions_data(conn)
        yellow_count, green_count = load_taxi_data_programmatically(conn)
        
        # output summary
        logging.info("DATA LOADING SUMMARY:")
        logging.info(f"Yellow trips: {yellow_count:,}")
        logging.info(f"Green trips: {green_count:,}")
        logging.info(f"Emissions records: {emissions_count}")
        
        print("Raw Row Counts (Before Cleaning):")
        print(f"Yellow Taxi: {yellow_count:,} trips")
        print(f"Green Taxi: {green_count:,} trips") 
        print(f"Vehicle Emissions: {emissions_count} records")
        
        conn.close()
        logging.info("Data loading completed successfully!")
        
    except Exception as e:
        logging.error(f"Data loading failed: {e}")
        raise

if __name__ == "__main__":
    main()
