import duckdb
import logging
import os

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/clean.log',
    filemode='a'
)
logger = logging.getLogger(__name__)

def clean_trips():
    con = None

    try:
        # Connect to DuckDB
        con = duckdb.connect(database='taxi_data.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        trip_tables = ["yellow_trips", "green_trips"]

        for table in trip_tables:
            logger.info(f"Cleaning table: {table}")

            # Remove duplicate trips
            con.execute(f"""
                DELETE FROM {table}
                WHERE rowid NOT IN (
                    SELECT MIN(rowid)
                    FROM {table}
                    GROUP BY *
                )
            """)
            logger.info(f"Removed duplicates from {table}")

            # Remove trips with 0 passengers
            con.execute(f"DELETE FROM {table} WHERE passenger_count <= 0")

            # Remove trips with 0 miles
            con.execute(f"DELETE FROM {table} WHERE trip_distance <= 0")

            # Remove trips longer than 100 miles
            con.execute(f"DELETE FROM {table} WHERE trip_distance > 100")

            # Remove trips lasting more than 1 day
            # Identify correct datetime columns depending on table
            pickup_col = "tpep_pickup_datetime" if "yellow" in table else "lpep_pickup_datetime"
            dropoff_col = "tpep_dropoff_datetime" if "yellow" in table else "lpep_dropoff_datetime"

            con.execute(f"""
                DELETE FROM {table}
                WHERE EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col})) > 86400
            """)

            # -----------------------------------------------------------------
            # Verification queries
            # -----------------------------------------------------------------
            dup_count = con.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT *) FROM {table}
            """).fetchone()[0]
            zero_passenger_count = con.execute(f"SELECT COUNT(*) FROM {table} WHERE passenger_count <= 0").fetchone()[0]
            zero_miles_count = con.execute(f"SELECT COUNT(*) FROM {table} WHERE trip_distance <= 0").fetchone()[0]
            long_miles_count = con.execute(f"SELECT COUNT(*) FROM {table} WHERE trip_distance > 100").fetchone()[0]
            long_duration_count = con.execute(f"""
                SELECT COUNT(*) FROM {table} 
                WHERE EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col})) > 86400
            """).fetchone()[0]

            # Log verification
            logger.info(f"Post-cleaning verification for {table}:")
            logger.info(f"Duplicate trips remaining: {dup_count}")
            logger.info(f"Trips with 0 passengers: {zero_passenger_count}")
            logger.info(f"Trips with 0 miles: {zero_miles_count}")
            logger.info(f"Trips >100 miles: {long_miles_count}")
            logger.info(f"Trips >1 day duration: {long_duration_count}")

            # Print verification to console as well
            print(f"\n=== {table} Post-Cleaning Verification ===")
            print(f"Duplicate trips remaining: {dup_count}")
            print(f"Trips with 0 passengers: {zero_passenger_count}")
            print(f"Trips with 0 miles: {zero_miles_count}")
            print(f"Trips >100 miles: {long_miles_count}")
            print(f"Trips >1 day duration: {long_duration_count}")

        logger.info("Trip cleaning completed successfully!")

    except Exception as e:
        logger.error(f"An error occurred during cleaning: {e}")
        print(f"An error occurred: {e}")

    finally:
        if con:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    clean_trips()
