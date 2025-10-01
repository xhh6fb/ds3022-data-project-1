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

def setup_database():
    try:
        conn = duckdb.connect(database='taxi_data.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def clean_yellow_trips(conn):

    try:
        logger.info("Cleaning Yellow taxi trips")

        # remove duplicates
        conn.execute("""
            CREATE TABLE yellow_trips_temp AS 
            SELECT DISTINCT * FROM yellow_trips
        """)
        conn.execute("DROP TABLE yellow_trips")
        conn.execute("ALTER TABLE yellow_trips_temp RENAME TO yellow_trips")

        # remove trips with 0 passengers
        before_passengers = conn.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
        conn.execute("DELETE FROM yellow_trips WHERE passenger_count = 0 OR passenger_count IS NULL")
        after_passengers = conn.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
        logger.info(f"Removed {before_passengers - after_passengers:,} trips with 0 passengers")

        # remove trips with 0 miles
        before_distance = after_passengers
        conn.execute("DELETE FROM yellow_trips WHERE trip_distance = 0 OR trip_distance IS NULL")
        after_distance = conn.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
        logger.info(f"Removed {before_distance - after_distance:,} trips with 0 miles")

        # remove trips > 100 miles
        before_long = after_distance
        conn.execute("DELETE FROM yellow_trips WHERE trip_distance > 100")
        after_long = conn.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
        logger.info(f"Removed {before_long - after_long:,} trips > 100 miles")

        # remove trips > 24 hours or invalid duration
        before_duration = after_long
        conn.execute("""
            DELETE FROM yellow_trips 
            WHERE EXTRACT('epoch' FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 86400
               OR EXTRACT('epoch' FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) <= 0
        """)
        after_duration = conn.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
        logger.info(f"Removed {before_duration - after_duration:,} trips > 24 hours or invalid duration")

        final_count = after_duration
        logger.info(f"Yellow trips cleaning completed. Final count: {final_count:,}")
        return final_count

    except Exception as e:
        logger.error(f"Failed to clean yellow trips: {e}")
        raise

def clean_green_trips(conn):

    try:
        logger.info("Cleaning Green taxi trips")

        # remove duplicates
        conn.execute("""
            CREATE TABLE green_trips_temp AS 
            SELECT DISTINCT * FROM green_trips
        """)
        conn.execute("DROP TABLE green_trips")
        conn.execute("ALTER TABLE green_trips_temp RENAME TO green_trips")

        # remove trips with 0 passengers
        before_passengers = conn.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]
        conn.execute("DELETE FROM green_trips WHERE passenger_count = 0 OR passenger_count IS NULL")
        after_passengers = conn.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]
        logger.info(f"Removed {before_passengers - after_passengers:,} trips with 0 passengers")

        # remove trips with 0 miles
        before_distance = after_passengers
        conn.execute("DELETE FROM green_trips WHERE trip_distance = 0 OR trip_distance IS NULL")
        after_distance = conn.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]
        logger.info(f"Removed {before_distance - after_distance:,} trips with 0 miles")

        # remove trips > 100 miles
        before_long = after_distance
        conn.execute("DELETE FROM green_trips WHERE trip_distance > 100")
        after_long = conn.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]
        logger.info(f"Removed {before_long - after_long:,} trips > 100 miles")

        # Remove trips > 24 hours or invalid duration
        before_duration = after_long
        conn.execute("""
            DELETE FROM green_trips 
            WHERE EXTRACT('epoch' FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) > 86400
               OR EXTRACT('epoch' FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) <= 0
        """)
        after_duration = conn.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]
        logger.info(f"Removed {before_duration - after_duration:,} trips > 24 hours or invalid duration")

        final_count = after_duration
        logger.info(f"Green trips cleaning completed. Final count: {final_count:,}")
        return final_count

    except Exception as e:
        logger.error(f"Failed to clean green trips: {e}")
        raise

def verify_cleaning(conn):
    
    try:
        logger.info("Verifying cleaning results...")

        yellow_issues = conn.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE passenger_count = 0) as zero_passengers,
                COUNT(*) FILTER (WHERE trip_distance = 0) as zero_distance,
                COUNT(*) FILTER (WHERE trip_distance > 100) as long_distance,
                COUNT(*) FILTER (WHERE EXTRACT('epoch' FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 86400) as long_duration
            FROM yellow_trips
        """).fetchone()

        green_issues = conn.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE passenger_count = 0) as zero_passengers,
                COUNT(*) FILTER (WHERE trip_distance = 0) as zero_distance,
                COUNT(*) FILTER (WHERE trip_distance > 100) as long_distance,
                COUNT(*) FILTER (WHERE EXTRACT('epoch' FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) > 86400) as long_duration
            FROM green_trips
        """).fetchone()

        logger.info("VERIFICATION RESULTS:")
        logger.info(f"Yellow trips with issues: {sum(yellow_issues)}")
        logger.info(f"Green trips with issues: {sum(green_issues)}")

        if sum(yellow_issues) == 0 and sum(green_issues) == 0:
            logger.info("All cleaning conditions verified - no issues remain")
        else:
            logger.warning("Some issues may still exist")

        print("Verification Results:")
        print(f"Yellow issues: {yellow_issues}")
        print(f"Green issues: {green_issues}")

        return yellow_issues, green_issues

    except Exception as e:
        logger.error(f"Failed to verify cleaning: {e}")
        raise

def main():
    try:
        logger.info("Starting data cleaning process")

        conn = setup_database()

        # clean tables
        final_yellow = clean_yellow_trips(conn)
        final_green = clean_green_trips(conn)

        # verify cleaning
        verify_cleaning(conn)

        logger.info("Data cleaning completed successfully!")
        print(f"Final Counts -> Yellow: {final_yellow:,}, Green: {final_green:,}")

        conn.close()
        logger.info("Closed DuckDB connection")

    except Exception as e:
        logger.error(f"Data cleaning failed: {e}")
        print(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()
