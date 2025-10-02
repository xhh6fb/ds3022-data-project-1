"""
Goals of this File:
- Remove duplicates.
- Remove trips with 0 passengers.
- Remove 0-mile trips.
- Remove trips longer than 100 miles.
- Remove trips lasting > 24 hours or <= 0 duration.
- Verify that all invalid conditions have been removed.
"""

import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log',
    filemode='a'
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

def clean_trips(con):

    try:
        logger.info("Cleaning taxi trips")
        trips = ['yellow_trips', 'green_trips']

        for trip in trips:
            logging.info(f"Cleaning for {trip}")

            # remove duplicates
            con.execute(f"""
                CREATE OR REPLACE TABLE {trip} AS
                SELECT DISTINCT * FROM {trip};
            """)
            logging.info(f"Duplicates have been removed for {trip}")

            # remove trips with 0 passengers
            before_passengers = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
            con.execute(f"""
                DELETE FROM {trip} WHERE passenger_count = 0;
            """)
            after_passengers = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
            logger.info(f"Removed {before_passengers - after_passengers:,} trips with 0 passengers")

            # remove trips with 0 or negative miles
            before_distance = after_passengers
            con.execute(f"""
                DELETE FROM {trip} WHERE trip_distance <= 0;
            """)
            after_distance = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
            logger.info(f"Removed {before_distance - after_distance:,} trips with 0 miles")

            # remove trips > 100 miles
            before_long = after_distance
            con.execute(f"""
                DELETE FROM {trip} WHERE trip_distance > 100;
            """)
            after_long = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
            logger.info(f"Removed {before_long - after_long:,} trips > 100 miles")

            # remove trips > 24 hours or invalid duration
            before_duration = after_long
            con.execute(f"""
            DELETE FROM {trip}
            WHERE EXTRACT('epoch' FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 86400
               OR EXTRACT('epoch' FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) <= 0;
            """)
            after_duration = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
            logger.info(f"Removed {before_duration - after_duration:,} trips > 24 hours or invalid duration")

            final_count = after_duration
            logger.info(f"{trip} cleaning completed. Final count: {final_count:,}")
            return final_count

            logger.info("Verifying cleaning results")

            zero_passenger = con.execute(f"SELECT COUNT(*) FROM {table} WHERE passenger_count = 0;").fetchone()[0]
            print(f"For {table}, trips with 0 passengers: {zero_passenger}")
            logging.info(f"For {table}, trips with 0 passengers: {zero_passenger}")

            zero_distance = con.execute(f"SELECT COUNT(*) FROM {table} WHERE trip_distance <= 0;").fetchone()[0]
            print(f"For {table}, trips with 0 or negative distance: {zero_distance}")
            logging.info(f"For {table}, trips with 0 or negative distance: {zero_distance}")

            long_distance = con.execute(f"SELECT COUNT (*) FROM {table} WHERE trip_distance > 100;").fetchone()[0]
            print(f"For {table}, trips with distance over 100 miles: {long_distance}")
            logging.info(f"For {table}, trips with distance over 100 miles: {long_distance}")

            long_duration = con.execute(f"SELECT COUNT(*) FROM {table} WHERE (julian(dropoff_datetime) - julian(pickup_datetime)) * 86400 > 86400;").fetchone()[0]
            print(f"For {table}, trips lasting more than 1 day: {long_duration}")
            logging.info(f"For {table}, trips lasting more than 1 day: {long_duration}")

            logging.info(f" VERIFICATION TESTS COMPLETED for {table} table -- Cleaning COMPLETED --")

    except Exception as e:
        logger.error(f"Failed to clean {trip}: {e}")
        raise

def verify_cleaning(conn):

    try:
        logger.info("Verifying cleaning results")

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

        con = setup_database()

        clean_trips(con)
        verify_cleaning(con)

        con.close()
        logger.info("Closed DuckDB connection")

    except Exception as e:
        logger.error(f"Data cleaning failed: {e}")
        print(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()
