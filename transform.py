import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/transform.log'),
        logging.StreamHandler()
    ]
)

def setup_database():
    try:
        conn = duckdb.connect('taxi_data.duckdb', read_only=False)
        logging.info("Database connection established")
        return conn
    except Exception as e:
        logging.error(f"Database setup failed: {e}")
        raise

def transform_yellow_trips(conn):
    try:
        logging.info("Transforming Yellow taxi trips")
        
        # add calculated columns
        conn.execute("""
            ALTER TABLE yellow_trips 
            ADD COLUMN IF NOT EXISTS trip_co2_kgs DOUBLE,
            ADD COLUMN IF NOT EXISTS avg_mph DOUBLE,
            ADD COLUMN IF NOT EXISTS hour_of_day INTEGER,
            ADD COLUMN IF NOT EXISTS day_of_week INTEGER,
            ADD COLUMN IF NOT EXISTS week_of_year INTEGER,
            ADD COLUMN IF NOT EXISTS month_of_year INTEGER
        """)
        
        # calculate co2 emissions (trip_distance * co2_grams_per_mile / 1000)
        conn.execute("""
            UPDATE yellow_trips 
            SET trip_co2_kgs = (
                trip_distance * 
                (SELECT co2_grams_per_mile FROM vehicle_emissions WHERE vehicle_type = 'yellow_taxi')
            ) / 1000.0
        """)
        
        # calculate average mph
        conn.execute("""
            UPDATE yellow_trips 
            SET avg_mph = CASE 
                WHEN EXTRACT('epoch' FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 0 
                THEN trip_distance / (EXTRACT('epoch' FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0)
                ELSE 0 
            END
        """)
        
        # extract time components
        conn.execute("""
            UPDATE yellow_trips SET 
                hour_of_day = EXTRACT('hour' FROM tpep_pickup_datetime),
                day_of_week = EXTRACT('dow' FROM tpep_pickup_datetime),
                week_of_year = EXTRACT('week' FROM tpep_pickup_datetime),
                month_of_year = EXTRACT('month' FROM tpep_pickup_datetime)
        """)
        
        # verify transformations
        sample = conn.execute("""
            SELECT trip_distance, trip_co2_kgs, avg_mph, hour_of_day, day_of_week, week_of_year, month_of_year
            FROM yellow_trips 
            WHERE trip_co2_kgs IS NOT NULL 
            LIMIT 5
        """).fetchall()
        
        logging.info(f"Yellow trips transformation completed. Sample: {sample[0] if sample else 'No data'}")
        
    except Exception as e:
        logging.error(f"Failed to transform yellow trips: {e}")
        raise

def transform_green_trips(conn):
    try:
        logging.info("Transforming Green taxi trips")
        
        # add calculated columns
        conn.execute("""
            ALTER TABLE green_trips 
            ADD COLUMN IF NOT EXISTS trip_co2_kgs DOUBLE,
            ADD COLUMN IF NOT EXISTS avg_mph DOUBLE,
            ADD COLUMN IF NOT EXISTS hour_of_day INTEGER,
            ADD COLUMN IF NOT EXISTS day_of_week INTEGER,
            ADD COLUMN IF NOT EXISTS week_of_year INTEGER,
            ADD COLUMN IF NOT EXISTS month_of_year INTEGER
        """)
        
        # calculate co2 emissions
        conn.execute("""
            UPDATE green_trips 
            SET trip_co2_kgs = (
                trip_distance * 
                (SELECT co2_grams_per_mile FROM vehicle_emissions WHERE vehicle_type = 'green_taxi')
            ) / 1000.0
        """)
        
        # calculate average mph
        conn.execute("""
            UPDATE green_trips 
            SET avg_mph = CASE 
                WHEN EXTRACT('epoch' FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) > 0 
                THEN trip_distance / (EXTRACT('epoch' FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600.0)
                ELSE 0 
            END
        """)
        
        # extract time components
        conn.execute("""
            UPDATE green_trips SET 
                hour_of_day = EXTRACT('hour' FROM lpep_pickup_datetime),
                day_of_week = EXTRACT('dow' FROM lpep_pickup_datetime),
                week_of_year = EXTRACT('week' FROM lpep_pickup_datetime),
                month_of_year = EXTRACT('month' FROM lpep_pickup_datetime)
        """)
        
        # verify transformations
        sample = conn.execute("""
            SELECT trip_distance, trip_co2_kgs, avg_mph, hour_of_day, day_of_week, week_of_year, month_of_year
            FROM green_trips 
            WHERE trip_co2_kgs IS NOT NULL 
            LIMIT 5
        """).fetchall()
        
        logging.info(f"Green trips transformation completed. Sample: {sample[0] if sample else 'No data'}")
        
    except Exception as e:
        logging.error(f"Failed to transform green trips: {e}")
        raise

def verify_transformations(conn):
    try:
        logging.info("Verifying transformations")
        
        # check yellow trips
        yellow_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_trips,
                COUNT(trip_co2_kgs) as trips_with_co2,
                AVG(trip_co2_kgs) as avg_co2,
                MIN(hour_of_day) as min_hour,
                MAX(hour_of_day) as max_hour
            FROM yellow_trips
        """).fetchone()
        
        # check green trips
        green_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_trips,
                COUNT(trip_co2_kgs) as trips_with_co2,
                AVG(trip_co2_kgs) as avg_co2,
                MIN(hour_of_day) as min_hour,
                MAX(hour_of_day) as max_hour
            FROM green_trips
        """).fetchone()
        
        logging.info("TRANSFORMATION VERIFICATION:")
        logging.info(f"Yellow trips: {yellow_stats[0]:,} total, {yellow_stats[1]:,} with CO2")
        logging.info(f"Green trips: {green_stats[0]:,} total, {green_stats[1]:,} with CO2")
        logging.info(f"Average CO2 - Yellow: {yellow_stats[2]:.3f}kg, Green: {green_stats[2]:.3f}kg")
        
        print("Transformation Summary:")
        print(f"Yellow Trips: {yellow_stats[0]:,} transformed")
        print(f"Green Trips: {green_stats[0]:,} transformed") 
        print(f"Average CO2 per trip - Yellow: {yellow_stats[2]:.3f}kg")
        print(f"Average CO2 per trip - Green: {green_stats[2]:.3f}kg")
        
        return yellow_stats, green_stats
        
    except Exception as e:
        logging.error(f"Failed to verify transformations: {e}")
        raise

def main():
    try:
        logging.info("Starting data transformation process")
        
        # setup database
        conn = setup_database()
        
        # transform data
        transform_yellow_trips(conn)
        transform_green_trips(conn)
        
        # verify transformations
        yellow_stats, green_stats = verify_transformations(conn)
        
        conn.close()
        logging.info("Data transformation completed successfully")
        
    except Exception as e:
        logging.error(f"Data transformation failed: {e}")
        raise

if __name__ == "__main__":
    main()
