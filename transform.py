"""
Goals of this File:
- Add derived columns for CO2 (kg), average mph, and time dimensions (hour, day, week, month).
- Use runtime lookup from vehicle_emissions for co2_grams_per_mile (not hard-coded) to compute per-trip CO2.
"""

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

# N/A -- DBT Method in separate subdirectory
