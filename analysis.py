import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/analysis.log'),
        logging.StreamHandler()
    ]
)

def setup_database():
    try:
        conn = duckdb.connect('taxi_data.duckdb', read_only=True)
        logging.info("Database connection established")
        return conn
    except Exception as e:
        logging.error(f"Database setup failed: {e}")
        raise

def analyze_largest_carbon_trips(conn):
    try:
        logging.info("Analyzing largest carbon producing trips")
        
        # yellow taxi largest trip
        yellow_max = conn.execute("""
            SELECT trip_distance, trip_co2_kgs, tpep_pickup_datetime, tpep_dropoff_datetime
            FROM yellow_trips 
            WHERE trip_co2_kgs = (SELECT MAX(trip_co2_kgs) FROM yellow_trips)
            LIMIT 1
        """).fetchone()
        
        # green taxi largest trip
        green_max = conn.execute("""
            SELECT trip_distance, trip_co2_kgs, lpep_pickup_datetime, lpep_dropoff_datetime
            FROM green_trips 
            WHERE trip_co2_kgs = (SELECT MAX(trip_co2_kgs) FROM green_trips)
            LIMIT 1
        """).fetchone()
        
        logging.info(f"LARGEST CARBON TRIPS:")
        logging.info(f"Yellow: {yellow_max[1]:.3f}kg CO2 ({yellow_max[0]:.1f} miles)")
        logging.info(f"Green: {green_max[1]:.3f}kg CO2 ({green_max[0]:.1f} miles)")
        
        print("Largest Carbon Producing Trips of 2024:")
        print(f"Yellow Taxi: {yellow_max[1]:.3f} kg CO2 ({yellow_max[0]:.1f} miles)")
        print(f"Green Taxi: {green_max[1]:.3f} kg CO2 ({green_max[0]:.1f} miles)")
        
        return yellow_max, green_max
        
    except Exception as e:
        logging.error(f"Failed to analyze largest trips: {e}")
        raise

def analyze_by_hour(conn):
    try:
        logging.info("Analyzing emissions by hour of day")
        
        # yellow taxi by hour
        yellow_hours = conn.execute("""
            SELECT hour_of_day, AVG(trip_co2_kgs) as avg_co2
            FROM yellow_trips 
            WHERE trip_co2_kgs IS NOT NULL
            GROUP BY hour_of_day
            ORDER BY hour_of_day
        """).fetchall()
        
        # green taxi by hour
        green_hours = conn.execute("""
            SELECT hour_of_day, AVG(trip_co2_kgs) as avg_co2
            FROM green_trips 
            WHERE trip_co2_kgs IS NOT NULL
            GROUP BY hour_of_day
            ORDER BY hour_of_day
        """).fetchall()
        
        # finding the heaviest and lightest hours
        yellow_heaviest = max(yellow_hours, key=lambda x: x[1])
        yellow_lightest = min(yellow_hours, key=lambda x: x[1])
        green_heaviest = max(green_hours, key=lambda x: x[1])
        green_lightest = min(green_hours, key=lambda x: x[1])
        
        logging.info(f"EMISSIONS BY HOUR:")
        logging.info(f"Yellow - Heaviest: Hour {yellow_heaviest[0]} ({yellow_heaviest[1]:.3f}kg)")
        logging.info(f"Yellow - Lightest: Hour {yellow_lightest[0]} ({yellow_lightest[1]:.3f}kg)")
        logging.info(f"Green - Heaviest: Hour {green_heaviest[0]} ({green_heaviest[1]:.3f}kg)")
        logging.info(f"Green - Lightest: Hour {green_lightest[0]} ({green_lightest[1]:.3f}kg)")
        
        print("Carbon Emissions by Hour of Day:")
        print(f"Yellow - Heaviest: Hour {yellow_heaviest[0]} ({yellow_heaviest[1]:.3f}kg avg)")
        print(f"Yellow - Lightest: Hour {yellow_lightest[0]} ({yellow_lightest[1]:.3f}kg avg)")
        print(f"Green - Heaviest: Hour {green_heaviest[0]} ({green_heaviest[1]:.3f}kg avg)")
        print(f"Green - Lightest: Hour {green_lightest[0]} ({green_lightest[1]:.3f}kg avg)")
        
        return yellow_hours, green_hours
        
    except Exception as e:
        logging.error(f"Failed to analyze by hour: {e}")
        raise

def analyze_by_day_of_week(conn):
    try:
        logging.info("Analyzing emissions by day of week")
        
        # yellow taxi by day of week (key: 0 is sunday, 6 is saturday)
        yellow_days = conn.execute("""
            SELECT day_of_week, AVG(trip_co2_kgs) as avg_co2
            FROM yellow_trips 
            WHERE trip_co2_kgs IS NOT NULL
            GROUP BY day_of_week
            ORDER BY day_of_week
        """).fetchall()
        
        # green taxi by day of week
        green_days = conn.execute("""
            SELECT day_of_week, AVG(trip_co2_kgs) as avg_co2
            FROM green_trips 
            WHERE trip_co2_kgs IS NOT NULL
            GROUP BY day_of_week
            ORDER BY day_of_week
        """).fetchall()
        
        day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
        
        yellow_heaviest = max(yellow_days, key=lambda x: x[1])
        yellow_lightest = min(yellow_days, key=lambda x: x[1])
        green_heaviest = max(green_days, key=lambda x: x[1])
        green_lightest = min(green_days, key=lambda x: x[1])
        
        logging.info(f"EMISSIONS BY DAY OF WEEK:")
        logging.info(f"Yellow - Heaviest: {day_names[yellow_heaviest[0]]} ({yellow_heaviest[1]:.3f}kg)")
        logging.info(f"Yellow - Lightest: {day_names[yellow_lightest[0]]} ({yellow_lightest[1]:.3f}kg)")
        
        print("Carbon Emissions by Day of Week:")
        print(f"Yellow - Heaviest: {day_names[yellow_heaviest[0]]} ({yellow_heaviest[1]:.3f}kg avg)")
        print(f"Yellow - Lightest: {day_names[yellow_lightest[0]]} ({yellow_lightest[1]:.3f}kg avg)")
        print(f"Green - Heaviest: {day_names[green_heaviest[0]]} ({green_heaviest[1]:.3f}kg avg)")
        print(f"Green - Lightest: {day_names[green_lightest[0]]} ({green_lightest[1]:.3f}kg avg)")
        
        return yellow_days, green_days
        
    except Exception as e:
        logging.error(f"Failed to analyze by day of week: {e}")
        raise

def analyze_by_week(conn):
    try:
        logging.info("Analyzing emissions by week of year")
        
        # yellow taxi by week
        yellow_weeks = conn.execute("""
            SELECT week_of_year, AVG(trip_co2_kgs) as avg_co2
            FROM yellow_trips 
            WHERE trip_co2_kgs IS NOT NULL
            GROUP BY week_of_year
            ORDER BY week_of_year
        """).fetchall()
        
        # green taxi by week
        green_weeks = conn.execute("""
            SELECT week_of_year, AVG(trip_co2_kgs) as avg_co2
            FROM green_trips 
            WHERE trip_co2_kgs IS NOT NULL
            GROUP BY week_of_year
            ORDER BY week_of_year
        """).fetchall()
        
        yellow_heaviest = max(yellow_weeks, key=lambda x: x[1])
        yellow_lightest = min(yellow_weeks, key=lambda x: x[1])
        green_heaviest = max(green_weeks, key=lambda x: x[1])
        green_lightest = min(green_weeks, key=lambda x: x[1])
        
        logging.info(f"EMISSIONS BY WEEK:")
        logging.info(f"Yellow - Heaviest: Week {yellow_heaviest[0]} ({yellow_heaviest[1]:.3f}kg)")
        logging.info(f"Yellow - Lightest: Week {yellow_lightest[0]} ({yellow_lightest[1]:.3f}kg)")
        
        print("Carbon Emissions by Week of Year:")
        print(f"Yellow - Heaviest: Week {yellow_heaviest[0]} ({yellow_heaviest[1]:.3f}kg avg)")
        print(f"Yellow - Lightest: Week {yellow_lightest[0]} ({yellow_lightest[1]:.3f}kg avg)")
        print(f"Green - Heaviest: Week {green_heaviest[0]} ({green_heaviest[1]:.3f}kg avg)")
        print(f"Green - Lightest: Week {green_lightest[0]} ({green_lightest[1]:.3f}kg avg)")
        
        return yellow_weeks, green_weeks
        
    except Exception as e:
        logging.error(f"Failed to analyze by week: {e}")
        raise

def analyze_by_month(conn):
    try:
        logging.info("Analyzing emissions by month")
        
        # yellow taxi by month
        yellow_months = conn.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs) as total_co2, AVG(trip_co2_kgs) as avg_co2
            FROM yellow_trips 
            WHERE trip_co2_kgs IS NOT NULL
            GROUP BY month_of_year
            ORDER BY month_of_year
        """).fetchall()
        
        # green taxi by month
        green_months = conn.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs) as total_co2, AVG(trip_co2_kgs) as avg_co2
            FROM green_trips 
            WHERE trip_co2_kgs IS NOT NULL
            GROUP BY month_of_year
            ORDER BY month_of_year
        """).fetchall()
        
        month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        
        yellow_heaviest = max(yellow_months, key=lambda x: x[2])
        yellow_lightest = min(yellow_months, key=lambda x: x[2])
        green_heaviest = max(green_months, key=lambda x: x[2])
        green_lightest = min(green_months, key=lambda x: x[2])
        
        logging.info(f"EMISSIONS BY MONTH:")
        logging.info(f"Yellow - Heaviest: {month_names[yellow_heaviest[0]]} ({yellow_heaviest[2]:.3f}kg avg)")
        logging.info(f"Yellow - Lightest: {month_names[yellow_lightest[0]]} ({yellow_lightest[2]:.3f}kg avg)")
        
        print("Carbon Emissions by Month:")
        print(f"Yellow - Heaviest: {month_names[yellow_heaviest[0]]} ({yellow_heaviest[2]:.3f}kg avg)")
        print(f"Yellow - Lightest: {month_names[yellow_lightest[0]]} ({yellow_lightest[2]:.3f}kg avg)")
        print(f"Green - Heaviest: {month_names[green_heaviest[0]]} ({green_heaviest[2]:.3f}kg avg)")
        print(f"Green - Lightest: {month_names[green_lightest[0]]} ({green_lightest[2]:.3f}kg avg)")
        
        return yellow_months, green_months
        
    except Exception as e:
        logging.error(f"Failed to analyze by month: {e}")
        raise

def create_monthly_co2_plot(yellow_months, green_months):
    try:
        logging.info("Creating monthly CO2 emissions plot")
        
        months = [x[0] for x in yellow_months]
        yellow_totals = [x[1] for x in yellow_months]
        green_totals = [x[1] for x in green_months]
        
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        plt.figure(figsize=(14, 8))
        plt.plot(months, yellow_totals, marker='o', linewidth=3, markersize=8, 
                label='Yellow Taxi', color='#FFD700', alpha=0.8)
        plt.plot(months, green_totals, marker='s', linewidth=3, markersize=8, 
                label='Green Taxi', color='#228B22', alpha=0.8)
        
        plt.title('NYC Taxi CO2 Emissions by Month (2024)', fontsize=16, fontweight='bold', pad=20)
        plt.xlabel('Month', fontsize=12, fontweight='bold')
        plt.ylabel('Total CO2 Emissions (kg)', fontsize=12, fontweight='bold')
        plt.xticks(months, month_names, rotation=45)
        plt.legend(fontsize=12, loc='upper right')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        ax = plt.gca()
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1000:.0f}K'))
        
        # saving the plot
        plt.savefig('co2_emissions_by_month.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        logging.info("Monthly CO2 plot saved as 'co2_emissions_by_month.png'")
        
        return True
        
    except Exception as e:
        logging.error(f"Failed to create plot: {e}")
        raise

def main():
    try:
        logging.info("Starting data analysis process")
        
        conn = setup_database()
        
        print("NYC TAXI CO2 EMISSIONS ANALYSIS FOR 2024")
        print("\n")
        
        analyze_largest_carbon_trips(conn)
        print()
        analyze_by_hour(conn)
        print()
        analyze_by_day_of_week(conn)
        print()
        analyze_by_week(conn)
        print()
        yellow_months, green_months = analyze_by_month(conn)
        print()
        
        create_monthly_co2_plot(yellow_months, green_months)
        
        conn.close()
        logging.info("Data analysis completed successfully")
        print("Analysis complete. Check 'co2_emissions_by_month.png' for the visualization.")
        
    except Exception as e:
        logging.error(f"Data analysis failed: {e}")
        raise

if __name__ == "__main__":
    main()
