"""
Goals of this File:
- Report on:
  1) Single largest CO2 trip (yellow/green).
  2) Most/least carbon-heavy hours.
  3) Most/least carbon-heavy days of week.
  4) Most/least carbon-heavy weeks.
  5) Most/least carbon-heavy months.
- Render time-series plot with the month on the X-axis and CO2 totals on the Y-axis for yellow and green.
- Save plot in PNG format and print output labels.
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import logging
import os

os.makedirs('output', exist_ok=True)
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/analysis.log'),
        logging.StreamHandler()
    ]
)

def analyze_largest_carbon_trips(con):
    try:

        for taxi_color in ['yellow', 'green']:
            logger.info(f"Analyzing the largest carbon trips for {taxi_color}")

            #largest trip in general
            result = con.execute(f"""
                SELECT pickup_datetime, trip_co2_kgs FROM transform_trips WHERE taxi_type = '{taxi_color}'
                ORDER BY trip_co2_kgs 
                DESC LIMIT 1;
                """).fetchall()
            print(f"The largest CO2 trip is {result}")
            logger.info(f"The largest total CO2 trip for {taxi_color} taxi is {result}")

            #by hour
            heaviest_hour = con.execute(f"""
                SELECT trip_hour FROM transform_trips WHERE taxi_type = '{taxi_color}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1;""").fetchone()[0]
            print(f"The most carbon heavy hour for {taxi_color} taxis is {heaviest_hour}")
            logger.info(f"The most carbon heavy hour for {taxi_color} taxis is {heaviest_hour}")

            lightest_hour = con.execute(f"""
                SELECT trip_hour FROM transform_trips WHERE taxi_type = '{taxi_color}' 
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) ASC LIMIT 1;""").fetchone()[0]
            print(f"The least carbon light hour for {taxi_color} taxis is {lightest_hour}")
            logger.info(f"The least carbon light hour for {taxi_color} taxis is {lightest_hour}")

            #by day
            heaviest_day = con.execute(f"""
                SELECT trip_day_of_week FROM transform_trips WHERE taxi_type = '{taxi_color}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1;""").fetchone()[0]
            print(f"The most carbon heavy day for {taxi_color} taxis is {heaviest_day}")
            logger.info(f"The most carbon heavy day for {taxi_color} taxis is {heaviest_day}")

            lightest_day = con.execute(f"""
                SELECT trip_day_of_week FROM transform_trips WHERE taxi_type = '{taxi_color}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) ASC LIMIT 1;""").fetchone()[0]
            print(f"The least carbon light day for {taxi_color} taxis is {lightest_day}")
            logger.info(f"The least carbon light day for {taxi_color} taxis is {lightest_day}")

            #by week
            heaviest_week = con.execute(f"""
                SELECT trip_week_number FROM transform_trips WHERE taxi_type = '{taxi_color}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1;""").fetchone()[0]
            print(f"The most carbon heavy week for {taxi_color} taxis is {heaviest_week}")
            logger.info(f"The least carbon heavy week for {taxi_color} taxis is {heaviest_week}")

            lightest_week = con.execute(f"""
                SELECT trip_week_number FROM transform_trips WHERE taxi_type = '{taxi_color}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) ASC LIMIT 1;""").fetchone()[0]
            print(f"The least carbon light week for {taxi_color} taxis is {lightest_week}")
            logger.info(f"The least carbon light week for {taxi_color} taxis is {lighest_week}")   

            #by month of the year:
            heaviest_month = con.execute(f"""
                SELECT trip_month FROM transform_trips WHERE taxi_type = '{taxi_color}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1;""").fetchone()[0]
            print(f"The most carbon heavy month for {taxi_color} taxis is {heaviest_month}")
            logger.info(f"The most carbon heavy month for {taxi_color} taxis is {heaviest_month}")

            lightest_month = con.execute(f"""
                SELECT trip_month FROM transform_trips WHERE taxi_type = '{taxi_color}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) ASC LIMIT 1;""").fetchone()[0]
            print(f"The least carbon light month for {taxi_color} taxis is {lightest_month}")
            logger.info(f"The least carbon light month for {taxi_color} taxis is {lightest_month}")

        plot_df = con.execute(f"""
            SELECT
                EXTRACT(year FROM pickup_datetime) AS year,
                trip_month,
                taxi_type,
                SUM(trip_co2_kgs) AS total_co2
                    FROM transform_trips
                        WHERE EXTRACT(year FROM pickup_datetime) >= 2015
                              GROUP BY year, trip_month, taxi_type
                              ORDER BY year, trip_month, taxi_type;
            """).fetchdf()
            
        #trip_month vs co2 emissions
        plot_df['date'] = pd.to_datetime(plot_df['year'].astype(str) + '-' + plot_df['trip_month'].astype(str).str.zfill(2) + '-01')
        pivot_df = plot_df.pivot_table(index='date', columns='taxi_type', values='total_co2', fill_value=0).sort_index()

        plt.style.use('ggplot')
        fig, ax = plt.subplots(figsize=(16, 8))
        bar_width = 15
        taxi_colors = {'yellow': '#F9DC5C', 'green': '#3BAF75'}
        bar_offsets = {'yellow': -bar_width, 'green': bar_width}
        for taxi in ['yellow', 'green']:
            ax.bar(
                pivot_df.index + pd.Timedelta(days=bar_offsets[taxi]),
                pivot_df[taxi],
                width=bar_width,
                label=f"{taxi.title()} Taxi",
                color=taxi_colors[taxi],
                alpha=0.9,
                edgecolor='black'
            )

        ax.set_title('NYC Taxi Monthly CO₂ Emissions (2015-2024)', fontsize=18, pad=20)
        ax.set_xlabel('Month')
        ax.set_ylabel('CO₂ Emissions (kg)')
        ax.xaxis.set_major_locator(ticker.MultipleLocator(180))
        fig.autofmt_xdate(rotation=50)
        ax.legend(loc='upper left')
        ax.grid(visible=True, which='major', linestyle='-.', linewidth=1)
        fig.tight_layout(rect=[0, 0, 1, 0.95])

        plot_filename = 'output/taxi_co2_emissions_2015to2024.png'
        plt.savefig(plot_filename)
        print(f"\nPlot has been successfully saved to {plot_filename}")
        logger.info(f"Plot has been successfully saved to {plot_filename}")

    except Exception as e:
        print(f"There was an error during analysis: {e}")
        logger.error(f"There was an error during analysis: {e}") 

    finally:
        if con:
            con.close()
            logger.info("Database connection closed")

def main():
    try:
        con = duckdb.connect('taxi_data.duckdb', read_only=True)
        logging.info("Database connection established")
        analyze_largest_carbon_trips(con)
    except Exception as e:
        print(f"Database error: {e}")
        logging.error(f"Database error: {e}")

if __name__ == "__main__":
    main()
