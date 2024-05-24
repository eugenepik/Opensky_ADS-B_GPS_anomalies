import pandas as pd
from datetime import datetime, timedelta
from trino.dbapi import connect
from trino.auth import OAuth2Authentication
import logging
from geopy.distance import geodesic

# Setup logging
logging.basicConfig(filename='query_execution.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Initialize variables
start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 1, 1)
delta = timedelta(days=1)

def connect_to_database():
    """Connects to the Trino database using OAuth2 Authentication and returns the cursor and connection if successful."""
    try:
        conn = connect(
            host="trino.opensky-network.org",
            port=443,
            user="myUserID",
            auth=OAuth2Authentication(),
            http_scheme="https",
            catalog="minio",
            schema="osky"
        )
        return conn.cursor(), conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {str(e)}")
        raise

def calculate_distance(row):
    """Calculates the geodesic distance in meters between two latitude and longitude points."""
    return geodesic((row['previous_latitude'], row['previous_longitude']), (row['next_latitude'], row['next_longitude'])).meters

def main():
    try:
        cursor, conn = connect_to_database()  # Establish database connection
        logging.info("Database connection established successfully.")
    except Exception as e:
        logging.error(f"Could not establish database connection: {str(e)}")
        return  # Terminate if connection is not established

    # Prepare a named prepared statement 'flight_analysis' for later execution. 
    # This preparation optimizes execution by allowing the database to parse, optimize, and compile the SQL once and run it multiple times with different parameters.
    prepared_statement = """ """  # prepared statements 'flight_analysis.sql' and 'spoofing_analysis.sql' are included as separate files in this repository
  
    try:
        cursor.execute(prepared_statement)
        logging.info("SQL statement prepared and executed successfully.")
    except Exception as e:
        logging.error(f"Failed to prepare the SQL statement: {str(e)}")
        cursor.close()
        conn.close()
        return  # Stop execution if preparation fails

    current_date = start_date
    while current_date < end_date:
        next_date = current_date + delta
        # Convert dates to UNIX timestamps
        current_unix_time = int(current_date.timestamp())
        next_unix_time = int(next_date.timestamp())
        
        # Correctly formulating the query execution with parameters
        execute_statement = f'''
            EXECUTE flight_analysis USING {current_unix_time}, {next_unix_time}
        '''
        # Try executing the prepared SQL statement with the current time range
        try:
            cursor.execute(execute_statement)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)

            # Calculate distance and rearrange columns
            df['between_coords_distance_m'] = df.apply(calculate_distance, axis=1)
            df = df[['icao24', 'callsign', 'null_start_time', 'null_end_time', 'time_of_previous_not_null_coords', 'time_of_next_not_null_coords', 'previous_latitude', 'previous_longitude', 'next_latitude', 'next_longitude', 'between_coords_distance_m', 'null_duration_seconds', 'between_coords_duration_seconds', 'avg_nic', 'min_nic', 'max_nic']]

            # File output with dynamic naming based on the date range processed
            general_file_name = f"{current_unix_time}-{next_unix_time}_{current_date.strftime('%Y-%m-%d')}_to_{next_date.strftime('%Y-%m-%d')}.xlsx"
            with pd.ExcelWriter(general_file_name, mode='w') as writer:
                df.to_excel(writer, index=False)
            logging.info(f"Data written to {general_file_name}")
            print(f"Data written to {general_file_name}")

        except Exception as e:
            logging.error(f"Failed during SQL execution or file operation: {str(e)}")

        # Move to the next date range
        current_date = next_date

    # Clean up by closing cursor and connection
    try:
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Failed to close database connection: {str(e)}")

if __name__ == "__main__":
    main()
