# ADS-B GPS Anomalies Analysis using OpenSky Trino Database

This repository contains SQL scripts and corresponding Python code for analyzing ADS-B messages stored in the OpenSky Trino database. The analysis focuses on identifying gaps in the aircraft route positional data (possible GPS jamming) and GPS coordinate jumps from a route (potential GPS spoofing) incidents for airborne aircraft.

![ezgif com-optimize](https://github.com/eugenepik/Opensky_ADS-B_GPS_anomalies/assets/32197349/e18125fa-1ef3-4963-8f09-7203a64bb308)

## Part 1 - GPS Anomalies: Gaps in Aircraft Routes, Possible Jamming Incidents

This analysis identifies gaps in aircraft routes where either latitude or longitude information is missing, indicating possible data transmission issues in the ADS-B system. The focus is on gaps lasting at least 60 seconds. The quality of navigational data is checked using Navigation Integrity Category (NIC) values, ensuring they are between 0 and 11 or NULL. The accuracy of position data is verified by confirming that the time difference between the recorded time in the ADS-B message and the last position update is no more than 2 seconds. The `icao24` aircraft identifier is checked to ensure it is a valid hexadecimal number to eliminate any malformed ADS-B messages.

### SQL Script: `flight_analysis`

The SQL script initializes a named prepared statement called `flight_analysis` for optimal repeated executions. This script structures its query using multiple Common Table Expressions (CTEs) to perform detailed analysis of aircraft tracking data.

#### Key Components:

1. **Data Source and Initial Filtering (FilteredData CTE)**
   - Filters data from the `state_vectors_data4` dataset to include only airborne aircraft during specific hours.
   - Ensures data integrity by selecting valid `icao24` aircraft identifiers formatted in a hexadecimal pattern.

2. **Temporal Analysis Using Window Functions**
   - Uses window functions (`LAG` and `LEAD`) to fetch adjacent latitude, longitude, and timestamp data for each aircraft.
   - Identifies data continuity or the onset of data gaps.

3. **Detection and Sessionization of Data Gaps (Sessionized CTE)**
   - Detects transitions from valid to null geographic coordinates, marking the start of potential data gaps.

4. **Validation of Geographic Data Points (ValidPrevious and ValidNext CTEs)**
   - Validates geographic data points based on their proximity to the last known position updates within a 2-second window.

5. **Aggregation of Null Data Sessions (CollapsedNulls CTE)**
   - Aggregates contiguous null data records into sessions, capturing valid geographic coordinates immediately before and after the gaps.
   - Filters out gaps shorter than 60 seconds.

6. **Data Quality Assessment (NICData CTE)**
   - Extracts NIC values during identified null data periods, assessing data quality through average, minimum, and maximum NIC values.

7. **Extraction and Validation of Coordinates (ValidCoords CTE)**
   - Calculates the duration of null periods and the time intervals between valid data points.
   - Provides a comprehensive view of each data gap paired with NIC values.

8. **Final Selection and Output**
   - Outputs detailed information about each identified data gap, including validated coordinates, durations, and NIC values.
   - Results are sorted by aircraft identifier and the start time of the null period.

### Execution

The prepared statement `flight_analysis` is optimized for execution with different parameters using the `EXECUTE` command.

## Part 2 - GPS Anomalies: Jumps from the Route, Possible Spoofing Incidents

This analysis identifies potential GPS spoofing incidents by filtering ADS-B messages for unusually large distances traveled between consecutive data points relative to the time elapsed. The analysis assumes a maximum plausible speed of 600 m/s for aircraft to calculate expected movement and flags any discrepancies that suggest positional anomalies potentially caused by GPS spoofing. The script focuses on airborne aircraft, ensuring data precision and relevance by confirming that the GPS coordinates recorded in the ADS-B messages are up-to-date and the aircraft identifiers are valid.

### SQL Script: `spoofing_analysis`

The SQL script `spoofing_analysis` is structured using multiple Common Table Expressions (CTEs) to organize the logic and enhance readability.

#### Key Components:

1. **Data Source and Initial Filtering (FilteredData CTE)**
   - Extracts ADS-B data from the `state_vectors_data4` table, focusing on records within a specific hour range and ensuring the aircraft is airborne.
   - Filters messages based on several criteria including valid hexadecimal ICAO 24-bit addresses and properly formatted latitude and longitude data.

2. **Distance and Time Calculations (DistanceCalculations CTE)**
   - Utilizes Trino's geospatial functions to calculate the spherical distance between consecutive positions logged in the ADS-B messages.
   - Computes the time differences between these logs to establish whether the recorded distances exceed plausible limits based on the 600 m/s speed assumption.

3. **Anomaly Detection**
   - Assesses whether the traveled distance between two consecutive points significantly exceeds the expected maximum based on the aircraft's speed.
   - Flags incidents where the distance surpasses both the calculated threshold and a minimum distance of 2000 meters, indicative of potential spoofing.

4. **Final Selection and Output**
   - Generates a report listing all detected potential spoofing incidents, including the aircraft identifier, callsign, timestamps, and coordinates before and at the point of detection, as well as the calculated distance and time difference.

### Execution

The prepared statement `spoofing_analysis` is optimized for execution with different parameters using the `EXECUTE` command.

## Python Script

The Python script automates data extraction and analysis from the OpenSky Trino database, focusing on identifying gaps in aircraft ADS-B transmission data. It constructs a complex SQL query using multiple CTEs to process aircraft state vectors, identifying gaps in data where ADS-B transmissions are missing. The script operates within a specified date range, executing the SQL query iteratively for each day, and processes the data into pandas DataFrames for further analysis.

The script includes logging and error handling mechanisms to ensure stable execution and facilitate troubleshooting. It executes the SQL described in "Part 1 - GPS Anomalies: Possible Jamming Incidents." A similar script, subject to minor modifications, is used for "Part 2 - GPS Anomalies: Possible Spoofing Incidents."

## Author

Eugene Pik  
[eugene.pik@mevocopter.com](mailto:eugene.pik@mevocopter.com)  
ORCID Profile https://orcid.org/0000-0001-6296-919X  
LinkedIn Profile https://www.linkedin.com/in/eugene/

## DOI 
10.5281/zenodo.11279424  
DOI URL https://doi.org/10.5281/zenodo.11279424

## License

This project is licensed under the Apache License, Version 2.0. See the `LICENSE` file for details.

## Date

May 2024
