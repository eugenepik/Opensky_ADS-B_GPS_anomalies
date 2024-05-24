-- Prepare a named prepared statement 'flight_analysis' for later execution. 
-- This preparation optimizes execution by allowing the database to parse, optimize, and compile the SQL once and run it multiple times with different parameters.
PREPARE flight_analysis FROM

-- Define a series of Common Table Expressions (CTEs) to modularize and structure the query for better readability and maintenance.
WITH 
-- FilteredData CTE: Selects relevant columns and rows from table 'state_vectors_data4' tailored to airborne state conditions for a specified hourly range.
FilteredData AS (
    SELECT
        hour,  -- Partition key, used to optimize data retrieval by reducing the dataset size to relevant time frames.
        icao24,  -- Unique identifier for the aircraft, crucial for tracking individual flight paths.
        callsign,  -- Identifying signal of the aircraft, which can provide additional context in tracking and analysis.
        time,  -- Timestamp of the ADS-B message, used for chronological analysis and sequence detection.
        lat AS current_lat,  -- Latitude value at the time of message, renamed for clarity in subsequent computations.
        lon AS current_lon,  -- Longitude value at the time of message, similarly renamed.
        lastposupdate,  -- Timestamp for the last position update, used to verify the recency of location data.
        -- Use window functions LAG and LEAD to retrieve geographical and temporal data points before and after the current row within each partition.
        LAG(lat) OVER (PARTITION BY icao24 ORDER BY time) AS previous_lat,  -- Retrieves latitude of the previous position report for the same aircraft.
        LAG(lon) OVER (PARTITION BY icao24 ORDER BY time) AS previous_lon,  -- Retrieves longitude of the previous position report.
        LAG(time) OVER (PARTITION BY icao24 ORDER BY time) AS previous_time,  -- Retrieves timestamp of the previous position report.
        LAG(lastposupdate) OVER (PARTITION BY icao24 ORDER BY time) AS previous_lastposupdate,  -- Retrieves last position update time before the current row.
        LEAD(lat) OVER (PARTITION BY icao24 ORDER BY time) AS next_lat,  -- Retrieves latitude of the next position report for the same aircraft.
        LEAD(lon) OVER (PARTITION BY icao24 ORDER BY time) AS next_lon,  -- Retrieves longitude of the next position report.
        LEAD(time) OVER (PARTITION BY icao24 ORDER BY time) AS next_time,  -- Retrieves timestamp of the next position report.
        LEAD(lastposupdate) OVER (PARTITION BY icao24 ORDER BY time) AS next_lastposupdate  -- Retrieves last position update time after the current row.
    FROM state_vectors_data4
    WHERE
        hour >= ? AND hour < ?  -- Defines the hourly range for the data extraction, with placeholders for parameter binding.
        AND onground = false  -- Excludes records where the aircraft is on the ground, focusing on airborne data only.
        AND icao24 IS NOT NULL  -- Ensures that only records with a valid aircraft identifier are selected.
        AND LENGTH(TRIM(icao24)) = 6  -- Checks that the aircraft identifier has exactly six characters after trimming any whitespace.
        AND regexp_like(icao24, '^[0-9a-fA-F]{6}$')  -- Ensures that the aircraft identifier matches a hexadecimal format.
),

-- Sessionized CTE: Detects the start of sequences where latitude or longitude data is missing, indicating potential data gaps.
Sessionized AS (
    SELECT *,
        -- Case statement to identify transitions from valid to null latitudes or longitudes, marking potential starts of data gaps.
        CASE 
            WHEN (LAG(current_lat) OVER (PARTITION BY icao24 ORDER BY time) IS NOT NULL AND current_lat IS NULL) OR
                 (LAG(current_lon) OVER (PARTITION BY icao24 ORDER BY time) IS NOT NULL AND current_lon IS NULL) THEN 1
            ELSE 0
        END AS is_start_of_null  -- Binary flag indicating the start of a missing data segment, accounting for either latitude or longitude.
    FROM FilteredData
),

-- ValidPrevious and ValidNext CTEs: Define the validity of geographical data points based on their proximity to the last known position updates.
ValidPrevious AS (
    SELECT *,
        -- Case statement to validate previous data points by checking the time difference between the last position update and the reported time.
        CASE
            WHEN previous_lat IS NOT NULL AND previous_lon IS NOT NULL AND ABS(previous_time - previous_lastposupdate) <= 2 THEN TRUE
            ELSE FALSE
        END AS is_valid_previous  -- Boolean flag indicating if the previous data points are valid based on timing criteria.
    FROM Sessionized
),
ValidNext AS (
    SELECT *,
        -- Similar validation for the next data points using similar criteria as for previous points.
        CASE
            WHEN next_lat IS NOT NULL AND next_lon IS NOT NULL AND ABS(next_time - next_lastposupdate) <= 2 THEN TRUE
            ELSE FALSE
        END AS is_valid_next  -- Boolean flag indicating if the next data points are valid.
    FROM ValidPrevious
),

-- CollapsedNulls CTE: Groups contiguous records with null latitude or longitude data into sessions and records valid surrounding data for possible interpolation.
CollapsedNulls AS (
    SELECT 
        icao24,
        hour,
        callsign,  -- Propagate the callsign through to this aggregation stage for inclusion in the final output.
        MIN(time) AS null_start_time,  -- Captures the earliest time in a session of contiguous null latitude or longitude records.
        MAX(time) AS null_end_time,  -- Captures the latest time in the same session.
        -- Aggregates valid latitude and longitude values from previous and next data points for each null data session.
        MAX(CASE WHEN is_valid_previous THEN previous_lat ELSE NULL END) AS valid_previous_lat,
        MAX(CASE WHEN is_valid_previous THEN previous_lon ELSE NULL END) AS valid_previous_lon,
        MAX(CASE WHEN is_valid_previous THEN previous_time ELSE NULL END) AS valid_previous_time,
        MIN(CASE WHEN is_valid_next THEN next_lat ELSE NULL END) AS valid_next_lat,
        MIN(CASE WHEN is_valid_next THEN next_lon ELSE NULL END) AS valid_next_lon,
        MIN(CASE WHEN is_valid_next THEN next_time ELSE NULL END) AS valid_next_time
    FROM (
        SELECT *, SUM(is_start_of_null) OVER (PARTITION BY icao24 ORDER BY time) AS null_group  -- Sums the start of null flags to form unique sessions for each null sequence of latitude or longitude.
        FROM ValidNext
        WHERE current_lat IS NULL OR current_lon IS NULL  -- Filters for records where either latitude or longitude is null, indicating missing data.
    ) AS NullGroups
    GROUP BY icao24, null_group, hour, callsign  -- Groups by aircraft, null sequence identifier, hour, and callsign to keep sessions distinct.
    HAVING (MAX(time) - MIN(time)) >= 60  -- Filters sessions to include only those with a significant duration (60 seconds), ensuring relevance.
),

-- NICData CTE: Gathers NIC values from 'position_data4' for periods identified as having null latitude or longitude data, ensuring data integrity.
NICData AS (
    SELECT
        p.icao24,
        -- Calculates average, minimum, and maximum NIC values from position_data4 where NIC values are within specified range and are not null.
        AVG(p.nic) AS avg_nic,  -- Computes the average NIC value for the aircraft during the specified time frame.
        MIN(p.nic) AS min_nic,  -- Finds the minimum NIC value, indicating the lowest quality of positional accuracy.
        MAX(p.nic) AS max_nic  -- Finds the maximum NIC value, indicating the highest quality of positional accuracy.
    FROM position_data4 p
    JOIN CollapsedNulls c ON p.icao24 = c.icao24 AND p.hour = c.hour  -- Joins on aircraft identifier and hour for alignment with state data.
    WHERE p.mintime BETWEEN c.null_start_time AND c.null_end_time  -- Filters NIC data to the periods when latitude or longitude data was missing.
      AND p.nic BETWEEN 0 AND 11  -- Ensures NIC values are within the acceptable range for ADS-B data.
      AND p.nic IS NOT NULL  -- Excludes records where NIC data is missing, ensuring completeness.
    GROUP BY p.icao24  -- Aggregates results by aircraft identifier.
),

-- ValidCoords CTE: Focuses on timestamps and coordinates validated around null periods without calculating geographical distances.
ValidCoords AS (
    SELECT 
        c.icao24,
        c.callsign,  -- Include the callsign in the final dataset to link data with specific flight identifiers.
        c.null_start_time,
        c.null_end_time,
        c.valid_previous_lat AS previous_latitude,  -- Uses previously validated latitude as the starting point for temporal analysis.
        c.valid_previous_lon AS previous_longitude,  -- Uses previously validated longitude as the starting point.
        c.valid_previous_time AS time_of_previous_not_null_coords,  -- Timestamp of the valid previous coordinates.
        c.valid_next_lat AS next_latitude,  -- Uses next validated latitude as the end point for temporal analysis.
        c.valid_next_lon AS next_longitude,  -- Uses next validated longitude as the end point.
        c.valid_next_time AS time_of_next_not_null_coords,  -- Timestamp of the valid next coordinates.
        (c.null_end_time - c.null_start_time) AS null_duration_seconds,  -- Calculates the total duration of the null data period in seconds.
        (c.valid_next_time - c.valid_previous_time) AS between_coords_duration_seconds,  -- Calculates the time interval between valid previous and next coordinates.
        n.avg_nic,  -- Includes the average NIC value in the output for quality assessment.
        n.min_nic,  -- Includes the minimum NIC value to indicate positional accuracy.
        n.max_nic  -- Includes the maximum NIC value to indicate the best positional accuracy during the null period.
    FROM CollapsedNulls c
    LEFT JOIN NICData n ON c.icao24 = n.icao24  -- Joins NIC data with the calculated valid coordinates.
    WHERE c.valid_previous_lat IS NOT NULL AND c.valid_previous_lon IS NOT NULL  -- Ensures only records with valid starting coordinates are selected.
      AND c.valid_next_lat IS NOT NULL AND c.valid_next_lon IS NOT NULL  -- Ensures only records with valid ending coordinates are included.
)

-- The main SELECT statement outputs detailed information about each identified null period including its context, duration, and associated data quality metrics.
SELECT
    icao24,  -- Aircraft identifier, linking the output to specific flights.
    callsign,  -- Callsign of the aircraft, providing a direct link to the flight's operational identifier.
    null_start_time,  -- Start time of the null data period, useful for temporal analysis.
    null_end_time,  -- End time of the null data period, marking the extent of the gap.
    previous_latitude,  -- Latitude at the start of the gap, potentially useful for path reconstruction.
    previous_longitude,  -- Longitude at the start of the gap.
    time_of_previous_not_null_coords,  -- Timestamp associated with the starting valid coordinates.
    next_latitude,  -- Latitude at the end of the gap, useful for understanding the trajectory post-gap.
    next_longitude,  -- Longitude at the end of the gap.
    time_of_next_not_null_coords,  -- Timestamp associated with the ending valid coordinates.
    null_duration_seconds,  -- Duration of the gap in seconds, important for assessing the significance of the data loss.
    between_coords_duration_seconds,  -- Time interval between the valid coordinates surrounding the gap, useful for estimating the missing data.
    avg_nic,  -- Average NIC value for the period, indicating average data quality.
    min_nic,  -- Minimum NIC value, indicating the lowest data quality observed.
    max_nic  -- Maximum NIC value, indicating the highest data quality observed.
FROM ValidCoords
ORDER BY
    icao24,  -- Sorting by aircraft identifier to group output by aircraft.
    null_start_time  -- Sorting by the start time of the null period for chronological analysis.


-- The EXECUTE command uses a predefined plan, to analyze ADS-B data within a timeframe. 
-- This approach allows the database to optimize and store the query in a ready state, enhancing performance by reusing the plan with different time parameters without recompilation.
-- 
-- EXECUTE flight_analysis USING {from_unix_hour}, {until_unix_hour}
