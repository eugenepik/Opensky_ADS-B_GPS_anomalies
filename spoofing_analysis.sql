PREPARE spoofing_analysis FROM
-- Define the common table expressions (CTEs) for the analysis.
WITH 
FilteredData AS (
    -- Select and preprocess the data, ensuring all conditions for a valid analysis are met.
    SELECT
        time, -- Timestamp of the ADS-B message.
        icao24, -- Unique ICAO 24-bit address of the aircraft.
        callsign, -- Textual callsign associated with the aircraft.
        lat AS current_lat, -- Current latitude of the aircraft.
        lon AS current_lon, -- Current longitude of the aircraft.
        lastposupdate, -- Timestamp of the last position update.
        LAG(lat) OVER (PARTITION BY icao24 ORDER BY time) AS prev_lat, -- Latitude of the previous position for this aircraft.
        LAG(lon) OVER (PARTITION BY icao24 ORDER BY time) AS prev_lon, -- Longitude of the previous position for this aircraft.
        LAG(time) OVER (PARTITION BY icao24 ORDER BY time) AS prev_time, -- Timestamp of the previous message for this aircraft.
        LAG(lastposupdate) OVER (PARTITION BY icao24 ORDER BY time) AS prev_lastposupdate -- Timestamp of the previous last position update for this aircraft.
    FROM
        state_vectors_data4
    WHERE 
        hour >= ? AND hour < ? -- Filter records within a specified hour range.
        AND onground = false -- Exclude data where the aircraft is reported to be on the ground.
        AND icao24 IS NOT NULL -- Ensure there is a valid ICAO address.
        AND LENGTH(TRIM(icao24)) = 6 -- ICAO address must be exactly 6 characters long.
        AND regexp_like(icao24, '^[0-9a-fA-F]{6}$') -- ICAO address must be a valid hex number.
        AND lat IS NOT NULL AND lon IS NOT NULL -- Ensure that latitude and longitude are not null.
        AND lat > -90 AND lat < 90 -- Latitude must be within normal earth bounds.
        AND lon > -180 AND lon < 180 -- Longitude must be within normal earth bounds.
        AND ABS(time - lastposupdate) < 2 -- Current coordinates must be updated within the last two seconds.
),
DistanceCalculations AS (
    -- Calculate distances between consecutive points and prepare data for anomaly detection.
    SELECT
        icao24,
        callsign,
        prev_time AS time_before_spoofing,
        prev_lat AS lat_before_spoofing,
        prev_lon AS lon_before_spoofing,
        prev_lastposupdate,
        time AS time_of_spoofing,
        current_lat AS lat_of_spoofing,
        current_lon AS lon_of_spoofing,
        CAST(ROUND(ST_Distance( -- Compute the spherical geographic distance between consecutive points.
            to_spherical_geography(ST_Point(current_lon, current_lat)), -- Convert current coordinates to a geographic point.
            to_spherical_geography(ST_Point(prev_lon, prev_lat)) -- Convert previous coordinates to a geographic point.
        )) AS INTEGER) AS distance, -- Round the distance to the nearest integer for clearer analysis.
        (time - prev_time) * 600 AS max_allowed_distance, -- Calculate the maximum distance an aircraft can travel at 600 m/s.
        time - prev_time AS time_difference -- The time difference in seconds between consecutive points, useful for speed calculations.
    FROM
        FilteredData
    WHERE
        ABS(prev_time - prev_lastposupdate) < 2 -- Ensure previous coordinates are also updated within the last two seconds.
)
-- Select potential spoofing incidents based on the calculated distances exceeding the threshold.
SELECT
    icao24, -- Unique ICAO 24-bit address of the aircraft, used for identification.
    callsign, -- Textual callsign associated with the aircraft, varies by flight.
    time_before_spoofing, -- Timestamp just before a potential spoofing incident.
    lat_before_spoofing, -- Latitude coordinate recorded just before potential spoofing.
    lon_before_spoofing, -- Longitude coordinate recorded just before potential spoofing.
    time_of_spoofing, -- Timestamp of the ADS-B message where potential spoofing is detected.
    lat_of_spoofing, -- Latitude coordinate at the time of the suspected spoofing.
    lon_of_spoofing, -- Longitude coordinate at the time of the suspected spoofing.
    distance, -- The calculated distance that flagged the anomaly, meters.
    time_difference -- The time difference in seconds between consecutive data points.
FROM
    DistanceCalculations
WHERE
    distance > max_allowed_distance AND distance > 2000 -- Only consider distances that exceed both the calculated max allowable and a fixed minimum of 2000 meters.
