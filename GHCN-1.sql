SELECT 
id, 
date, 
element, 
value 
FROM 
`bigquery-public-data.ghcn_d.ghcnd_2024` 
LIMIT 100;

SELECT 
 date,
 -- Average Max Temp across all stations (converted to Celsius)
AVG(CASE WHEN element = 'TMAX' THEN value / 10 END) AS avg_max_temp,
-- Total Precipitation (converted to mm)
SUM(CASE WHEN element = 'PRCP' THEN value / 10 END) AS total_rainfall,
 -- Average Min Temp (converted to Celsius)
AVG(CASE WHEN element = 'TMIN' THEN value / 10 END) AS avg_min_temp
FROM 
 `bigquery-public-data.ghcn_d.ghcnd_2024`
WHERE 
qflag IS NULL -- Removes data with quality errors
GROUP BY 
 date
ORDER BY 
 date ASC;


SELECT
 -- Extracting the year from the table name logic (or using the 'date' column)
 EXTRACT(YEAR FROM date) AS year,
 -- GHCN temperatures are in tenths of degrees Celsius, so we divide by 10
 AVG(value / 10) AS avg_max_temp
FROM
 `bigquery-public-data.ghcn_d.ghcnd_20*` -- This wildcard pulls all tables from 2000-2025
WHERE
 element = 'TMAX' -- We only want Maximum Temperature
 AND qflag IS NULL -- Quality control: Filter out data with errors
GROUP BY
 year
ORDER BY
year ASC;


SELECT
  date,
  -- Identify days with extreme heat (e.g., > 35°C)
  COUNTIF(element = 'TMAX' AND value / 10 > 35) AS extreme_heat_stations,
  -- Identify days with heavy rainfall (e.g., > 50mm)
  COUNTIF(element = 'PRCP' AND value / 10 > 50) AS heavy_rain_stations
FROM `bigquery-public-data.ghcn_d.ghcnd_2024`
WHERE qflag IS NULL
GROUP BY date
HAVING heavy_rain_stations > 0 OR extreme_heat_stations > 0
ORDER BY date ASC;



SELECT
  EXTRACT(MONTH FROM date) AS month,
  ROUND(AVG(CASE WHEN element = 'TMAX' THEN value / 10 END), 2) AS monthly_avg_max_temp,
  ROUND(SUM(CASE WHEN element = 'PRCP' THEN value / 10 END), 2) AS monthly_total_precip
FROM `bigquery-public-data.ghcn_d.ghcnd_2024`
WHERE qflag IS NULL
GROUP BY 1  -- This refers to the first column (EXTRACT(MONTH FROM date))
ORDER BY 1;

 SELECT 
  s.id,
  s.name,
  s.latitude,
  s.longitude,
  -- Extracting country code from the ID (first 2 chars)
  SUBSTR(s.id, 1, 2) AS country_code,
  AVG(t.value / 10) AS avg_temp_2024
FROM `bigquery-public-data.ghcn_d.ghcnd_2024` AS t
JOIN `bigquery-public-data.ghcn_d.ghcnd_stations` AS s
  ON t.id = s.id
WHERE t.element = 'TMAX'
  AND t.qflag IS NULL
GROUP BY 1, 2, 3, 4, 5
HAVING avg_temp_2024 IS NOT NULL
ORDER BY avg_temp_2024 DESC
LIMIT 100; 



WITH baseline_stats AS (
  -- Calculating the 1960-1990 baseline
  SELECT 
    id, 
    AVG(value / 10) AS baseline_temp
  FROM `bigquery-public-data.ghcn_d.ghcnd_*`
  WHERE _TABLE_SUFFIX BETWEEN '1960' AND '1990'
    AND element = 'TMAX'
    AND qflag IS NULL 
  GROUP BY id
),
current_stats AS (
  -- Calculating the 2024 average
  SELECT 
    id, 
    AVG(value / 10) AS temp_2024
  FROM `bigquery-public-data.ghcn_d.ghcnd_2024`
  WHERE element = 'TMAX'
    AND qflag IS NULL
  GROUP BY id
)

SELECT 
  s.id,
  s.name,
  s.latitude,
  s.longitude,
  SUBSTR(s.id, 1, 2) AS country_code,
  ROUND(b.baseline_temp, 2) AS baseline_avg,
  ROUND(c.temp_2024, 2) AS current_avg,
  ROUND(c.temp_2024 - b.baseline_temp, 2) AS anomaly
FROM current_stats c
JOIN baseline_stats b ON c.id = b.id
JOIN `bigquery-public-data.ghcn_d.ghcnd_stations` s ON c.id = s.id
WHERE ABS(c.temp_2024 - b.baseline_temp) < 20 -- Filter for realistic outliers
ORDER BY anomaly DESC
LIMIT 500;


SELECT 
  id,
  COUNT(DISTINCT date) AS days_recorded,
  ROUND(COUNT(DISTINCT date) / 366 * 100, 2) AS percent_complete
FROM `bigquery-public-data.ghcn_d.ghcnd_2024`
WHERE element = 'TMAX'
GROUP BY 1
HAVING percent_complete < 90
ORDER BY percent_complete ASC
LIMIT 10;

WITH historical_stats AS (
  -- Calculate Mean and StdDev for every station (1960-1990)
  SELECT 
    id,
    AVG(value/10) as mean_temp,
    STDDEV(value/10) as stddev_temp
  FROM `bigquery-public-data.ghcn_d.ghcnd_*`
  WHERE _TABLE_SUFFIX BETWEEN '1960' AND '1990'
    AND element = 'TMAX'
    AND qflag IS NULL
  GROUP BY id
  HAVING stddev_temp > 0 -- Avoid division by zero
),
current_data AS (
  -- Get 2024 data
  SELECT id, date, (value/10) as daily_temp
  FROM `bigquery-public-data.ghcn_d.ghcnd_2024`
  WHERE element = 'TMAX' AND qflag IS NULL
)

SELECT 
  c.id, 
  c.date, 
  c.daily_temp,
  h.mean_temp,
  -- Formula: (Value - Mean) / StdDev
  ROUND((c.daily_temp - h.mean_temp) / h.stddev_temp, 2) AS z_score
FROM current_data c
JOIN historical_stats h ON c.id = h.id
WHERE ABS((c.daily_temp - h.mean_temp) / h.stddev_temp) > 3 -- Find "Extreme" anomalies
ORDER BY z_score DESC;

SELECT 
  w.id AS station_id,
  s.name AS station_name,
  -- The first two characters of the ID represent the Country Code
  LEFT(w.id, 2) AS country_code,
  s.latitude,
  s.longitude,
  w.date,
  -- Average Max Temp (converted to Celsius)
  AVG(CASE WHEN element = 'TMAX' THEN value / 10 END) AS avg_max_temp,
  -- Total Precipitation (converted to mm)
  SUM(CASE WHEN element = 'PRCP' THEN value / 10 END) AS total_rainfall
FROM 
  `bigquery-public-data.ghcn_d.ghcnd_2024` AS w
JOIN 
  `bigquery-public-data.ghcn_d.ghcnd_stations` AS s
  ON w.id = s.id
WHERE 
  w.qflag IS NULL 
GROUP BY 
  station_id, station_name, country_code, s.latitude, s.longitude, w.date
ORDER BY 
  w.date ASC;


  SELECT 
  s.name AS station_name,
  SUBSTR(s.id, 1, 2) AS country,
  w.date,
  ROUND(value / 10, 1) AS temp_celsius
FROM `bigquery-public-data.ghcn_d.ghcnd_2024` AS w
JOIN `bigquery-public-data.ghcn_d.ghcnd_stations` AS s ON w.id = s.id
WHERE w.element = 'TMAX' AND w.qflag IS NULL
ORDER BY w.date DESC
LIMIT 100;