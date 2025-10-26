-- create bronze raw data table (streaming)

CREATE OR REFRESH STREAMING TABLE `first-project-dev`.bronze_raw
AS
SELECT *
FROM STREAM 
  read_files(
  '/Volumes/workspace/first-project-dev/bike_data',
  format => 'csv'
);

-- create silver transformed data table (streaming)
CREATE OR REFRESH STREAMING TABLE `first-project-dev`.silver_transformed
AS 
SELECT
  *,
  ROUND(empty_slots / CAST(slots AS DOUBLE)*100, 2) AS percent_empty_slots,
  ROUND(free_bikes / CAST(slots AS DOUBLE)*100, 2) AS percent_free_bikes
FROM STREAM(`first-project-dev`.bronze_raw);