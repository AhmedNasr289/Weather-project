WITH base AS (
  SELECT DISTINCT CAST(record_date AS DATE) AS date
  FROM {{ source('staging', 'WEATHER_DATA') }}
)
SELECT
  ROW_NUMBER() OVER (ORDER BY date) AS date_id,
  date,
  EXTRACT(YEAR FROM date) AS year,
  EXTRACT(MONTH FROM date) AS month,
  EXTRACT(DAY FROM date) AS day,
  TO_CHAR(date, 'Day') AS weekday
FROM base
