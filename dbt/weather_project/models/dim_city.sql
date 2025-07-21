WITH unique_cities AS (
  SELECT DISTINCT PARSE_JSON(details_json_raw):city.name::STRING AS city_name
  FROM {{ source('staging', 'WEATHER_DATA') }}
)
SELECT
  city_name,
  ROW_NUMBER() OVER (ORDER BY city_name) AS city_id
FROM unique_cities