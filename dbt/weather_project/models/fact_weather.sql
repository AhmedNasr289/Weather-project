WITH parsed AS (
  SELECT
    id,
    PARSE_JSON(details_json_raw) AS details_json,
    CAST(record_date AS DATE) AS record_day,
    PARSE_JSON(details_json_raw):main.temp::FLOAT - 273.15 AS temperature_c,
    PARSE_JSON(details_json_raw):main.humidity::INTEGER AS humidity,
    PARSE_JSON(details_json_raw):wind.speed::FLOAT AS wind_speed,
    PARSE_JSON(details_json_raw):main.pressure::INTEGER AS pressure,
    PARSE_JSON(details_json_raw):weather[0].description::STRING AS weather_description,
    PARSE_JSON(details_json_raw):city.name::STRING AS city_name,
    record_date AS recorded_at,
    PARSE_JSON(details_json_raw):dt_txt::STRING AS prediction_time
  FROM {{ source('staging', 'WEATHER_DATA') }}
  WHERE processing_status = 'pending'
),

city_map AS (
  SELECT * FROM {{ ref('dim_city') }}
),

date_map AS (
  SELECT * FROM {{ ref('dim_date') }}
),

final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['id']) }} AS weather_id,
    cm.city_id,
    dm.date_id,
    parsed.temperature_c,
    parsed.humidity,
    parsed.wind_speed,
    parsed.pressure,
    parsed.weather_description,
    parsed.recorded_at,
    parsed.prediction_time
  FROM parsed
  JOIN city_map cm ON parsed.city_name = cm.city_name
  JOIN date_map dm ON parsed.record_day = dm.date
)

SELECT * FROM final
