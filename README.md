Data Flow architecture
<img width="1645" height="682" alt="image" src="https://github.com/user-attachments/assets/92c1c213-a3d0-44e6-9bbb-bc03c9a6af6e" />

+---------------------+
| OpenWeatherMap API  |
+----------+----------+
           |
           v
+---------------------+
|  Airflow DAG        |
|  (Orchestration)    |
+----------+----------+
           |
           v
+---------------------+
| Python Extract      |
| Script (Hourly      |
| Forecast, Egypt)    |
+----------+----------+
           |
           v
+---------------------+
| Snowflake Staging   |
| Table               |
+----------+----------+
           |
           v
+---------------------+
| dbt Transformations |
| (Facts & Dims)      |
+----------+----------+
           |
           v
+---------------------+
| Snowflake Modeled   |
| Tables (Analytics)  |
+---------------------+

Description:

The Airflow DAG orchestrates the workflow.
The Python script extracts hourly forecast data for all Egyptian cities from the OpenWeatherMap API and loads it into a Snowflake staging table.
dbt models transform and structure the data into analytics-ready tables in Snowflake.
