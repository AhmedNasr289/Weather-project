import requests
import gzip
import json
import sys

def load_city_list(city_list_path, country='EG', limit=None):
    """
    Load city list from a JSON or GZ file. Returns a list of dicts with name, lat, lon, filtered by country.
    """
    if city_list_path.endswith('.gz'):
        with gzip.open(city_list_path, 'rt', encoding='utf-8') as f:
            all_cities = json.load(f)
    else:
        with open(city_list_path, 'r', encoding='utf-8') as f:
            all_cities = json.load(f)
    cities = [
        {"name": city['name'], "lat": city['coord']['lat'], "lon": city['coord']['lon']}
        for city in all_cities
        if city.get('country') == country and 'coord' in city and 'lat' in city['coord'] and 'lon' in city['coord']
    ]
    if limit:
        cities = cities[:limit]
    return cities

def extract_and_load(city_list_path='city.list.json.gz', country='EG', limit=None):
    import os
    import json
    import uuid
    import snowflake.connector

    # Connect to Snowflake
    sf_conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER", "AhmedNasr789"),
        password=os.getenv("SNOWFLAKE_PASSWORD", "PP6uYJa67fCNeZS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT", "bhndecn-zmc99792"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database="WEATHER_DB",
        schema="STAGING"
    )

    # Drop and recreate the staging table before inserting new data
    try:
        cursor = sf_conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS staging.weather_data")
        cursor.execute("""
            CREATE TABLE staging.weather_data (
                ID STRING,
                DETAILS_JSON_RAW STRING,
                RECORD_DATE STRING,
                PROCESSING_STATUS STRING
            )
        """)
        cursor.close()
        print("Dropped and recreated staging.weather_data table.")
    except Exception as e:
        print(f"Failed to recreate staging.weather_data: {e}")
        sf_conn.rollback()

    # Load cities from file, filtering by country
    cities = load_city_list(city_list_path, country=country, limit=limit)
    print(f"Loaded {len(cities)} cities for country {country}.")
    api_key = os.getenv("OPENWEATHER_API_KEY", '5fd1117db022e76b3148b1db999c20f8')
    all_records = []
    for city in cities:
        if len(all_records) >= 1000:
            break
        url = 'https://api.openweathermap.org/data/2.5/forecast'
        params = {
            'lat': city['lat'],
            'lon': city['lon'],
            'appid': api_key
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            print(f"API status code for {city['name']}:", response.status_code)
            for record in data.get('list', []):
                if len(all_records) >= 1000:
                    break
                record['city'] = {"name": city['name']}
                all_records.append(record)
        except Exception as e:
            print(f"Failed to fetch weather data for {city['name']}: {e}")
            continue

    # Insert weather data into Snowflake, using current UTC time as RECORD_DATE
    from datetime import datetime, timezone
    insert_sql = """
        INSERT INTO staging.weather_data (ID, DETAILS_JSON_RAW, RECORD_DATE, PROCESSING_STATUS)
        VALUES (%s, %s, %s, 'pending')
    """
    try:
        cursor = sf_conn.cursor()
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        for record in all_records[:1000]:
            cursor.execute(insert_sql, (str(uuid.uuid4()), json.dumps(record), now_utc))
        cursor.close()
        sf_conn.commit()
        print(f"Weather data extracted and loaded successfully. Inserted {min(len(all_records), 1000)} records.")
    except Exception as e:
        print(f"Failed to insert data into Snowflake: {e}")
        sf_conn.rollback()

if __name__ == "__main__":
    # Accept city list path, country, and record limit from command line if provided
    city_list_path = sys.argv[1] if len(sys.argv) > 1 else 'city.list.json.gz'
    country = sys.argv[2] if len(sys.argv) > 2 else 'EG'
    try:
        record_limit = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
    except Exception:
        record_limit = 1000
    extract_and_load(city_list_path, country=country, limit=record_limit)