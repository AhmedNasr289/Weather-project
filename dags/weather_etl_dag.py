from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import os
from datetime import datetime, timedelta
import subprocess
import sys

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 1, 1),
}

def run_extract_script():
    """
    Run the weather data extraction script.
    """
    import os
    script_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'extract_weather.py')
    script_path = os.path.abspath(script_path)
    city_list_path = '/opt/airflow/scripts/city.list.json.gz'
    country = 'EG'
    record_limit = '1000'
    try:
        subprocess.run([
            sys.executable, script_path, city_list_path, country, record_limit
        ], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running extract_weather.py: {e}")
        raise

def run_dbt_transformations():
    """
    Run dbt deps and dbt run --select Facts Dims using subprocess, with error handling and log output.
    """
    import os
    import subprocess
    dbt_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dbt', 'weather_project'))
    env = os.environ.copy()
    env['PATH'] = env.get('PATH', '') + ':/home/airflow/.local/bin'
    cmds = [
        ['dbt', '--version'],
        ['dbt', 'deps'],
        ['dbt', 'run', '--select', 'Facts', 'Dims']
    ]
    try:
        if not os.path.isdir(os.path.join(dbt_project_path, 'models')):
            raise RuntimeError('models directory not found in dbt project')
        print(f"Current directory: {dbt_project_path}")
        print(f"PATH={env['PATH']}")
        for cmd in cmds:
            print(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, cwd=dbt_project_path, env=env, capture_output=True, text=True)
            print(result.stdout)
            if result.returncode != 0:
                print(result.stderr)
                # Print dbt logs if available
                log_path = os.path.join(dbt_project_path, 'logs', 'dbt.log')
                if os.path.exists(log_path):
                    with open(log_path) as f:
                        print("\n--- dbt logs ---\n" + f.read())
                raise RuntimeError(f"Command {' '.join(cmd)} failed with exit code {result.returncode}")
    except Exception as e:
        print(f"Error running dbt transformations: {e}")
        raise

with DAG(
    dag_id='weather_etl_pipeline',
    description='Extract weather data and load into Snowflake, then run dbt models',
    schedule='@hourly',
    default_args=default_args,
    catchup=False,
    tags=["weather", "etl"]
) as dag:

    extract_weather_data = PythonOperator(
        task_id='extract_weather_data',
        python_callable=run_extract_script,
    )

    # Set dbt project path relative to AIRFLOW_HOME (which is the root of your Airflow project)
    dbt_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dbt', 'weather_project'))
    run_dbt_transform = PythonOperator(
        task_id='run_dbt_transformations',
        python_callable=run_dbt_transformations,
    )

    extract_weather_data >> run_dbt_transform