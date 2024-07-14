from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago

import pandas as pd
import requests
import json

# API key for OpenWeatherMap
api_key = 'x'

# Database connection details
tb_name = 'weather_data'

province_data = [
    {"lat": 18.79, "lon": 98.98, "province": "chiang_mai"},
    {"lat": 16.43, "lon": 102.82, "province": "khon_kean"},
    {"lat": 13.73, "lon": 100.52, "province": "bangkok"},
    {"lat": 7.878978, "lon": 98.398392, "province": "phuket"}
]

def fetch_weather_data(lat, lon, state, api_key, **kwargs):
    """
    Fetch weather data from OpenWeatherMap API for a given latitude and longitude.
    Save the data to a JSON file named after the state.

    :param lat: Latitude of the location
    :param lon: Longitude of the location
    :param state: State name for file naming
    :param api_key: API key for OpenWeatherMap
    :return: JSON data if request is successful, None otherwise
    """
    weather_api_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
    try:
        response = requests.get(weather_api_url)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        data = response.json()
        
        with open(f'/tmp/{state}_data.json', 'w') as f:
            json.dump(data, f)
        
        kwargs['ti'].xcom_push(key='weather_data', value=data)
        print(f"Fetched weather data for {state}")
        
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data for {state}: {e}")
        return None

def json_to_dataframe(**kwargs):
    """
    Normalize JSON data from the OpenWeatherMap API response into a pandas DataFrame.
    Save the DataFrame to a CSV file and push the DataFrame to XCom.

    :return: None
    """
    # Fetching the data from XCom
    json_data = kwargs['ti'].xcom_pull(key='weather_data', task_ids=f'fetch_weather_data_{kwargs["state"]}')

    if not json_data:
        print(f"No JSON data found for {kwargs['state']}. Skipping.")
        return

    try:
        # Extracting data from JSON
        coord_data = json_data.get('coord', {})
        weather_data = json_data.get('weather', [{}])[0]  # Assuming only one weather object
        main_data = json_data.get('main', {})
        wind_data = json_data.get('wind', {})
        clouds_data = json_data.get('clouds', {})
        rain_data = json_data.get('rain', {})
        snow_data = json_data.get('snow', {})
        sys_data = json_data.get('sys', {})

        # Create a dictionary with flattened structure
        flat_data = {
            'lon': coord_data.get('lon'),
            'lat': coord_data.get('lat'),
            'weather_id': weather_data.get('id'),
            'weather_main': weather_data.get('main'),
            'weather_description': weather_data.get('description'),
            'weather_icon': weather_data.get('icon'),
            'base': json_data.get('base'),
            'temp': main_data.get('temp'),
            'feels_like': main_data.get('feels_like'),
            'pressure': main_data.get('pressure'),
            'humidity': main_data.get('humidity'),
            'temp_min': main_data.get('temp_min'),
            'temp_max': main_data.get('temp_max'),
            'sea_level': main_data.get('sea_level'),
            'grnd_level': main_data.get('grnd_level'),
            'visibility': json_data.get('visibility'),
            'wind_speed': wind_data.get('speed'),
            'wind_deg': wind_data.get('deg'),
            'wind_gust': wind_data.get('gust', None),  # Default to None if 'gust' is missing
            'clouds_all': clouds_data.get('all'),
            'rain_1h': rain_data.get('1h', None),  # Default to None if '1h' is missing
            'rain_3h': rain_data.get('3h', None),  # Default to None if '3h' is missing
            'snow_1h': snow_data.get('1h', None),  # Default to None if '1h' is missing
            'snow_3h': snow_data.get('3h', None),  # Default to None if '3h' is missing
            'dt': json_data.get('dt'),
            'sys_type': sys_data.get('type'),
            'sys_id': sys_data.get('id'),
            'sys_message': sys_data.get('message'),
            'sys_country': sys_data.get('country'),
            'sys_sunrise': sys_data.get('sunrise'),
            'sys_sunset': sys_data.get('sunset'),
            'timezone': json_data.get('timezone'),
            'city_id': json_data.get('id'),
            'city_name': json_data.get('name'),
            'cod': json_data.get('cod')
        }

        # Convert dictionary to DataFrame
        df = pd.DataFrame([flat_data])

        # Save DataFrame to CSV
        state = kwargs['state']
        df.to_csv(f"/tmp/{state}_data.csv", index=False)

        # Push DataFrame to XCom
        kwargs['ti'].xcom_push(key='dataframe', value=df)
        return df

        print(f"Converted JSON data to DataFrame for {state}")
    except Exception as e:
        print(f"Error processing JSON data for {kwargs['state']}: {e}")
        raise

def save_to_database(**kwargs):
    """
    Save the DataFrame from XCom to a PostgreSQL database.

    :return: None
    """
    try:
        # Fetching the DataFrame from XCom
        df = kwargs['ti'].xcom_pull(key='dataframe', task_ids='json_to_dataframe')
        
        if df is None:
            raise ValueError("DataFrame is None. Check previous task logs for details.")
                
        # Connecting to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=psql_weather)
        engine = pg_hook.get_sqlalchemy_engine()
        
        # Insert DataFrame into SQL table
        df.to_sql(tb_name, engine, if_exists='append', index=False)
        print(f"Inserted data into {tb_name} table.")
    except Exception as e:
        print(f"Error saving data to database: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Fetch weather data and save to PostgreSQL',
    schedule_interval='@daily',
)

for province in province_data:
    lat = province["lat"]
    lon = province["lon"]
    state = province["province"]
    
    fetch_weather_data_task = PythonOperator(
        task_id=f'fetch_weather_data_{state}',
        python_callable=fetch_weather_data,
        op_kwargs={'lat': lat, 'lon': lon, 'state': state, 'api_key': api_key},
        provide_context=True,
        dag=dag,
    )

    json_to_dataframe_task = PythonOperator(
        task_id=f'json_to_dataframe_{state}',
        python_callable=json_to_dataframe,
        op_kwargs={'state': state},
        provide_context=True,
        dag=dag,
    )

    save_to_database_task = PythonOperator(
        task_id=f'save_to_database_{state}',
        python_callable=save_to_database,
        provide_context=True,
        dag=dag,
    )

    fetch_weather_data_task >> json_to_dataframe_task >> save_to_database_task
