import pandas as pd
import requests
import json
from sqlalchemy import create_engine

# API key for OpenWeatherMap
api_key = 'x'

# Database connection details
db_user = 'postgres'
db_password = 'sasql'
db_host = 'localhost'
db_port = '5432'
db_name = 'weather'
tb_name = 'weather_data'

def fetch_weather_data(lat, lon, state, api_key):
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
        
        with open(f'{state}_data.json', 'w') as f:
            json.dump(data, f)
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data for {state}: {e}")
        return None

def json_to_dataframe(json_data):
    """
    Normalize JSON data from the OpenWeatherMap API response into a pandas DataFrame.

    :param json_data: JSON data from the OpenWeatherMap API
    :return: pandas DataFrame with normalized data
    """
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
    return df

# Create SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Load province data from JSON file
province_file_path = "province.json"
try:
    with open(province_file_path, 'r') as file:
        province_data = json.load(file)

    # Fetch weather data for each province and save it to the database
    for province in province_data:
        lat = province["lat"]
        lon = province["lon"]
        state = province["province"]

        # Fetch weather data
        weather_data = fetch_weather_data(lat, lon, state, api_key)
        if weather_data:
            # Convert JSON to DataFrame
            df = json_to_dataframe(weather_data)
            # Save DataFrame to CSV file
            df.to_csv(f"{state}_data.csv")
            print(f"Exported {state} data to CSV.")

            # Insert DataFrame into SQL table
            df.to_sql(tb_name, engine, if_exists='append', index=False)
            print(f"Inserted {state} data into {tb_name} table.")

except FileNotFoundError:
    print(f"File {province_file_path} not found.")
except json.JSONDecodeError:
    print(f"Error decoding JSON from file {province_file_path}.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    print("Script execution completed.")
