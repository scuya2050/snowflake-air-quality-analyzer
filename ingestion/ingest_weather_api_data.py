import json
import requests
import os
import datetime
from snowflake.snowpark import Session
import logging
from dotenv import load_dotenv
from pathlib import Path

logger = logging.getLogger()
logger.setLevel(logging.INFO)



def load_config():
    """
    Load configuration with priority:
    1. Environment variables (GitHub Secrets)
    2. .env file (local development)
    """

    # Load environment variables first
    api_token = os.getenv("API_TOKEN")
    api_url = os.getenv("API_URL")
    snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_user = os.getenv("SNOWFLAKE_USER")
    snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")

    # If missing → fallback to .env
    if not api_token or not api_url:
        load_dotenv()  # loads variables from .env if it exists
        api_token = api_token or os.getenv("API_TOKEN")
        api_url = api_url or os.getenv("API_URL")
        snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
        snowflake_user = os.getenv("SNOWFLAKE_USER")
        snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")


    if not api_token or not api_url:
        raise RuntimeError("Missing required config values")

    return {
        "API_TOKEN": api_token,
        "API_URL": api_url,
        "SNOWFLAKE_ACCOUNT": snowflake_account,
        "SNOWFLAKE_USER": snowflake_user,
        "SNOWFLAKE_PASSWORD": snowflake_password
    }


def snowpark_basic_auth(config) -> Session:
    connection_parameters = {
        "account": config["SNOWFLAKE_ACCOUNT"],
        "user": config["SNOWFLAKE_USER"],
        "password": config["SNOWFLAKE_PASSWORD"],
        "role":"SYSADMIN",
        "database":"dev_db",
        "schema":"stage_sch",
        "warehouse":"load_wh"
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()


def get_lima_air_quality_data(config):

    country = "Peru" 
    city = "Lima"
    districts = [
        "Ancon", "Ate", "Barranco", "Brena", "Carabayllo", "Chaclacayo", "Chorrillos",
        "Cieneguilla", "Comas", "El Agustino", "Independencia", "Jesus Maria", "La Molina",
        "La Victoria", "Lima", "Lince", "Los Olivos", "Lurigancho", "Lurin", "Magdalena del Mar",
        "Miraflores", "Pachacamac", "Pucusana", "Pueblo Libre", "Puente Piedra", "Punta Hermosa",
        "Punta Negra", "Rimac", "San Bartolo", "San Borja", "San Isidro", "San Juan de Lurigancho",
        "San Juan de Miraflores", "San Luis", "San Martin de Porres", "San Miguel", "Santa Anita",
        "Santa Maria del Mar", "Santa Rosa", "Santiago de Surco", "Surquillo", "Villa El Salvador",
        "Villa Maria del Triunfo"
    ]

    try:
        logging.info('Establishing Snowflake connection')
        sf_session = snowpark_basic_auth(config)
        logging.info('Snowflake connection established successfully')
    except Exception as e:
        logging.error(f'Error connecting to Snowflake: {e}')
        return

    for d in districts:
        try:
            # Query parameters
            params = {
                "key": {config["API_TOKEN"]},
                "q": f"{d}, {city}, {country}",
                "aqi": 'yes'
            }
            timestamp = datetime.datetime.now(datetime.timezone.utc)
            timestamp_string = timestamp.strftime("%Y%m%dT%H%M%SZ")
            year = timestamp.strftime("%Y")
            month = timestamp.strftime("%m")
            day = timestamp.strftime("%d")
            response = requests.get(url=config["API_URL"], params=params)
            response.raise_for_status()
            data = response.json()

            file_name = f'weather_api_measurement_{timestamp_string}.json'
            partition = f'{country.lower().replace(" ", "-")}/{city.lower().replace(" ", "-")}/{d.lower().replace(" ", "-")}/{year}/{month}/{day}/'

            local_file_path = Path.cwd() / 'data' / partition / file_name
            local_file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(local_file_path, 'w') as json_file:
                json.dump(data, json_file, indent=2)
            # logger.info(f"API call successful for {d}")
        except requests.HTTPError as e:
            logger.warning(f"HTTP Error with {d}: {e}")
            continue  # Skip the current iteration and move to the next URL
        except Exception as e:
            logger.error(f"Unexpected error with {d}: {e}")
            continue  # Handle other errors and continue with the next URL

        try:
            stg_location = f'@dev_db.stage_sch.raw_stg/{partition}'
            
            logging.info(f'Placing the file, the file name is {file_name} and stage location is {stg_location}')
            sf_session.file.put(str(local_file_path), stg_location)
            
            logging.info('JSON File placed successfully in stage location in snowflake')
            lst_query = f'list {stg_location}{file_name}.gz'
            
            logging.info(f'list query to fetch the stage file to check if they exist there or not = {lst_query}')
            result_lst = sf_session.sql(lst_query).collect()
            
            logging.info(f'File is placed in snowflake stage location= {result_lst}')

        except Exception as e:
            logger.error(f"Snowflake upload failed  for {d}: {e}")
    
    logger.info("Data ingestion completed.")


if __name__ == "__main__":
    config = load_config()
    get_lima_air_quality_data(config)