from collections.abc import MutableMapping
from operator import index
from os.path import isfile
# import flatdict
from pathlib import Path
import datetime
import os
import re
import numpy as np
from typing import Dict, Iterable, List
from airflow.models.dag import DAG
from airflow.decorators import task, dag, task_group
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable, baseoperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import requests
import json
from uuid import uuid4
from random import randint

# AIRFLOW_VAR_PROJECT_HOME='$HOME/Projects/BIProjects'

Variable.set(key="date_format", value="%Y-%m-%d")
# Variable.set(key="project_home", value=)
Variable.set(key="staging_area", value='/home/jolek/Projects/BIProject/staging/')
Variable.set(key="city", value='/home/jolek/Projects/BIProject/data/uscities.csv')
Variable.set(key="openweather_api_key", value="f50682fc9c765b69ac045a8c267b0759")
Variable.set(key="google_maps_api_key", value="aizasybzb0oz0re1jjbol0_jyvd4jamrqeo0zvi")

custom_data = {
                    "path": "/home/jolek/Projects/BIProject/data/",
                    "extensions": [".csv", ".xlsx"],
                    "regex": ["([0-9]{2,}).(csv|xlsx)"]
                }

Variable.set(key="data", value=custom_data, serialize_json=True)

data_dict = Variable.get("data", deserialize_json=True)
# drop_dict = Variable.get("drop", deserialize_json=True)

cities_list_path = Variable.get("city", deserialize_json=False)
staging_area_path = Variable.get("staging_area")

save_file_weather = "./weather_requests_cache.json"

google_requests_cache = dict()

if os.path.isfile(save_file_weather):
    with open(save_file_weather, "r") as file:
        weather_requests_cache = json.loads(file.read())
else:
    weather_requests_cache = dict()


def create_file_id(id):
    return str(id) + "#" + str(randint(1, 1000))


def flatten(dictionary, parent_key='', separator='_'):
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)


def is_city(city):
    cities = pd.read_csv(cities_list_path)['city'].to_list()
    cities = list(map(lambda x: x.lower(), cities))
    return city.lower() in cities

#         dag_id="shootings_dag",
#         schedule="@daily",
#         catchup=False,
#         start_date=datetime.datetime(2023, 3, 5)


# @dag(schedule="@daily", catchup=False, dag_id="shootings_dag",
#      start_date=datetime.datetime(2023, 3, 5))
@dag(catchup=False, dag_id="shootings_dag",
     start_date=datetime.datetime(2023, 3, 5), schedule_interval="@daily")
def our_dag():

    @task()
    def extract_from_combined_csv():
        file_path: str | Path = data_dict["path"]
        # TODO(11jolek11): Fill drop_columns param
        drop_columns: List[str] = []
        return_df = pd.read_csv(str(file_path))
        return_df.drop(colums=drop_columns, in_place=True)

        run_id = create_file_id(str(uuid4()))
        return_df.to_csv(f"{staging_area_path}/{run_id}.csv")
        Variable.set(key="run_id", value=run_id)

        return run_id

    @task()
    def extract_from_csv():
        paths: Iterable[str] | Iterable[Path] = data_dict["path"]
        patterns: Iterable[str] = data_dict["patterns"]
        extensions: Iterable[str] = data_dict["extensions"]
        # TODO(11jolek11): Fill drop_columns param
        # drop_columns: List[str] = []

        file_paths = []
        return_df = pd.DataFrame()

        for path in paths:
            if patterns:
                for pattern in patterns:
                    file_paths.extend(
                            filter(re.compile(pattern).match(), os.listdir(str(path)))
                            )
            elif extensions:
                scanned = os.scandir(path)
                for extension in extensions:
                    for obj in scanned:
                        if obj.is_file() and extension in obj.name:
                            file_paths.append(obj.path)

        for file in file_paths:
            if ".csv" in str(file):
                df = pd.read_csv(file.path, encoding="utf-8")
                pd.concat([return_df, df])
                continue
            if ".excel" in str(file):
                df = pd.read_excel(file.path, encoding="utf-8")
                pd.concat([return_df, df])
                continue

        # return_df.drop(colums=drop_columns, in_place=True)

        run_id = create_file_id(str(uuid4()))
        Variable.set(key="run_id", value=run_id)
        return_df.to_csv(f"{staging_area_path}/{run_id}.csv")

        return run_id

    @task()
    def unify_date_format(id):
        extracted_data = pd.read_csv(f"{staging_area_path}/{id}.csv")

        target_format = "%Y %m %d"
        if "Incident Date" in extracted_data.columns and str(extracted_data["Incident Date"][0][0]).isupper():
            target_format = "%B %d, %Y"
        extracted_data["Incident Date"] = pd.to_datetime(extracted_data["Incident Date"], format=target_format)
        extracted_data.to_csv(f"{staging_area_path}/{id}.csv")

        return id

    @task()
    def get_coordinates(id):
        extracted_data = pd.read_csv(f"{staging_area_path}/{id}.csv")
        locations = extracted_data["Address"]
        coords_lat = np.zeros([len(locations.index), 1])
        coords_lon = np.zeros([len(locations.index), 1])

        for location_idx in locations.index:
            if " and " in locations[location_idx]:
                locations[location_idx] = locations[location_idx].split(" and ")[0]
            google_payload = {"textQuery": locations.loc[location_idx]}
            google_url = 'https://places.googleapis.com/v1/places:searchText'
            google_headers = {'Content-Type': 'application/json',
                              'X-Goog-FieldMask': 'places.location',
                              'X-Goog-Api-Key': Variable.get("google_maps_api_key")}

            if google_url not in google_requests_cache.keys():
                resp = requests.post(url=google_url, data=google_payload, headers=google_headers)
                if resp.status_code == 200:
                    temp_location = resp.json()["places"][0]["location"]
                    google_requests_cache[google_url] = temp_location
            else:
                temp_location = google_requests_cache[google_url]
                coords_lat[location_idx] = [temp_location["latitude"]]
                coords_lon[location_idx] = [temp_location["longitude"]]
        extracted_data["Lat"] = coords_lat
        extracted_data["Lon"] = coords_lon

        extracted_data.to_csv(f"{staging_area_path}/{id}.csv")
        return id

    @task()
    def extract_weather(id):
        extracted_data = pd.read_csv(f"{staging_area_path}/{id}.csv")
        weather_df = pd.DataFrame(columns=["lat", "lon", "tr", "date", "cloud_cover_afternoon", "humidity_afternoon",
                                           "precipitation_total", "pressure_afternoon", "temperature", "wind_max_speed", "wind_max_direction"])
        lats = extracted_data["Lat"].values
        lons = extracted_data["Lon"].values

        for data_idx in extracted_data.index:
            weather_url = f'https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lats[data_idx]}&lon={lons[data_idx]}&date={extracted_data["Incident Date"]}&appid={Variable.get("openweather_api_key")}'
            if weather_url not in weather_requests_cache.keys():
                resp = requests.post(url=weather_url)

                if resp.status_code == 200:
                    temp_weather = resp.json()
                    # temp_weather = dict(flatdict.FlatDict(temp_weather, delimiter="_"))

                    temp_weather = flatten(temp_weather)

                    for key in temp_weather.keys():
                        if key not in weather_df.columns:
                            del temp_weather[key]
                    weather_requests_cache[weather_url] = temp_weather
            else:
                temp_weather = weather_requests_cache[weather_url]

        if not os.path.isfile(save_file_weather):
            save = json.dumps(weather_requests_cache)
            with open(save_file_weather, "w") as file:
                file.write(save)

        extracted_data.to_csv(f"{staging_area_path}/{id}.csv")
        weather_id = create_file_id(Variable.get("run_id"))
        weather_df.to_csv(f"{staging_area_path}/{weather_id}.csv")
        return {"extracted": id, "weather": weather_id}

    @task()
    def add_count_or_city(ids_dict):
        id = ids_dict["extracted"]
        extracted_data = pd.read_csv(f"{staging_area_path}/{id}")
        # FIXME(11jolek11): Fix column name
        mixed_data = extracted_data["CityorCounty"]

        cities_file = pd.read_csv(cities_list_path)
        cities = cities_file['city'].to_list()
        cities = list(map(lambda x: x.lower(), cities))

        temp_city_list = []
        temp_county_list = []

        for idx in mixed_data.index:
            if str(mixed_data[idx]).lower() in cities:
                temp_city_list.append(mixed_data.loc[idx])
                temp_county_list.append(cities_file['county_name'])
            else:
                temp_city_list.append("")
                temp_county_list.append(mixed_data[idx])

        extracted_data["City"] = temp_city_list
        extracted_data["County"] = temp_county_list

        extracted_data.to_csv(f"{staging_area_path}/{id}.csv")
        return ids_dict
    @task_group
    def all_tasks():
        pass

    # baseoperator.chain(extract_from_csv,
    #                    unify_date_format,
    #                    get_coordinates,
    #                    extract_weather,
    #                    add_count_or_city)

    extract_from_csv >> unify_date_format >> get_coordinates >> extract_weather >> add_count_or_city


our_dag()

# with DAG(
#         dag_id="shootings_dag",
#         schedule_interval="@daily",
#         catchup=False,
#         start_date=datetime.datetime(2023, 3, 5)
#         ) as dag:
#     pass




    # create_table_postgres = PostgresOperator()
    # agregacje jako widoki

