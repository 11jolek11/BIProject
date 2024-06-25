from collections.abc import MutableMapping
from operator import index
from os.path import isfile
# import flatdict
from pathlib import Path
import datetime
import os
import re
from urllib.request import urlretrieve
import urllib
from urllib.error import URLError
import zipfile
import numpy as np
from typing import Dict, Iterable, List
from airflow.models.dag import DAG
from airflow.decorators import task, dag
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable, baseoperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import requests
import json
from uuid import uuid4
from random import randint
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
# AIRFLOW_VAR_PROJECT_HOME='$HOME/Projects/BIProjects'

download_path = "https://github.com/11jolek11/BIProject/raw/main/data.zip" 

Variable.set(key="date_format", value="%Y-%m-%d")
# Variable.set(key="project_home", value=)
Variable.set(key="staging_area", value='./staging')
Variable.set(key="city", value='./data/uscities.csv')
Variable.set(key="openweather_api_key", value="f50682fc9c765b69ac045a8c267b0759")
Variable.set(key="google_maps_api_key", value="AIzaSyBZb0OZ0rE1jJbOL0_Jyvd4JAMrqeO0ZvI")

custom_data = {
                    "path": ["./data/"],
                    "extensions": [".csv", ".xlsx"],
                    "regex": ["([0-9]{2,}).(csv|xlsx)"]
                }

Variable.set(key="data", value=custom_data, serialize_json=True)

# data_dict = Variable.get("data", deserialize_json=True)
# drop_dict = Variable.get("drop", deserialize_json=True)

cities_list_path = Variable.get("city", deserialize_json=False)
staging_area_path = Variable.get("staging_area")


if not os.path.exists(staging_area_path):
    os.makedirs(staging_area_path)

save_file_weather = "./weather_requests_cache.json"
save_file_google = "./google_requests_cache.json"

if os.path.isfile(save_file_google):
    with open(save_file_google, "r") as file:
        google_requests_cache = json.loads(file.read())
else:
    google_requests_cache = dict()


if os.path.isfile(save_file_weather):
    with open(save_file_weather, "r") as file:
        weather_requests_cache = json.loads(file.read())
else:
    weather_requests_cache = dict()

# urlretrieve("https://github.com/11jolek11/BIProject/raw/main/data.zip", "./data.zip")
# with zipfile.ZipFile("./data.zip", 'r') as zip_ref:
#     zip_ref.extractall(".")


def create_file_id(id):
    return str(id) + "_" + str(randint(1, 1000))


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

@task
def extract_from_combined_csv():
    data_dict = Variable.get("data", deserialize_json=True)

    file_path = data_dict["path"][0] + "combined.csv"
    # TODO(11jolek11): Fill drop_columns param
    # drop_columns: List[str] = []
    return_df = pd.read_csv(str(file_path))
    # return_df.drop(colums=drop_columns, in_place=True)

    global_run_id = str(uuid4())
    Variable.set(key="global_run_id", value=global_run_id)
    file_id = create_file_id(global_run_id)
    return_df.to_csv(f"{staging_area_path}/{file_id}.csv")

    return file_id

@task()
def extract_from_csv():
    try:
        urlretrieve("https://github.com/11jolek11/BIProject/raw/main/data.zip", "./data.zip")
    except URLError:
        print("No connection")
    with zipfile.ZipFile("./data.zip", 'r') as zip_ref:
        zip_ref.extractall(".")
    data_dict = custom_data
    paths = data_dict["path"]
    patterns = data_dict["regex"]
    # extensions = data_dict["extensions"]
    # TODO(11jolek11): Fill drop_columns param
    # drop_columns: List[str] = []

    file_paths = []
    df_list = []
    return_df = pd.DataFrame()

    for path in paths:
        # if patterns:
        for pattern in patterns:
            file_paths.extend(
                    filter(re.compile(pattern).match,
                           os.listdir(str(path)))
                    )

    for file in file_paths:
        if ".csv" in str(file):
            df = pd.read_csv("./data/" + file)
            df_list.append(df)
            continue
        if ".xlsx" in str(file):
            df = pd.read_excel("./data/" + file)
            df_list.append(df)
            continue

    return_df = pd.concat(df_list, ignore_index=True)
    global_run_id = str(uuid4())
    Variable.set(key="global_run_id", value=global_run_id)
    file_id = create_file_id(global_run_id)
    return_df.to_csv(f"{staging_area_path}//{file_id}.csv")

    Variable.set(key="row_no", value=len(return_df.index))
    return file_id

@task()
def unify_date_format(id):
    extracted_data = pd.read_csv(f"{staging_area_path}/{id}.csv")
    print(extracted_data.columns)
    for idx in extracted_data.index:
        target_format = "%Y-%m-%d %H:%M:%S"
        if str(extracted_data.loc[idx, "Incident Date"][0]).isupper() and str(extracted_data.loc[idx, "Incident Date"][0]).isalpha():
            target_format = "%B %d, %Y"

        extracted_data.loc[idx, "Incident Date"] = pd.to_datetime(extracted_data.loc[idx, "Incident Date"], format=target_format).strftime("%Y-%m-%d")

    extracted_data.to_csv(f"{staging_area_path}/{id}.csv")
    print(f"{staging_area_path}/{id}.csv")

    return id

@task()
def get_coordinates(id):
    # FIXME(11jolek11): Read adds 2x extra Unnamed columns
    extracted_data = pd.read_csv(f"{staging_area_path}/{id}.csv")
    locations = extracted_data["Address"]
    # print(locations)
    coords_lat = np.zeros([len(locations.index), 1])
    coords_lon = np.zeros([len(locations.index), 1])

    for location_idx in locations.index:
        # location_idx = 815
        print(f"Getting {location_idx}")
        if " and " in str(locations[location_idx]):
            extracted_data.loc[location_idx, "Address"] = extracted_data.loc[location_idx, "Address"].split(" and ")[0]
        google_payload = {"textQuery": str(extracted_data.loc[location_idx, "Address"])}
        google_url = 'https://places.googleapis.com/v1/places:searchText'
        google_headers = {'Content-Type': 'application/json',
                          'X-Goog-FieldMask': 'places.location',
                          'X-Goog-Api-Key': Variable.get("google_maps_api_key")}

        if str(google_payload) not in google_requests_cache.keys():
            # print(f"URL: {google_url}")
            # print(f"Headers: {google_headers}")
            # print(f"Payload: {google_payload}")
            # print(f'{pd.api.types.is_float(extracted_data.loc[location_idx, "Address"])} -- {type(extracted_data.loc[location_idx, "Address"])}')
            resp = requests.post(url=google_url, json=google_payload, headers=google_headers)
            # print(resp.text)
            print(resp.json())
            if resp.status_code == 200:
                temp_location = {"latitude": 0.0, "longitude": 0.0}
                if resp.json():
                    temp_location = resp.json()["places"][0]["location"]
                elif (len(str(extracted_data.loc[location_idx, "City Or County"]))):
                    google_payload = {"textQuery": str(extracted_data.loc[location_idx, "City Or County"])}
                    resp = requests.post(url=google_url, json=google_payload, headers=google_headers)
                    if resp.status_code == 200 and resp.json():
                        temp_location = resp.json()["places"][0]["location"]
                google_requests_cache[str(google_payload)] = temp_location
                save = json.dumps(google_requests_cache)
                with open(save_file_google, "w") as file:
                    file.write(save)
            else:
                print(f"REQUEST ERROR: {resp} - {resp.text}")

        else:
            print("get from cache")
            temp_location = google_requests_cache[str(google_payload)]
            coords_lat[location_idx] = [temp_location["latitude"]]
            coords_lon[location_idx] = [temp_location["longitude"]]
    extracted_data["Lat"] = coords_lat
    extracted_data["Lon"] = coords_lon

    extracted_data.to_csv(f"{staging_area_path}/{id}.csv")
    return id

@task()
def extract_weather(id):
    # TODO(11jolek11): What to do when lon and lat values are missing (when lon and lat == 0.0 ) because of fail in prev node?
    # TODO(11jolek11): What to do when lon and lat values are missing because of lack data on OpenWeather portal?
    extracted_data = pd.read_csv(f"{staging_area_path}/{id}.csv")
    ttt = list(extracted_data.columns).copy()
    # TODO(11jolek11): Add checks,, if lat and lon in request == lat and lon in response
    expected_columns = ["lat", "lon", "date", "cloud_cover_afternoon", "humidity_afternoon",
                                       "precipitation_total", "pressure_afternoon", "temperature_max", "wind_max_speed", "wind_max_direction"]
    weather_df = pd.DataFrame(columns=expected_columns)
    lats = extracted_data["Lat"].values
    lons = extracted_data["Lon"].values
    df_list = []

    for data_idx in extracted_data.index:
        # print(f"Getting {data_idx} weather")
        target_date = extracted_data.loc[data_idx, "Incident Date"]
        weather_url = f'https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lats[data_idx]}&lon={lons[data_idx]}&date={target_date}&appid={Variable.get("openweather_api_key")}'
        default_values = [lats[data_idx], lats[data_idx], extracted_data.loc[data_idx, "Incident Date"], 0, 0, 0, 0, 0.0, 0.0, 0]
        temp_weather = dict()

        for key, value in zip(expected_columns, default_values):
            temp_weather[key] = [value]
        if lats[data_idx] != 0.0 and lons[data_idx] != 0.0:
            if weather_url not in weather_requests_cache.keys():
                try:
                    resp = requests.get(url=weather_url)
                    if resp.status_code == 200 and resp.json():
                        temp_dict = resp.json().copy()
                        # print(temp_dict["temperature"])
                        keys_collection = list(temp_dict.keys()).copy()
                        for key in keys_collection:
                            # print(f"{key}")
                            # FIXME(11jolek11): Fix this!
                            if key not in weather_df.columns:
                                if key != "temperature":
                                    del temp_dict[key]

                        for key, value in flatten(temp_dict).items():
                            # FIXME(11jolek11): Fix this!
                            if (key.startswith("temperature") and not key.endswith("max")) or key == "temperature":
                                continue
                            temp_weather[key] = [value]
                        if temp_weather["lat"] != lats[data_idx] or temp_weather["lon"] != lons[data_idx]:
                            print("Error {} -- {} || {} -- {} ".format(temp_weather["lat"], lats[data_idx], temp_weather["lon"], lons[data_idx]))

                        weather_requests_cache[weather_url] = temp_weather
                        save = json.dumps(weather_requests_cache)
                        with open(save_file_weather, "w") as file:
                            file.write(save)
                    else:
                        print(f"Error: {resp} {resp.text} {resp.reason}")

                except requests.exceptions.ConnectionError:
                    print("Server WAF blocked connection")
                except URLError:
                    print("No connection")

            else:
                print("Get from cache")
                for key, value in weather_requests_cache[weather_url].items():
                    if key in expected_columns:
                        # Adding value as [value]
                        temp_weather[key] = [value]

        # print(temp_weather.keys())
        if list(temp_weather.keys()) == expected_columns:
            df_list.append(pd.DataFrame(temp_weather, columns=expected_columns))
        else:
            # pass
            print("Columns error")

    weather_df = pd.concat([*df_list, weather_df], ignore_index=True)
    # extracted_data.to_csv(f"{staging_area_path}/{id}.csv")
    weather_id = create_file_id(Variable.get("global_run_id"))
    weather_df.to_csv(f"{staging_area_path}/{weather_id}.csv")

    print(f"Weather: reading {ttt}")
    print(f"Path: weather {staging_area_path}/{weather_id}.csv, Extracted {staging_area_path}/{id}.csv")
    return {"extracted": id, "weather": weather_id}


@task()
def add_count_or_city(ids_dict):
    id = ids_dict["extracted"]
    extracted_data = pd.read_csv(f"{staging_area_path}/{id}.csv")
    print(f"Reading: {extracted_data.columns}")
    location_df_columns = ["Address", "Lat", "Lon", "State"]
    columns_to_drop = ["Address", "Lat", "Lon", "City Or County", "Operations"]
    location_df = extracted_data[location_df_columns].copy()
    # FIXME(11jolek11): Fix column name
    mixed_data = extracted_data["City Or County"]

    cities_file = pd.read_csv(cities_list_path)
    cities = cities_file['city'].to_list()
    cities = list(map(lambda x: x.lower(), cities))

    temp_city_list = []
    temp_county_list = []

    for idx in mixed_data.index:
        if str(mixed_data[idx]).lower() in cities:
            temp_city_list.append(mixed_data.loc[idx])
            # FIXME(11jolek11):
            # temp_county_list.append(cities_file.loc[cities_file["city"] == str(mixed_data[idx])].loc[0, "county_name"])
            candidates = list(cities_file.loc[cities_file["city"] == str(mixed_data[idx])]["county_name"].values)
            if candidates:
                temp_county_list.append(candidates[0])
            else:
                temp_county_list.append("")

        else:
            temp_city_list.append("")
            temp_county_list.append(mixed_data[idx])

    location_df["City"] = temp_city_list
    location_df["County"] = temp_county_list

    extracted_data.drop(columns_to_drop, axis="columns", inplace=True)

    # extracted_data.drop(["City Or County"], inplace=True)
    print(f"Writing {extracted_data.columns}")
    extracted_data.to_csv(f"{staging_area_path}/{id}.csv")

    location_id = create_file_id(Variable.get("global_run_id"))
    location_df.to_csv(f"{staging_area_path}/{location_id}.csv")
    ids_dict["location"] = location_id
    print(ids_dict)
    return ids_dict

def year_extractor(date: str, delimeter="-", year_loc=0):
    return date.split(delimeter)[year_loc]

@task
def add_temp_year_from_date(ids_dict):
    id = ids_dict["extracted"]
    extracted_data = pd.read_csv(f"{staging_area_path}/{id}.csv")
    dates = list(map(year_extractor, list(extracted_data["Incident Date"].values)))
    extracted_data["TEMP_YEAR"] = dates
    print(f"Writing {extracted_data.columns}")
    extracted_data.to_csv(f"{staging_area_path}/{id}.csv")
    return ids_dict

@task
def add_time_dim(ids_dict):
    gun_violence_dates = pd.read_csv(f"{staging_area_path}/{ids_dict["extracted"]}.csv")['Incident Date']
    dates = []
    for date in gun_violence_dates.iloc[[0, -1]]:
        dates.append(date.split(" ")[0])

    date_range = pd.date_range(start=dates[0], end=dates[1])

    df = pd.DataFrame(date_range, columns=['Date'])

    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['Day'] = df['Date'].dt.day
    df['DayOfWeek'] = df['Date'].dt.dayofweek
    df['Decade'] = (df['Year'] // 10) * 10
    df['Quarter'] = df['Date'].dt.quarter
    df['DayOfYear'] = df['Date'].dt.dayofyear
    df['WeekOfYear'] = df['Date'].dt.isocalendar().week
    df['DayName'] = df['Date'].dt.day_name()
    df['MonthName'] = df['Date'].dt.month_name()
    df['IsWeekend'] = df['Date'].dt.dayofweek >= 5

    # us_holidays = holidays.US()
    us_holidays = holidays.country_holidays("US")
    df['IsHoliday'] = df['Date'].isin(us_holidays)

    time_id = create_file_id(global_run_id)
    df.to_csv(f"{staging_area_path}/{time_id}.csv")
    ids_dict["time"] = time_id
    print(ids_dict)
    return ids_dict

@task
def add_ownership(ids_dict):
    file_gun_ownership = '../data/ownership.xlsx'

    gun_ownership = pd.read_excel(file_gun_ownership)
    gun_ownership = gun_ownership[['Year', 'STATE', 'permit']]
    gun_ownership = gun_ownership.rename(columns={"STATE": "State"})
    gun_ownership['Year'] = gun_ownership['Year'].astype(int)

    states = gun_ownership['State'].unique()
    years = list(range(gun_ownership['Year'].min(), 2025))
    all_combinations = pd.MultiIndex.from_product([years, states], names=['Year', 'State']).to_frame(index=False)
    gun_ownership = all_combinations.merge(gun_ownership, on=['Year', 'State'], how='left')
    gun_ownership['permit'] = gun_ownership.groupby('State')['permit'].ffill().bfill()

    ownership_id = create_file_id(global_run_id)
    gun_ownership.to_csv(f"{staging_area_path}/{ownership_id}.csv")
    ids_dict["ownership"] = ownership_id
    print(ids_dict)
    return ids_dict

def verify_row_number(ids_dict):
    id = ids_dict["extracted"]
    extracted_data = pd.read_csv(f"{staging_area_path}/{id}.csv")
    should_be = Variable.get("row_no")
    if should_be != len(extracted_data.index):
        raise ValueError("BAD Size")
    return ids_dict

def load(ids_dict):
    targets = list(ids_dict.keys())

    conn = BaseHook.get_connection("sqlserver")
    engine = create_engine(f"postgresql://{conn.login}:{conn.password}@{conn.host}/gunviolance")

    for key in targets:
        if key == "extracted":
            continue
        id = ids_dict[key]
        data = pd.read_csv(f"{staging_area_path}/{id}.csv")
        data.to_sql(name=f"{key}_dim", con=engine, if_exists="replace")

    key = "extracted"
    data = pd.read_csv(f"{staging_area_path}/{ids_dict[key]}.csv")
    data.to_sql(name="shooting_dim", con=engine, if_exists="replace")


# @task_group
# def all_tasks():
#     pass

    # baseoperator.chain(extract_from_csv,
    #                    unify_date_format,
    #                    get_coordinates,
    #                    extract_weather,
    #                    add_count_or_city)

    # extract_from_csv >> unify_date_format >> get_coordinates >> extract_weather >> add_count_or_city


# our_dag()

with DAG(
        dag_id="shootings_dag",
        schedule_interval=None,
        catchup=False,
        start_date=datetime.datetime(2024, 6, 25)
        ) as our_dag:
    get_csv = extract_from_combined_csv()
    # get_csv = extract_from_csv()
    dates = unify_date_format(get_csv)
    coords = get_coordinates(dates)
    wea = extract_weather(coords)
    countryd = add_count_or_city(wea)
    ty = add_temp_year_from_date(countryd)
    time = add_time_dim(ty)
    ownership = add_ownership(time)
    verify = verify_row_number(ownership)
    push = load(verify)

    get_csv >> dates >> coords >> wea >> countryd >> ty >> time >> ownership >> verify >> push

    # We start DAG manually

    # create_table_postgres = PostgresOperator()
    # agregacje jako widoki

# if __name__ == "__main__":
#     our_dag.test()

