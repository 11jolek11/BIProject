from collections.abc import MutableMapping
from operator import index
from os.path import isfile
import urllib
from urllib.error import URLError
# import flatdict
from pathlib import Path
import datetime
import os
import re
from urllib.request import urlretrieve
import zipfile
import numpy as np
from typing import Dict, Iterable, List
from numpy.lib.function_base import extract
import pandas as pd
import requests
import json
from uuid import uuid4
from random import randint
import holidays
from time import sleep

# AIRFLOW_VAR_PROJECT_HOME='$HOME/Projects/BIProjects'

download_path = "https://github.com/11jolek11/BIProject/raw/main/data.zip"

date_format = "%Y-%m-%d"
# Variable.set(key="project_home", value=)
staging_area = './staging'
city = './data/uscities.csv'

global_run_id = 0
cities_list_path = city
staging_area_path = staging_area

openweather_api_key = "f50682fc9c765b69ac045a8c267b0759"
# f"{staging_area_path}/{weather_id}.csv"
google_maps_api_key = "AIzaSyBZb0OZ0rE1jJbOL0_Jyvd4JAMrqeO0ZvI"

custom_data = {
                    "path": ["./data/"],
                    "extensions": [".csv", ".xlsx"],
                    "regex": ["([0-9]{2,}).(csv|xlsx)"]
                }



if not os.path.exists(staging_area_path):
    os.makedirs(staging_area_path)

save_file_weather = "./weather_requests_cache.json"
save_file_google = "./google_requests_cache.json"
save_file_raw = "./weather_raw_cache.json"
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

weather_raw = dict()
if os.path.isfile(save_file_raw):
    with open(save_file_raw, "r") as file:
        # weather_raw = json.loads(file.read())
        weather_raw = dict()
else:
    weather_raw = dict()

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


def extract_from_combined_csv():
    global global_run_id
    data_dict = custom_data
    file_path = data_dict["path"]
    # TODO(11jolek11): Fill drop_columns param
    # drop_columns: List[str] = []
    return_df = pd.read_csv(str(file_path))
    # return_df.drop(colums=drop_columns, in_place=True)

    global_run_id = str(uuid4())
    file_id = create_file_id(global_run_id)
    return_df.to_csv(f"{staging_area_path}/{file_id}.csv")

    return file_id


def extract_from_csv():
    global global_run_id
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
    file_id = create_file_id(global_run_id)
    return_df.to_csv(f"{staging_area_path}//{file_id}.csv")

    return file_id


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
                          'X-Goog-Api-Key': google_maps_api_key}

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
            print("got from cache")
            temp_location = google_requests_cache[str(google_payload)]
            coords_lat[location_idx] = [temp_location["latitude"]]
            coords_lon[location_idx] = [temp_location["longitude"]]
    extracted_data["Lat"] = coords_lat
    extracted_data["Lon"] = coords_lon

    extracted_data.to_csv(f"{staging_area_path}/{id}.csv")
    return id

# Test data
# {'lat': 39.9067061, 'lon': -86.05705019999999, 'tz': '-05:00', 'date': '2020-12-31', 'units': 'standard', 'cloud_cover': {'afternoon': 75.0}, 'humidity': {'afternoon': 92.0}, 'precipitation': {'total': 0.0}, 'temperature': {'min': 269.14, 'max': 271.97, 'afternoon': 270.4, 'night': 271.97, 'evening': 270.68, 'morning': 269.14}, 'pressure': {'afternoon': 1026.0}, 'wind': {'max': {'speed': 5.1, 'direction': 310.0}}}

def extract_weather(id):
    # TODO(11jolek11): What to do when lon and lat values are missing (when lon and lat == 0.0 ) because of fail in prev node?
    # TODO(11jolek11): What to do when lon and lat values are missing because of lack data on OpenWeather portal?
    extracted_data = pd.read_csv(f"{staging_area_path}/{id}.csv")
    print(extracted_data.columns)
    # return 1
    # TODO(11jolek11): Add checks,, if lat and lon in request == lat and lon in response
    expected_columns = ["lat", "lon", "date", "cloud_cover_afternoon", "humidity_afternoon",
                                       "precipitation_total", "pressure_afternoon", "temperature", "wind_max_speed", "wind_max_direction"]
    weather_df = pd.DataFrame(columns=expected_columns)
    lats = extracted_data["Lat"].values
    lons = extracted_data["Lon"].values
    df_list = []

    for data_idx in extracted_data.index:
        print(f"Getting {data_idx} weather")
        target_date = extracted_data.loc[data_idx, "Incident Date"]
        weather_url = f'https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lats[data_idx]}&lon={lons[data_idx]}&date={target_date}&appid={openweather_api_key}'
        default_values = [lats[data_idx], lats[data_idx], extracted_data.loc[data_idx, "Incident Date"], 0, 0, 0, 0, 0.0, 0.0, 0]
        temp_weather = dict()

        for key, value in zip(expected_columns, default_values):
            temp_weather[key] = [value]
        if lats[data_idx] != 0.0 and lons[data_idx] != 0.0:
            if weather_url not in weather_requests_cache.keys():
                try:
                    resp = requests.get(url=weather_url)
                    state = resp.status_code
                    json_content = resp.json()
                    #
                    # Mocks
                    # state = 200
                    # json_content = {'lat': 39.9067061, 'lon': -86.05705019999999, 'tz': '-05:00', 'date': '2020-12-31', 'units': 'standard', 'cloud_cover': {'afternoon': 75.0}, 'humidity': {'afternoon': 92.0}, 'precipitation': {'total': 0.0}, 'temperature': {'min': 269.14, 'max': 271.97, 'afternoon': 270.4, 'night': 271.97, 'evening': 270.68, 'morning': 269.14}, 'pressure': {'afternoon': 1026.0}, 'wind': {'max': {'speed': 5.1, 'direction': 310.0}}}

                    if state == 200 and json_content:
                        temp_dict = json_content.copy()
                        with open("weather_raw_cache.json", "a") as file:
                            weather_raw[weather_url] = temp_dict
                            file.write(json.dumps(weather_raw))

                        # print(f">> {list(flatten(temp_dict).keys())}")
                        for key, value in flatten(temp_dict).items():
                            # ['temperature_min', 'temperature_max', 'temperature_afternoon', 'temperature_night', 'temperature_evening', 'temperature_morning']
                            if key in ['temperature_min', 'temperature_max', 'temperature_night', 'temperature_evening', 'temperature_morning']:
                                continue
                            if key == 'temperature_afternoon':
                                key = 'temperature'
                                temp_weather[key] = [float(value) - 273.15]  # sub 273.15
                                continue
                            if key not in expected_columns:
                                print(f"Throw {key}")
                                continue
                            # FIXME(11jolek11): Fix this!
                            # if (key.startswith("temperature") and not key.endswith("max")) or key == "temperature":
                            #     continue
                            # print(f"Inser {key}")
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
                print("Got from cache")
                for key, value in weather_requests_cache[weather_url].items():
                    if key in expected_columns:
                        # Adding value as [value]
                        temp_weather[key] = value

        # print(f"{temp_weather["temperature"]} -- {tttt}")
        # if list(temp_weather.keys()) == expected_columns:
        #     df_list.append(pd.DataFrame(temp_weather, columns=expected_columns))
        # else:
        #     print("Columns error")

        temp = pd.DataFrame(temp_weather)
        # print(f">> {temp}")
        df_list.append(temp)
    weather_df = pd.concat([*df_list, weather_df], ignore_index=True)
    weather_df["Address"] = extracted_data["Address"].to_numpy().copy()
    # weather_df["Incident ID"] = extracted_data["Incident ID"].to_numpy()
    # extracted_data.to_csv(f"{staging_area_path}/{id}.csv")
    weather_id = create_file_id(global_run_id)
    weather_df.to_csv(f"{staging_area_path}/{weather_id}.csv")

    print(f"Path: weather {staging_area_path}/{weather_id}.csv, Extracted {staging_area_path}/{id}.csv")
    return {"extracted": id, "weather": weather_id}


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

    location_id = create_file_id(global_run_id)
    location_df.to_csv(f"{staging_area_path}/{location_id}.csv")
    ids_dict["location"] = location_id
    print(ids_dict)
    return ids_dict

def year_extractor(date: str, delimeter="-", year_loc=0):
    return date.split(delimeter)[year_loc]

def add_temp_year_from_date(ids_dict):
    id = ids_dict["extracted"]
    extracted_data = pd.read_csv(f"{staging_area_path}/{id}.csv")
    dates = list(map(year_extractor, list(extracted_data["Incident Date"].values)))
    extracted_data["TEMP_YEAR"] = dates
    print(f"Writing {extracted_data.columns}")
    extracted_data.to_csv(f"{staging_area_path}/{id}.csv")
    return ids_dict

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

if __name__ == "__main__":
    extract_weather(get_coordinates(unify_date_format(extract_from_csv())))
    # add_temp_year_from_date(add_count_or_city(extract_weather(get_coordinates(unify_date_format(extract_from_csv())))))
    # get_coordinates(unify_date_format(extract_from_csv()))

    # test = {'lat': 39.9067061, 'lon': -86.05705019999999, 'tz': '-05:00', 'date': '2020-12-31', 'units': 'standard', 'cloud_cover': {'afternoon': 75.0}, 'humidity': {'afternoon': 92.0}, 'precipitation': {'total': 0.0}, 'temperature': {'min': 269.14, 'max': 271.97, 'afternoon': 270.4, 'night': 271.97, 'evening': 270.68, 'morning': 269.14}, 'pressure': {'afternoon': 1026.0}, 'wind': {'max': {'speed': 5.1, 'direction': 310.0}}}
    # print(flatten(test))
