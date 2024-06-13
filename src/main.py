from operator import index
from pathlib import Path
import os
import re
import numpy as np
from typing import Dict, Iterable, List
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import pandas as pd
import requests


data_dict = Variable.get("data", deserialize_json=True)


@task
def extract_from_combined_csv(file_path: str | Path, drop_columns: List[str]) -> pd.DataFrame:
    return_df = pd.read_csv(str(file_path))
    return_df.drop(colums=drop_columns, in_place=True)

    return return_df

@task
def extract_from_csv(paths: Iterable[str] | Iterable[Path],
                     patterns: Iterable[str], extensions: Iterable[str], drop_columns: List[str]) -> pd.DataFrame:

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

    return_df.drop(colums=drop_columns, in_place=True)

    return return_df


@task
def unify_date_format(extracted_data: pd.DataFrame) -> pd.DataFrame:
    pass


@task
def get_coordinates(extracted_data: pd.DataFrame) -> pd.DataFrame:
    locations = extracted_data["Address"]
    coords_lat = np.zeros([len(locations.index), 1])
    coords_lon = np.zeros([len(locations.index), 1])

    for location_idx in locations.index:
        if " and " in locations:
            continue
 
        google_payload = {"textQuery": locations.loc[location_idx]}
        google_url = 'https://places.googleapis.com/v1/places:searchText'
        google_headers = {'Content-Type': 'application/json',
                          # 'X-Goog-FieldMask': 'places.displayName,places.location',
                          'X-Goog-FieldMask': 'places.location',
                          'X-Goog-Api-Key': Variable.get("google_maps_api_key")}
        resp = requests.post(url=google_url, data=google_payload, headers=google_headers)
        if resp.status_code == 200:
            temp_location = resp.json()["places"][0]["location"]
            coords_lat[location_idx] = [temp_location["latitude"]]
            coords_lon[location_idx] = [temp_location["longitude"]]
    extracted_data["Lat"] = coords_lat
    extracted_data["Lon"] = coords_lon

    return extracted_data

@task(multiple_outputs=True)
def extract_weather(extracted_data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    weather_df = pd.DataFrame(columns=["lat", "lon", "tr", "date", "cloud_cover", "humidity", "precipitation", "pressure", "temperature", "wind_speed", "wind_direction"])
    lats = extracted_data["Lat"].values
    lons = extracted_data["Lon"].values
    
    for data_idx in extracted_data.index:
        weather_url = f'https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lats[data_idx]}&lon={lons[data_idx]}&date={extracted_data["Incident Date"]}&appid={Values.get("openweather_api_key")}'
        resp = requests.post(url=weather_url)

        if resp.status_code == 200:
            pass

    return {"org": extracted_data, "weather": weather_df}

