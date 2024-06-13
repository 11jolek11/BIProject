import pandas as pd

def is_city(city):
    cities = pd.read_csv('../data/uscities.csv')['city'].to_list()
    cities = list(map(lambda x: x.lower(), cities))
    return city.lower() in cities