import pandas as pd
from create_gun_violence_fact import get_gun_violence
from is_city import is_city
import numpy as np


def get_date(to_csv=False):
    gun_violence = get_gun_violence()
    addresses = gun_violence[['Incident ID', 'State', 'City Or County', 'Address']]
    addresses['City'] = addresses['City Or County'].apply(lambda x: x if is_city(x) else None)
    addresses['County'] = np.where(addresses['City'], None, addresses['City Or County'])
    # addresses['County'] = addresses['City Or County'].apply(lambda x: x if not is_city(x) else None)

    if to_csv:
        csv_name = 'address_dim.csv'
        addresses.to_csv(f'../../data/dimensions/{csv_name}', index=False)

    return addresses

get_date(to_csv=True)