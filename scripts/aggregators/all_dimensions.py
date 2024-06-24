import pandas as pd

gun_violence = pd.read_csv('../../data/dimensions/gun_violence_fact.csv')
gun_ownership = pd.read_csv('../../data/dimensions/gun_ownership_dim.csv')
time = pd.read_csv('../../data/dimensions/time_dim.csv')
address = pd.read_csv('../../data/dimensions/address_dim.csv')

gun_violence_gun_ownership = pd.merge(gun_violence, gun_ownership, left_on=['State', 'Incident_year'], right_on=['State', 'Year'], how="inner", suffixes=('', '_y'))
gun_violence_gun_ownership.drop(gun_violence_gun_ownership.filter(regex='_y$').columns, axis=1, inplace=True)

gun_violence_gun_ownership_address = pd.merge(gun_violence_gun_ownership, address, left_on=['Incident ID'], right_on=['Incident ID'], how="inner", suffixes=('', '_y'))
gun_violence_gun_ownership_address.drop(gun_violence_gun_ownership_address.filter(regex='_y$').columns, axis=1, inplace=True)

all_dimensions = pd.merge(gun_violence_gun_ownership_address, time, left_on=['Incident Date'], right_on=['Date'], how='inner', suffixes=('', '_y'))
all_dimensions.drop(all_dimensions.filter(regex='_y$').columns, axis=1, inplace=True)

all_dimensions.to_csv('../../data/aggregators/all_dimensions.csv', index=False)
print(all_dimensions)
