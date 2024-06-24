import pandas as pd

gun_violence = pd.read_csv('../../data/dimensions/gun_violence_fact.csv')
gun_ownership = pd.read_csv('../../data/dimensions/gun_ownership_dim.csv')
time = pd.read_csv('../../data/dimensions/time_dim.csv')

gun_violence_gun_ownership = pd.merge(gun_violence, gun_ownership, left_on=['State', 'Incident_year'], right_on=['State', 'Year'], how="inner")

all_dimensions = pd.merge(gun_violence_gun_ownership, time, left_on=['Incident Date'], right_on=['Date'], how='inner')

all_dimensions.to_csv('../../data/aggregators/add_dimensions.csv')
print(all_dimensions)
