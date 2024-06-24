import pandas as pd

gun_violence = pd.read_csv('../../data/dimensions/gun_violence_fact.csv')
time = pd.read_csv('../../data/dimensions/time_dim.csv')

gun_violence_time = pd.merge(gun_violence, time, left_on=['Incident Date'], right_on=['Date'], how="inner")

gun_violence_time.to_csv('../../data/aggregators/gun_violence_time.csv')
print(gun_violence_time)