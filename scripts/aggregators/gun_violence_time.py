import pandas as pd

gun_violence = pd.read_csv('../../data/dimensions/gun_violence_fact.csv')
time = pd.read_csv('../../data/dimensions/time_dim.csv')

gun_violence_time = pd.merge(gun_violence, time, left_on=['Incident Date'], right_on=['Date'], how="inner", suffixes=('', '_y'))
gun_violence_time.drop(gun_violence_time.filter(regex='_y$').columns, axis=1, inplace=True)

gun_violence_time.to_csv('../../data/aggregators/gun_violence_time.csv', index=False)
print(gun_violence_time)