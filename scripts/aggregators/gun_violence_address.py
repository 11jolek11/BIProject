import pandas as pd

gun_violence = pd.read_csv('../../data/dimensions/gun_violence_fact.csv')
address = pd.read_csv('../../data/dimensions/address_dim.csv')

gun_violence_address = pd.merge(gun_violence, address, left_on=['Incident ID'], right_on=['Incident ID'], how="inner", suffixes=('', '_y'))
gun_violence_address.drop(gun_violence_address.filter(regex='_y$').columns, axis=1, inplace=True)

gun_violence_address.to_csv('../../data/aggregators/gun_violence_address.csv', index=False)
print(gun_violence_address)
