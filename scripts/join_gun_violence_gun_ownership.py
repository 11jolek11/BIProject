import pandas as pd
from create_gun_ownership_dim import get_gun_ownership
from create_gun_violence_fact import get_gun_violence

gun_violence = get_gun_violence()
gun_ownership = get_gun_ownership()

states = gun_ownership['State'].unique()
years = list(range(gun_ownership['Year'].min(), 2025))
all_combinations = pd.MultiIndex.from_product([years, states], names=['Year', 'State']).to_frame(index=False)
gun_ownership = all_combinations.merge(gun_ownership, on=['Year', 'State'], how='left')
gun_ownership['permit'] = gun_ownership.groupby('State')['permit'].ffill().bfill()

inner_gun_violence = pd.merge(gun_violence, gun_ownership, left_on=['State', 'Incident_year'], right_on=['State', 'Year'], how="inner")

print(inner_gun_violence)
