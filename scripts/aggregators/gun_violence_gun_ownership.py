import pandas as pd
# from create_gun_ownership_dim import get_gun_ownership
# from create_gun_violence_fact import get_gun_violence

# gun_violence = get_gun_violence()
# gun_ownership = get_gun_ownership()

gun_violence = pd.read_csv('../../data/dimensions/gun_violence_fact.csv')
gun_ownership = pd.read_csv('../../data/dimensions/gun_ownership_dim.csv')

gun_violence_gun_ownership = pd.merge(gun_violence, gun_ownership, left_on=['State', 'Incident_year'], right_on=['State', 'Year'], how="inner")

gun_violence_gun_ownership.to_csv('../../data/aggregators/gun_violence_gun_ownership.csv')
print(gun_violence_gun_ownership)
