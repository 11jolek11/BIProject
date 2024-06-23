import pandas as pd

def strip_date(date):
    return pd.to_datetime(date).strftime('%Y-%m-%d')

file_gun_violence = '../data/combined.csv'
file_gun_ownership = '../data/ownership.csv'
gun_violence = pd.read_csv(file_gun_violence).drop(columns=["Unnamed: 0", 'Operations'])
gun_violence['State'] = gun_violence['State'].replace('District of Columbia', 'Washington')
gun_violence['Incident Date'] = gun_violence['Incident Date'].apply(strip_date)
dates = gun_violence['Incident Date']
years = []
for date in dates:
    year = date.split('-')[0].split(',')[-1].strip()
    years.append(year)

gun_violence.insert(2, "Incident_year", years, True)
gun_violence.rename(columns={"STATE": "State"})
gun_violence['Incident_year'] = gun_violence['Incident_year'].astype(int)


gun_ownership = pd.read_csv(file_gun_ownership, delimiter=';')
gun_ownership = gun_ownership[['Year', 'STATE', 'permit']]
gun_ownership = gun_ownership.rename(columns={"STATE": "State"})
gun_ownership['Year'] = gun_ownership['Year'].astype(int)

states = gun_ownership['State'].unique()
years = list(range(gun_ownership['Year'].min(), 2025))
all_combinations = pd.MultiIndex.from_product([years, states], names=['Year', 'State']).to_frame(index=False)
gun_ownership = all_combinations.merge(gun_ownership, on=['Year', 'State'], how='left')
gun_ownership['permit'] = gun_ownership.groupby('State')['permit'].ffill().bfill()

inner_gun_violence = pd.merge(gun_violence, gun_ownership, left_on=['State', 'Incident_year'], right_on=['State', 'Year'], how="inner")

# print(inner_gun_violence)
