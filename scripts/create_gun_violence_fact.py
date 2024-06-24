import pandas as pd


def get_gun_violence():
    def strip_date(date):
        return pd.to_datetime(date).strftime('%Y-%m-%d')

    file_gun_violence = '../data/combined.csv'
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
    return gun_violence