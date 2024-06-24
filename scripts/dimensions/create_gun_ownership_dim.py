import pandas as pd


def get_gun_ownership(to_csv=False):
    file_gun_ownership = '../../data/ownership.csv'

    gun_ownership = pd.read_csv(file_gun_ownership, delimiter=';')
    gun_ownership = gun_ownership[['Year', 'STATE', 'permit']]
    gun_ownership = gun_ownership.rename(columns={"STATE": "State"})
    gun_ownership['Year'] = gun_ownership['Year'].astype(int)

    states = gun_ownership['State'].unique()
    years = list(range(gun_ownership['Year'].min(), 2025))
    all_combinations = pd.MultiIndex.from_product([years, states], names=['Year', 'State']).to_frame(index=False)
    gun_ownership = all_combinations.merge(gun_ownership, on=['Year', 'State'], how='left')
    gun_ownership['permit'] = gun_ownership.groupby('State')['permit'].ffill().bfill()

    if to_csv:
        csv_name = 'gun_ownership_dim.csv'
        gun_ownership.to_csv(f'../../data/dimensions/{csv_name}', index=False)

    return gun_ownership


get_gun_ownership(to_csv=True)