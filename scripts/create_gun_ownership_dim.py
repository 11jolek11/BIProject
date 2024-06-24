import pandas as pd


def get_gun_ownership():
    file_gun_ownership = '../data/ownership.csv'

    gun_ownership = pd.read_csv(file_gun_ownership, delimiter=';')
    gun_ownership = gun_ownership[['Year', 'STATE', 'permit']]
    gun_ownership = gun_ownership.rename(columns={"STATE": "State"})
    gun_ownership['Year'] = gun_ownership['Year'].astype(int)

    return gun_ownership