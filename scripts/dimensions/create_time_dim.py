import pandas as pd
import holidays

def get_date_dim(to_csv=False):
    file_gun_violence = '../../data/combined.csv'
    gun_violence_dates = pd.read_csv(file_gun_violence)['Incident Date']
    dates = []
    for date in gun_violence_dates.iloc[[0, -1]]:
        dates.append(date.split(" ")[0])

    date_range = pd.date_range(start=dates[0], end=dates[1])

    df = pd.DataFrame(date_range, columns=['Date'])

    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['Day'] = df['Date'].dt.day
    df['DayOfWeek'] = df['Date'].dt.dayofweek
    df['Decade'] = (df['Year'] // 10) * 10
    df['Quarter'] = df['Date'].dt.quarter
    df['DayOfYear'] = df['Date'].dt.dayofyear
    df['WeekOfYear'] = df['Date'].dt.isocalendar().week
    df['DayName'] = df['Date'].dt.day_name()
    df['MonthName'] = df['Date'].dt.month_name()
    df['IsWeekend'] = df['Date'].dt.dayofweek >= 5

    us_holidays = holidays.US()
    df['IsHoliday'] = df['Date'].isin(us_holidays)

    if to_csv:
        csv_name = 'time_dim.csv'
        df.to_csv(f'../../data/dimensions/{csv_name}')

    return df

# print(get_date_dim())
get_date_dim(to_csv=True)