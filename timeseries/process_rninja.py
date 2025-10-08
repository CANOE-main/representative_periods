import utils
import os
import pandas as pd


def get_csvs():
    this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
    filenames = os.listdir(this_dir)
    return [filename
            for filename in filenames
            if filename.endswith('csv')
            and filename.startswith('renewables_ninja_country_CA')]

csvs = [
    'ninja-weather-country-CA-cloud_cover_pop_wtd-merra2.csv',
    'ninja-weather-country-CA-humidity_pop_wtd-merra2.csv',
    'ninja-weather-country-CA-irradiance_surface_pop_wtd-merra2.csv',
    'ninja-weather-country-CA-precipitation_pop_wtd-merra2.csv',
    'ninja-weather-country-CA-temperature_pop_wtd-merra2.csv',
    'ninja-weather-country-CA-wind_speed_pop_wtd-merra2.csv'
]

for csv in csvs:

    df = pd.read_csv('timeseries/'+csv, skiprows=3, index_col=0)['CA.ON']
    df = utils.realign_timezone(df, 'UTC', 'EST')
    df = df.loc[df.index.year == 2018]
    df = df.loc[(df.index.dayofyear != 1) & (df.index.dayofyear != 365)]

    name = csv.split("-")[4].split('_')[-3]

    df.name = name

    print(name)

    df.to_csv(f"{name}.csv")