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
    # 'ninja-weather-country-CA-irradiance_surface_pop_wtd-merra2.csv',
    # 'ninja-weather-country-CA-precipitation_pop_wtd-merra2.csv',
    # 'ninja-weather-country-CA-temperature_pop_wtd-merra2.csv',
    # 'ninja-weather-country-CA-wind_speed_pop_wtd-merra2.csv',
    'ninja-weather-country-CA-humidity_pop_wtd-merra2.csv',
    'ninja-weather-country-CA-cloud_cover_pop_wtd-merra2.csv'
]

regions = {
    'AB': 'ab',
    'BC': 'bc',
    'MB': 'mb',
    'NB': 'nb',
    'NF': 'nllab',
    'NS': 'ns',
    'ON': 'on',
    'PE': 'pei',
    'QC': 'qc',
    'SK': 'sk',
}

for csv in csvs:
    for rn_region, region in regions.items():

        df = pd.read_csv('timeseries/'+csv, skiprows=3, index_col=0)['CA.'+rn_region]
        df = utils.realign_timezone(df, 'UTC', 'EST')
        df = df.loc[df.index.year == 2018]
        df = df.loc[(df.index.dayofyear != 1) & (df.index.dayofyear != 365)]

        name = region + '_' + csv.split("-")[4][0:-8]
        df.name = name
        print(region, name)

        dir = f'timeseries/{region}'
        if not os.path.isdir(dir):
            os.mkdir(dir)

        df.to_csv(f"{dir}/{name}.csv")