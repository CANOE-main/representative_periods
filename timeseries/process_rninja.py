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
    'renewables_ninja_country_CA_cloud-cover_merra-2_pop-wtd.csv',
    'renewables_ninja_country_CA_humidity_merra-2_pop-wtd.csv',
    'renewables_ninja_country_CA_irradiance-surface_merra-2_pop-wtd.csv',
    'renewables_ninja_country_CA_precipitation_merra-2_pop-wtd.csv',
    'renewables_ninja_country_CA_temperature_merra-2_pop-wtd.csv',
    'renewables_ninja_country_CA_wind-speed_merra-2_pop-wtd.csv'
]

for csv in csvs:

    df = pd.read_csv('renewables_ninja_country_CA_cloud-cover_merra-2_pop-wtd.csv', skiprows=3, index_col=0)['CA.ON']
    df = utils.realign_timezone(df, 'UTC', 'EST')
    df = df.loc[df.index.year == 2018]
    df = df.loc[(df.index.dayofyear != 1) & (df.index.dayofyear != 365)]

    name = csv.split("_")[4].replace("-","_")

    df.name = name

    print(name)

    df.to_csv(f"{name}.csv")