"""
Aligns a Temoa database with representative days configured in days.csv
"""

import sqlite3
import os
import pandas as pd
import shutil
import utils
import sys

this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
input_dir = this_dir + "input_sqlite/"
output_dir = this_dir + "output_sqlite/"

schema = this_dir + "canoe_schema_v3_1.sql"

df_period: pd.DataFrame
initialised = False

# Need to copy these over first (and in order)
index_tables = [
    'DataSet',
    'DataSource',
    'CommodityType',
    'Commodity',
    'Region',
    'SeasonLabel',
    'Technology',
    'TimeOfDay',
    'TimePeriod',
]

direct_copy_tables = {
    'CapacityCredit',
    'CapacityToActivity',
    'ConstructionInput',
    'CostEmission',
    'CostFixed',
    'CostInvest',
    'CostVariable',
    'Demand',
    'Efficiency',
    'EmissionActivity',
    'EmissionEmbodied',
    'EmissionEndOfLife',
    'EndOfLifeOutput',
    'ExistingCapacity',
    'LifetimeProcess',
    'LifetimeSurvivalCurve',
    'LifetimeTech',
    'LimitActivity',
    'LimitActivityShare',
    'LimitAnnualCapacityFactor',
    'LimitCapacity',
    'LimitCapacityShare',
    'LimitDegrowthCapacity',
    'LimitDegrowthNewCapacity',
    'LimitDegrowthNewCapacityDelta',
    'LimitNewCapacity',
    'LimitNewCapacityShare',
    'LimitResource',
    'LimitTechInputSplit',
    'LimitTechInputSplitAnnual',
    'LimitTechOutputSplit',
    'LimitTechOutputSplitAnnual',
    'LinkedTech',
    'LoanLifetimeProcess',
    'LoanRate',
    'PlanningReserveMargin',
    'RampDownHourly',
    'RampUpHourly',
    'RPSRequirement',
    'StorageDuration',
    'TechGroup',
    'TechGroupMember',
}

# For season tables, only copy where the season is in the rep day set
season_tables = [
    'DemandSpecificDistribution',
    'CapacityFactorTech',
    'CapacityFactorProcess',
    'EfficiencyVariable',
    'LimitSeasonalCapacityFactor',
    'LimitStorageLevelFraction',
    'ReserveCapacityDerate',
]


def init():

    global df_period, initialised
    if initialised: return

    df_period = pd.read_csv(this_dir + "periods.csv", index_col=0).astype(float)
    df_period['weight'] = df_period['weight'] / df_period['weight'].sum()

    # Split e.g. D001-D003 into D001, D002, D003
    if utils.config['disaggregate_multiday'] and utils.config['days_per_period'] > 1:
        for period, wgt in df_period.iterrows():
            days = period_to_days(period)
            weight = wgt.iloc[0] / len(days)

            for day in days:
                df_period.loc[day, 'weight'] = weight

            df_period = df_period.drop(period, axis='index')

    print("\nApplying the following periods to v3.1 databases:\n")
    print(df_period)

    initialised = True
    print("\nInitialised database processing.\n")



def process_all():
    init()
    databases = _get_sqlite_databases()
    for database in databases: process_database(database)
    print("\nFinished.\n")



def process_database(database: str):

    if _get_schema_version(database) != (3, 1): return

    init()

    print(f"Processing {database}...")

    # Get database files
    db_file = f"{database}.sqlite"

    if utils.config['disaggregate_multiday']: n_hours = 24
    else: n_hours = 24*utils.config['days_per_period']
    
    if n_hours < 100: hours = [utils.stringify_hour(hour+1) for hour in range(n_hours)]
    else: hours = [utils.stringify_day(hour+1).replace("D","H") for hour in range(n_hours)]

    if utils.config['days_per_period'] == 1 or utils.config['disaggregate_multiday']: process_single_day_period(db_file, hours)
    elif utils.config['days_per_period'] > 1:
        print("Multiday periods are not currently supported by Temoa. Turn on dissaggregate_multiday.")
        return
        #process_multiday_period(db_file, hours)



def process_single_day_period(db_file, hours: list):

    conn = sqlite3.connect(output_dir + db_file)
    curs = conn.cursor()
    # db_in = sqlite3.connect(input_dir + db_file)
    conn.execute(f"ATTACH DATABASE '{input_dir + db_file}' AS dbin")
    
    curs.executescript(open(schema, 'r').read())
    conn.commit()

    conn.execute('PRAGMA foreign_keys = 0;')

    in_tables = [t[0] for t in curs.execute("SELECT name FROM dbin.sqlite_master WHERE type='table';").fetchall()]
    
    for table in index_tables:
        if table not in in_tables: continue
        cols = str([row[1] for row in curs.execute(f"PRAGMA table_info({table})").fetchall()])[1:-1].replace("'","")
        curs.execute(f"REPLACE INTO main.{table}({cols}) SELECT {cols} FROM dbin.{table}")

    for table in direct_copy_tables:
        if table not in in_tables: continue # might be a db variant without the table
        cols = str([row[1] for row in curs.execute(f"PRAGMA table_info({table})").fetchall()])[1:-1].replace("'","")
        curs.execute(f"REPLACE INTO main.{table}({cols}) SELECT {cols} FROM dbin.{table}")

    periods = tuple(df_period.index.unique())
    for table in season_tables:
        if table not in in_tables: continue # might be a db variant without the table
        cols = str([row[1] for row in curs.execute(f"PRAGMA table_info({table})").fetchall()])[1:-1].replace("'","")
        curs.execute(f"REPLACE INTO main.{table}({cols}) SELECT {cols} FROM dbin.{table} WHERE season IN {periods}")

    # DemandSpecificDistribution
    for period, weight in df_period.iterrows():
        curs.execute(f"""UPDATE DemandSpecificDistribution
                    SET dsd = dsd * {weight.iloc[0]} * 365
                    WHERE season == '{period}'""")

    for year in utils.config['model_years']:
        for i, (period, weight) in enumerate(df_period.iterrows()):
            for hour in hours:

                # TimeSegmentFraction
                curs.execute(f"""REPLACE INTO
                            TimeSegmentFraction(period, season, tod, segfrac, notes)
                            VALUES({year}, '{period}', '{hour}', {weight.iloc[0] / 24}, "Weight from clustering")""")
            
            # TimeSeason
            curs.execute(f"""REPLACE INTO
                        TimeSeason(period, sequence, season)
                        VALUES({year}, {i}, '{period}')""")

            # TimeSeasonSequential
            # TODO
        
    # Renormalise DSD
    df_dsd = pd.read_sql_query("SELECT * FROM DemandSpecificDistribution", conn)
    df_dsd = df_dsd.groupby(['period','region','demand_name'])
    for grp in df_dsd.groups:
        total_dsd = df_dsd.get_group(grp)['dsd'].sum()
        curs.execute(f"""UPDATE DemandSpecificDistribution
                    SET dsd = dsd / {total_dsd}
                    WHERE period = '{grp[0]}'
                    AND region = '{grp[1]}'
                    AND demand_name == '{grp[2]}'""")
        
        # If preserving absolute hourly values, adjust annual totals to sum of clustered periods
        if utils.config['demand_preservation'] == 'hourly':
            curs.execute(f"""UPDATE Demand SET demand = demand * {total_dsd}
                        WHERE region = '{grp[0]}'
                        AND commodity == '{grp[1]}'""")

    conn.commit()

    conn.execute("VACUUM;")
    conn.commit()

    conn.execute('PRAGMA FOREIGN_KEYS=1;')
    try:
        data = conn.execute('PRAGMA FOREIGN_KEY_CHECK;').fetchall()
        if data:
            print(f'The following foreign keys failed to validate for {db_file}:')
            print('(Table, Row ID, Reference Table, (fkid) )')
            for row in data:
                print(f'{row}')
    except sqlite3.OperationalError as e:
        print(f'Foreign keys failed on activation for {db_file}. Something may be wrong with the schema.')
        print(e)

    conn.close()



# Collects sqlite databases into a dictionary of form {name: path}
def _get_sqlite_databases():

    databases = []

    for dirs in os.walk(input_dir):
        files = dirs[2]

        for file in files:
            split = os.path.splitext(file)
            if split[1] == '.sqlite': databases.append(split[0])

    return databases



def _get_schema_version(database):

    conn = sqlite3.connect(input_dir + f"{database}.sqlite")
    curs = conn.cursor()

    tables = {t[0] for t in curs.execute("SELECT name FROM sqlite_schema").fetchall()}
    if 'MetaData' not in tables:
        print(f"Could not get schema version for {database}. Skipped.")
        return 0

    mj_vers = curs.execute("SELECT value FROM MetaData WHERE element == 'DB_MAJOR'").fetchone()[0]
    mn_vers = curs.execute("SELECT value FROM MetaData WHERE element == 'DB_MINOR'").fetchone()[0]

    return mj_vers, mn_vers



def period_to_days(period: str):

    if "-" not in period: return (period)
    else:
        days = [utils.destringify_day(day) for day in period.split("-")]
        days = [utils.stringify_day(day) for day in range(days[0],days[1]+1,1)]
        return tuple(days)



if __name__ == "__main__":

    if len(sys.argv) <= 1: process_all()
    else:
        process_database(sys.argv[1])
        print("Finished.")