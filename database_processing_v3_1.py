"""
Aligns a Temoa database with representative days configured in days.csv
"""

import sqlite3
import os
import pandas as pd
import utils
import sys
import math

this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
input_dir = input_dir = this_dir + "input_sqlite/"
output_dir = this_dir + "output_sqlite/"

schema = this_dir + "canoe_schema_v3_1.sql"

df_period: pd.DataFrame
initialised = False

# Need to copy these over first (and in order)
index_tables = {
    'DataSet',
    'DataSource',
    'CommodityType',
    'Commodity',
    'Region',
    'Technology',
    'TimeOfDay',
    'TimePeriod',
}

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
season_tables = {
    'DemandSpecificDistribution',
    'CapacityFactorTech',
    'CapacityFactorProcess',
    'EfficiencyVariable',
    'LimitSeasonalCapacityFactor',
    'LimitStorageLevelFraction',
    'ReserveCapacityDerate',
    'SeasonLabel',
}


def init():

    global df_period, df_sequence, initialised
    if initialised: return

    df_period = pd.read_csv(this_dir + "periods.csv", index_col=0)

    df_sequence = pd.read_csv(this_dir + "sequence.csv", index_col=0)
    change_points = df_sequence['period'] != df_sequence['period'].shift()
    group_id = change_points.cumsum()
    collapsed = df_sequence.groupby(group_id, as_index=False).agg({'period': 'first'})
    collapsed['count'] = df_sequence.groupby(group_id).size().values
    df_sequence = collapsed

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

    if utils.config['disaggregate_multiday']: n_hours = 24
    else: n_hours = 24*utils.config['days_per_period']
    
    if n_hours < 100: hours = [utils.stringify_hour(hour+1) for hour in range(n_hours)]
    else: hours = [utils.stringify_day(hour+1).replace("D","H") for hour in range(n_hours)]

    if utils.config['days_per_period'] == 1 or utils.config['disaggregate_multiday']: process_single_day_period(database, hours)
    elif utils.config['days_per_period'] > 1:
        print("Multiday periods are not currently supported by Temoa. Turn on dissaggregate_multiday.")
        return
        #process_multiday_period(db_file, hours)



def process_single_day_period(database: str, hours: list):

    out_file = output_dir + database + f"_{len(df_period)}d.sqlite"

    # Check if database exists or needs to be built
    build_db = not os.path.exists(out_file)
    
    # Connect to the new database file
    conn = sqlite3.connect(out_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    # Build the database if it doesn't exist. Otherwise clear all data if forced
    if build_db: curs.executescript(open(schema, 'r').read())
    else:
        tables = [t[0] for t in curs.execute("""SELECT name FROM sqlite_master WHERE type='table';""").fetchall()]
        for table in tables: curs.execute(f"DELETE FROM '{table}'")
        curs.executescript(open(schema, 'r').read())

    conn.commit()
    conn.execute(f"ATTACH DATABASE '{input_dir + database + '.sqlite'}' AS dbin") # Attach the input database
    conn.execute('PRAGMA foreign_keys = 0;') # Turn off foreign keys while copying over

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

    total_days = df_period['weight'].sum()
    curs.execute(f"REPLACE INTO MetaData VALUES('days_per_period', {total_days}, 'count of days in each period')")

    for year in utils.config['model_years']:
        for i, (period, weight) in enumerate(df_period.iterrows()):
            for hour in hours:

                # TimeSegmentFraction
                curs.execute(f"""REPLACE INTO
                            TimeSegmentFraction(period, season, tod, segfrac, notes)
                            VALUES({year}, '{period}', '{hour}', {weight.iloc[0] / (24 * total_days)}, "Weight from clustering")""")
            
            # TimeSeason
            curs.execute(f"""REPLACE INTO
                        TimeSeason(period, sequence, season)
                        VALUES({year}, {i}, '{period}')""")

        # TimeSeasonSequential
        for i, row in df_sequence.iterrows():
            zeros = math.floor(math.log10(len(df_sequence))) - (0 if i==0 else math.floor(math.log10(i)))
            period_seq = f"S{'0'*zeros}{i}"
            curs.execute(f"""REPLACE INTO
                        TimeSeasonSequential(period, sequence, seas_seq, season, num_days, notes)
                        VALUES({year}, {i}, '{period_seq}', '{row['period']}', {row['count']}, 'Reconstructed original year from clustering')""")
            
    # TimeOfDay
    for h in range(24):
        tod = f"H0{h+1}" if h+1 < 10 else f"H{h+1}"
        curs.execute(f'REPLACE INTO TimeOfDay(sequence, tod) VALUES({h+1}, "{tod}")')

    # DemandSpecificDistribution
    # This is renormalised to sum to 1 below
    for period, weight in df_period.iterrows():
        curs.execute(f"""UPDATE DemandSpecificDistribution
                    SET dsd = dsd * {weight.iloc[0]}
                    WHERE season == '{period}'""")
        
    # Renormalise DSD
    df_dsd = pd.read_sql_query("SELECT * FROM DemandSpecificDistribution", conn)
    df_dsd = df_dsd.groupby(['region','period','demand_name'])
    for rpd in df_dsd.groups:

        # Drop threshold lower percentile
        df = df_dsd.get_group(rpd).sort_values('dsd').reset_index()

        # This is a safety net for low numbers of clusters where you might catch
        # a day with zero demand throughout, which is not normalisable
        if df['dsd'].sum() == 0:
            print(
                f"There was no DSD remaining for demand {rpd}! "
                "Filling with flatline demand for now but different periods "
                "should be used!"
            )
            flatline_fill = 1 / len(df)
            df['dsd'] = flatline_fill
            curs.execute(
                f"""UPDATE DemandSpecificDistribution 
                SET dsd = {flatline_fill}
                WHERE region = '{rpd[0]}' 
                AND period = '{rpd[1]}' 
                AND demand_name == '{rpd[2]}'"""
            )

        # Get a running proportion sum of DSD
        df['run_sum'] = df['dsd'].cumsum()/df['dsd'].sum()
        # Get the smallest DSD above thresh to zero out actual table
        thresh_dsd = df['dsd'].loc[df['run_sum'] < utils.config['dsd_threshold']].max()
        
        # There might be nothing under the threshold if using few rep days
        if not pd.isna(thresh_dsd):
            # Set to zero where the proportion exceeds the threshold
            # If there are duplicate dsd values on the threshold these are all left in
            # This leaves everything in in the case of a flatline demand
            df['dsd'] = df['dsd'].where(df['dsd'] >= thresh_dsd, 0)
            
            curs.execute(
                f"""UPDATE DemandSpecificDistribution 
                SET dsd = 0 
                WHERE region = '{rpd[0]}' 
                AND period = '{rpd[1]}' 
                AND demand_name == '{rpd[2]}' 
                AND dsd < {thresh_dsd}"""
            )

        # Renormalise
        total_dsd = df['dsd'].sum()
        curs.execute(f"""UPDATE DemandSpecificDistribution
                    SET dsd = dsd / {total_dsd}
                    WHERE region = '{rpd[0]}'
                    AND period = '{rpd[1]}'
                    AND demand_name == '{rpd[2]}'""")
        
        # If preserving absolute hourly values, adjust annual demand to sum of representative periods
        if utils.config['demand_preservation'] == 'hourly':
            curs.execute(f"""UPDATE Demand SET demand = demand * {total_dsd}
                        WHERE region = '{rpd[0]}'
                        AND period = '{rpd[1]}'
                        AND commodity == '{rpd[2]}'""")

    conn.commit()

    conn.execute("VACUUM;")
    conn.commit()

    conn.execute('PRAGMA FOREIGN_KEYS=1;')
    try:
        data = conn.execute('PRAGMA FOREIGN_KEY_CHECK;').fetchall()
        if data:
            for row in data:
                print(f'{row}')
            print('(Table, Row ID, Reference Table, (fkid) )')
            print(f'The above foreign keys failed to validate for {out_file}')
    except sqlite3.OperationalError as e:
        print(f'Foreign keys failed on activation for {out_file}. Something may be wrong with the schema.')
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