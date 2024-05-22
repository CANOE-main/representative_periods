"""
Aligns a Temoa database with representative days configured in days.csv
"""

import sqlite3
import os
import pandas as pd
import shutil
import utils

this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
input_dir = this_dir + "input_sqlite/"
output_dir = this_dir + "output_sqlite/"

df_period: pd.DataFrame
initialised = False


def init():

    global df_period, initialised
    if initialised: return

    df_period = pd.read_csv(this_dir + "periods.csv", index_col=0).astype(float)
    df_period['weight'] = df_period['weight'] / df_period['weight'].sum()

    if utils.config['disaggregate_multiday'] and utils.config['days_per_period'] > 1:
        for period, wgt in df_period.iterrows():
            days = period_to_days(period)
            weight = wgt.iloc[0] / len(days)

            for day in days:
                df_period.loc[day, 'weight'] = weight

            df_period = df_period.drop(period, axis='index')

    print("\nApplying the following periods to v3 databases:\n")
    print(df_period)

    initialised = True
    print("\nInitialised database processing.\n")



def process_all():

    init()

    databases = _get_sqlite_databases()

    for database in databases:

        if _get_schema_version(database) < 3: continue

        print(f"Processing {database}...")
        process_database(database)

    print("\nFinished.\n")



def process_database(database: str):

    init()

    # Copy the input database to the output directory and connect
    shutil.copy(input_dir + f"{database}.sqlite", output_dir + f"{database}.sqlite", )

    if utils.config['disaggregate_multiday']: n_hours = 24
    else: n_hours = 24*utils.config['days_per_period']
    
    if n_hours < 100: hours = [utils.stringify_hour(hour+1) for hour in range(n_hours)]
    else: hours = [utils.stringify_day(hour+1).replace("D","H") for hour in range(n_hours)]

    if utils.config['days_per_period'] == 1 or utils.config['disaggregate_multiday']: process_single_day_period(database, hours)
    elif utils.config['days_per_period'] > 1: process_multiday_period(database, hours)

    # Vacuum to clean up empty data
    conn = sqlite3.connect(output_dir + f"{database}.sqlite")
    conn.execute("VACUUM;")
    conn.commit()
    conn.close()



def process_multiday_period(database, hours):

    conn = sqlite3.connect(output_dir + f"{database}.sqlite")
    curs = conn.cursor()

    # Tables that reference time season
    season_tables = [
        'DemandSpecificDistribution',
        'CapacityFactorTech',
        'CapacityFactorProcess',
        'MinSeasonalActivity',
        'MaxSeasonalActivity'
    ]

    # Empty the season reference table and add representative days back in
    curs.execute(f"DELETE FROM TimeSeason")
    curs.execute(f"DELETE FROM TimeOfDay")
    curs.execute(f"DELETE FROM TimeSegmentFraction")

    for hour in hours: curs.execute(f"INSERT INTO TimeOfDay(tod) VALUES('{hour}')")

    # Delete unnecessary days for starters
    all_days = []
    for period in df_period.index: 
        for period in period_to_days(period): all_days.append(period)

    for table in season_tables:
        curs.execute(f"DELETE FROM {table} WHERE season NOT IN {tuple(all_days)}")

    for period, weight in df_period.iterrows():

        period_days = period_to_days(period)

        # Aggregate Seasonal Activity tables by new period
        for table in ['MinSeasonalActivity','MaxSeasonalActivity']:
            val_col = table[:3].lower() + '_act'
            df_seas = pd.read_sql_query(f"SELECT * FROM {table} WHERE season IN {period_days}", conn)
            df_seas = df_seas.groupby(['region','period','tech'])
            for grp in df_seas.groups:
                curs.execute(f"""UPDATE {table}
                            SET {val_col} = (SELECT SUM({val_col}) FROM {table} WHERE
                                season IN {period_days}
                                AND region == '{grp[0]}'
                                AND period == {grp[1]}
                                AND tech == '{grp[2]}'),
                            season = '{period}'
                            WHERE season == '{period_days[0]}'""")

        curs.execute(f"INSERT INTO TimeSeason(season) VALUES('{period}')")

        # TimeSegmentFraction
        for hour in hours:
            curs.execute(f"""REPLACE INTO
                        TimeSegmentFraction(season, tod, segfrac, notes)
                        VALUES('{period}', '{hour}', {weight.iloc[0] / len(hours)}, "Weight from clustering")""")
        
        curs.execute(f"""UPDATE DemandSpecificDistribution
                    SET dsd = dsd * {weight.iloc[0]}
                    WHERE season IN {period_days}""")
        
        # Rename days and hours
        for table in ['DemandSpecificDistribution', 'CapacityFactorTech', 'CapacityFactorProcess']:
            
            for d in range(len(period_days)):
                for h in range(24):
                    curs.execute(f"""UPDATE {table}
                                SET tod = '{hours[24*d + h]}'
                                WHERE season == '{period_days[d]}'
                                AND tod == '{utils.stringify_hour(h+1)}'""")
                    
            curs.execute(f"""UPDATE {table}
                        SET season = '{period}'
                        WHERE season IN {period_days}""")
            

    # Delete any seasons that aren't in the representative period
    for table in season_tables:
        curs.execute(f"DELETE FROM {table} WHERE season NOT IN (SELECT season from TimeSeason)")

    # Renormalise DSD
    df_dsd = pd.read_sql_query("SELECT * FROM DemandSpecificDistribution", conn)
    df_dsd = df_dsd.groupby(['region','demand_name'])
    for grp in df_dsd.groups:
        total_dsd = df_dsd.get_group(grp)['dsd'].sum()
        curs.execute(f"""UPDATE DemandSpecificDistribution
                    SET dsd = dsd / {total_dsd}
                    WHERE region = '{grp[0]}'
                    AND demand_name == '{grp[1]}'""")
            
    conn.commit()
    conn.close()



def process_single_day_period(database, hours):

    conn = sqlite3.connect(output_dir + f"{database}.sqlite")
    curs = conn.cursor()

    # Empty the season reference table and add representative days back in
    curs.execute(f"DELETE FROM TimeSeason")
    curs.execute(f"DELETE FROM TimeSegmentFraction")

    # Update TimeSegmentFraction and DSD based on rep day weights
    for period, weight in df_period.iterrows():

        curs.execute(f"INSERT INTO TimeSeason(season) VALUES('{period}')")

        # TimeSegmentFraction
        for hour in hours:
            curs.execute(f"""REPLACE INTO
                        TimeSegmentFraction(season, tod, segfrac, notes)
                        VALUES('{period}', '{hour}', {weight.iloc[0] / 24}, "Weight from clustering")""")
        
        # DemandSpecificDistribution
        curs.execute(f"""UPDATE DemandSpecificDistribution
                    SET dsd = dsd * {weight.iloc[0]}
                    WHERE season == '{period}'""")

    # Delete any seasons that aren't in the representative days
    for table in [
        'DemandSpecificDistribution',
        'CapacityFactorTech',
        'CapacityFactorProcess',
        'MinSeasonalActivity',
        'MaxSeasonalActivity',
    ]:
        curs.execute(f"DELETE FROM {table} WHERE season NOT IN (SELECT season from TimeSeason)")

    # Renormalise DSD
    df_dsd = pd.read_sql_query("SELECT * FROM DemandSpecificDistribution", conn)
    df_dsd = df_dsd.groupby(['region','demand_name'])
    for grp in df_dsd.groups:
        total_dsd = df_dsd.get_group(grp)['dsd'].sum()
        curs.execute(f"""UPDATE DemandSpecificDistribution
                    SET dsd = dsd / {total_dsd}
                    WHERE region = '{grp[0]}'
                    AND demand_name == '{grp[1]}'""")

    conn.commit()
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
    if 'MetaData' not in tables: return 0

    mj_vers = curs.execute("SELECT value FROM MetaData WHERE element == 'DB_MAJOR'").fetchone()[0]

    return mj_vers



def period_to_days(period: str):

    if "-" not in period: return (period)
    else:
    
        days = [utils.destringify_day(day) for day in period.split("-")]
        days = [utils.stringify_day(day) for day in range(days[0],days[1]+1,1)]
        return tuple(days)



if __name__ == "__main__":

    process_all()