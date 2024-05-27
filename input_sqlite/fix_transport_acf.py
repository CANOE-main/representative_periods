import sqlite3
import pandas as pd

conn = sqlite3.connect('canoe_8760.sqlite')
curs = conn.cursor()

model_periods = [2021, 2025, 2030, 2035, 2040, 2045, 2050]

for table in ['MaxAnnualCapacityFactor','MinAnnualCapacityFactor']:
    print(table)
    df = pd.read_sql_query(f"SELECT * FROM {table} WHERE tech LIKE 'T_%_N'", conn)
    df = df.groupby('tech')
    dfs = []
    for grp in df.groups:
        group = df.get_group(grp)
        row = group.iloc[0]
        periods = group['period'].values
        rows = []
        missing = [period for period in model_periods if period not in periods]
        if len(missing) == 0: continue
        for period in missing:
            row_2 = row.copy()
            row_2['period'] = period
            rows.append(row_2.to_frame().T)
        df2: pd.DataFrame = pd.concat(rows, axis='rows')
        dfs.append(df2)

    if len(dfs) == 0: continue
    df = pd.concat(dfs, axis='index')
    print(df)
    df.to_sql(table, conn, if_exists='append', index=False)