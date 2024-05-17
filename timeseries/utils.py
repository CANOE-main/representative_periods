"""
Various tools
Written by Ian David Elder for the TEMOA Canada / CANOE model
"""


import os
import shutil
from openpyxl import load_workbook
import sqlite3
import pandas as pd
import requests
import xmltodict
import urllib.request
import zipfile
import datetime
import pytz
import pickle



# Identify existing or tech variants
def is_exs(tech: str) -> bool: return tech.endswith('-EXS')



def fill_references_table():

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()

    references = set()

    # Get all tables
    all_tables = [fetch[0] for fetch in curs.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]

    for table in all_tables:
        if table == 'references': continue

        # For any table with a reference column
        cols = [description[0] for description in curs.execute(f"SELECT * FROM '{table}'").description]
        if 'reference' in cols:

            # Get all the unique references and add them to the set
            refs = curs.execute(f"SELECT DISTINCT reference FROM '{table}' WHERE length(reference) > 1")
            for ref in refs:
                for r in ref[0].split('; '):
                    references.add(r)

    # Add all references in the set to the references tables
    for reference in references:
        if len(reference) > 1: curs.execute(f"REPLACE INTO 'references'(reference) VALUES('{reference}')")

    conn.commit()
    conn.close()



# Cleans up strings for filenames, databases, etc.
def string_cleaner(string):

    return ''.join(letter for letter in string if letter in '- /()–' or letter.isalnum())



def string_letters(string):

    return ''.join(letter for letter in string_cleaner(string) if letter not in '123456789')



def clean_index(df):

    df.index = [string_letters(idx).lower() for idx in df.index]


# Converts the timezone of a dataframe then shifts rows around so that row 0 is hour 0 again
def realign_timezone(df: pd.DataFrame, from_timezone:str=None, to_timezone:str=None, from_utc_offset:int=None, to_utc_offset:int=None, time_col=None):

    df_shifted = df.copy()

    # Get the timestamp column or assume it is the index if not specified
    if time_col is None:
        time = pd.to_datetime(df_shifted.index)
        df_shifted.index = time
    else:
        time = pd.DatetimeIndex(pd.to_datetime(df_shifted[time_col]))
        df_shifted[time_col] = time

    # Get the original timezone, if specified that first otherwise from the data itself
    if from_timezone is not None: tz = from_timezone
    elif from_utc_offset is not None: tz = pytz.FixedOffset(from_utc_offset*60)
    else: tz = time.tz

    if tz is None: raise Exception("Could not identify the original timezone. Try specifying one instead.")

    # Localise if not already timezone aware
    if time.tzinfo is None: time = time.tz_localize(tz)

    # Convert to base timezone
    if to_timezone is not None: new_tz = to_timezone
    elif to_utc_offset is not None: new_tz = tz = pytz.FixedOffset(to_utc_offset*60)
    else: new_tz = 'EST'
    new_time = time.tz_convert(new_tz)

    # Find where the zeroeth hour ended up
    zero_hour = new_time[(new_time.month == 1) & (new_time.day == 1) & (new_time.time == datetime.time(0,0))]
    if len(zero_hour) == 0: zero_hour = new_time[new_time.time == datetime.time(0,0)] # workaround in case we have 8760 hours of a leap year (8784)
    n_shift = new_time.get_loc(zero_hour[-1]) # [-1] as there is only one value when things are working properly but take last in leap year workaround

    if n_shift == 0: return df_shifted # already aligned

    # Update the time column
    if time_col is None: df_shifted.index = new_time
    else: df_shifted[time_col] = new_time

    # Rearrange the hours so it starts at 00:00 in this new timezone, depending on which end of the year rolled over
    df_shifted = pd.concat([df_shifted.iloc[n_shift:], df_shifted.iloc[0:n_shift]])

    return df_shifted
    