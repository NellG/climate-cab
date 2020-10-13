import json
import requests
from datetime import datetime
import math
import psycopg2
import pandas as pd
import numpy as np
import csv


def read_config():
    """Read config file and return dict."""
    with open('/home/ubuntu/data-processing/.owm-config.csv') as infile:
        reader = csv.reader(infile)
        config = {row[0]: row[1] for row in reader}
    return config


def connect_postgres(config):
    """Connect to PostgreSQL and return connection."""
    pgconnect = psycopg2.connect(\
        host = config['pghost'], \
        port = config['pgport'],
        database = config['database'],
        user = config['pguser'],
        password = config['pgpassword'])
    return pgconnect


def get_weather(config):
    """Download weather from API and assemble dataframe."""
    url = 'https://api.openweathermap.org/data/2.5/onecall?' \
        + config['latlon'] \
        + '&units=imperial&appid=' \
        + config['key']
    with requests.get(url, allow_redirects=True) as f:
        forecast = json.loads(f.text)

    fore_table = []
    for h in [forecast['current']] + forecast['hourly'][1:]:
        time = datetime.fromtimestamp(h['dt']+forecast['timezone_offset'])
        precip = h.get('rain', {'1h':0})['1h']
        if precip == 0: prnd = 0
        elif precip <= 0.2: prnd = 0.2
        else: prnd = 1
        prnd = math.ceil(precip*5)/5
        tdry = h['temp']
        trnd = round(tdry, -1)
        day = time.weekday()+1
        hour = time.hour
        fore_table.append([time, tdry, precip, trnd, prnd, day, hour])

    foredf = pd.DataFrame(fore_table, 
        columns=['time', 'tdry', 'precip', 'trnd', 'prnd', 'day', 'hour'])
    return foredf


def calculate_criteria(df):
    """Calculate join criterion for as-of join."""
    df['crit'] = df['hour']*10000 + df['day']*1000 + df['trnd'] + df['prnd']/10
    df.loc[df['prnd'] > 0, 'crit'] = -1*df.loc[df['prnd'] > 0, 'crit']
    df.drop(['hour', 'day', 'prnd', 'trnd'], axis=1, inplace=True)
    df.sort_values('crit', inplace=True)


def get_history_data(pgcursor, table, d_min=1, d_max=7, t_min=-100, t_max=200):
    """Get history data from postgresql database."""
    col_str = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE "
    col_str += "TABLE_NAME = '" + table + "';"
    pgcursor.execute(col_str)
    col_names = pd.DataFrame(pgcursor.fetchall())

    sql_str = "SELECT * FROM " + table + " WHERE day IN ("
    for day in range(d_min, d_max):
        sql_str += str(day) + ", "
    sql_str += str(d_max) + ") AND trnd BETWEEN " 
    sql_str += str(t_min) + " AND " + str(t_max) + ";"
    pgcursor.execute(sql_str)
    histdf = pd.DataFrame(pgcursor.fetchall(), columns=col_names[0].values)
    return histdf


def join_tables(histdf, foredf):
    """Join history and forecast data on weather criterion and return."""
    joindf = pd.merge_asof(foredf[foredf.crit.notnull()], 
                       histdf[histdf.crit.notnull()], 
                       on='crit', direction='nearest') \
               .sort_values('time')
    return joindf


def save_table(pgcursor, table, df):
    """Clear existing data from postgres table and insert data from df."""
    clr_str = "TRUNCATE TABLE " + table + ";"
    pgcursor.execute(clr_str)

    columns = ''
    values = ''
    for col in df.columns.values:
        columns += col + ', '
        values += '%s, '
    
    sql_str = 'INSERT INTO ' \
            + table + ' (' \
            + columns[:-2] + ') VALUES (' \
            + values[:-2] + ');'
    for row in df.values.tolist():
        pgcursor.execute(sql_str, row)


def update_forecast():
    """Update cab forecast and save in postgreSQL."""
    # read config file for Open Weather API and
    # postres db, connect to db
    config = read_config()
    pgconnect = connect_postgres(config)
    pgcursor = pgconnect.cursor()
    
    # assemble weather forecast dataframe
    foredf = get_weather(config)
    t_min = min(foredf['trnd'])
    t_max = max(foredf['trnd'])
    d_min = min(foredf['day'])
    d_max = max(foredf['day'])
    calculate_criteria(foredf)

    # assemble and save city-level cab forecast
    citydf = get_history_data(pgcursor, 'cityhistory', d_min, d_max, t_min, t_max)
    calculate_criteria(citydf)
    citypred = join_tables(citydf, foredf)
    save_table(pgcursor, 'city_forecast', citypred)

    # assemble and save community area-level cab forecast
    areadf = get_history_data(pgcursor, 'areahistory', d_min, d_max, t_min, t_max)
    calculate_criteria(areadf)
    areadf['comm_pick'] = pd.to_numeric(areadf['comm_pick'])
    a_min = int(min(areadf['comm_pick']))
    a_max = int(max(areadf['comm_pick']))
    areapred = join_tables(areadf[areadf['comm_pick']==a_min], foredf)
    for area in range(a_min, a_max):
        adder = join_tables(areadf[areadf['comm_pick'] == area+1], foredf)
        areapred = areapred.append(adder, ignore_index=True)
    save_table(pgcursor, 'area_forecast', areapred)
    pgconnect.commit()
    pgcursor.close()
    pgconnect.close()


if __name__ == '__main__':
    update_forecast()