import ast
import json
import requests
from datetime import datetime
import math
import psycopg2
import pandas as pd
import numpy as np

print('Running owm-data')

def calculate_criteria(df):
    df['crit'] = df['hour']*10000 + df['day']*1000 + df['trnd'] + df['prnd']/10
    df.loc[df['prnd'] > 0, 'crit'] = -1*df.loc[df['prnd'] > 0, 'crit']
    df.drop(['hour', 'day', 'prnd', 'trnd'], axis=1, inplace=True)
    df.sort_values('crit', inplace=True)

# Read in Open Weather Map config file
configfile = '/home/ubuntu/code/.owm-config'
with open(configfile, 'r') as f:
    config = ast.literal_eval(f.read())

# Download one call weather data from API
url = 'https://api.openweathermap.org/data/2.5/onecall?' \
    + config['latlon'] \
    + '&units=imperial&appid=' \
    + config['key']
with requests.get(url, allow_redirects=True) as f:
    forecast = json.loads(f.text)

print('Got current forecast')

# Assemble forecast data table
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
calculate_criteria(foredf)

print('Made forecast df')

# Connect to postgreSQL database
pgconnect = psycopg2.connect(\
    host = config['pghost'], \
    port = config['pgport'],
    database = config['database'],
    user = config['pguser'],
    password = config['pgpassword'])
pgcursor = pgconnect.cursor()

print('Connected to pgsql')

# Scale factor for regional $/cab/hr: 1.39

# Download history data table
pgcursor.execute("SELECT * FROM cabhistory")
histdf = pd.DataFrame(pgcursor.fetchall(),
    columns=['trnd', 'prnd', 'day', 'hour', 'taxis', 
             'd_hr_cab', 'd_mile', 'd_min', 'avg_over'])
calculate_criteria(histdf)

print('Heading into join.')
# Join history and forecast data on weather criterion
foredf = pd.merge_asof(foredf[foredf.crit.notnull()], 
                       histdf[histdf.crit.notnull()], 
                       on='crit', direction='nearest')

# Clear previous forecast table and instert new data
pgcursor.execute('TRUNCATE TABLE cabforecast')
columns = '(time, tdry, precip, crit, taxis, d_hr_cab, d_mile, d_min, avg_over)'
sql = 'INSERT INTO cabforecast ' \
    + columns \
    + ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'
for row in foredf.sort_values('time').values.tolist():
        pgcursor.execute(sql, row)

pgconnect.commit()
