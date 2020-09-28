import ast
import json
import requests
from datetime import datetime
import math

# Read in Open Weather Map config file
configfile = '/home/ubuntu/code/.owm-config'
with open(configfile, 'r') as f:
    config = ast.literal_eval(f.read())

url = 'https://api.openweathermap.org/data/2.5/onecall?' \
    + config['latlon'] \
    + '&units=imperial&appid=' \
    + config['key']
with requests.get(url, allow_redirects=True) as f:
    forecast = json.loads(f.text)

fore_table = []

for h in [forecast['current']] + forecast['hourly'][1:]:
    time = datetime.fromtimestamp(h['dt']+forecast['timezone_offset'])
    prnd = math.ceil(h.get('rain', {'1h':0})['1h']*5)/5
    trnd = round(h['temp'], -1)
    day = time.weekday()+1
    hour = time.hour
    fore_table.append([time, trnd, prnd, day, hour])

print(fore_table)