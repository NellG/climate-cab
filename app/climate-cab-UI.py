import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd
import ast
import psycopg2

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Read in Open Weather Map config file
configfile = '/home/ubuntu/code/.plotly-config'
with open(configfile, 'r') as f:
    config = ast.literal_eval(f.read())

# Connect to postgreSQL database
pgconnect = psycopg2.connect(\
    host = config['pghost'], \
    port = config['pgport'],
    database = config['database'],
    user = config['pguser'],
    password = config['pgpassword'])
pgcursor = pgconnect.cursor()

# Download forecast data table
pgcursor.execute("SELECT * FROM cabforecast")
foredf = pd.DataFrame(pgcursor.fetchall(),
    columns=['time', 'tdry', 'precip', 'crit', 'taxis', 
             'd_hr_cab', 'd_mile', 'd_min', 'avg_over'])
