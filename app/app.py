import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import ast
import psycopg2
import json
import requests
import csv


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

# Create 4-row figure showing overall city statistics
fig = make_subplots(rows=4, cols=1, shared_xaxes=True, 
                    vertical_spacing=0.04)
fig.add_trace(go.Scatter(x=foredf['time'], y=foredf['tdry'], name='Temperature, F'),
              row=1, col=1)
fig.add_bar(x=foredf['time'], y=foredf['precip']*100,
            row=1, col=1, name='Rain, in x100')
fig.add_trace(go.Scatter(x=foredf['time'], y=foredf['taxis'], name='# Taxis'),
              row=2, col=1)
fig.add_trace(go.Scatter(x=foredf['time'], y=foredf['d_hr_cab'], name='$/hour/cab'),
              row=3, col=1)
fig.add_trace(go.Scatter(x=foredf['time'], y=foredf['d_mile'], name='$/mile'),
              row=4, col=1)
fig.update_layout(template='plotly_dark',
                  paper_bgcolor='#1E1E1E', 
                  plot_bgcolor='#1E1E1E',
                  autosize=False,
                  width=900,
                  height=800)
fig.update_xaxes(dtick=60*60*6*1000, 
                 tickformat='%-I%p\n%a %-m/%-d/%Y',
                 showgrid=True)
fig.update_yaxes(rangemode='tozero')

# Create map plot
fips_file = 'data/Boundaries.geojson'
with open(fips_file, 'r') as f:
    areas = json.loads(f.read())
#print(areas['features'][0]['properties'])

comm_file = 'data/community_data.csv'
commdf = pd.read_csv(comm_file, dtype={'fips':str})
#print(commdf.head(5))

mfig = px.choropleth_mapbox(commdf, 
                            geojson=areas, 
                            locations='fips', 
                            featureidkey='properties.area_numbe',
                            color='value',
                            color_continuous_scale='Viridis',
                            range_color=(0, 80),
                            mapbox_style='carto-positron',
                            zoom=10,
                            center={'lat':41.84, 'lon':-87.7},
                            opacity=0.5,
                            labels={'unemp':'Unemployment Rate'})
mfig.update_layout(margin={'r': 0, 't': 0, 'l': 0, 'b': 0},
                   autosize=False,
                   width=900,
                   height=800)

def weatherplot():
    fig_weather = px.line(foredf, x='time', y='tdry', 
                        color=px.Constant('Temperature, F'),
                        labels=dict(time='', tdry='', color=''))
    fig_weather.add_bar(x=foredf['time'], 
                        y=foredf['precip']*100, name='Precipitation, 100ths of in')
    fig_weather.update_xaxes(dtick=21600000, tickformat='%-I%p\n%-m/%-d/%Y', 
                            showgrid=True)


#external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
external_stylesheets = ['https://github.com/NellG/climate-cab/blob/master/app/style.css']

#app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1('Climate Cab'),
    html.H5('Weather-informed decision making for taxi drivers.'),
    dcc.Tabs([
        dcc.Tab(label='City Overview', children=[
            dcc.Graph(
                id='weather-forecast',
                figure=fig
            )
        ]),  
        dcc.Tab(label='Region Specific', children=[
            dcc.Graph(
                id='unemp-map',
                figure=mfig
            )
        ])
    ])
])

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', dev_tools_ui=False)