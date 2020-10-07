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




def get_pg_data(cursor, table):
    """Get data from table in postgresql database."""
    col_str = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE "
    col_str += "TABLE_NAME = '" + table + "';"
    cursor.execute(col_str)
    col_names = pd.DataFrame(cursor.fetchall())

    sql_str = "SELECT * FROM " + table + ";"
    cursor.execute(sql_str)
    df = pd.DataFrame(cursor.fetchall(), columns=col_names[0].values)
    return df


# Read in Open Weather Map config file
configfile = '.plotly-config'
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

foredf = get_pg_data(pgcursor, 'city_forecast')

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
area_df = get_pg_data(pgcursor, 'area_forecast')
plot_time = min(area_df['time'])
idle = 20
#area_df['d_hr_cab'] = area_df['d_ride']*60/(idle+area_df['d_ride']/area_df['d_min'])
area_df['d_hr_cab'] = area_df['d_ride'] * 60/(idle + area_df['d_ride']/area_df['d_min'])

mfig = px.choropleth_mapbox(area_df[area_df['time'] == plot_time], 
                            geojson=areas, 
                            locations='comm_pick', 
                            featureidkey='properties.area_numbe',
                            color='d_mile',
                            color_continuous_scale='Viridis',
                            range_color=(2, 12),
                            mapbox_style='carto-positron',
                            zoom=10,
                            center={'lat':41.84, 'lon':-87.7},
                            opacity=0.5,
                            labels={'d_hr_cab':'$/hr/cab'})
mfig.update_layout(margin={'r': 0, 't': 0, 'l': 0, 'b': 0},
                   autosize=False,
                   width=900,
                   height=800)


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