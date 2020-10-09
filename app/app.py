import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import psycopg2
import json
import requests
import csv
import calendar
import numpy as np


def read_config(filename):
    """Read config file and return dict."""
    with open(filename) as infile:
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


def get_all_tables(config):
    pgconnect = connect_postgres(config)
    pgcursor = pgconnect.cursor()
    city_df = get_pg_data(pgcursor, 'city_forecast')
    area_df = get_pg_data(pgcursor, 'area_forecast')
    pgcursor.close()
    pgconnect.close()
    return city_df, area_df


def slider_marks(df):
    """Define dict for slider tick mark labels."""
    ticks = {}
    start_hr = df.loc[0, 'time'].hour
    for i in df.index:
        if (i + start_hr) % 24 == 0:
            ticks[i] = (df.loc[i, 'time']).strftime('%a %-I %p')
        elif (i + start_hr) % 6 == 0:
            ticks[i] = (df.loc[i, 'time']).strftime('%-I %p')
        else:
            ticks[i] = ''
    ticks[0] = 'now'
    return ticks


def make_city_chart(city_df):
    """Plot overall city data."""
    fig = make_subplots(rows=4, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.04)
    fig.add_trace(go.Scatter(x=city_df['time'], y=city_df['tdry'], name='Temperature, F'),
                row=1, col=1)
    fig.add_bar(x=city_df['time'], y=city_df['precip']*100,
                row=1, col=1, name='Rain, in x100')
    fig.add_trace(go.Scatter(x=city_df['time'], y=city_df['rides'], name='# Rides/hour'),
                row=2, col=1)
    fig.add_trace(go.Scatter(x=city_df['time'], y=city_df['d_hr_cab'], name='$/hour/cab'),
                row=3, col=1)
    fig.add_trace(go.Scatter(x=city_df['time'], y=city_df['d_mile'], name='$/mile'),
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

    return fig


def make_area_map(area_df, time_slider, map_metric):
    """Plot area specific data in choropleth."""
    if map_metric != 'rides':
        c_max = np.ceil(area_df[map_metric].quantile(q=0.95))
        c_min = np.floor(area_df[map_metric].quantile(q=0.05))
    else:
        c_max = np.ceil(area_df[map_metric].quantile(q=0.99))
        c_min = 20
    mfig = px.choropleth_mapbox(area_df[area_df['time'] == time_slider], 
                                geojson=areas, 
                                locations='comm_pick', 
                                featureidkey='properties.area_numbe',
                                color=map_metric,
                                color_continuous_scale='Viridis',
                                range_color=(c_min, c_max),
                                mapbox_style='carto-positron',
                                zoom=10,
                                center={'lat':41.84, 'lon':-87.7},
                                opacity=0.5,
                                labels={'d_hr_cab':'$/hr/cab'})
    mfig.update_layout(margin={'r': 0, 't': 0, 'l': 0, 'b': 0},
                    autosize=False,
                    width=900,
                    height=800)

    return mfig


# Define dictionary of column names and display names for area forecast.
area_cols = {'d_mile': '$/mile',
             'd_min': '$/driving minute',
             'd_ride': '$/ride',
             'rides': 'rides/hour'}

# Read config file for postgres connection and fips .geojson file.
config = read_config('/home/ubuntu/code/.plotly-config.csv')
fips_file = 'data/Boundaries.geojson'
with open(fips_file, 'r') as f:
    areas = json.loads(f.read())

city_df, area_df = get_all_tables(config)
city_fig = make_city_chart(city_df)
area_map = make_area_map(area_df, min(city_df['time']), 'rides')


app = dash.Dash(__name__)

app.layout = html.Div([
    html.H2('Taximizer'),
    html.H4('Forecasting taxi income in Chicago.'),
    html.Br(),
    html.Div('Select time and metric to show on map.'),
    html.Br(),
    dcc.Slider(
        id='time-slider',
        updatemode='mouseup',
        min=0,
        max=47,
        value=0,
        marks=slider_marks(city_df),
        step=1,
        included=False
    ),
    html.Br(),
    dcc.Dropdown(
        id='metric-drop',
        options=[{'label': area_cols[i], 'value': i} for i in area_cols],
        value='d_mile'
    ),            
    html.Br(),
    dcc.Tabs([
        dcc.Tab(label='City Overview', children=[
            dcc.Graph(
                id='city-forecast',
                figure=city_fig)
        ]),  
        dcc.Tab(label='Region Specific', children=[
            html.Br(),
            html.Div(id='map-desc'),
            html.Br(),
            dcc.Graph(
                id='area-map',
                figure=area_map)
        ])
    ]),
    dcc.Interval(
        id='interval-counter',
        interval=15*60*1000, # 15 minutes
        n_intervals=0
    )
])

@app.callback(
    [Output('map-desc', 'children'), Output('area-map', 'figure'),
     Output('city-forecast', 'figure'), Output('time-slider', 'marks')],
    [Input('time-slider','value'), Input('metric-drop', 'value'),
     Input('interval-counter', 'n_intervals')]
)
def update_area_map(time_slider, map_metric, n_intervals):
    city_df, area_df = get_all_tables(config)
    text_field = 'Showing predicted ' + area_cols[map_metric] + ' for ' \
               + city_df.loc[time_slider, 'time'].strftime('%A, %-I %p')

    city_fig = make_city_chart(city_df)
    area_map = make_area_map(area_df, city_df.loc[time_slider, 'time'], map_metric)
    
    return text_field, area_map, city_fig, slider_marks(city_df)


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', dev_tools_ui=False)