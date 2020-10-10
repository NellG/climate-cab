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


def list_times(df):
    """Define dict for slider tick mark labels."""
    ticks = {}
    start_hr = df.loc[0, 'time'].hour
    for i in df.index:
        ticks[i] = (df.loc[i, 'time']).strftime('%a %-I %p')
    ticks[0] = 'now'
    return ticks


def make_city_chart(city_df):
    """Plot overall city data."""
    fig = make_subplots(rows=4, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.025)
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
                    paper_bgcolor='#1e1e1e', 
                    plot_bgcolor='#1e1e1e')
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
    mfig.update_layout(margin={'r': 10, 't': 10, 'l': 10, 'b': 10})

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
    html.Div(
        id="left-column",
        className="three columns",
        children=[
            html.H2('Taximizer'),
            html.H4('Forecasting taxi income in Chicago.'),
            html.Br(),
            html.Div('Select time point and metric to show on map.'),
            html.Br(),
            dcc.Dropdown(
                id='time-drop',
                options=[{'label': list_times(city_df)[i],
                          'value': i} for i in list_times(city_df)],
                value=0,
                style={'color':'#1e1e1e'}
            ),
            html.Br(),
            dcc.RadioItems(
                id='metric-radio',
                options=[{'label': area_cols[i], 'value': i} for i in area_cols],
                value='d_mile'
            )
        ]
    ),

    html.Div(
        id="right-column",
        className="nine columns",
        children=[            
            html.Br(),
            dcc.Tabs(
                id='forecast-tabs',
                value='city-tab',
                parent_className='custom-tabs',
                className='custom-tabs-container',
                children=[
                    dcc.Tab(
                        label='City Overview', 
                        value='city-tab',
                        className='custom-tab',
                        selected_className='custom-tab--selected',
                        children=[
                        dcc.Graph(
                            id='city-forecast',
                            figure=city_fig,
                            style={'height':'75vh'})
                        ]
                    ),  
                    dcc.Tab(
                        label='Region Specific',
                        value='area-tab',
                        className='custom-tab',
                        selected_className='custom-tab--selected', 
                        children=[
                            html.Br(),
                            html.Div(id='map-desc'),
                            html.Br(),
                            dcc.Graph(
                                id='area-map',
                                figure=area_map,
                                style={'height':'75vh'})
                        ]
                    )
                ]
            ),
            dcc.Interval(
                id='interval-counter',
                interval=15*60*1000, # 15 minutes
                n_intervals=0
            )
        ]
    )
], className="row")

@app.callback(
    [Output('map-desc', 'children'), Output('area-map', 'figure'),
     Output('city-forecast', 'figure'), Output('time-drop', 'marks')],
    [Input('time-drop','value'), Input('metric-radio', 'value'),
     Input('interval-counter', 'n_intervals')]
)
def update_area_map(time_slider, map_metric, n_intervals):
    city_df, area_df = get_all_tables(config)
    text_field = 'Showing predicted ' + area_cols[map_metric] + ' for ' \
               + city_df.loc[time_slider, 'time'].strftime('%A, %-I %p')

    city_fig = make_city_chart(city_df)
    area_map = make_area_map(area_df, city_df.loc[time_slider, 'time'], map_metric)
    
    return text_field, area_map, city_fig, list_times(city_df)


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', dev_tools_ui=False)