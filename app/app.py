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
                        vertical_spacing=0.05)
    fig.add_trace(go.Scatter(x=city_df['time'], y=city_df['tdry'], name='Temperature, F', 
                             marker=dict(color='#ffff72'),
                             hovertemplate='%{x|%b %-d, %-I %p}<br>'+
                                           '%{y:.0f}\u00B0F<extra></extra>'),
                row=1, col=1)
    fig.add_bar(x=city_df['time'], y=city_df['precip']*10, marker=dict(color='#578eb4'),
                row=1, col=1, name='Rain, in x10', customdata=city_df['precip'].values,
                hovertemplate='%{x|%b %-d, %-I %p}<br>'+
                              '%{customdata:.1f} inches<extra></extra>')
    fig.add_trace(go.Scatter(x=city_df['time'], y=city_df['rides'], name='# Rides/hour',
                             marker=dict(color='#5bdd9f'),
                             hovertemplate='%{x|%b %-d, %-I %p}<br>'+
                                           '%{y:.0f} rides/hour<extra></extra>'),
                row=2, col=1)
    fig.add_trace(go.Scatter(x=city_df['time'], y=city_df['d_hr_cab'], name='$/hour/cab',
                             marker=dict(color='#578eb4'),
                             hovertemplate='%{x|%b %-d, %-I %p}<br>'+
                                           '$%{y:.2f}/hour/cab<extra></extra>'),
                row=3, col=1)
    fig.add_trace(go.Scatter(x=city_df['time'], y=city_df['d_mile'], name='$/mile',
                             marker=dict(color='#914ea1'),
                             hovertemplate='%{x|%b %-d, %-I%p}<br>'+
                                           '$%{y:.2f}/mile<extra></extra>'),
                row=4, col=1)
    fig.update_layout(template='plotly_dark',
                    paper_bgcolor='#161a28', 
                    plot_bgcolor='#161a28', 
                    margin={'r':15, 't':25, 'l': 80, 'b': 15},
                    showlegend=False)
    fig.update_xaxes(dtick=60*60*12*1000, 
                    tickformat='%-I%p\n%a',
                    showgrid=True,
                    color='#d8d8d8')
    fig.update_yaxes(rangemode='tozero',
                    color='#d8d8d8')
    fig.update_yaxes(title_text='T \u00B0F <br> Rain 0.1 in', title_standoff = 7, row=1, col=1)
    fig.update_yaxes(title_text='# Rides/hour', title_standoff = 0, row=2, col=1)
    fig.update_yaxes(title_text='$/hour/cab', title_standoff = 14, row=3, col=1)
    fig.update_yaxes(title_text='$/mile', title_standoff = 21, row=4, col=1)

    return fig


def make_area_map(area_df, time_slider, map_metric):
    """Plot area specific data in choropleth."""
    area_df['Metrics'] = '<br>' \
                    + area_df['d_mile'].map('${:,.2f}/mile<br>'.format) \
                    + area_df['d_min'].map('${:,.2f}/minute<br>'.format) \
                    + area_df['d_ride'].map('${:,.2f}/ride<br>'.format) \
                    + area_df['rides'].map('{:,.0f} rides/hour'.format)
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
                                labels=area_cols,
                                hover_data = {'d_min':False,
                                              'Metrics':True,
                                              'd_mile':False,
                                              'rides':False,
                                              'd_ride':False,
                                              'comm_pick':False})
    mfig.update_layout(margin={'r': 15, 't': 15, 'l': 15, 'b': 15},
                       paper_bgcolor='#161a28',
                       coloraxis=dict(colorbar=dict(titlefont={'color': '#d8d8d8'},
                                                    tickfont={'color': '#d8d8d8'})),
                       hoverlabel=dict(bgcolor='#161a28'))
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
area_map = make_area_map(area_df, min(city_df['time']), 'd_mile')


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
                            html.Div(
                                id='map-desc',
                                style={
                                    'font-size': '1.5rem',  
                                    'margin-bottom': '0rem',
                                    'margin-top': '1.5rem',
                                    'margin-left': '1.5rem'
                                    }
                                ),
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
def update_charts(time_slider, map_metric, n_intervals):
    city_df, area_df = get_all_tables(config)
    text_field = 'Showing predicted ' + area_cols[map_metric] + ' for ' \
               + city_df.loc[time_slider, 'time'].strftime('%A, %-I %p')

    city_fig = make_city_chart(city_df)
    area_map = make_area_map(area_df, city_df.loc[time_slider, 'time'], map_metric)
    
    return text_field, area_map, city_fig, list_times(city_df)


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', dev_tools_ui=False)