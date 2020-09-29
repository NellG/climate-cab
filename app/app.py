import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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

fig = make_subplots(rows=4, cols=1, shared_xaxes=True, 
                    vertical_spacing=0.04)
fig.add_trace(go.Scatter(x=foredf['time'], y=foredf['tdry']),
              row=1, col=1)
fig.add_trace(go.Scatter(x=foredf['time'], y=foredf['taxis']),
              row=2, col=1)
fig.add_trace(go.Scatter(x=foredf['time'], y=foredf['d_hr_cab']),
              row=3, col=1)
fig.add_trace(go.Scatter(x=foredf['time'], y=foredf['d_mile']),
              row=4, col=1)

def weatherplot():
    fig_weather = px.line(foredf, x='time', y='tdry', 
                        color=px.Constant('Temperature, F'),
                        labels=dict(time='', tdry='', color=''))
    fig_weather.add_bar(x=foredf['time'], 
                        y=foredf['precip']*100, name='Precipitation, 100ths of in')
    fig_weather.update_xaxes(dtick=21600000, tickformat='%-I%p\n%-m/%-d/%Y', 
                            showgrid=True)

app.layout = html.Div(children=[
    html.H3(children='Climate Cab'),

    html.H5(children='Weather-informed decision making for taxi drivers.'),

    dcc.Graph(
        id='weather-forecast',
        figure=fig
    )
])

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')