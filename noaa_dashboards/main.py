import psycopg2
import psycopg2.extensions
import pandas as pd
import plotly.express as px
import select
import threading
import time
from dash import Dash, html, dcc

# Database connection parameters
db_params = {
    'dbname': 'default',
    'user': 'dev_user',
    'password': 'dev_pw',
    'host': 'postgres',
    'port': '5432'
}


app = Dash()
layout = [
    html.Div(children='NOAA weather dashboards with FLINK')
]
app.layout = layout


def fetch_countries(connection):
    q = "select distinct country from aggregated_data;"
    try:
        cursor = connection.cursor()
        cursor.execute(q)
        result = cursor.fetchall()
        if result:
            return result
        return None
    except Exception as error:
        print(f"Error fetching data: {error}")
        cursor.close()
        return None


def fetch_data(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        if result:
            return result
        return None
    except Exception as error:
        print(f"Error fetching data: {error}")
        cursor.close()
        return None


def create_linediagram_temperature(connection, country):
    q = f"SELECT aggregation_type, start_ts, value FROM aggregated_data WHERE country = '{country}' AND measurement_type = 'TEMPERATURE' AND aggregation_type IN ('AVG', 'MIN', 'MAX') ORDER BY start_ts;"
    data = fetch_data(connection, q)
    if data:
        df = pd.DataFrame(data, columns=['AggType', 'Day', 'Value'])
        fig = px.line(df, x="Day", y="Value", color='AggType', title=f"Temperature stats per day in {country}")
        layout.append(html.Div(children=[
            dcc.Graph(
                id=f'{country}-LineDiagram-Temperature',
                figure=fig
            )
        ]))


def create_linediagram_wind(connection, country):
    q = f"SELECT aggregation_type, start_ts, value FROM aggregated_data WHERE country = '{country}' AND measurement_type = 'WIND' AND aggregation_type IN ('AVG', 'MIN', 'MAX') ORDER BY start_ts;"
    data = fetch_data(connection, q)
    if data:
        df = pd.DataFrame(data, columns=['AggType', 'Day', 'Value'])
        fig = px.line(df, x="Day", y="Value", color='AggType', title=f"Wind stats per day in {country}")
        layout.append(html.Div(children=[
            dcc.Graph(
                id=f'{country}-LineDiagram-Wind',
                figure=fig
            )
        ]))


def create_linediagram_pressure(connection, country):
    q = f"SELECT aggregation_type, start_ts, value FROM aggregated_data WHERE country = '{country}' AND measurement_type = 'ATMOSPHERIC_PRESSURE' AND aggregation_type IN ('AVG', 'MIN', 'MAX') ORDER BY start_ts;"
    data = fetch_data(connection, q)
    if data:
        df = pd.DataFrame(data, columns=['AggType', 'Day', 'Value'])
        fig = px.line(df, x="Day", y="Value", color='AggType', title=f"Atmospheric Pressure stats per day in {country}")
        layout.append(html.Div(children=[
            dcc.Graph(
                id=f'{country}-LineDiagram-Pressure',
                figure=fig
            )
        ]))


def create_linediagram_liquid_precipitation(connection, country):
    q = f"SELECT aggregation_type, start_ts, value FROM aggregated_data WHERE country = '{country}' AND measurement_type = 'LIQUID_PRECIPITATION' AND aggregation_type IN ('AVG', 'MIN', 'MAX') ORDER BY start_ts;"
    data = fetch_data(connection, q)
    if data:
        df = pd.DataFrame(data, columns=['AggType', 'Day', 'Value'])
        fig = px.line(df, x="Day", y="Value", color='AggType', title=f"Liquid precipitation stats per day in {country}")
        layout.append(html.Div(children=[
            dcc.Graph(
                id=f'{country}-LineDiagram-Liquid-Precipitation',
                figure=fig
            )
        ]))


def update_plot(connection):
    countries = fetch_countries(connection)

    if countries:
        for country in countries:
            # because it's a tuple
            c = country[0]
            create_linediagram_temperature(connection, c)
            create_linediagram_wind(connection, c)
            create_linediagram_pressure(connection, c)
            create_linediagram_liquid_precipitation(connection, c)


def listen_notifications(callback):
    conn = psycopg2.connect(**db_params)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute("LISTEN custom_channel;")
    print("Listening for notifications on channel 'custom_channel'...")

    # initial call
    callback(conn)

    while True:
        if select.select([conn], [], [], 5) == ([], [], []):
            continue
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            print("Got NOTIFY:", notify.payload)
            callback(conn)
            # sleep to not update too continuously
            time.sleep(10)


if __name__ == "__main__":
    listener_thread = threading.Thread(target=listen_notifications, args=(update_plot,))
    listener_thread.start()
    app.run_server(host="0.0.0.0", port=8050)
