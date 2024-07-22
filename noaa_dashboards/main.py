import psycopg2
import psycopg2.extensions
import pandas as pd
import plotly.express as px
import select
import threading
import time
from decimal import Decimal
import datetime
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
app.layout = [
    html.Div(children='My First App with Data and a Graph')
]


def fetch_countries(connection):
    q = "select distinct country from aggregated_data;"
    try:
        cursor = connection.cursor()
        cursor.execute(q)
        result = cursor.fetchall()
        print(result)
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


def update_plot(connection):
    countries = fetch_countries(connection)

    if countries:
        for country in countries:
            # because it's a tuple
            c = country[0]
            q = f"select aggregation_type, start_ts, value from aggregated_data where country = '{c}' and measurement_type = 'TEMPERATURE' order by start_ts;"
            data = fetch_data(connection, q)
            if data:
                df = pd.DataFrame(data, columns=['AggType', 'Day', 'Value'])
                fig = px.line(df, x="Day", y="Value", color='AggType', title=f"Temperature per day in {c}")
                app.layout = html.Div(children=[
                    html.H1(children='Hello Dash'),

                    html.Div(children='''
                            Dash: A web application framework for Python.
                        '''),

                    dcc.Graph(
                        id='example-graph',
                        figure=fig
                    )
                ])


def listen_notifications(callback):
    conn = psycopg2.connect(**db_params)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute("LISTEN custom_channel;")
    print("Listening for notifications on channel 'custom_channel'...")

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
    app.run(debug=True)
