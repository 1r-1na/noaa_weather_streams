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


measurement_types_names = {
    "types": [
        'TEMPERATURE',
        'WIND',
        'ATMOSPHERIC_PRESSURE',
        'LIQUID_PRECIPITATION'
    ],
    "names": [
        'Temperature',
        'Wind speed',
        'Atmospheric pressure',
        'Liquid precipitation'
    ]
}


app = Dash()
app.layout = [
    html.Div(children='NOAA weather dashboards with FLINK'),
]


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


def fetch_one(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return result
        return None
    except Exception as error:
        print(f"Error fetching data: {error}")
        cursor.close()
        return None


def create_linediagram(connection, country, measurement_type, measurement_name):
    q = f"SELECT aggregation_type, start_ts, value FROM aggregated_data WHERE country = '{country}' AND measurement_type = '{measurement_type}' AND aggregation_type IN ('AVG', 'MIN', 'MAX') AND duration_days = 1 ORDER BY start_ts;"
    data = fetch_data(connection, q)
    if data:
        df = pd.DataFrame(data, columns=['AggType', 'Day', 'Value'])
        fig = px.line(df, x="Day", y="Value", color='AggType', title=f"{measurement_name} stats per day in {country}")
        
        return html.Div(children=[
            dcc.Graph(
                id=f'{country}-LineDiagram-{measurement_type}',
                figure=fig
            )
        ])


def create_boxplot(connection, country, measurement_type, measurement_name):
    query_monthly_data = f"SELECT start_ts, duration_days FROM aggregated_data WHERE (duration_days = 28 OR duration_days = 29 OR duration_days = 30 OR duration_days = 31) AND country = '{country}' AND measurement_type = '{measurement_type}' AND aggregation_type NOT IN ('COUNT', 'STD', 'AVG');"
    monthly_data = fetch_one(connection, query_monthly_data)
    if monthly_data:
        start_ts = monthly_data[0]
        duration_days = monthly_data[1]
        query_one_boxplotdata = f"SELECT aggregation_type, value FROM aggregated_data WHERE measurement_type = '{measurement_type}' AND aggregation_type NOT IN ('COUNT', 'STD', 'AVG') AND country = '{country}' and start_ts = '{start_ts}' AND duration_days = {duration_days} ORDER BY aggregation_type;"
        boxplotdata = fetch_data(connection, query_one_boxplotdata)
        if boxplotdata:
            df = pd.DataFrame(boxplotdata, columns=['Month', 'Value'])
            df.set_index('Month').loc[['MIN', 'WHISKER_L', 'Q1', 'MEDIAN', 'Q3', 'WHISKER_U', 'MAX']]
            fig = px.box(df, y="Value", title=f"{measurement_name} boxplot of one month starting at {start_ts} in {country}")
            return html.Div(children=[
                dcc.Graph(
                    id=f'{country}-BoxPlot-{measurement_type}',
                    figure=fig
                )
            ])


def create_linediagram_live_values(connection, country):
    q = f"SELECT measurement_type, timestamp, value FROM live_values WHERE country = '{country}' ORDER BY measurement_type, timestamp;"
    data = fetch_data(connection, q)
    if data:
        df = pd.DataFrame(data, columns=['Measurement Type', 'Timestamp', 'Value'])
        fig = px.line(df, x="Timestamp", y="Value", color='Measurement Type', title=f"Live values in {country}")
        return html.Div(children=[
            dcc.Graph(
                id=f'{country}-LiveValues',
                figure=fig
            )
        ])

            
def update_plot(connection):
    updated_layout = [
        html.Div(children='NOAA weather dashboards with FLINK')
    ]
    countries = fetch_countries(connection)

    if countries:
        for country in countries:
            # because it's a tuple
            c = country[0]
            count = len(measurement_types_names["types"])
            for i in range(count):
                div1 = create_linediagram(connection, c, measurement_types_names["types"][i], measurement_types_names["names"][i])
                if div1:
                    updated_layout.append(div1)
                div2 = create_boxplot(connection, c, measurement_types_names["types"][i], measurement_types_names["names"][i])
                if div2:
                    updated_layout.append(div2)
            div3 = create_linediagram_live_values(connection, c)
            if div3:
                updated_layout.append(div3)
    
    app.layout = updated_layout


def listen_notifications(callback):
    conn = None
    for i in range(100):
        try:
            conn = psycopg2.connect(**db_params)
            break
        except:
            print(i, "Connection refused...")
            time.sleep(60)

    if conn is not None:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # initial call
        callback(conn)

        cursor.execute("LISTEN custom_channel;")
        print("Listening for notifications on channel 'custom_channel'...")

        while True:
            if select.select([conn], [], [], 5) == ([], [], []):
                time.sleep(30)
                continue
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                print("Got NOTIFY:", notify.payload)
                callback(conn)
                # sleep to not update too continuously
                time.sleep(15)


if __name__ == "__main__":
    listener_thread = threading.Thread(target=listen_notifications, args=(update_plot,))
    listener_thread.start()
    app.run_server(host="0.0.0.0", port=8050)
