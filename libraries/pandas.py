import pandas as pd
from sqlalchemy import create_engine
import time
import statistics


def start_pandas():
    engine = create_engine('sqlite:///:memory:')

    taxi_data = pd.read_csv('..\\BDLab_3\\dataset\\nyc_yellow_tiny.csv')
    taxi_data.to_sql('nyc_yellow', con=engine, if_exists='replace', index=False)

    val = []
    query = 'SELECT VendorID, count(*) FROM nyc_yellow GROUP BY 1'
    for i in range(15):
        st = time.time()
        result = pd.read_sql(query, con=engine)
        fin = time.time()
        tmp = fin - st
        val.append(tmp)
    print("1 query:", statistics.median(val) * 1000, "ms")
    val.clear()

    query = """SELECT passenger_count, avg(total_amount)
                                FROM nyc_yellow
                                GROUP BY 1;"""
    for i in range(15):
        st = time.time()
        result = pd.read_sql(query, con=engine)
        fin = time.time()
        tmp = fin - st
        val.append(tmp)
    print("2 query:", statistics.median(val) * 1000, "ms")
    val.clear()

    query = """SELECT
                            passenger_count,
                            strftime('%Y', pickup_datetime),
                            count(*)
                            FROM nyc_yellow
                            GROUP BY 1, 2;"""
    for i in range(15):
        st = time.time()
        result = pd.read_sql(query, con=engine)
        fin = time.time()
        tmp = fin - st
        val.append(tmp)
    print("3 query:", statistics.median(val) * 1000, "ms")
    val.clear()

    query = """SELECT
                            passenger_count,
                            strftime('%Y', pickup_datetime),
                            round(trip_distance),
                            count(*)
                            FROM nyc_yellow
                            GROUP BY 1, 2, 3
                            ORDER BY 2, 4 desc;
                            """
    for i in range(15):
        st = time.time()
        result = pd.read_sql(query, con=engine)
        fin = time.time()
        tmp = fin - st
        val.append(tmp)
    print("4 query:", statistics.median(val) * 1000, "ms")
    val.clear()

    engine.dispose()
