import sqlite3
import pandas
import time
import statistics


def start_sqlite():

    with sqlite3.connect('..\\BDLab_3\\databases\\server.db') as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nyc_yellow (
                id INTEGER PRIMARY KEY,
                VendorID INTEGER,
                tpep_pickup_datetime TEXT,
                tpep_dropoff_datetime TEXT,
                passenger_count REAL,
                trip_distance REAL,
                RatecodeID REAL,
                store_and_fwd_flag TEXT,
                PULocationID INTEGER,
                DOLocationID INTEGER,
                payment_type INTEGER,
                fare_amount REAL,
                extra REAL,
                mta_tax REAL,
                tip_amount REAL,
                tolls_amount REAL,
                improvement_surcharge REAL,
                total_amount REAL,
                congestion_surcharge REAL,
                airport_fee REAL);
            """)

        cursor.execute("PRAGMA table_info(nyc_yellow)")

        taxi_data = pandas.read_csv('..\\BDLab_3\\\\dataset\\nyc_yellow_tiny.csv')
        taxi_data.to_sql('nyc_yellow', conn, if_exists='replace', index=False)

        val = []
        for i in range(15):
            st = time.time()
            cursor.execute('SELECT VendorID, count(*) FROM nyc_yellow GROUP BY 1')
            fin = time.time()
            tmp = fin - st
            val.append(tmp)
        print("1 query:", statistics.median(val) * 1000, "ms")
        val.clear()

        for i in range(15):
            st = time.time()
            cursor.execute("""SELECT passenger_count, avg(total_amount)
                            FROM nyc_yellow
                            GROUP BY 1""")
            fin = time.time()
            tmp = fin - st
            val.append(tmp)
        print("2 query:", statistics.median(val) * 1000, "ms")
        val.clear()

        for i in range(15):
            st = time.time()
            cursor.execute("""SELECT passenger_count, strftime('%Y', pickup_datetime), count(*) FROM nyc_yellow GROUP BY 1, 2;
                                """)
            fin = time.time()
            tmp = fin - st
            val.append(tmp)
        print("3 query:", statistics.median(val) * 1000, "ms")
        val.clear()

        for i in range(15):
            st = time.time()
            cursor.execute("""SELECT passenger_count, strftime('%Y', pickup_datetime),  
            round(trip_distance), count(*) FROM nyc_yellow GROUP BY 1, 2, 3 ORDER BY 2, 4 desc
                            """)
            fin = time.time()
            tmp = fin - st
            val.append(tmp)
        print("4 query:", statistics.median(val) * 1000, "ms")
        val.clear()

        conn.commit()
