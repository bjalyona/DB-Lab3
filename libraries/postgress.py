import psycopg2
import time
import statistics


def start_psycopg():
    db_params = {
        'dbname': 'lab3',
        'user': 'postgres',
        'password': 'kokodayo2121',
        'host': 'localhost',
        'port': '5432'
    }

    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        connection.autocommit = True

        cursor.execute("""CREATE TABLE nyc_yellow
                        (
                            id INT,
                            VendorID SMALLINT,
                            tpep_pickup_datetime TIMESTAMP,
                            tpep_dropoff_datetime TIMESTAMP,
                            passenger_count REAL,
                            trip_distance FLOAT,
                            RatecodeID REAL,
                            store_and_fwd_flag CHAR(1),
                            PULocationID INT,
                            DOLocationID INT,
                            payment_type SMALLINT,
                            fare_amount FLOAT,
                            extra FLOAT,
                            mta_tax FLOAT,
                            tip_amount FLOAT,
                            tolls_amount REAL,
                            improvement_surcharge REAL,
                            total_amount FLOAT,
                            congestion_surcharge REAL,
                            airport_fee REAL)
                            """)
        print("Table created successfully")

    # cursor.execute("DROP TABLE nyc_yellow")

        with open("nyc_yellow_tiny.csv", "r") as f:
            next(f)
            cursor.copy_from(f, "nyc_yellow", sep=",")

        val = []
        for i in range(15):
            st = time.time()
            cursor.execute("SELECT VendorID, count(*) FROM nyc_yellow GROUP BY 1;")
            fin = time.time()
            tmp = fin - st
            val.append(tmp)
        print("1 query:", statistics.median(val) * 1000, "ms")
        val.clear()

        for i in range(15):
            st = time.time()
            cursor.execute("""SELECT passenger_count, avg(total_amount)
                            FROM nyc_yellow
                            GROUP BY 1;""")
            fin = time.time()
            tmp = fin - st
            val.append(tmp)
        print("2 query:", statistics.median(val) * 1000, "ms")
        val.clear()

        for i in range(15):
            st = time.time()
            cursor.execute("""SELECT
                            passenger_count,
                            extract(year from tpep_pickup_datetime),
                            count(*)
                            FROM nyc_yellow
                            GROUP BY 1, 2;""")
            fin = time.time()
            tmp = fin - st
            val.append(tmp)
        print("3 query:", statistics.median(val) * 1000, "ms")
        val.clear()

        for i in range(15):
            st = time.time()
            cursor.execute("""SELECT
                            passenger_count,
                            extract(year from tpep_pickup_datetime),
                            round(trip_distance),
                            count(*)
                            FROM nyc_yellow
                            GROUP BY 1, 2, 3
                            ORDER BY 2, 4 desc;
                            """)
            fin = time.time()
            tmp = fin - st
            val.append(tmp)
        print("4 query:", statistics.median(val) * 1000, "ms")
        val.clear()

        cursor.close()
        connection.close()

    except psycopg2.Error as e:
        print('Ошибка подключения к базе данных', e)
