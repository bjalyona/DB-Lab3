import duckdb
import time
import statistics


def start_duckdb():

    conn = duckdb.connect(database=':memory:', read_only=False)

    conn.execute(
      """CREATE TABLE nyc_yellow AS SELECT * FROM read_csv_auto('..\\BDLab_3\\dataset\\nyc_yellow_tiny.csv');"""
    )

    val = []
    for i in range(15):
        st = time.time()
        result = conn.execute("SELECT VendorID, count(*) FROM nyc_yellow GROUP BY 1")
        fin = time.time()
        tmp = fin - st
        val.append(tmp)
    print("1 query:", statistics.median(val) * 1000, "ms")
    val.clear()

    for i in range(15):
        st = time.time()
        result = conn.execute("""SELECT passenger_count, avg(total_amount)
                            FROM nyc_yellow
                            GROUP BY 1;""")
        fin = time.time()
        tmp = fin - st
        val.append(tmp)
    print("2 query:", statistics.median(val) * 1000, "ms")
    val.clear()

    for i in range(15):
        st = time.time()
        result = conn.execute("""SELECT
                            passenger_count,
                            extract(year from pickup_datetime),
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
        result = conn.execute("""SELECT
                            passenger_count,
                            extract(year from pickup_datetime),
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

    conn.close()
