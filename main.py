from libraries.postgress import start_psycopg
from libraries.sqlite import start_sqlite
from libraries.duckdb import start_duckdb
from libraries.pandas import start_pandas

print("Выберите, скорость какой библиотеки нужно измерить\n"
      "1 - psycopg2\n"
      "2 - SQLite\n"
      "3 - DuckDB\n"
      "4 - Pandas\n"
      "5 - завершить программу")

name_lib = int(input())

while name_lib != 5:
    match name_lib:
        case 1:
            start_psycopg()
        case 2:
            start_sqlite()
        case 3:
            start_duckdb()
        case 4:
            start_pandas()
    name_lib = int(input())



