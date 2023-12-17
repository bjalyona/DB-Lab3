# Benchmark 4 queries 

Необходимо было написать бенчмарк для измерения скорости выполнения всех четырех запросов из бенчмарка 4queries для 4-х библиотек: psycopg2, SQLite, DuckDB, Pandas. 

Для измерений я использовала датасет nyc_yellow_tiny.

На графике представлен результат измерения скорости в миллисекундах работы библиотек на каждом из 4-х запросов.

![photo_2023-12-17_20-26-56](https://github.com/bjalyona/DB-Lab3/assets/146538270/402a9a08-506b-46b8-8ba1-3a31dd91092e)

Для измерения времени были произведены следующие запросы:

### 1 query

```bash
SELECT VendorID, count(*) 
FROM trips 
GROUP BY 1;
```

### 2 query

```bash
SELECT passenger_count, avg(total_amount) 
FROM trips 
GROUP BY 1;
```

### 3 query

```bash
SELECT passenger_count, extract(year from pickup_datetime), count(*) 
FROM trips 
GROUP BY 1, 2;
```

### 4 query

```bash
SELECT passenger_count, extract(year from pickup_datetime), round(trip_distance), count(*) 
FROM trips 
GROUP BY 1, 2, 3 
ORDER BY 2, 4 desc;
```

По моим наблюдениям о каждой из библиотек можно сделать такие выводы:

### Psycopg2

* Быстрее чем SQLite и Pandas
* Достаточно быстро исполняет запросы где не требуется группировать и сортировать данные

### SQLite

* На 2 и 3 запросе показывает себя быстрее, чем Pandas
* Не требует сложной настройки
* Простота и удобство использования

### DuckDB

* Выполняет запросы значительно быстрее других 3-х библиотек
* Простота установки и использования

### Pandas

* Медленно выполняет запросы, незначительно превосходит SQLite на 1 и 4 запросе


