"""
    Запускает ETL процесс
    Управляет загрузкой данных из Postgres
    Управляет трансформацией данных
    Управляет загрузкой данных в ES
    Управляет сохранением состояния 
"""


from settings import dsl, EXTRACT_BATCH_SIZE
from etl.pg_extract import extract_updated_movies
from datetime import datetime


extract_batch = EXTRACT_BATCH_SIZE
dt = datetime.fromisoformat('2021-06-15 20:14:09.514476+00')

movies = extract_updated_movies(dsl, extract_batch, dt)
print(next(movies)[0])
print(next(movies)[0])
