from settings import dsl, EXTRACT_BATCH_SIZE
from etl.pg_extract import PostgresExtractor
import psycopg2
from etl.state_etl import JsonFileStorage, State
import psycopg2.extras
from etl.es_load import ElasticsearchLoader


batch_size = EXTRACT_BATCH_SIZE
storage = JsonFileStorage('storage.txt')
state = State(storage)
loader = ElasticsearchLoader('movies', es_host='127.0.0.1', es_port=9200)

with psycopg2.connect(**dsl) as conn:
        extract = PostgresExtractor(conn, batch_size, state)
        batch_movies_generator = extract.get_updated_movies()
        movies = []
        for batch in batch_movies_generator:
            loader.load_movies(batch, batch_size)
