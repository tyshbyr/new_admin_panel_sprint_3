from settings import pg_settings, etl_settings, es_settings, logger
from etl.pg_extract import PostgresExtractor
from etl.state_etl import JsonFileStorage, State
from etl.es_load import ElasticsearchLoader
import time
import backoff


backoff_max_time = etl_settings.backoff_max_time

@backoff.on_exception(backoff.expo, Exception, max_time=backoff_max_time, logger=logger) 
def etl():
    while True:
        storage = JsonFileStorage(etl_settings.state_file)
        state = State(storage)
        loader = ElasticsearchLoader(es_settings.index_name)
        loader.es_connect(es_settings.scheme, es_settings.host, es_settings.port)
        extract = PostgresExtractor(etl_settings.batch_size, state)
        extract.pg_connect(pg_settings.dict())
        batch_movies_generator = extract.get_updated_movies()
        for batch in batch_movies_generator:
            loader.load_movies(batch, etl_settings.batch_size)
        extract.pg_close()
        print('Ok!')
        time.sleep(etl_settings.timeout)

if __name__ == '__main__':
    etl()
