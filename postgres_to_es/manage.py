import time

import backoff
from etl.es_load import ElasticsearchLoader
from etl.pg_extract import PostgresExtractor
from etl.state_etl import JsonFileStorage, State
from settings import es_settings, etl_settings, logger, pg_settings

backoff_max_time = etl_settings.backoff_max_time


@backoff.on_exception(backoff.expo, Exception,
                      max_time=backoff_max_time, logger=logger)
def etl():
    storage = JsonFileStorage(etl_settings.state_file)
    state = State(storage)
    while True:
        logger.info('ETL запущен.')
        try:
            loader = ElasticsearchLoader(es_settings.index_name)
            logger.info('Соединение с Elasticsearch')
            loader.es_connect(
                es_settings.scheme,
                es_settings.host,
                es_settings.port)
            extract = PostgresExtractor(etl_settings.batch_size, state)
            logger.info('Соединение с Postgresql')
            extract.pg_connect(pg_settings.dict())
            logger.info('Поиск обновлненых фильмов')
            batch_movies_generator = extract.get_updated_movies()
            for batch in batch_movies_generator:
                loader.load_movies(batch, etl_settings.batch_size)
        except Exception as error:
            logger.error('ETL прерван ошибкой: %s', error)
        finally:
            extract.pg_close()
            logger.info('Конец цикла. Сон %s сек.', etl_settings.timeout)
            time.sleep(etl_settings.timeout)


if __name__ == '__main__':
    etl()
