import logging
from typing import Generator

import backoff
from elasticsearch import (ConnectionError, Elasticsearch, RequestError,
                           SerializationError, TransportError)
from elasticsearch.helpers import BulkIndexError, bulk

from etl.es_index import es_index
from etl.transform import Movie
from settings import etl_settings, logger


backoff_max_time = etl_settings.backoff_max_time

class ElasticsearchLoader:
    def __init__(self, index_name: str):
        self.index_name = index_name
        self.index_created = False
        self.es = None

    @backoff.on_exception(backoff.expo,
                          (ConnectionError,
                           TransportError),
                          max_time=backoff_max_time,
                          logger=logger)
    def es_connect(self, scheme, host, port):
        self.es = Elasticsearch(
            [{'scheme': scheme, 'host': host, 'port': port}])

    @backoff.on_exception(backoff.expo, SerializationError,
                          max_time=backoff_max_time, logger=logger)
    def load_movies(self,
                    movie_generator: Generator[dict,
                                               None,
                                               None],
                    batch_size: int = 1000):
        docs = []
        for movie in movie_generator:
            docs.append(Movie(movie).as_dict())
            if len(docs) == batch_size:
                self._load_batch(docs)
                docs = []
        if len(docs) > 0:
            self._load_batch(docs)

    def _load_batch(self, docs):
        if not self.es.indices.exists(index=self.index_name):
            self._create_index()
            self.index_created = True
        documents = [{'_index': self.index_name, '_id': doc.get(
            'id'), "_source": doc} for doc in docs]
        try:
            rows_count, errors = bulk(self.es, documents)
            logger.info(
                'Количесвто документов отправленных в Elasticsearch: %s',
                rows_count)
            if errors:
                logger.info('Ошибки: %s', errors)
        except BulkIndexError as err:
            logging.exception(
                "BulkIndexError occurred, errors: %s",
                err.errors)
            for error in err.errors:
                logging.error(
                    "error: %s, value: %s",
                    error.get('error'),
                    error.get('index'))

    @backoff.on_exception(backoff.expo, RequestError,
                          max_time=backoff_max_time, logger=logger)
    def _create_index(self):
        self.es.indices.create(index=self.index_name, body=es_index)
