from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from typing import Generator
from etl.transform import Movie
import logging


class ElasticsearchLoader:
    def __init__(self, index_name: str, es_host: str = 'localhost', es_port: int = 9200):
        self.index_name = index_name
        self.es = Elasticsearch([{'scheme':'http', 'host': es_host, 'port': es_port}])
        self.index_created = True

    def load_movies(self, movie_generator: Generator[dict, None, None], batch_size: int = 1000):
      docs = []
      for movie in movie_generator:
        docs.append(Movie(movie).as_dict())
        if len(docs) == batch_size:
            self._load_batch(docs)
            docs = []
      if len(docs) > 0:
          self._load_batch(docs)
    
    def _load_batch(self, docs):
      if not self.index_created:
        self._create_index()
        self.index_created = True
      documents = [{'_index': self.index_name, '_id': doc.get('id'), "_source": doc} for doc in docs]
      try:
          rows_count, errors = bulk(self.es, documents)
          print(rows_count, '\n', errors)
      except BulkIndexError as err:
        logging.exception("BulkIndexError occurred, errors: %s", err.errors)
        for error in err.errors:
          logging.error("error: %s, value: %s", error.get('error'), error.get('index'))
        
    def _create_index(self):
        self.es.indices.create(index=self.index_name, body={
  "settings": {
    "refresh_interval": "1s",
    "analysis": {
      "filter": {
        "english_stop": {
          "type":       "stop",
          "stopwords":  "_english_"
        },
        "english_stemmer": {
          "type": "stemmer",
          "language": "english"
        },
        "english_possessive_stemmer": {
          "type": "stemmer",
          "language": "possessive_english"
        },
        "russian_stop": {
          "type":       "stop",
          "stopwords":  "_russian_"
        },
        "russian_stemmer": {
          "type": "stemmer",
          "language": "russian"
        }
      },
      "analyzer": {
        "ru_en": {
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "english_stop",
            "english_stemmer",
            "english_possessive_stemmer",
            "russian_stop",
            "russian_stemmer"
          ]
        }
      }
    }
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "id": {
        "type": "keyword"
      },
      "imdb_rating": {
        "type": "float"
      },
      "genre": {
        "type": "keyword"
      },
      "title": {
        "type": "text",
        "analyzer": "ru_en",
        "fields": {
          "raw": { 
            "type":  "keyword"
          }
        }
      },
      "description": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "director": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "actors_names": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "writers_names": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "actors": {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
          "id": {
            "type": "keyword"
          },
          "name": {
            "type": "text",
            "analyzer": "ru_en"
          }
        }
      },
      "writers": {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
          "id": {
            "type": "keyword"
          },
          "name": {
            "type": "text",
            "analyzer": "ru_en"
          }
        }
      }
    }
  }
})