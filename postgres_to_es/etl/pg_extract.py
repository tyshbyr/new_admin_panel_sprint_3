import collections.abc as collections_abc
from datetime import datetime

import backoff
import psycopg2
from psycopg2 import DatabaseError, OperationalError, ProgrammingError
from psycopg2.extras import DictCursor, RealDictCursor

from etl.pg_queries import get_updated_movies_query
from settings import etl_settings, logger


backoff_max_time = etl_settings.backoff_max_time

class PostgresExtractor:

    def __init__(self, batch_size, state,) -> None:
        self.pg_conn = None
        self.batch_size = batch_size
        self.state = state

    @backoff.on_exception(backoff.expo, OperationalError,
                          max_time=backoff_max_time, logger=logger)
    def pg_connect(self, dsl):
        self.pg_conn = psycopg2.connect(**dsl)

    def pg_close(self):
        self.pg_conn.close()

    @backoff.on_exception(backoff.expo,
                          (DatabaseError,
                           ProgrammingError),
                          max_time=backoff_max_time,
                          logger=logger)
    def get_ids(self, table) -> collections_abc.Iterator[list]:
        """
        Функция для получения id фильмов, персон или жанров.
        """
        state = self.state
        last_modified = state.get_state(
            table +
            '_last_modified') if state.get_state(
            table +
            '_last_modified') else datetime.min
        with self.pg_conn.cursor(cursor_factory=DictCursor) as curs:
            while True:
                last_id = state.get_state(
                    table +
                    '_last_id') if state.get_state(
                    table +
                    '_last_id') else None
                query = f'SELECT id, modified FROM content.{table}'
                query_args = []
                if last_id:
                    query += " WHERE id > %s and modified > %s"
                    query_args.append(last_id)
                    query_args.append(last_modified)
                else:
                    query += " WHERE modified > %s"
                    query_args.append(last_modified)
                query += " ORDER BY id LIMIT %s"
                query_args.append(self.batch_size)
                curs.execute(query, query_args)
                if not curs.rowcount:
                    state.set_state(table +
                                    '_last_modified', str(datetime.now()))
                    state.set_state(table + '_last_id', None)
                    break
                batch = tuple(row[0] for row in curs.fetchall())
                yield batch
                state.set_state(table + '_last_id', batch[-1])

    @backoff.on_exception(backoff.expo,
                          (DatabaseError,
                           ProgrammingError),
                          max_time=backoff_max_time,
                          logger=logger)
    def get_filmwork_ids_for_table(self, table):
        """
        Функция для получения id фильмов в которых были обновлены
        персоны или жанры
        """
        generator_batch_of_ids = self.get_ids(table)
        query = f"""
                    SELECT DISTINCT(film_work_id)
                    FROM content.{table}_film_work
                    WHERE {table}_id IN %s;
                """
        with self.pg_conn.cursor(cursor_factory=DictCursor) as curs:
            for batch_of_ids in generator_batch_of_ids:
                curs.execute(query, (batch_of_ids,))
                batch = tuple(row[0] for row in curs.fetchall())
                yield batch

    @backoff.on_exception(backoff.expo,
                          (DatabaseError,
                           ProgrammingError),
                          max_time=backoff_max_time,
                          logger=logger)
    def get_updated_movies(self):
        """
        Функция для получения фильмов со всей связанной информацией
        """
        film_work_ids_by_person_generator = self.get_filmwork_ids_for_table(
            'person')
        film_work_ids_by_genre_generator = self.get_filmwork_ids_for_table(
            'genre')
        film_work_ids_by_film_work_generator = self.get_ids('film_work')
        film_work_ids_generators = (
            film_work_ids_by_person_generator,
            film_work_ids_by_genre_generator,
            film_work_ids_by_film_work_generator
        )
        with self.pg_conn.cursor(cursor_factory=RealDictCursor) as curs:
            for filmwork_ids_generator in film_work_ids_generators:
                for batch_ids in filmwork_ids_generator:
                    curs.execute(get_updated_movies_query, (batch_ids,))
                    batch_movies = [row for row in curs.fetchall()]
                    yield batch_movies
