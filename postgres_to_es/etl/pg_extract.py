"""
    Получает данные из Postgress пачками.
    Обрабатывает падение Postgres.
    После падения Postgres начинает читать с последней обработанной
    записи.
"""

from psycopg2.extras import DictCursor
import collections.abc as collections_abc
from psycopg2.extras import DictCursor
from datetime import datetime


class PostgresExtractor:

    def __init__(self, pg_conn, batch_size, state) -> None:
        self.pg_conn = pg_conn
        self.batch_size = batch_size
        self.state = state
        
    def get_ids(self, table) -> collections_abc.Iterator[list]:
        """
        Функция возвращает генератор кортежей id
        """
        state = self.state
        last_modified = state.get_state(table + '_last_modified') if state.get_state(table + '_last_modified') else datetime.min
        with self.pg_conn.cursor(cursor_factory=DictCursor) as curs:
            while True:
                last_id = state.get_state(table + '_last_id') if state.get_state(table + '_last_id') else None
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
                    state.set_state(table + '_last_modified', str(datetime.now()))
                    state.set_state(table + '_last_id', None)
                    break
                batch = tuple(row[0] for row in curs.fetchall())
                yield batch
                state.set_state(table + '_last_id', batch[-1])

        
    def get_filmwork_ids_for_table(self, table):
        """
        Функция принимает генератор кортежей id таблицы персон или жанров
        и возвращает генератор кортежей id связанных фильмов
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
            
            
    def get_updated_movies(self):
        film_work_ids_by_person_generator = self.get_filmwork_ids_for_table('person')
        film_work_ids_by_genre_generator = self.get_filmwork_ids_for_table('genre')
        film_work_ids_by_film_work_generator = self.get_ids('film_work')
        film_work_ids_generators = (
            film_work_ids_by_person_generator,
            film_work_ids_by_genre_generator,
            film_work_ids_by_film_work_generator
        )
        with self.pg_conn.cursor(cursor_factory=DictCursor) as curs:
            for filmwork_ids_generator in film_work_ids_generators:
                for batch_ids in filmwork_ids_generator:
                    curs.execute("""
                                SELECT
                                    fw.id as fw_id, 
                                    fw.title, 
                                    fw.description, 
                                    fw.rating, 
                                    fw.type, 
                                    fw.created, 
                                    fw.modified, 
                                    STRING_AGG(DISTINCT pfw.role, ',') as roles, 
                                    STRING_AGG(DISTINCT CONCAT(p.id, ':', p.full_name), ',') as persons,
                                    STRING_AGG(DISTINCT g.name, ',') as genres
                                FROM content.film_work fw
                                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                                LEFT JOIN content.person p ON p.id = pfw.person_id
                                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                                LEFT JOIN content.genre g ON g.id = gfw.genre_id
                                WHERE fw.id IN %s
                                GROUP BY fw.id, fw.title, fw.description, fw.rating, fw.type, fw.created, fw.modified 
                            """, (batch_ids,))
                    batch_movies = tuple(dict(row) for row in curs.fetchall())
                    yield batch_movies
