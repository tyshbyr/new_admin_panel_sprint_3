import collections.abc as collections_abc
from psycopg2.extras import DictCursor, RealDictCursor
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
        with self.pg_conn.cursor(cursor_factory=RealDictCursor) as curs:
            for filmwork_ids_generator in film_work_ids_generators:
                for batch_ids in filmwork_ids_generator:
                    curs.execute("""
                                SELECT fw.id,
                                    fw.rating AS imdb_rating,
                                    array_agg(DISTINCT g.name) AS genre,
                                    fw.title,
                                    fw.description,
                                    array_agg(DISTINCT p.full_name) FILTER (WHERE pf.role = 'director') AS director,
                                    array_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pf.role = 'actor') AS actors,
                                    array_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pf.role = 'writer') AS writers,
                                    array_agg(DISTINCT p.full_name) FILTER (WHERE pf.role = 'actor') AS actors_names,
                                    array_agg(DISTINCT p.full_name) FILTER (WHERE pf.role = 'writer') AS writers_names
                                FROM content.film_work fw
                                LEFT JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
                                LEFT JOIN content.genre g ON gfw.genre_id = g.id
                                LEFT JOIN content.person_film_work pf ON fw.id = pf.film_work_id
                                LEFT JOIN content.person p ON pf.person_id = p.id
                                WHERE fw.id IN %s
                                GROUP BY fw.id, fw.title, fw.description, fw.rating
                                ORDER BY fw.title;
                            """, (batch_ids,))
                    batch_movies = [row for row in curs.fetchall()]
                    yield batch_movies
