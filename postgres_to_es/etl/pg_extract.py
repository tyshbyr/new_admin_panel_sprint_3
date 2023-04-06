"""
    Получает данные из Postgress пачками.
    Обрабатывает падение Postgres.
    После падения Postgres начинает читать с последней обработанной
    записи.
"""

import psycopg2
from psycopg2.extras import DictCursor
import collections.abc as collections_abc
from psycopg2.extras import DictCursor


class PostgresExtractor:

    def __init__(self, pg_conn, batch_size, dt) -> None:
        self.pg_conn = pg_conn
        self.batch_size = batch_size
        self.dt = dt
        
    def get_ids(self, table) -> collections_abc.Iterator[list]:
        """
        Функция возвращает генератор кортежей id
        """
        last_id = None
        with self.pg_conn.cursor(cursor_factory=DictCursor) as curs:
            while True:
                query = f'SELECT id FROM content.{table}'
                query_args = []
                if last_id:
                    query += f" WHERE id > %s AND modified > %s"
                    query_args.append(last_id)
                else:
                    query += f" WHERE modified > %s"

                query += " ORDER BY id LIMIT %s"
                query_args.append(self.dt)
                query_args.append(self.batch_size)
                curs.execute(query, query_args)
                if not curs.rowcount:
                    break
                batch = tuple(row[0] for row in curs.fetchall())
                yield batch
                last_id = batch[-1]

        

    def get_filmwork_ids_for_table(self, table):
        """
        Функция принимает генератор кортежей id таблицы персон или жанров
        и возвращает генератор кортежей id связанных фильмов
        """
        ids = self.get_ids(table)
        query = f"""
                    SELECT id
                    FROM content.film_work
                    WHERE id IN (
                        SELECT film_work_id 
                        FROM content.{table}_film_work 
                        WHERE {table}_id IN %s);
                """
        with self.pg_conn.cursor(cursor_factory=DictCursor) as curs:
            while True:
                curs.execute(query, (next(ids),))
                if not curs.rowcount:
                    break
                batch = tuple(row[0] for row in curs.fetchall())
                yield batch
            
    def get_updated_movies(self, filmwork_ids):
        """
        Функция принимает генератор кортежей id таблицы film_work
        и возвращает генератор кортежей в которых в виде словарей представлена вся инфа о фильмах
        """
        with self.pg_conn.cursor(cursor_factory=DictCursor) as curs:
            while True:
                curs.execute("""
                            SELECT
                                fw.id as fw_id, 
                                fw.title, 
                                fw.description, 
                                fw.rating, 
                                fw.type, 
                                fw.created, 
                                fw.modified, 
                                pfw.role, 
                                p.id, 
                                p.full_name,
                                g.name
                            FROM content.film_work fw
                            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                            LEFT JOIN content.person p ON p.id = pfw.person_id
                            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                            LEFT JOIN content.genre g ON g.id = gfw.genre_id
                            WHERE fw.id IN %s; 
                        """, (next(filmwork_ids),))
                
                if not curs.rowcount:
                    break
                batch = tuple(dict(row) for row in curs.fetchall())
                yield batch
                
                
def combine_generators(*generators):
    res = []
    for generator in generators:
        [res.append(item) for item in next(generator)]
        yield tuple(res)
                

    
def extract_updated_movies(dsl, batch_size, dt):
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as conn:
        extract = PostgresExtractor(conn, batch_size, dt)
        movies_by_person = extract.get_filmwork_ids_for_table('person')
        movies_by_genre = extract.get_filmwork_ids_for_table('genre')
        movies_by_film_work = extract.get_ids('film_work')
        
        movies = combine_generators(
                                    movies_by_person,
                                    movies_by_genre,
                                    movies_by_film_work)
        
        return extract.get_updated_movies(movies)

        
       
        
   