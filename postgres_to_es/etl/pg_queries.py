get_updated_movies_query = """
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
"""
