from dataclasses import dataclass
from typing import List
from uuid import UUID

@dataclass
class Person:
    id: UUID
    name: str
    
    def as_dict(self):
        return {'id': str(self.id), 'name': self.name}


@dataclass
class Movie:
    id: UUID
    imdb_rating: float
    genre: List[str]
    title: str
    description: str
    director: List[str]
    actors: List[Person]
    writers: List[Person]
    actors_names: List[str]
    writers_names: List[str]

    def __init__(self, row):
        self.id = UUID(row['id'])
        self.imdb_rating = row['imdb_rating']
        self.genre = row['genre']
        self.title = row['title']
        self.description = row['description']
        self.director = row['director'] if row['director'] else ''
        if row['actors'] is not None:
            self.actors = [Person(UUID(actor['id']), actor['name']) for actor in row['actors']]
        else:
            self.actors = []
        if row['writers'] is not None:
            self.writers = [Person(UUID(writer['id']), writer['name']) for writer in row['writers']]
        else:
            self.writers = []
        self.actors_names = row['actors_names']
        self.writers_names = row['writers_names']
        
    def as_dict(self):
        return {
            'id': str(self.id),
            'imdb_rating': self.imdb_rating,
            'genre': self.genre,
            'title': self.title,
            'description': self.description,
            'director': self.director,
            'actors': [actor.as_dict() for actor in self.actors],
            'writers': [writer.as_dict() for writer in self.writers],
            'actors_names': self.actors_names,
            'writers_names': self.writers_names
        }
