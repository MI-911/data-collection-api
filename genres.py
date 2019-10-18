from dataset import genres_unique, movies
from neo import create_genre, driver


def _genre_uri(genre):
    return f'genre/{genre.lower()}'


def insert_genres():
    for genre in list(genres_unique.genre):
        uri = _genre_uri(genre)
        print(uri)

        create_genre(genre, uri)


def insert_relations():
    with driver.session() as session:
        tx = session.begin_transaction()
        for index, movie in movies.iterrows():
            movie_uri = str(movie.uri)

            for genre in movie['genres']:
                genre_uri = _genre_uri(genre)

                tx.run("MATCH (m:Movie {uri: $movieUri}), (g:Genre {uri: $genreUri}) "
                       "CREATE (m)-[r:`HAS_GENRE`]->(g)", movieUri=movie_uri, genreUri=genre_uri)

        print("Commit")
        tx.commit()


if __name__ == "__main__":
    # insert_genres()
    insert_relations()
