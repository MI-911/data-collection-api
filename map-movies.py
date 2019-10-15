from neo4j import GraphDatabase

from dataset import get_movies_iter


def map_movies():
    mapping = {}
    for i, movie in get_movies_iter():
        if i > 100:
            break
        title = movie.title

        res = get_dbmovies(title)

        if res != None:
            mapping[movie.movieId] = res.value()._properties['uri']
        else:
            mapping[movie.movieId] = None

    t = len([m for m, d in mapping.items() if d is not None])

    print(t / len(mapping))


def get_dbmovies(title):
    uri = "bolt://52.136.231.143:7778"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "root123"))

    with driver.session() as session:
        res = session.read_transaction(read_movie_nodes, name=title).single()

    return res


def read_movie_nodes(tx, name):
    return tx.run("MATCH (m: Movie) WHERE m.`http://xmlns.com/foaf/0.1/name` = $name RETURN m", name=name)


if __name__ == "__main__":
    map_movies()