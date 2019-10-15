from neo4j import GraphDatabase

from dataset import get_movies_iter


def map_movies():
    mapping = {}
    for i, movie in get_movies_iter():
        if i > 100:
            break

        res = get_dbmovies(movie.title, str(movie.year))

        if res != None:
            mapping[movie.movieId] = res.value()._properties['uri']
        else:
            mapping[movie.movieId] = None

    t = len([m for m, d in mapping.items() if d is not None])

    print(t / len(mapping))


def get_dbmovies(title, year):
    uri = "bolt://52.136.231.143:7778"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "root123"))

    with driver.session() as session:
        res = session.read_transaction(read_movie_nodes, name=title, year=year)

    return res.single()


def read_movie_nodes(tx, name, year):
    query = "MATCH (m: Movie) " \
            "WHERE m.`http://xmlns.com/foaf/0.1/name` = $name " \
            "AND m.`http://wikidata.dbpedia.org/ontology/abstract` CONTAINS $year " \
            "RETURN m"

    return tx.run(query, name=name, year=year)


if __name__ == "__main__":
    map_movies()