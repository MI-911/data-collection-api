import pandas as pd
from neo4j import GraphDatabase

from dataset import get_movies_iter


def map_movies(path):
    mapping = {}
    saved = 0
    i = 0
    for i, movie in get_movies_iter():
        if i % 100 == 0:
            print(i)

        res = get_dbmovies(movie.title, str(movie.year))

        if res:
            mapping[movie.movieId] = res._properties['uri']
            saved += 1

    print(f"Saved {saved} out of {i}")

    keys = list(mapping.keys())
    values = list(mapping.values())

    data = {'movieId': keys, 'uri': values}

    df = pd.DataFrame.from_dict(data)
    df.to_csv(path)


def get_dbmovies(title, year):
    uri = "bolt://52.136.231.143:7778"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "root123"))

    with driver.session() as session:
        res = session.read_transaction(read_movie_nodes, name=title).value()
        if len(res) > 1:
            try:
                res = next(movie for movie in res
                           if 'http://wikidata.dbpedia.org/ontology/abstract' in movie._properties
                           and year in movie._properties['http://wikidata.dbpedia.org/ontology/abstract'])
            except StopIteration:
                res = []
        elif res:
            res = res[0]

    return res


def read_movie_nodes(tx, name):
    query = "MATCH (m: Movie) " \
            "WHERE m.`http://xmlns.com/foaf/0.1/name` = $name " \
            "RETURN m"

    return tx.run(query, name=name)


if __name__ == "__main__":
    map_movies("/home/theis/Projects/sw9/data-collection-api/dataset/mapping.csv")