from neo4j import GraphDatabase
from os import environ

_uri = environ.get('BOLT_URI', 'bolt://localhost:7778')
driver = GraphDatabase.driver(_uri, auth=("neo4j", "root123"))


def _generic_get(tx, query, args=None):
    if args:
        return tx.run(query, **args)
    else:
        return tx.run(query)


def get_number_entities():
    query = """
            MATCH (n) RETURN COUNT(n) as count
            """

    with driver.session() as session:
        res = session.read_transaction(_generic_get, query)
        res = res.single()

    return res['count']


def get_counts():
    query = """
            CALL apoc.meta.stats() YIELD labels
            RETURN labels {.Person, .Category, .Decade, .Company, .Movie} AS counts
            """

    with driver.session() as session:
        res = session.read_transaction(_generic_get, query).single()

        return res[0]


def get_entities():
    query = """
            MATCH (n) RETURN n.uri AS uri, n.name AS name, LABELS(n) AS labels
            """
            
    with driver.session() as session:
        res = session.read_transaction(_generic_get, query)

    return [record for record in res]


def get_triples():
    query = """
            MATCH (h)-[r]-(t) RETURN h.uri AS head_uri, TYPE(r) AS relation, t.uri AS tail_uri
            """

    with driver.session() as session:
        res = session.read_transaction(_generic_get, query)

    return [record for record in res]


def get_last_batch(source_uris, seen):
    # count(r) is the number of connections to the movies
    query = """
            MATCH (r)<--(m:Movie) WHERE r.uri IN $uris AND NOT m.uri IN $seen
                WITH m.uri AS uri, m.pagerank * log(1 + count(r)) * log(1 + m.weight) AS score, m.weight AS weight
            RETURN uri, score, weight
            """
    args = {'uris': source_uris, 'seen': seen}

    with driver.session() as session:
        res = session.read_transaction(_generic_get, query, args)
        res = [{'uri': r['uri'], 'score': r['score'], 'weight': r['weight']} for r in res]

    return res


def get_relevant_neighbors(uri_list, seen_uri_list):
    query = """
            MATCH (n)--(m) WHERE m.uri IN $uris WITH id(n) AS nodeId, count(n) AS connections
            MATCH (n) WHERE id(n) = nodeId AND NOT n.uri IN $seen
                WITH DISTINCT id(n) AS id, connections AS multiplier, n.name AS name
            OPTIONAL MATCH (r)<--(m:Movie) WHERE id(r) = id
                WITH algo.asNode(id) AS r, m, multiplier
            ORDER BY m.weight DESC
                WITH r, collect(DISTINCT m)[..5] as movies, multiplier
            RETURN r:Director AS director, r:Actor AS actor, r.imdb AS imdb, r:Subject AS subject, r:Movie as movie,
                   r:Company AS company, r:Decade AS decade, r.uri AS uri, r.name AS name, r:Genre as genre,
                   r:Person as person, r:Category as category, r.image AS image, r.year AS year, movies,
                   r.pagerank * log(1 + multiplier) AS score
            """

    args = {'uris': uri_list, 'seen': seen_uri_list}

    with driver.session() as session:
        res = session.read_transaction(_generic_get, query, args)
        res = [r for r in res]

    return res


if __name__ == '__main__':
    print(sum(get_counts().values()))
