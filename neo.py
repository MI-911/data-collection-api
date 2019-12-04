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
    query = """
            MATCH (r)<--(m:Movie) WHERE r.uri IN $uris AND NOT m.uri IN $seen
                WITH m.uri AS uri, m.pagerank AS pr, m.weight AS weight, count(r) AS connected
                WITH collect({uri: uri, pr: pr, weight: weight, c: connected}) AS movies, sum(connected) AS total
                UNWIND movies as m
                WITH collect({uri: m.uri, weight: m.weight, pr: m.pr, c: 1.0 * m.c / total}) as movies
            UNWIND movies AS movie
                WITH movie.uri AS uri, movie.pr AS pr, movie.weight AS weight, movie.c AS c
            RETURN uri, pr AS score, sum(c) AS links, weight AS weight
            ORDER BY links DESC LIMIT 10
            """
    args = {'uris': source_uris, 'seen': seen}

    with driver.session() as session:
        res = session.read_transaction(_generic_get, query, args)
        res = [{'uri': r['uri'], 'score': r['score'], 'weight': r['weight']} for r in res]

    return res


def get_relevant_neighbors(uri_list, seen_uri_list):
    query = """
            MATCH (n)--(m) WHERE n.uri IN $uris WITH id(m) AS nodeId
            MATCH (m) WHERE id(m) = nodeId AND NOT m.uri IN $seen
                WITH DISTINCT id(m) AS id, m.pagerank AS score
            ORDER BY score
            OPTIONAL MATCH (r)<--(m:Movie) WHERE id(r) = id
                WITH algo.asNode(id) AS r, m, RAND() AS random, score
            ORDER BY m.weight DESC
                WITH r, collect(DISTINCT m)[..5] as movies, score
            RETURN r:Director AS director, r:Actor AS actor, r.imdb AS imdb, r:Subject AS subject, r:Movie as movie,
                r:Company AS company, r:Decade AS decade, r.uri AS uri, r.name AS name, r:Genre as genre,
                r:Person as person, r:Category as category, r.image AS image, r.year AS year, r.weight AS weight,
                movies, score
            """

    args = {'uris': uri_list, 'seen': seen_uri_list}

    with driver.session() as session:
        res = session.read_transaction(_generic_get, query, args)
        res = [r for r in res]

    return res
