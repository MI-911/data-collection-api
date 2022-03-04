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
            MATCH (h)-[r]->(t) RETURN h.uri AS head_uri, TYPE(r) AS relation, t.uri AS tail_uri
            """

    with driver.session() as session:
        res = session.read_transaction(_generic_get, query)

    return [record for record in res]


def get_last_batch(source_uris, seen):
    query = """
            MATCH (n) WHERE n.uri IN $uris WITH COLLECT(n) AS nLst
            CALL particlefiltering(nLst, 0, 100) YIELD nodeId, score
            MATCH (n) WHERE n:Movie AND id(n) = nodeId AND NOT n.uri IN $seen RETURN n.uri AS uri, score
            ORDER BY score DESC
            LIMIT 10
    """

    args = {'uris': source_uris, 'seen': seen}

    with driver.session() as session:
        res = session.read_transaction(_generic_get, query, args)
        res = [{'uri': r['uri'], 'score': r['score']} for r in res]

    return res


def get_relevant_neighbors(uri_list, seen_uri_list, k=25):
    query = """
             MATCH (n) WHERE n.uri IN $uris WITH COLLECT(n) AS nLst
            CALL particlefiltering(nLst, 0, 100) YIELD nodeId, score
            MATCH (n) WHERE id(n) = nodeId AND NOT n.uri IN $seen
                WITH DISTINCT id(n) AS id, score, n.name AS name, labels(n) AS l
            ORDER BY score DESC
                WITH DISTINCT l, collect({id: id, s: score, n: name})[..$k] AS topk
            UNWIND topk AS t
                WITH t.id AS id, t.s AS score, t.n AS name
            OPTIONAL MATCH (r)<--(m:Movie) WHERE id(r) = id AND NOT r:Movie
                WITH algo.asNode(id) AS r, m, score
            ORDER BY m.weight DESC
                WITH r, collect(DISTINCT m)[..5] as movies, score
            RETURN r:Director AS director, r:Actor AS actor, r.imdb AS imdb, r:Subject AS subject, r:Movie as movie,
                   r:Company AS company, r:Decade AS decade, r.uri AS uri, r.name AS name, r:Genre as genre,
                   r:Person as person, r:Category as category, r.image AS image, r.year AS year, movies, score
            """

    args = {'uris': uri_list, 'seen': seen_uri_list, 'k': k}

    with driver.session() as session:
        res = session.read_transaction(_generic_get, query, args)
        res = [r for r in res]

    return res
