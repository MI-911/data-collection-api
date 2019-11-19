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


def get_triples():
    query = """
            MATCH (h)-[r]-(t) RETURN h.uri AS head_uri, TYPE(r) AS relation, t.uri AS tail_uri
        """
    with driver.session() as session:
        res = session.read_transaction(_generic_get, query)

    return [record for record in res]


def get_last_batch(source_uris, seen):
    query = """
        MATCH (m:Movie)-->(r) WHERE m.uri IN $uris AND NOT r IN $seen
            WITH id(r) AS id
        MATCH (r)<--(m:Movie) WHERE id(r) = id AND NOT m.uri IN $seen
            WITH m.uri AS uri, m.pagerank AS pr,  count(r) AS connected
            WITH collect({uri: uri, pr: pr, c: connected}) as movies, sum(connected) AS total
            UNWIND movies as m
            WITH collect({uri: m.uri, pr: m.pr, c: 1.0 * m.c / total}) as movies
        MATCH (r)<--(m:Movie) WHERE r.uri IN $uris AND NOT m.uri IN $seen
            WITH movies, m.uri AS uri, m.pagerank AS pr,  count(r) AS connected
            WITH movies, collect({uri: uri, pr: pr, c: connected}) AS movies2, sum(connected) AS total
            UNWIND movies2 as m
            WITH movies + collect({uri: m.uri, pr: m.pr, c: 1.0* m.c / total}) as movies
        UNWIND movies AS movie
            WITH movie.uri AS uri, movie.pr AS pr, movie.c AS c
        RETURN uri, pr, sum(c) AS s
        ORDER BY s DESC, pr DESC
        LIMIT 10
    """
    args = {'uris': source_uris, 'seen': seen}

    with driver.session() as session:
        res = session.read_transaction(_generic_get, query, args)
        res = [r['uri'] for r in res]

    return res


def get_one_hop_entities(uri):
    query = "MATCH (m)-[]-(t)  WHERE m.uri = $uri RETURN t"
    args = {'uri': uri}

    with driver.session() as session:
        res = session.read_transaction(_generic_get, query, args)

    return [_get_schema_label(n) for n in res.value()]


def _get_schema_label(node): 
    if 'http://www.w3.org/2000/01/rdf-schema#label' in node._properties: 
        return node._properties['http://www.w3.org/2000/01/rdf-schema#label']
    else: 
        return 'N/A'


def get_unseen_entities(source_uris, seen, limit):
    query = """ 
        MATCH (m:Movie)-->(r) WHERE m.uri IN $suris AND NOT r.uri IN $seen
          WITH DISTINCT r
        ORDER BY r.pagerank DESC
          WITH LABELS(r)[1] AS label, COLLECT(r)[..25] AS nodes
          UNWIND nodes AS n
          WITH label, n, RAND() AS r
        ORDER BY r
          WITH label, COLLECT(n)[..3] AS nodes
          UNWIND nodes AS n WITH n
        ORDER BY n.pagerank DESC
          LIMIT $lim
          WITH id(n) AS id
        OPTIONAL MATCH (n)<-[r]-(m:Movie) WHERE id(n) = id
        WITH algo.asNode(id) AS n, m, type(r) AS relation
        ORDER BY m.weight DESC
            WITH n, m, collect(relation) as r
            WITH n, collect(DISTINCT m)[..5] as movies, r
        UNWIND (CASE movies WHEN [] then [null] else movies end) AS m
            WITH n, COLLECT({movie:m, relation: r}) AS movies
        RETURN n:Director AS director, n:Actor AS actor, n.imdb AS imdb, n:Subject AS subject, n:Movie as movie,
            n:Company AS company, n:Decade AS decade, n.uri AS uri, n.name AS name, n:Genre as genre, n.image AS image,
            n.year AS year, movies
    """
    args = {'suris': source_uris, 'seen': seen, 'lim': limit}
    with driver.session() as session:
        res = session.read_transaction(_generic_get, query, args)
        res = [r for r in res]

    return res


def get_relevant_neighbors(uri_list, seen_uri_list):
    query = """
        MATCH (n)--(m) WHERE n.uri IN $uris WITH id(m) AS nodeId
        MATCH (m) WHERE id(m) = nodeId AND NOT m.uri IN $seen
            WITH DISTINCT id(m) AS id, m.pagerank AS score
        ORDER BY score DESC LIMIT 50
        OPTIONAL MATCH (n)<-[r]-(m:Movie) WHERE id(n) = id
        WITH algo.asNode(id) AS n, m, score, type(r) AS relation
        ORDER BY m.weight DESC
            WITH n, m, collect(relation) as r, score
            WITH n, collect(DISTINCT m)[..5] as movies, r, score
        UNWIND (CASE movies WHEN [] then [null] else movies end) AS m
            WITH n, COLLECT({movie:m, relation: r}) AS movies, score
        RETURN n:Director AS director, n:Actor AS actor, n.imdb AS imdb, n:Subject AS subject, n:Movie as movie,
            n:Company AS company, n:Decade AS decade, n.uri AS uri, n.name AS name, n:Genre as genre, n.image AS image,
            n.year AS year, movies, score
        """

    args = {'uris': uri_list, 'seen': seen_uri_list}

    with driver.session() as session:
        res = session.read_transaction(_generic_get, query, args)
        res = [r for r in res]

    return res
    

def create_genre(genre, uri):
    query = "CREATE (n:Genre { `http://www.w3.org/2000/01/rdf-schema#label`: $genre, uri: $uri })"
    args = {'genre': genre, 'uri': uri}
    with driver.session() as session:
        res = session.run(_generic_get, query, args)

    return res
