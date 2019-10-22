from neo4j import GraphDatabase
from os import environ

_uri = environ.get('BOLT_URI', 'bolt://localhost:7778')
driver = GraphDatabase.driver(_uri, auth=("neo4j", "root123"))


def _get_last_batch(tx, source_uris, seen):
    query = """
        MATCH (m:Movie)-->(r:MovieRelated) WHERE m.uri IN $uris AND NOT r IN $seen
            WITH id(r) AS id
        MATCH (r:MovieRelated)<--(m:Movie) WHERE id(r) = id AND NOT m.uri IN $seen
            WITH m.uri AS uri, m.pagerank AS pr,  count(r) AS connected
            WITH collect({uri: uri, pr: pr, c: connected}) as movies, sum(connected) AS total
            UNWIND movies as m
            WITH collect({uri: m.uri, pr: m.pr, c: 1.0* m.c / total}) as movies
        MATCH (r:MovieRelated)<--(m:Movie) WHERE r.uri IN $uris AND NOT m.uri IN $seen
            WITH movies, m.uri AS uri, m.pagerank AS pr,  count(r) AS connected
            WITH movies, collect({uri: uri, pr: pr, c: connected}) AS movies2, sum(connected) AS total
            UNWIND movies as m
            WITH movies + collect({uri: m.uri, pr: m.pr, c: 1.0* m.c / total}) as movies
        UNWIND movies AS movie
            WITH movie.uri AS uri, movie.pr AS pr, movie.c AS c
        RETURN uri, pr, sum(c) AS s
        ORDER BY s DESC, pr DESC
        LIMIT 10
    """

    return tx.run(query, uris=source_uris, seen=seen)


def get_last_batch(source_uris, seen):
    with driver.session() as session:
        res = session.read_transaction(_get_last_batch, source_uris, seen)
        res = [r['uri'] for r in res]

    return res


def _get_one_hop_entities(tx, uri): 
    query = "MATCH (m)-[]-(t)  WHERE m.uri = $uri RETURN t" 

    return tx.run(query, uri=uri)


def get_one_hop_entities(uri):
    with driver.session() as session:
        res = session.read_transaction(_get_one_hop_entities, uri=uri)

    return [_get_schema_label(n) for n in res.value()]


def _get_schema_label(node): 
    if 'http://www.w3.org/2000/01/rdf-schema#label' in node._properties: 
        return node._properties['http://www.w3.org/2000/01/rdf-schema#label']
    else: 
        return 'N/A'


def _get_unseen_entities(tx, uris, limit):
    query = """ 
    MATCH (m:MovieRelated)  WHERE NOT m.uri IN $uris
        WITH id(m) as id, rand() AS number
    ORDER BY number
    LIMIT 1000
    MATCH (m) WHERE id(m) = id with m, size((m)<--(:MovieRelated)) as c, number
        WITH COLLECT({entity: m, count:c, number: number}) as data, count(c) as total
    UNWIND data as d
        WITH id(d.entity) as id, d.number + (d.count / total) as score
    ORDER BY score DESC
    LIMIT $lim
    MATCH (r:MovieRelated)<--(m:Movie) WHERE id(r) = id
        WITH r, m, score
    ORDER BY score DESC, m.pagerank DESC
        WITH r, collect(m)[..5] as movies, score
    RETURN r.`http://www.w3.org/2000/01/rdf-schema#label` AS label, r:Director AS director, r:Actor AS actor, 
        r:Subject AS subject, r:Movie as movie, r.uri AS uri, r.`http://xmlns.com/foaf/0.1/name` AS name,
        r:Genre as genre, movies, score
    """

    return tx.run(query, uris=uris, lim=limit)


def get_unseen_entities(uris, limit):
    with driver.session() as session:
        res = session.read_transaction(_get_unseen_entities, uris, limit)
        res = [r for r in res]

    return res


def get_relevant_neighbors(uri_list, seen_uri_list):
    with driver.session() as session:
        res = session.read_transaction(_get_relevant_neighbors, uri_list, seen_uri_list)
        res = [r for r in res]

    return res
    

def create_genre(genre, uri):
    with driver.session() as session:
        tx = session.begin_transaction()
        tx.run("CREATE (n:Genre { `http://www.w3.org/2000/01/rdf-schema#label`: $genre, uri: $uri })",
               genre=genre, uri=uri)
        tx.commit()


def _get_relevant_neighbors(tx, uri_list, seen_uri_list):
    q = """
        MATCH (n:Movie) WHERE n.uri IN $uris WITH collect(n) AS movies
        CALL algo.pageRank.stream(
          'MovieRelated',
          null,
          {iterations: 50, dampingFactor: 0.95, sourceNodes: movies, direction: 'BOTH'}
        ) YIELD nodeId, score
        MATCH (m) where id(m) = nodeId AND NOT m.uri in $seen AND NOT m:Movie
            WITH id(m) as id, score
        ORDER BY score DESC LIMIT 50
        MATCH (r:MovieRelated)<--(m:Movie) WHERE id(r) = id
            WITH r, m, score
        ORDER BY score DESC, m.pagerank DESC
            WITH r, collect(m)[..5] as movies, score
        RETURN r.`http://www.w3.org/2000/01/rdf-schema#label` AS label, r:Director AS director, r:Actor AS actor, 
            r:Subject AS subject, r:Movie as movie, r.uri AS uri, r.`http://xmlns.com/foaf/0.1/name` AS name,
            r:Genre as genre, movies, score"""

    return tx.run(q, uris=uri_list, seen=seen_uri_list)
