from neo4j import GraphDatabase


query = "MATCH (n:Movie) WHERE n.`http://xmlns.com/foaf/0.1/name` IN [$entities] WITH collect(n) as movies "\
        "CALL algo.pageRank.stream('MovieRelated', 'http://wikidata.dbpedia.org/ontology/starring',"\
        "{iterations: 300, dampingFactor: 0.85, sourceNodes: movies, direction: 'BOTH'}) YIELD nodeId, score "\
        "RETURN algo.asNode(nodeId).`http://xmlns.com/foaf/0.1/name` AS page,score "\
        "ORDER BY score DESC LIMIT 50"

_uri = "bolt://localhost:7778"
driver = GraphDatabase.driver(_uri, auth=("neo4j", "root123"))


def _get_last_batch(tx, source_uris, seen):
    query = """
        MATCH (m:MovieRelated)-[]-(o:Movie) WHERE m.uri IN $uris AND NOT o.uri IN $seen
        RETURN o.pagerank AS pr, o.uri AS uri, count(m) AS c
        ORDER BY c DESC, pr DESC
        LIMIT 10
    """

    return tx.run(query, uris=source_uris, seen=seen)


def get_last_batch(source_uris, seen):
    with driver.session() as session:
        res = session.read_transaction(_get_last_batch, source_uris, seen)
        res = [r['uri'] for r in res]

    return res


def _get_related_entities(tx, entities):
    for record in tx.run(query.replace('%entities', entities)):
        print(record)


def _get_one_hop_entities(tx, uri): 
    query = "MATCH (m)-[]-(t)  WHERE m.uri = $uri RETURN t" 

    return tx.run(query, uri=uri)


def get_one_hop_entities(uri):
    with driver.session() as session:
        res = session.read_transaction(_get_one_hop_entities, uri=uri)

    return [_get_schema_label(n) for n in res.value()]


def get_related_entities(entities):
    with driver.session() as session:
        for record in session.run(query, entities=list(entities)):
            print(record)


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
    WITH d.entity as m, d.number + (d.count / total) as score
    RETURN m.`http://www.w3.org/2000/01/rdf-schema#label` AS label, m:Director AS director, m:Actor AS actor, 
           m:Subject AS subject, m:Movie as movie, m.uri AS uri, m.`http://xmlns.com/foaf/0.1/name` AS name,
           m:Genre as genre
    ORDER BY score DESC
    LIMIT $lim
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
        )
        YIELD nodeId, score
        MATCH (m) where id(m) = nodeId AND NOT m.uri in $seen AND NOT m:Movie
        RETURN m.`http://www.w3.org/2000/01/rdf-schema#label` AS label, m:Director AS director, m:Actor AS actor, 
               m:Subject AS subject, m:Movie as movie, m.uri AS uri, m.`http://xmlns.com/foaf/0.1/name` AS name,
               m:Genre as genre
        ORDER BY score DESC LIMIT 50"""

    return tx.run(q, uris=uri_list, seen=seen_uri_list)
