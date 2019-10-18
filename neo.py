from neo4j import GraphDatabase


query = "MATCH (n:Movie) WHERE n.`http://xmlns.com/foaf/0.1/name` IN [$entities] WITH collect(n) as movies "\
        "CALL algo.pageRank.stream('MovieRelated', 'http://wikidata.dbpedia.org/ontology/starring',"\
        "{iterations: 300, dampingFactor: 0.85, sourceNodes: movies, direction: 'BOTH'}) YIELD nodeId, score "\
        "RETURN algo.asNode(nodeId).`http://xmlns.com/foaf/0.1/name` AS page,score "\
        "ORDER BY score DESC LIMIT 50"

_uri = "bolt://172.19.2.123:7778"
driver = GraphDatabase.driver(_uri, auth=("neo4j", "root123"))



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


def _get_unseen_entities(tx, uris): 
    query = """ 
        MATCH (m: MovieRelated) WHERE NOT m.uri IN $uris RETURN m.uri
    """

    return tx.run(query, uris=uris)


def get_unseen_entities(uris): 
    with driver.session() as session:
        res = session.read_transaction(_get_unseen_entities, uris=uris)

    return res.value()


def get_relevant_neighbors(uri_list):
    with driver.session() as session:
        res = session.read_transaction(_get_relevant_neighbors, uri_list)

    relevant_uris = [n["uri"] for n in res.value()]

    return relevant_uris


def create_genre(genre, uri):
    with driver.session() as session:
        tx = session.begin_transaction()
        tx.run("CREATE (n:Genre { `http://www.w3.org/2000/01/rdf-schema#label`: $genre, uri: $uri })",
               genre=genre, uri=uri)
        tx.commit()


def _get_relevant_neighbors(tx, uri_list):
    q = """
        MATCH (n:Movie) WHERE n.uri IN $uris WITH collect(n) AS movies
        CALL algo.pageRank.stream(
          'MovieRelated',
          null,
          {iterations: 50, dampingFactor: 0.95, sourceNodes: movies, direction: 'BOTH'}
        )
        YIELD nodeId, score
        RETURN algo.asNode(nodeId) AS page, score
        ORDER BY score DESC LIMIT 50"""

    return tx.run(q, uris=uri_list)


if __name__ == "__main__":
    a = get_unseen_entities([])
    print('http://wikidata.dbpedia.org/resource/Q208108' not in a)
    print('http://wikidata.dbpedia.org/resource/Q241309' not in a)