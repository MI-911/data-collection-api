from neo4j import GraphDatabase


query = "MATCH (n:Movie) WHERE n.`http://xmlns.com/foaf/0.1/name` IN [$entities] WITH collect(n) as movies "\
        "CALL algo.pageRank.stream('MovieRelated', 'http://wikidata.dbpedia.org/ontology/starring',"\
        "{iterations: 300, dampingFactor: 0.85, sourceNodes: movies, direction: 'BOTH'}) YIELD nodeId, score "\
        "RETURN algo.asNode(nodeId).`http://xmlns.com/foaf/0.1/name` AS page,score "\
        "ORDER BY score DESC LIMIT 50"




def _get_related_entities(tx, entities):
    for record in tx.run(query.replace('%entities', entities)):
        print(record)


def _get_one_hop_entities(tx, uri): 
    query = "MATCH (m)-[]-(t)  WHERE m.uri = $uri RETURN t" 

    return tx.run(query, uri=uri)


def get_one_hop_entities(uri): 
    uri = "bolt://52.136.231.143:7778"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "root123"))

    with driver.session() as session:
        res = session.read_transaction(_get_one_hop_entities, uri=uri)

    return res.value()


def get_related_entities(entities):
    uri = "bolt://52.136.231.143:7778"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "root123"))

    with driver.session() as session:
        for record in session.run(query, entities=list(entities)):
            print(record)


