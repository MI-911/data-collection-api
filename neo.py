from neo4j import GraphDatabase


query = "MATCH (n:Movie) WHERE n.`http://xmlns.com/foaf/0.1/name` IN [%entities] OR n.`http://www.w3.org/2000/01/rdf-schema#label` IN [%entities] WITH collect(n) as movies "\
        "CALL algo.pageRank.stream('MovieRelated', 'http://wikidata.dbpedia.org/ontology/starring',"\
        "{iterations: 300, dampingFactor: 0.95, sourceNodes: movies, direction: 'BOTH'}) YIELD nodeId, score "\
        "RETURN algo.asNode(nodeId).`http://xmlns.com/foaf/0.1/name` AS page,score "\
        "ORDER BY score DESC LIMIT 50"


def _get_related_entities(tx, entities):
    for record in tx.run(query.replace('%entities', entities)):
        print(record)


def get_related_entities(entities):
    flattened = ', '.join([f"'{entity}'" for entity in entities])
    print(flattened)

    uri = "bolt://52.136.231.143:7778"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "root123"))

    with driver.session() as session:
        session.read_transaction(_get_related_entities, flattened)
