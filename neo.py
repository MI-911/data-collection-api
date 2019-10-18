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


def _get_unseen_entities(tx, uris, limit):
    query = """ 
        MATCH (m: MovieRelated) WHERE NOT m.uri IN $uris 
        WITH m, rand() AS number
        RETURN m.`http://www.w3.org/2000/01/rdf-schema#label` AS label, m:Director AS director, m:Actor AS actor, 
               m:Subject AS subject, m.uri AS uri, m.`http://xmlns.com/foaf/0.1/name` AS name
        ORDER BY number
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


def sample_relevant_neighbours(entities, n_actors=None, n_directors=None, n_subjects=None): 
    '''
    Attempts to sample n_actors, n_directors and n_subjects from the entities. 
    Returns an array of entities of size n_actors + n_directors + n_subjects.
    If there are not enough of either type of entity, the remaining space is filled 
    out with entities from the entity list, sampled in order of PageRank.  
    '''
    actors = [r for r in entities if r['actor']]
    directors = [r for r in entities if ['director']]
    subjects = [r for r in entities if ['subject']]

    all_entities = actors[:n_actors] + directors[:n_directors] + subjects[n_subjects]
    if len(all_entities) < n_actors + n_directors + n_subjects: 
        to_add = (n_actors + n_directors + n_subjects) - len(all_entities)
        not_added = [r for r in entities if r not in all_entities]
        to_add += not_added[:to_add]

    return all_entities
    


def _get_relevant_neighbors(tx, uri_list, seen_uri_list):
    q = """
        MATCH (n:Movie) WHERE n.uri IN $uris WITH collect(n) AS movies
        CALL algo.pageRank.stream(
          'MovieRelated',
          null,
          {iterations: 50, dampingFactor: 0.95, sourceNodes: movies, direction: 'BOTH'}
        )
        YIELD nodeId, score
        MATCH (m) where id(m) = nodeId AND NOT m.uri in $seen
        RETURN m.`http://www.w3.org/2000/01/rdf-schema#label` AS label, m:Director AS director, m:Actor AS actor, 
               m:Subject AS subject, m.uri AS uri, m.`http://xmlns.com/foaf/0.1/name` AS name
        ORDER BY score DESC LIMIT 50"""

    return tx.run(q, uris=uri_list, seen=seen_uri_list)


if __name__ == "__main__":
    pass