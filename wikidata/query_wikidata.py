from SPARQLWrapper import SPARQLWrapper, JSON

endpoint_url = 'https://query.wikidata.org/sparql'
user_agent = 'MI911 <mi911e19@cs.aau.dk>'

genre_query = """SELECT ?genre ?genreLabel ?film WHERE {
                 SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                 ?film wdt:P136 ?genre.
                 ?film wdt:P345 "%s".
              }"""

sequel_query = """SELECT ?sequel WHERE {
                  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                  wd:%s wdt:P156 ?sequel.
               }"""

series_sequel_query = """SELECT ?sequel {
                         SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                         wd:%s p:P179 ?series.
                         ?series pq:P156 ?sequel.
                      }"""

genre_subclass_query = """SELECT ?subclass WHERE {
                           SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                           wd:%s wdt:P279 ?subclass.
                       }"""

subject_query = """SELECT ?subject ?subjectLabel WHERE {
                   SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                   wd:%s wdt:P921 ?subject.
                }"""

company_query = """SELECT ?company ?companyLabel WHERE {
                   SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                   wd:%s wdt:P272 ?company.
                }"""

actor_query = """SELECT ?actor ?actorLabel ?actorImdb ?actorImage WHERE {
                 SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                 {wd:%s wdt:P161 ?actor} UNION {wd:%s wdt:P725 ?actor}.
                 ?actor wdt:P345 ?actorImdb.
                 OPTIONAL { ?actor wdt:P18 ?actorImage }.
              }"""

director_query = """SELECT ?director ?directorLabel ?directorImdb ?directorImage WHERE {
                      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                      wd:%s wdt:P57 ?director.
                      ?director wdt:P345 ?directorImdb.
                      OPTIONAL { ?director wdt:P18 ?directorImage }.
                    }"""


def get_results(query):
    sparql = SPARQLWrapper(endpoint_url)
    sparql.addCustomHttpHeader('User-Agent', user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()['results']['bindings']


def get_genres(imdb_id):
    genres = dict()
    movie_uri = None

    for result in get_results(genre_query % imdb_id):
        if not movie_uri:
            movie_uri = result['film']['value']
        genres[result['genre']['value']] = result['genreLabel']['value']

    return movie_uri, genres


def get_sequels(entity_id):
    sequels = set()

    for result in get_results(series_sequel_query % entity_id):
        sequels.add(result['sequel']['value'])

    for result in get_results(sequel_query % entity_id):
        sequels.add(result['sequel']['value'])

    return sequels


def get_subclasses(entity_id):
    subclasses = set()

    for result in get_results(genre_subclass_query % entity_id):
        subclasses.add(result['subclass']['value'])

    return subclasses


def get_subjects(entity_id):
    subjects = dict()

    for result in get_results(subject_query % entity_id):
        subjects[result['subject']['value']] = result['subjectLabel']['value']

    return subjects


def get_companies(entity_id):
    companies = dict()

    for result in get_results(company_query % entity_id):
        companies[result['company']['value']] = result['companyLabel']['value']

    return companies


def get_people(entity_id):
    actors = dict()
    directors = dict()

    for result in get_results(actor_query % (entity_id, entity_id)):
        actor_uri = result['actor']['value']
        actor_imdb = result['actorImdb']['value']
        actor_name = result['actorLabel']['value']
        actor_image = result['actorImage']['value'] if 'actorImage' in result else None

        actors[actor_uri] = dict(imdb=actor_imdb, name=actor_name, image=actor_image)

    for result in get_results(director_query % entity_id):
        director_uri = result['director']['value']
        director_imdb = result['directorImdb']['value']
        director_name = result['directorLabel']['value']
        director_image = result['directorImage']['value'] if 'directorImage' in result else None

        directors[director_uri] = dict(imdb=director_imdb, name=director_name, image=director_image)

    return actors, directors


if __name__ == "__main__":
    print(get_sequels('Q181803'))
