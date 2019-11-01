from dataset import get_actor_id, movies


def sample_relevant_neighbours(entities, n_actors=None, n_directors=None, n_subjects=None):
    """
    Attempts to sample n_actors, n_directors and n_subjects from the entities.
    Returns an array of entities of size n_actors + n_directors + n_subjects.
    If there are not enough of either type of entity, the remaining space is filled
    out with entities from the entity list, sampled in order of PageRank.
    """
    actors = [r for r in entities if r['actor']]
    directors = [r for r in entities if r['director']]
    subjects = [r for r in entities if r['subject']]

    all_entities = actors[:n_actors] + directors[:n_directors] + subjects[:n_subjects]
    if len(all_entities) < n_actors + n_directors + n_subjects:
        to_add = (n_actors + n_directors + n_subjects) - len(all_entities)
        not_added = [r for r in entities if r not in all_entities]
        all_entities += not_added[:to_add]

    return all_entities


def get_description(record):
    titles = []

    if record['director']:
        titles.append('Director')
    if record['actor']:
        titles.append('Actor')
    if record['subject']:
        titles.append('Subject')
    if record['movie']:
        titles.append('Movie')
    if record['genre']:
        titles.append('Genre')

    return ', '.join(titles)


def _person(record):
    return record['director'] or record['actor']


def get_resource(record):
    if _person(record):
        return 'actor'
    elif record['movie']:
        return 'movie'

    return 'subject'


def get_id(record):
    if _person(record) or record['movie']:
        return record['imdb']

    return None


def get_image(record):
    print(record)
    if record['image']:
        return record['image']
    elif record['movie']:
        return f'https://www.mindreader.tech/static/movie/{record["imdb"]}'
    elif _person(record):
        return f'https://www.mindreader.tech/static/actor/{record["imdb"]}'

    pass


def record_to_entity(record):
    return {
        "name": record['name'],
        "resource": get_resource(record),
        "uri": record['uri'],
        "image": get_image(record),
        "description": get_description(record),
        "movies": ['{title} ({year})'.format(**_movie_from_uri(node['uri'])) for node in record['movies']]
    }


def _movie_from_uri(uri):
    row = iter(movies.loc[movies['uri'] == uri, movies.columns].values)
    return {
        attr: val
        for attr, val in zip(movies.columns, next(row, []))
    }
