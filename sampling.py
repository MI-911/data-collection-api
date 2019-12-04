from random import shuffle

from dataset import get_actor_id, movies


def sample_relevant_neighbours(entities, num_entities):
    """
    Attempts to sample n_actors, n_directors and n_subjects from the entities.
    Returns an array of entities of size n_actors + n_directors + n_subjects.
    If there are not enough of either type of entity, the remaining space is filled
    out with entities from the entity list, sampled in order of PageRank.
    """
    person = [r for r in entities if r['person']]
    categories = [r for r in entities if r['categories']]
    decade = [r for r in entities if r['decade']]
    company = [r for r in entities if r['company']]
    movie = [r for r in entities if r['movie']]

    allentities = [person, categories, decade, company, movie]
    shuffle(allentities)

    taken = []

    cur_index = 0
    for i in range(num_entities):
        # Todo sample from one
        take = allentities[cur_index][0]

        while list(filter(lambda x: x['uri'] == take['uri'], taken)):
            take = allentities[cur_index][1]  # Todo sample

        taken.append(take)
        cur_index = (cur_index + 1) % 6

    return taken


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
    if record['decade']:
        titles.append('Decade')
    if record['company']:
        titles.append('Studio')

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


def record_to_entity(record):
    movie = _movie_from_uri(record['uri']) if record['movie'] else None

    return {
        "name": f'{movie["title"]} ({movie["year"]})' if movie else record['name'],
        "uri": record['uri'],
        "imdb": record['imdb'],
        "description": get_description(record),
        "summary": movie["summary"] if movie else None,
        "movies": ['{title} ({year})'.format(**_movie_from_uri(node['uri'])) for node in record['movies']]
    }


def _movie_from_uri(uri):
    try:
        row = iter(movies.loc[movies['uri'] == uri, movies.columns].values)
        if not row:
            return None

        return {
            attr: val
            for attr, val in zip(movies.columns, next(row, []))
        }
    except Exception:
        return None