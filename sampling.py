from random import shuffle
from numpy import random, asarray, log2, arange

from dataset import movies
from queries import get_counts

ENTITY_COUNTS = get_counts()


def _choice(lst, weights, remove=True):
    if not lst:
        return None

    w_sum = weights.sum(axis=0)
    indices = arange(len(lst))

    if w_sum:
        idx = random.choice(indices, p=weights / w_sum, replace=True)
    else:
        idx = random.choice(indices)

    element = lst[idx]
    if remove:
        lst.pop(idx)

    return element


def _weights(records):
    return asarray([entity['score'] for entity in records])


def _record_choice(records, n=1):
    if n == 1:
        return _choice(records, _weights(records))
    else:
        result = []

        while records and len(result) < n:
            result.append(_choice(records, _weights(records)))

        return result


def _subselection(entities, entity_type):
    return [entity for entity in entities if entity[entity_type]]


def multiplier(entity_type):
    return {
        'decade': 0.25,
        'company': 0.5
    }.get(entity_type, 1.0)


def sample_relevant_neighbours(entities, num_entities):
    """
    Attempts to sample n_actors, n_directors and n_subjects from the entities.
    Returns an array of entities of size n_actors + n_directors + n_subjects.
    If there are not enough of either type of entity, the remaining space is filled
    out with entities from the entity list, sampled in order of PageRank.
    """
    all_entities = [(log2(value) * multiplier(key.lower()), _subselection(entities, key.lower()), key) for key, value in ENTITY_COUNTS.items()]

    result = list()
    print(len(all_entities))
    while len(result) < num_entities and any(subselection for _, subselection, _ in all_entities):
        count, subset, name = _choice(all_entities, asarray([count for count, _, _ in all_entities]), remove=False)
        if not subset:
            continue

        result.append(_record_choice(subset))

    return result


def list_concatenation(item_list):
    if not item_list:
        return 'N/A'

    if len(item_list) == 1:
        return item_list[0].capitalize()

    out = ', '.join([item.lower() for item in item_list[:-1]])

    return '{} and {}'.format(out, item_list[-1].lower()).capitalize()


def get_description(record):
    titles = []

    if record['actor']:
        titles.append('actor')
    if record['director']:
        titles.append('director')
    if record['subject']:
        titles.append('movie subject')
    if record['movie']:
        titles.append('movie')
    if record['genre']:
        titles.append('genre')
    if record['decade']:
        titles.append('decade')
    if record['company']:
        titles.append('production studio')

    return list_concatenation(titles)


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