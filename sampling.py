from random import shuffle

from numpy import random, asarray

from dataset import get_actor_id, movies


def _choice(lst, weights):
    if not lst:
        return None

    w_sum = weights.sum(axis=0)
    indices = [idx for idx in range(len(lst))]

    if w_sum:
        idx = random.choice(indices, p=weights / w_sum, replace=True)
    else:
        idx = random.choice(indices)

    element = lst[idx]
    lst.pop(idx)

    return element


def _weights(records):
    return asarray([entity['weight'] if entity['weight'] else entity['score'] for entity in records])


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


def sample_relevant_neighbours(entities, num_entities):
    """
    Attempts to sample n_actors, n_directors and n_subjects from the entities.
    Returns an array of entities of size n_actors + n_directors + n_subjects.
    If there are not enough of either type of entity, the remaining space is filled
    out with entities from the entity list, sampled in order of PageRank.
    """
    all_entities = [_subselection(entities, 'person'), _subselection(entities, 'category'),
                    _subselection(entities, 'decade'), _subselection(entities, 'company')[:1],
                    _subselection(entities, 'movie')]
    shuffle(all_entities)

    result = []

    seen = True
    while len(result) < num_entities and seen:
        seen = False

        for subset in all_entities:
            if len(result) >= num_entities:
                break

            if not subset:
                continue

            seen = True
            result.append(_record_choice(subset))

    return result


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