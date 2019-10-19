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
        titles.append('Subject')

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
    if _person(record):
        return get_actor_id(record['name'])

    return None


def record_to_entity(record):
    return {
        "name": record['name'] if record['name'] else record['label'],
        "id": get_id(record),
        "resource": get_resource(record),
        "uri": record['uri'],
        "description": get_description(record)
    }
