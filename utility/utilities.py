import itertools
import json
import os
from os.path import join

from pandas import DataFrame
from tqdm import tqdm

from dataset import movie_uris_set

SESSIONS_PATH = 'sessions'
RATINGS_MAP = {'liked': 1, 'disliked': -1, 'unknown': 0}


def get_ratings_dataframe(final_only=False, versions=None):
    return DataFrame(get_user_entity_pairs(final_only, versions))


def get_user_entity_pairs(final_only=False, versions=None):
    user_uri_ratings = get_ratings(filter_final=final_only, filter_empty=True, versions=versions)

    user_entity_pairs = {'userId': [], 'uri': [], 'isItem': [], 'sentiment': []}

    for user, rating_uris in user_uri_ratings.items():
        for rating, uris in rating_uris.items():
            for uri in uris:
                user_entity_pairs['userId'].append(user)
                user_entity_pairs['sentiment'].append(RATINGS_MAP[rating])
                user_entity_pairs['uri'].append(uri)
                user_entity_pairs['isItem'].append(uri in movie_uris_set)

    return user_entity_pairs


def get_ratings(filter_final=False, filter_empty=False, versions=None):
    uuid_sessions = {}
    categories = ['liked', 'disliked', 'unknown']

    # Combine user sessions
    for session_id in tqdm(os.listdir(SESSIONS_PATH)):
        uuid = session_id.split('+')[0]

        if uuid not in uuid_sessions:
            uuid_sessions[uuid] = {'liked': set(), 'disliked': set(), 'unknown': set()}

        with open(join(SESSIONS_PATH, session_id)) as fp:
            session = json.load(fp)

            if versions:
                if 'version' not in session or session['version'] not in versions:
                    continue

            if filter_final and ('final' not in session or not session['final']):
                continue

            if filter_empty and is_empty(session):
                continue

            [uuid_sessions[uuid][key].update(set(item)) for key, item in session.items() if key in categories and item]

    # Generate all 2-length combinations of categories
    category_combinations = list(itertools.combinations(categories, 2))

    # Rating not shared among categories
    print("Removing duplicates")
    for uuid in tqdm(uuid_sessions.keys()):
        for primary, secondary in category_combinations:
            item_intersection = uuid_sessions[uuid][primary].intersection(uuid_sessions[uuid][secondary])

            if item_intersection:
                uuid_sessions[uuid][primary] = uuid_sessions[uuid][primary].symmetric_difference(item_intersection)
                uuid_sessions[uuid][secondary] = uuid_sessions[uuid][secondary].symmetric_difference(item_intersection)

    return uuid_sessions


def get_sessions(filter_empty=True, versions=None):
    sessions = []
    for session_id in os.listdir(SESSIONS_PATH):
        with open(join(SESSIONS_PATH, session_id)) as fp:
            session = json.load(fp)

            if filter_empty:
                if is_empty(session):
                    continue

            if versions:
                if 'version' not in session or session['version'] not in versions:
                    continue

            sessions.append(session)

    return sessions


def get_unique_uuids(filter_final=False, filter_empty=False, versions=None):
    if filter_final or filter_empty or versions:
        tokens = []
        for session_id in os.listdir(SESSIONS_PATH):
            with open(join(SESSIONS_PATH, session_id)) as fp:
                session = json.load(fp)

                if filter_empty:
                    if is_empty(session):
                        continue

                if filter_final:
                    if not session['final']:
                        continue

                if versions:
                    if 'version' not in session or session['version'] not in versions:
                        continue

                tokens.append(session_id)

        return set([token.replace('.json', '').split('+')[0] for token in tokens])

    return set([token.replace('.json', '').split('+')[0] for token in os.listdir(SESSIONS_PATH)])


def is_empty(session):
    return not (session['liked'] or session['disliked'] or session['unknown'])
