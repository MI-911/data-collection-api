import json
import os
from collections import Counter
from os.path import join

from pandas import DataFrame
from tqdm import tqdm

from dataset import movie_uris_set, DATA_PATH

SESSIONS_PATH = 'sessions'
RATINGS_MAP = {'liked': 1, 'disliked': -1, 'unknown': 0}


def get_ratings_dataframe():
    user_entity = get_user_entity_pairs()

    print(len(user_entity))

    df = DataFrame(user_entity)

    return df


def get_user_entity_pairs():
    user_uri_ratings = get_ratings(filter_empty=True)

    user_entity_pairs = {'userId': [], 'uri': [], 'isItem': [], 'sentiment': []}

    for user, rating_uris in user_uri_ratings.items():
        for rating, uris in rating_uris.items():
            for uri in uris:
                user_entity_pairs['userId'].append(user)
                user_entity_pairs['sentiment'].append(RATINGS_MAP[rating])
                user_entity_pairs['uri'].append(uri)
                user_entity_pairs['isItem'].append(uri in movie_uris_set)

    return user_entity_pairs


def get_ratings(filter_final=False, filter_empty=False):
    uuid_sessions = {}
    categories = ['liked', 'disliked', 'unknown']
    print("Combining user sessions")
    for session_id in tqdm(os.listdir(SESSIONS_PATH)):
        uuid = session_id.split('+')[0]

        if uuid not in uuid_sessions:
            uuid_sessions[uuid] = {'liked': set(), 'disliked': set(), 'unknown': set()}

        with open(join(SESSIONS_PATH, session_id)) as fp:
            sess = json.load(fp)

            if filter_final and not sess['final']:
                continue

            if filter_empty and is_empty(sess):
                continue

            [uuid_sessions[uuid][key].update(set(item)) for key, item in sess.items() if key in categories and item]

    # Rating not shared among categories
    print("Removing duplicates")
    for uuid, type_items in tqdm(uuid_sessions.items()):

        # Flatten items
        collected = [item for items in type_items.values() for item in items]

        # Get count
        collected = Counter(collected)

        # Remove from all categories if count is above 1 (aka. shared among categories)
        for item, count in collected.items():
            if count > 1:
                for rating in uuid_sessions[uuid].keys():
                    uuid_sessions[uuid][rating].remove(item)

    return uuid_sessions


def get_sessions(filter_empty=True):
    sessions = []
    for session in os.listdir(SESSIONS_PATH):
        with open(join(SESSIONS_PATH, session)) as fp:
            sess = json.load(fp)
            if filter_empty:
                if is_empty(sess):
                    continue

            sessions.append(sess)

    return sessions


def get_unique_uuids(filter_final=False, filter_empty=False):
    if filter_final or filter_empty:
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

                tokens.append(session_id)

        return set([token.replace('.json', '').split('+')[0] for token in tokens])

    return set([token.replace('.json', '').split('+')[0] for token in os.listdir(SESSIONS_PATH)])


def is_empty(session):
    return not (session['liked'] or session['disliked'] or session['unknown'])


if __name__ == "__main__":
    get_ratings_dataframe()