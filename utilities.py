import json
import os
from os.path import join

from pandas import DataFrame

from dataset import movie_uris_set, DATA_PATH

SESSIONS_PATH = 'sessions'
RATINGS_MAP = {'liked': 1, 'disliked': -1, 'unknown': 0}


def post_ratings():
    user_entity = get_user_entity_pairs()

    print(len(user_entity))

    df = DataFrame(user_entity)

    df.to_csv(join(DATA_PATH, 'pairs.csv'))

    print(df.shape)


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
    categories = {'liked', 'disliked', 'unknown'}
    for session_id in os.listdir(SESSIONS_PATH):
        uuid = session_id.split('+')[0]

        if uuid not in uuid_sessions:
            uuid_sessions[uuid] = {'liked': [], 'disliked': [], 'unknown': []}

        with open(join(SESSIONS_PATH, session_id)) as fp:
            sess = json.load(fp)

            if filter_final and not sess['final']:
                continue

            if filter_empty and is_empty(sess):
                continue

            [uuid_sessions[uuid][key].extend(item) for key, item in sess.items() if key in categories and item]

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
    post_ratings()