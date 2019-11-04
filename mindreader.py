import glob
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, wait

from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS

import dataset
from neo import get_relevant_neighbors, get_unseen_entities, get_last_batch
from sampling import sample_relevant_neighbours, record_to_entity, _movie_from_uri

app = Flask(__name__)
app.secret_key = "XD"
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

MIN_QUESTIONS = 10
MINIMUM_SEED_SIZE = 5
SESSION = {}
N_QUESTIONS = 9
N_ENTITIES = N_QUESTIONS // 3

UUID_LENGTH = 36

LIKED = 'liked'
DISLIKED = 'disliked'
UNKNOWN = 'unknown'
TIMESTAMPS = 'timestamps'

SESSION_PATH = 'sessions'

if not os.path.exists(SESSION_PATH):
    os.mkdir(SESSION_PATH)


@app.route('/static/movie/<movie>')
def get_poster(movie):
    return send_from_directory('movie_images', f'{movie}.jpg')


@app.route('/static/actor/<actor>')
def get_profile(actor):
    print(f'{actor} requested')

    return send_from_directory('actor_images', f'{actor}.jpg')


def _get_samples():
    samples = dataset.sample(5, get_cross_session_seen_entities())
    return [_get_movie_from_row(row) for index, row in samples.iterrows()]


def _get_movie_from_row(row):
    res = {
        'name': f'{row["title"]} ({row["year"]})',
        'image': f'https://www.mindreader.tech/static/movie/{row["imdbId"]}',
        'uri': f'{row["uri"]}',
        'resource': "movie",
        'description': "Movie",
        'movies': []
    }
    return res


@app.route('/api/movies')
def movies():
    if is_invalid_request():
        return abort(400)

    # Initializes an empty but timestamped session
    update_session([], [], [])

    return jsonify(_get_samples())


def _get_movie_uris():
    return set(dataset.movies.uri)


def _has_both_sentiments():
    movie_uris = _get_movie_uris()

    return set(get_liked_entities()).difference(movie_uris) and set(get_disliked_entities()).difference(movie_uris)


def is_done():
    return len(get_rated_entities()) >= MIN_QUESTIONS


@app.route('/api/final', methods=['POST'])
def update_session_wrapper():
    json_data = request.json
    update_session(set(json_data[LIKED]), set(json_data[DISLIKED]), set(json_data[UNKNOWN]))
    return jsonify('Mah Man! You know inspect mode!')


@app.route('/api/feedback', methods=['POST'])
def feedback():
    if is_invalid_request():
        return abort(400)

    json_data = request.json
    update_session(set(json_data[LIKED]), set(json_data[DISLIKED]), set(json_data[UNKNOWN]))

    seen_entities = get_cross_session_seen_entities()

    rated_entities = get_rated_entities()

    if is_done():
        liked = get_liked_entities()
        disliked = get_disliked_entities()
        parallel = list()

        parallel.append([get_last_batch, liked, seen_entities])
        parallel.append([get_last_batch, disliked, seen_entities])

        liked_res, disliked_res = get_next_entities(parallel)

        for uri in set(liked_res).intersection(set(disliked_res)):
            liked_res = list(filter(lambda u: u != uri, liked_res))
            disliked_res = list(filter(lambda u: u != uri, disliked_res))
            print(f'Should not be in list {uri}, in list = {uri in liked_res}, {uri in disliked_res}')

        print(f'l: {liked_res}')
        print(f'd: {disliked_res}')


        return jsonify({
            'prediction': True,
            'likes': [_get_movie_from_row(_movie_from_uri(uri)) for uri in liked_res][:5],
            'dislikes': [_get_movie_from_row(_movie_from_uri(uri)) for uri in disliked_res][:5]
        })

    parallel = []
    num_rand = N_ENTITIES
    if json_data[LIKED]:
        parallel.append([get_related_entities, list(json_data[LIKED]), seen_entities])
    else:
        num_rand += N_ENTITIES

    if json_data[DISLIKED]:
        parallel.append([get_related_entities, list(json_data[DISLIKED]), seen_entities])
    else:
        num_rand += N_ENTITIES

    random_entities = _get_samples()[:num_rand]

    if len(rated_entities) < MINIMUM_SEED_SIZE:
        print('Less than minimum seed size')

        # Find the relevant neighbors (with page rank) from the liked and disliked seeds
        results = get_next_entities(parallel)
        requested_entities = [entity for result in results for entity in result]
        result_entities = random_entities + [record_to_entity(x) for x in requested_entities]
    else:
        print('Minimum seed size met')

        parallel.append([get_unseen_entities, [item['uri'] for item in random_entities], seen_entities, num_rand])
        results = get_next_entities(parallel)
        requested_entities = [entity for result in results for entity in result]
        result_entities = [record_to_entity(x) for x in requested_entities]

    for result in results:
        print(len(result))

    no_duplicates = []
    [no_duplicates.append(entity) for entity in result_entities if entity not in no_duplicates]

    no_duplicates = sorted(no_duplicates, key=lambda x: x['description'])

    return jsonify(no_duplicates)


@app.route('/api')
def main():
    return 'test'


def get_next_entities(parallel):
    f = []
    with ThreadPoolExecutor(max_workers=5) as e:
        for args in parallel:
            f.append(e.submit(*args))

    wait(f)

    return [element.result() for element in f]


def get_related_entities(entities, seen_entities):
    liked_relevant = get_relevant_neighbors(entities, seen_entities)
    liked_relevant_list = sample_relevant_neighbours(liked_relevant, n_actors=N_ENTITIES // 3,
                                                     n_directors=N_ENTITIES // 3, n_subjects=N_ENTITIES // 3)
    return liked_relevant_list


def get_session_path(header):
    return os.path.join(SESSION_PATH, f'{header}.json')


def update_session(liked, disliked, unknown):
    header = get_authorization()
    user_session_path = get_session_path(header)

    if header not in SESSION:
        if os.path.exists(user_session_path):
            with open(user_session_path, 'r') as fp:
                SESSION[header] = json.load(fp)
        else:
            SESSION[header] = {
                LIKED: [],
                DISLIKED: [],
                UNKNOWN: [],
                TIMESTAMPS: []
            }

        if len(header) > UUID_LENGTH:
            set_all_sessions(header)

    SESSION[header][TIMESTAMPS] += [time.time()]
    SESSION[header][LIKED] += list(liked)
    SESSION[header][DISLIKED] += list(disliked)
    SESSION[header][UNKNOWN] += list(unknown)

    print(f'Updating with:')
    print(f'    Likes:    {liked}')
    print(f'    Dislikes: {disliked}')
    print()
    print(f'Full history for this user: ')
    print(f'    Likes:    {SESSION[header][LIKED]}')
    print(f'    Dislikes: {SESSION[header][DISLIKED]}')

    with open(user_session_path, 'w+') as fp:
        json.dump(SESSION[header], fp, indent=True)


def get_seen_entities():
    header = get_authorization()

    if header not in SESSION:
        return []

    return get_rated_entities() + SESSION[header][UNKNOWN]


def get_rated_entities():
    header = get_authorization()

    if header not in SESSION:
        return []

    return get_liked_entities() + get_disliked_entities()


def get_liked_entities():
    header = get_authorization()

    if header not in SESSION:
        return []

    return SESSION[header][LIKED]


def get_disliked_entities():
    header = get_authorization()

    if header not in SESSION:
        return []

    return SESSION[header][DISLIKED]


def is_invalid_request():
    return get_authorization() is None


def get_authorization():
    return request.headers.get("Authorization")


def get_cross_session_seen_entities():
    header = get_authorization()

    if header not in SESSION:
        return []

    return get_cross_session_entities_generic(header, LIKED) + \
           get_cross_session_entities_generic(header, DISLIKED) + \
           get_cross_session_entities_generic(header, UNKNOWN)


def get_cross_session_entities_generic(header, type):
    results = []
    head = header.split('+')[0]
    for key, value in SESSION.items():
        if key.startswith(head):
            results.extend(value[type])
            
    return results


def set_all_sessions(header):
    head = header.split('+')[0]  # Get initial head

    # Match all headers containing initial head
    for filename in glob.glob(os.path.join(SESSION_PATH, f'{head}+*.json')):
        with open(filename, 'r') as f:
            h = os.path.basename(os.path.splitext(filename)[0])
            if h == header:
                continue

            SESSION[h] = json.load(f)


if __name__ == "__main__":
    app.run()
else:
    application = app  # For GUnicorn
