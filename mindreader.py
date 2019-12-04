import glob
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, wait
from numpy.random import shuffle

from flask import Flask, jsonify, request, abort, make_response
from flask_cors import CORS
from pandas import DataFrame

import dataset
from util.encoder import NpEncoder
from neo import get_relevant_neighbors, get_unseen_entities, get_last_batch, get_triples, get_entities
from sampling import sample_relevant_neighbours, record_to_entity, _movie_from_uri
from statistics import compute_statistics
from util.utilities import get_ratings_dataframe

app = Flask(__name__)
app.secret_key = "XD"
app.json_encoder = NpEncoder
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

MIN_QUESTIONS = 25
MINIMUM_SEED_SIZE = 5
SESSION = {}
N_QUESTIONS = 9
N_ENTITIES = N_QUESTIONS // 3
CURRENT_VERSION = '2019-11-22'

LAST_N_QUESTIONS = 6
LAST_N_RATED_QUESTIONS = 3

UUID_LENGTH = 36

LIKED = 'liked'
DISLIKED = 'disliked'
UNKNOWN = 'unknown'
TIMESTAMPS = 'timestamps'
FINAL = 'final'
VERSION = 'version'

SESSION_PATH = 'sessions'

if not os.path.exists(SESSION_PATH):
    os.mkdir(SESSION_PATH)


def _get_samples(amount):
    samples = dataset.sample(amount, get_cross_session_seen_entities())
    return [_get_movie_from_row(row) for index, row in samples.iterrows()]


def _get_movie_from_row(row):
    res = {
        'name': f'{row["title"]} ({row["year"]})',
        'imdb': row["imdbId"],
        'uri': row["uri"],
        'description': "Movie",
        'summary':  row["summary"] if row["summary"] else ""
    }

    return res


@app.route('/api/statistics')
def statistics():
    versions = request.args.get('versions')
    if versions:
        versions = versions.split(',')

    return jsonify(compute_statistics(versions))


@app.route('/api/movies')
def movies():
    if is_invalid_request():
        return abort(400)

    # Initializes an empty but timestamped session
    update_session([], [], [])

    return jsonify(_get_samples(10))


def _get_movie_uris():
    return set(dataset.movies.uri)


def _has_both_sentiments():
    movie_uris = _get_movie_uris()

    return set(get_liked_entities()).difference(movie_uris) and set(get_disliked_entities()).difference(movie_uris)


def is_done():
    return len(get_rated_entities()) >= MIN_QUESTIONS


def _make_csv(csv, file_name):
    output = make_response(csv)

    output.headers['Content-Disposition'] = f'attachment; filename={file_name}'
    output.headers['Content-Type'] = 'text/csv'

    return output


@app.route('/api/ratings', methods=['GET'])
def get_ratings():
    df = get_ratings_dataframe()

    return _make_csv(df.to_csv(), 'ratings.csv')


@app.route('/api/triples', methods=['GET'])
def get_all_triples():
    data = get_triples()
    df = DataFrame.from_records(data)
    if data:
        df.columns = data[0].keys()

    return _make_csv(df.to_csv(), 'triples.csv')


@app.route('/api/entities', methods=['GET'])
def get_all_entities():
    data = get_entities()
    df = DataFrame.from_records(data)
    if data:
        df.columns = data[0].keys()
    
    df.set_index('uri', inplace=True)
    df['labels'] = df['labels'].str.join('|')

    return _make_csv(df.to_csv(), 'entities.csv')


@app.route('/api/final', methods=['POST'])
def final_feedback():
    json_data = request.json
    update_session(set(json_data[LIKED]), set(json_data[DISLIKED]), set(json_data[UNKNOWN]), final=True)
    return jsonify('Mah man! You know inspect mode!')


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

        samples = _get_samples(LAST_N_QUESTIONS * 2)

        # Get only N RATED
        liked_res = [_get_movie_from_row(_movie_from_uri(uri)) for uri in liked_res][:LAST_N_RATED_QUESTIONS]
        disliked_res = [_get_movie_from_row(_movie_from_uri(uri)) for uri in disliked_res][:LAST_N_RATED_QUESTIONS]

        for movie in liked_res + disliked_res:
            samples = list(filter(lambda m: m['uri'] != movie['uri'], samples))

        liked_res = liked_res + samples[:LAST_N_QUESTIONS - len(liked_res)]
        disliked_res = disliked_res + samples[-(LAST_N_QUESTIONS - len(disliked_res)):]

        shuffle(liked_res)
        shuffle(disliked_res)

        return jsonify({
            'prediction': True,
            'likes': liked_res,
            'dislikes': disliked_res
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

    random_entities = _get_samples(num_rand)

    if len(rated_entities) < MINIMUM_SEED_SIZE:
        print('Less than minimum seed size')

        # Find the relevant neighbors (with page rank) from the liked and disliked seeds
        results = get_next_entities(parallel)
        requested_entities = [entity for result in results for entity in result]
        result_entities = random_entities + [record_to_entity(x) for x in requested_entities]
    else:
        print('Minimum seed size met')

        parallel.append([get_related_entities, [item['uri'] for item in random_entities], seen_entities, num_rand])
        results = get_next_entities(parallel)
        requested_entities = [entity for result in results for entity in result]
        result_entities = [record_to_entity(x) for x in requested_entities]

    for result in results:
        print(len(result))

    no_duplicates = sorted({r['uri']: r for r in result_entities}.values(), key=lambda x: x['description'])

    return jsonify(no_duplicates)


def get_next_entities(parallel):
    f = []
    with ThreadPoolExecutor(max_workers=5) as e:
        for args in parallel:
            f.append(e.submit(*args))

    wait(f)

    return [element.result() for element in f]


def get_related_entities(entities, seen_entities, lim=None):
    liked_relevant = get_relevant_neighbors(entities, seen_entities)
    liked_relevant_list = sample_relevant_neighbours(liked_relevant, lim if lim else N_ENTITIES)
    return liked_relevant_list


def get_session_path(header):
    return os.path.join(SESSION_PATH, f'{header}.json')


def update_session(liked, disliked, unknown, final=False):
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
                TIMESTAMPS: [],
                FINAL: False,
                VERSION: CURRENT_VERSION
            }

        if len(header) > UUID_LENGTH:
            set_all_sessions(header)

    SESSION[header][TIMESTAMPS] += [time.time()]
    SESSION[header][LIKED] += list(liked)
    SESSION[header][DISLIKED] += list(disliked)
    SESSION[header][UNKNOWN] += list(unknown)
    SESSION[header][FINAL] = final

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
    authorization = get_authorization()

    return not authorization or '+' not in authorization


def get_authorization():
    return request.headers.get("Authorization")


def get_cross_session_seen_entities():
    header = get_authorization()

    if header not in SESSION:
        return []

    return get_cross_session_entities_generic(header, LIKED) + get_cross_session_entities_generic(header, DISLIKED) + \
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
