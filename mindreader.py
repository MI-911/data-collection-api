import glob
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, wait
from functools import reduce
from random import shuffle

from flask import Flask, jsonify, request, abort, make_response
from flask_cors import CORS
from pandas import DataFrame

import dataset
from queries import get_relevant_neighbors, get_last_batch, get_triples, get_entities
from sampling import sample_relevant_neighbours, record_to_entity, _movie_from_uri
from statistics import compute_statistics
from utility.encoder import NpEncoder
from utility.utilities import get_ratings_dataframe

app = Flask(__name__)
app.json_encoder = NpEncoder
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

# How many questions to ask the user before showing predictions
MIN_QUESTIONS = 30

# How many movies they must answer before random entities are shown
MINIMUM_SEED_SIZE = 10

# How many questions are shown per page before predictions
N_QUESTIONS = 9

# How many entities are shown per group (like, dislike, random)
N_ENTITIES = N_QUESTIONS // 3

# How many entities are shown per recommendation group (like, dislike)
LAST_N_QUESTIONS = 6

# How many entities predicted per group (like, dislike)
LAST_N_REC_QUESTIONS = 3

# All sessions are saved with their current session
CURRENT_VERSION = 'thesis-ppr'

# Maintains all relevant sessions
SESSION = {}

# Maintains a set of heads (user tokens) that have been loaded from files
LOADED_HEADS = set()

# Various constants
LIKED = 'liked'
DISLIKED = 'disliked'
UNKNOWN = 'unknown'
TIMESTAMPS = 'timestamps'
FINAL = 'final'
VERSION = 'version'
POPULARITY = 'popularity_sampled'
SESSION_PATH = 'sessions'

if not os.path.exists(SESSION_PATH):
    os.mkdir(SESSION_PATH)


def _get_samples(amount):
    liked, disliked, unknown, seen_entities = get_cross_session_entities()
    samples = dataset.sample(amount, seen_entities)
    update_session([], [], [], list(samples.uri))

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


@app.route('/api/sessions')
def sessions():
    return jsonify(len(glob.glob(os.path.join(SESSION_PATH, '*.json'))))


@app.route('/api/statistics')
def statistics():
    versions = request.args.get('versions')
    if versions:
        versions = versions.split(',')

    return jsonify(compute_statistics(versions))


def _get_recommendations(liked, disliked, seen_entities):
    parallel = list()

    parallel.append([get_last_batch, liked, seen_entities])
    parallel.append([get_last_batch, disliked, seen_entities])

    liked_res, disliked_res = get_next_entities(parallel)

    for uri in set([item['uri'] for item in liked_res]).intersection(set([item['uri'] for item in disliked_res])):
        liked_res = list(filter(lambda u: u['uri'] != uri, liked_res))
        disliked_res = list(filter(lambda u: u['uri'] != uri, disliked_res))

    # Map to URIs
    liked_res = [item['uri'] for item in liked_res[:LAST_N_REC_QUESTIONS]]
    disliked_res = [item['uri'] for item in disliked_res[:LAST_N_REC_QUESTIONS]]

    # Get random samples and filter out URIs already in liked or disliked
    samples = _get_samples(LAST_N_QUESTIONS * 2)
    for uri in liked_res + disliked_res:
        samples = list(filter(lambda m: m['uri'] != uri, samples))

    # Get rows from movies
    liked_res = [_get_movie_from_row(_movie_from_uri(uri)) for uri in liked_res]
    disliked_res = [_get_movie_from_row(_movie_from_uri(uri)) for uri in disliked_res]

    # Add random samples to liked and disliked (from different directions.
    liked_res = liked_res + samples[:LAST_N_QUESTIONS - len(liked_res)]
    disliked_res = disliked_res + samples[-(LAST_N_QUESTIONS - len(disliked_res)):]

    # Shuffle recommendations with random samples
    shuffle(liked_res)
    shuffle(disliked_res)

    return {
        'prediction': True,
        'likes': liked_res,
        'dislikes': disliked_res
    }


@app.route('/api/recommendations')
def recommendations():
    # Ensure that the user's sessions are loaded into memory
    get_sessions(get_authorization())

    # Get the user's preferences across all her previous sessions
    liked, disliked, unknown, seen_entities = get_cross_session_entities()

    return jsonify(_get_recommendations(liked, disliked, seen_entities))


@app.route('/api/movies')
def movies():
    if is_invalid_request():
        return abort(400)

    return jsonify(_get_samples(10))


def _get_movie_uris():
    return set(dataset.movies.uri)


def _has_both_sentiments():
    movie_uris = _get_movie_uris()

    return set(get_liked_entities()).difference(movie_uris) and set(get_disliked_entities()).difference(movie_uris)


def is_done():
    return len(get_current_session_entities()) >= MIN_QUESTIONS


def _make_csv(csv, file_name):
    output = make_response(csv)

    output.headers['Content-Disposition'] = f'attachment; filename={file_name}'
    output.headers['Content-Type'] = 'text/csv'

    return output


@app.route('/api/ratings', methods=['GET'])
def get_ratings():
    final_only = request.args.get('final')
    final_only = final_only and final_only == 'yes'

    versions = request.args.get('versions')
    if versions:
        versions = versions.split(',')

    df = get_ratings_dataframe(final_only, versions)

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
    update_session(set(json_data[LIKED]), set(json_data[DISLIKED]), set(json_data[UNKNOWN]), [], final=True)

    return jsonify({'success': True})


@app.route('/api/feedback', methods=['POST'])
def feedback():
    if is_invalid_request():
        return abort(400)

    json_data = request.json
    update_session(set(json_data[LIKED]), set(json_data[DISLIKED]), set(json_data[UNKNOWN]), [])

    liked, disliked, unknown, seen_entities = get_cross_session_entities()

    rated_entities = get_current_session_entities()

    if is_done():
        return jsonify(_get_recommendations(liked, disliked, seen_entities))

    parallel = []
    num_rand = N_ENTITIES

    extra = 0
    if bool(json_data[LIKED]) != bool(json_data[DISLIKED]):
        extra = N_ENTITIES // 2

    if json_data[LIKED]:
        parallel.append([get_related_entities, list(json_data[LIKED]), seen_entities,
                         (N_ENTITIES + extra) if extra else None])
    else:
        num_rand += (N_ENTITIES - (N_ENTITIES // 2)) if extra else N_ENTITIES

    if json_data[DISLIKED]:
        parallel.append([get_related_entities, list(json_data[DISLIKED]), seen_entities,
                         (N_ENTITIES + extra) if extra else None])
    else:
        num_rand += (N_ENTITIES - (N_ENTITIES // 2)) if extra else N_ENTITIES

    random_entities = _get_samples(num_rand)

    if len(rated_entities) < MINIMUM_SEED_SIZE:
        # Find the relevant neighbors (with page rank) from the liked and disliked seeds
        results = get_next_entities(parallel)
        requested_entities = [entity for result in results for entity in result]
        result_entities = random_entities + [record_to_entity(entity) for entity in requested_entities]
    else:
        parallel.append([get_related_entities, [item['uri'] for item in random_entities], seen_entities, num_rand])
        results = get_next_entities(parallel)
        requested_entities = [entity for result in results for entity in result]
        result_entities = [record_to_entity(entity) for entity in requested_entities]

    no_duplicates = sorted({r['uri']: r for r in result_entities}.values(), key=lambda x: x['description'])

    return jsonify(no_duplicates)


def get_next_entities(parallel):
    f = []
    with ThreadPoolExecutor(max_workers=5) as e:
        for args in parallel:
            f.append(e.submit(*args))

    wait(f)

    return [element.result() for element in f]


def get_related_entities(entities, seen_entities, limit=None):
    return sample_relevant_neighbours(get_relevant_neighbors(entities, seen_entities), limit if limit else N_ENTITIES)


def get_session_path(header):
    return os.path.join(SESSION_PATH, f'{header}.json')


def update_session(liked, disliked, unknown, popularity_sampled, final=False):
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
                POPULARITY: [],
                FINAL: False,
                VERSION: CURRENT_VERSION
            }

        # Ensure that all the user's sessions are loaded into memory
        get_sessions(header)

    SESSION[header][TIMESTAMPS] += [time.time()]
    SESSION[header][LIKED] += list(liked)
    SESSION[header][DISLIKED] += list(disliked)
    SESSION[header][POPULARITY] += list(popularity_sampled)
    SESSION[header][UNKNOWN] += list(unknown)
    SESSION[header][FINAL] = final

    with open(user_session_path, 'w+') as fp:
        json.dump(SESSION[header], fp, indent=True)


def get_seen_entities():
    header = get_authorization()

    if header not in SESSION:
        return []

    return get_current_session_entities() + SESSION[header][UNKNOWN]


def get_current_session_entities():
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


def get_cross_session_entities():
    header = get_authorization()
    
    entities = [get_cross_session_entities_generic(header, name) for name in [LIKED, DISLIKED, UNKNOWN]]
    entities.append(reduce(lambda a, b: a + b, entities))

    return entities


def get_cross_session_entities_generic(header, type):
    results = []
    head = header.split('+')[0]

    for key, value in SESSION.items():
        if key.startswith(head):
            results.extend(value[type])

    return results


def get_sessions(header):
    head = header.split('+')[0]  # Get initial head
    if head in LOADED_HEADS:
        return
    
    # Match all headers containing initial head
    for filename in glob.glob(os.path.join(SESSION_PATH, f'{head}+*.json')):
        with open(filename, 'r') as f:
            file_head = os.path.basename(os.path.splitext(filename)[0])
            if file_head == header:
                continue

            SESSION[file_head] = json.load(f)

    LOADED_HEADS.add(head)


if __name__ == "__main__":
    app.run()
else:
    application = app  # For GUnicorn
