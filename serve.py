import json
import os
from concurrent.futures import ThreadPoolExecutor, wait
from random import shuffle

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

import dataset
from neo import get_relevant_neighbors, get_unseen_entities, get_last_batch
from sampling import sample_relevant_neighbours, record_to_entity, _movie_from_uri

app = Flask(__name__)
app.secret_key = "XD"
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

MAX_QUESTIONS = 5
MINIMUM_SEED_SIZE = 10
SESSION = {} 
N_QUESTIONS = 9
N_ENTITIES = N_QUESTIONS // 3

LIKED = 'liked'
DISLIKED = 'disliked'
UNKNOWN = 'unknown'

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
    samples = dataset.sample(50, get_seen_entities())

    return [{
        "name": f"{item['title']} ({item['year']})",
        "id": item['movieId'],
        "resource": "movie",
        "uri": item['uri'],
        "description": "Movie"
    } for index, item in samples[:5].iterrows()]


@app.route('/api/begin')
def begin():
    return jsonify(_get_samples())


def _get_movie_uris():
    return set(dataset.movies.uri)


def _has_both_sentiments():
    movie_uris = _get_movie_uris()

    return set(get_liked_entities()).difference(movie_uris) and set(get_disliked_entities()).difference(movie_uris)


@app.route('/api/entities', methods=['POST'])
def feedback():
    json_data = request.json
    update_session(set(json_data[LIKED]), set(json_data[DISLIKED]), set(json_data[UNKNOWN]))

    seen_entities = get_seen_entities()

    rated_entities = get_rated_entities()

    # Only ask at max N_QUESTIONS
    if len(seen_entities) >= MAX_QUESTIONS and _has_both_sentiments():
        liked = get_liked_entities()
        disliked = get_disliked_entities()
        parallel = list()

        parallel.append([get_last_batch, liked, seen_entities])
        parallel.append([get_last_batch, disliked, seen_entities])

        liked_res, disliked_res = get_next_entities(parallel)

        print(f'l: {liked_res}')
        print(f'd: {disliked_res}')

        return "Done."  # TODO: PageRank over all likes and dislikes

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

    if len(rated_entities) < MINIMUM_SEED_SIZE:

        # Find the relevant neighbors (with page rank) from the liked and disliked seeds
        results = get_next_entities(parallel)
        random_entities = _get_samples()[:num_rand]
        requested_entities = [entity for result in results for entity in result]
        result_entities = random_entities + [record_to_entity(x) for x in requested_entities]
    else:
        parallel.append([get_unseen_entities, seen_entities, num_rand])
        results = get_next_entities(parallel)
        requested_entities = [entity for result in results for entity in result]
        result_entities = [record_to_entity(x) for x in requested_entities]

    no_duplicates = []
    [no_duplicates.append(entity) for entity in result_entities if entity not in no_duplicates]

    shuffle(no_duplicates)

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
    liked_relevant_list = sample_relevant_neighbours(liked_relevant, n_actors=N_ENTITIES // 3, n_directors=N_ENTITIES // 3, n_subjects=N_ENTITIES // 3)
    return liked_relevant_list


def update_session(liked, disliked, unknown):
    header = get_authorization()
    user_session_path = os.path.join(SESSION_PATH, f'{header}.json')

    if header not in SESSION:
        if os.path.exists(user_session_path):
            with open(user_session_path, 'r') as fp:
                SESSION[header] = json.load(fp)
        else:
            SESSION[header] = {
                LIKED: [],
                DISLIKED: [],
                UNKNOWN: []
            }

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


def get_authorization():
    return request.headers.get("Authorization")


if __name__ == "__main__":
    app.run()
