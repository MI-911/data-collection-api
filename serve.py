from concurrent.futures import ThreadPoolExecutor, wait
from random import sample, shuffle

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from multiprocessing import Pool
from itertools import product

from random import choice, sample, shuffle
import json, os, dataset

import dataset
from neo import get_relevant_neighbors, get_unseen_entities
from sampling import sample_relevant_neighbours, record_to_entity

app = Flask(__name__)
app.secret_key = "XD"
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

MAX_QUESTIONS = 50
MINIMUM_SEED_SIZE = 1
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


@app.route('/api/entities', methods=['POST'])
def feedback():
    json_data = request.json
    update_session(set(json_data[LIKED]), set(json_data[DISLIKED]), set(json_data[UNKNOWN]))

    seen_entities = get_seen_entities()

    # Only ask at max N_QUESTIONS
    if len(get_rated_entities()) < MINIMUM_SEED_SIZE:
        return jsonify(_get_samples())
    elif len(seen_entities) >= MAX_QUESTIONS:
        return "Done."  # TODO: PageRank over all likes and dislikes

    # Find the relevant neighbors (with page rank) from the liked and disliked seeds
    liked_relevant, disliked_relevant, random_entities = get_next_entities(json, seen_entities)

    random_entities = [e for e in random_entities if e not in liked_relevant and e not in disliked_relevant][:N_ENTITIES]

    # Return them all to obtain user feedback
    requested_entities = liked_relevant + disliked_relevant + random_entities
    shuffle(requested_entities)

    print(len(requested_entities))
    
    return jsonify([record_to_entity(x) for x in requested_entities])


@app.route('/api')
def main():
    return 'test'


def get_next_entities(json_data, seen):
    f = []
    with ThreadPoolExecutor(max_workers=3) as e:
        f.append(e.submit(get_related_entities, list(json_data['liked']), seen))
        f.append(e.submit(get_related_entities, list(json_data['disliked']), seen))
        f.append(e.submit(get_unseen_entities, seen, N_QUESTIONS))

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
        json.dump(SESSION, fp, indent=True)


def get_seen_entities():
    header = get_authorization()

    if header not in SESSION:
        return []

    return get_rated_entities() + SESSION[header][UNKNOWN]


def get_rated_entities():
    header = get_authorization()

    if header not in SESSION: 
        return []
        
    return SESSION[header][LIKED] + SESSION[header][DISLIKED]


def get_authorization():
    return request.headers.get("Authorization")


if __name__ == "__main__":
    app.run()
