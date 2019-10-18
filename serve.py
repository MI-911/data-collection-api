
from random import sample, shuffle

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

import dataset
from neo import get_relevant_neighbors, get_unseen_entities

app = Flask(__name__)
app.secret_key = "XD"
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

MAX_QUESTIONS = 50
MINIMUM_SEED_SIZE = 10
SESSION = {} 
N_QUESTIONS = 6
N_ENTITIES = N_QUESTIONS // 3

LIKED = 'liked'
DISLIKED = 'disliked'
UNKNOWN = 'unknown'


@app.route('/static/movie/<movie>')
def get_poster(movie):
    return send_from_directory('movie_images', f'{movie}.jpg')


@app.route('/static/actor>/<actor>')
def get_profile(actor):
    return send_from_directory('actor_images', f'{actor}.jpg')


def _get_samples():
    samples = dataset.sample(50, get_seen_entities())

    return [{
        "name": f"{item['title']} ({item['year']})",
        "id": item['movieId'],
        "resource": "movie",
        "uri": item['uri']
    } for index, item in samples[:5].iterrows()]


@app.route('/api/begin')
def begin():
    return jsonify(_get_samples())


@app.route('/api/entities', methods=['POST'])
def entities():
    json = request.json
    update_session(set(json[LIKED]), set(json[DISLIKED]), set(json[UNKNOWN]))

    seen_entities = get_seen_entities()

    # Only ask at max N_QUESTIONS
    if len(seen_entities) >= MAX_QUESTIONS:
        return "Done."  # TODO: PageRank over all likes and dislikes

    # Find the relevant neighbors (with page rank) from the liked and disliked seeds
    liked_relevant = get_relevant_neighbors(list(json[LIKED]), seen_entities)[:N_ENTITIES]
    liked_relevant_list = [n['uri'] for n in liked_relevant]

    disliked_relevant = get_relevant_neighbors(list(json[DISLIKED]), seen_entities + liked_relevant_list)[:N_ENTITIES]
    disliked_relevant_list = [n['uri'] for n in disliked_relevant]

    random_entities = get_unseen_entities(seen_entities + liked_relevant_list + disliked_relevant_list, N_ENTITIES)

    # Return them all to obtain user feedback
    requested_entities = liked_relevant_list + disliked_relevant_list + random_entities
    shuffle(requested_entities)

    print(len(requested_entities))
    
    return jsonify(requested_entities)


@app.route('/api')
def main():
    return 'test'


def update_session(liked, disliked, unknown):
    header = get_authorization()
    if header not in SESSION: 
        SESSION[header] = {
            LIKED:    [],
            DISLIKED: [],
            UNKNOWN:  []
        }

    SESSION[header][LIKED] += list(liked)
    SESSION[header][DISLIKED] += list(disliked)
    SESSION[header][UNKNOWN] += list(unknown)

    print(f'Updating with:')
    print(f'    Likes:    {liked}')
    print(f'    Dislikes: {disliked}')
    print()
    print(f'Full history for this user: ')
    print(f'    Likes:    {SESSION[header]["liked"]}')
    print(f'    Dislikes: {SESSION[header]["disliked"]}')


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
