from random import choice, sample

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

import dataset
from neo import get_one_hop_entities

app = Flask(__name__)
app.secret_key = "XD"
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

N_QUESTIONS = 50
MINIMUM_SEED_SIZE = 10
SESSION = {} 

LIKED = 'liked'
DISLIKED = 'disliked'
UNKNOWN = 'unknown'


@app.route('/static/movie/<movie>')
def get_poster(movie):
    return send_from_directory('movie_images', f'{movie}.jpg')


def _get_samples():
    samples = dataset.sample(50, get_seen_movies())
    samples = samples[~samples.uri.isin(get_seen_movies())]

    return [{
        "name": f"{sample['title']} ({sample['year']})",
        "id": sample['movieId'],
        "resource": "movie",
        "uri": sample['uri']
    } for index, sample in samples[:5].iterrows()]


@app.route('/api/begin')
def begin():
    return jsonify(_get_samples())


@app.route('/api/entities', methods=['POST'])
def entities():
    
    json = request.json
    update_session(set(json[LIKED]), set(json[DISLIKED]), set(json[UNKNOWN]))

    # Only ask at max N_QUESTIONS
    if len(get_rated_movies()) < MINIMUM_SEED_SIZE:
        return jsonify(_get_samples())

    # Choose one seed from liked and disliked at random
    # liked_choice = choice(list(liked))
    # disliked_choice = choice(list(disliked))

    # Find the one-hop entities from the liked and disliked seeds
    # liked_one_hop_entities = get_one_hop_entities(liked_choice)
    # disliked_one_hop_entities = get_one_hop_entities(disliked_choice)

    # Sample 2 entities from liked_one_hop_entities and disliked_one_hop_entities, respectively,
    # then sample 2 entities randomly from the KG 
    # TODO: Sample this properly - perhaps based on PageRank
    # liked_one_hop_entities = sample(liked_one_hop_entities, 2)
    # disliked_one_hop_entities = sample(disliked_one_hop_entities, 2)
    # random_entities = dataset.sample(2)

    # Return them all to obtain user feedback
    return jsonify([])


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


def get_seen_movies():
    header = get_authorization()

    if header not in SESSION:
        return []

    return get_rated_movies() + SESSION[header][UNKNOWN]


def get_rated_movies():
    header = get_authorization()

    if header not in SESSION: 
        return []
        
    return SESSION[header][LIKED] + SESSION[header][DISLIKED]


def get_authorization():
    return request.headers.get("Authorization")


if __name__ == "__main__":
    app.run()
