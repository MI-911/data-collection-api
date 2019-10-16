from flask import Flask, jsonify, request, send_from_directory, abort, session
from flask_cors import CORS
from random import choice, sample

import dataset
from neo import get_related_entities, get_one_hop_entities

app = Flask(__name__)
app.secret_key = "XD"
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

N_QUESTIONS = 50
SESSION = {} 


@app.route('/static/movie/<movie>')
def get_poster(movie):
    return send_from_directory('movie_images', f'{movie}.jpg')


def _get_samples():
    samples = dataset.sample(100)  # .sort_values(by='variance', ascending=False)
    samples = samples[~samples.uri.isin(get_seen_movies(request))]

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
    liked = set(json['liked'])
    disliked = set(json['disliked'])
    unknown = set(json['unknown'])
    update_session(request, liked, disliked, unknown)

    # Only ask at max N_QUESTIONS
    if len(get_seen_movies(request)) < N_QUESTIONS:
        return jsonify(_get_samples())

    # Choose one seed from liked and disliked at random
    liked_choice = choice(list(liked))
    disliked_choice = choice(list(disliked))

    # Find the one-hop entities from the liked and disliked seeds
    liked_one_hop_entities = get_one_hop_entities(liked_choice)
    disliked_one_hop_entities = get_one_hop_entities(disliked_choice)

    # Sample 2 entities from liked_one_hop_entities and disliked_one_hop_entities, respectively,
    # then sample 2 entities randomly from the KG 
    # TODO: Sample this properly - perhaps based on PageRank
    liked_one_hop_entities = sample(liked_one_hop_entities, 2)
    disliked_one_hop_entities = sample(disliked_one_hop_entities, 2)
    random_entities = dataset.sample(2)

    # Return them all to obtain user feedback
    return jsonify(liked_one_hop_entities + disliked_one_hop_entities + random_entities)


@app.route('/api')
def main():
    return 'test'


def update_session(request, liked, disliked, unknown): 
    header = request.headers.get("Authorization")
    if header not in SESSION: 
        SESSION[header] ={
            'liked' :    [], 
            'disliked' : [],
            'unknown' :  []
        }

    SESSION[header]['liked'] += list(liked)
    SESSION[header]['disliked'] += list(disliked)
    SESSION[header]['unknown'] += list(unknown)


def get_seen_movies(request): 
    header = request.headers.get("Authorization")

    if header not in SESSION:
        return []

    return SESSION[header]['liked'] + SESSION[header]['disliked'] + SESSION[header]['unknown']

def get_rated_movies(request): 
    header = request.headers.get("Authorization")
    return SESSION[header]['liked'] + SESSION[header]['disliked']



if __name__ == "__main__":
    app.run()

