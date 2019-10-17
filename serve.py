from flask import Flask, jsonify, request, send_from_directory, abort, session
from flask_cors import CORS
from random import choice, sample, shuffle

import dataset
from neo import get_related_entities, get_one_hop_entities, get_relevant_neighbors, get_unseen_entities

app = Flask(__name__)
app.secret_key = "XD"
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

N_QUESTIONS = 50
SESSION = {} 
N_QUESTIONS = 6
N_ENTITIES = N_QUESTIONS // 3


@app.route('/static/movie/<movie>')
def get_poster(movie):
    return send_from_directory('movie_images', f'{movie}.jpg')


def _get_samples():
    samples = dataset.sample(100)  # .sort_values(by='variance', ascending=False)
    samples = samples[~samples.uri.isin(get_seen_entities(request))]

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

    seen_entities = get_seen_entities(request)

    # Only ask at max N_QUESTIONS
    if len(seen_entities) < N_QUESTIONS:
        return jsonify(_get_samples())


    # Find the relevant neighbors (with page rank) from the liked and disliked seeds
    liked_relevant = [n for n in get_relevant_neighbors(list(liked)) if n not in seen_entities][:N_ENTITIES]
    disliked_relevant = [n for n in get_relevant_neighbors(list(disliked)) if n not in seen_entities and n not in liked_relevant][:N_ENTITIES]
    random_entities = sample(get_unseen_entities(seen_entities + liked_relevant + disliked_relevant), N_ENTITIES)

    # Return them all to obtain user feedback
    entities = liked_relevant + disliked_relevant + random_entities
    shuffle(entities)
    return jsonify(entities)


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


def get_seen_entities(request):
    header = request.headers.get("Authorization")

    if header not in SESSION:
        return []

    return SESSION[header]['liked'] + SESSION[header]['disliked'] + SESSION[header]['unknown']


def get_rated_entities(request):
    header = request.headers.get("Authorization")

    if header not in SESSION: 
        return []
        
    return SESSION[header]['liked'] + SESSION[header]['disliked']



if __name__ == "__main__":
    app.run()

