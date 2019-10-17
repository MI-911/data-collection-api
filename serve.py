from flask import Flask, jsonify, request, send_from_directory, abort, session
from flask_cors import CORS
from random import choice, sample

import dataset
from neo import get_related_entities, get_one_hop_entities, get_relevant_neighbors

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

    # Choose one seed from liked and disliked at random
    liked_choice = choice(list(liked))
    disliked_choice = choice(list(disliked))

    # Find the relevant neighbors (with page rank) from the liked and disliked seeds
    liked_relevant = get_relevant_neighbors(liked_choice)
    disliked_relevant = get_relevant_neighbors(disliked_choice)


    # Sample 2 entities from liked_relevant and disliked_relevant, respectively,
    # then sample 2 entities randomly from the KG 
    # TODO: Sample this properly - perhaps based on PageRank
    liked_relevant = sample(liked_relevant, 2)
    disliked_relevant = sample(disliked_relevant, 2)
    random_entities = sample(dataset.get_unseen(seen_entities), 2)

    # Return them all to obtain user feedback
    return jsonify(liked_relevant + disliked_relevant + random_entities)


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

