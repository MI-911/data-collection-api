from flask import Flask, jsonify, request, send_from_directory, abort, session
from flask_cors import CORS
from random import choice, sample

import dataset
from neo import get_related_entities, get_one_hop_entities

app = Flask(__name__)
app.secret_key = "XD"
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})


@app.route('/static/movie/<movie>')
def get_poster(movie):
    return send_from_directory('movie_images', f'{movie}.jpg')


@app.route('/api/begin')
def begin():
    samples = dataset.sample(50)  # .sort_values(by='variance', ascending=False)
    print(samples)

    return jsonify([{
        "name": f"{sample['title']} ({sample['year']})",
        "id": sample['movieId'],
        "resource": "movie",
        "uri": sample['uri']
    } for index, sample in samples[:10].iterrows()])


@app.route('/api/entities', methods=['POST'])
def entities():
    json = request.json
    liked = set(json['liked'])
    disliked = set(json['disliked'])
    add_movies_to_session(liked.union(disliked))

    # Choose one seed from liked and disliked at random
    liked_choice = choice(liked)
    disliked_choice = choice(disliked)

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


def add_movies_to_session(movies): 
    if 'rated' not in session:
        session['rated'] = [] 

    if movies: 
        session['rated'] = session['rated'] + movies


if __name__ == "__main__":
    app.run()

