from flask import Flask, jsonify, request
from flask_cors import CORS
import flask_login

import dataset
from neo import get_related_entities

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})


@app.route('/api/begin')
def begin():
    samples = dataset.sample(50)  # .sort_values(by='variance', ascending=False)
    print(samples)

    return jsonify([{
        "title": sample['title'],
        "id": sample['movieId'],
        "year": sample['year'],
        "imdb": str(sample['imdbId']).zfill(7)
    } for index, sample in samples[:10].iterrows()])


@app.route('/api/entities', methods=['POST'])
def entities():
    json = request.json
    liked = set(json['liked'])
    disliked = set(json['disliked'])

    dataset.get_top_genres(liked.union(disliked))
    print(get_related_entities(liked.union(disliked)))

    return 'test'


@app.route('/api')
def main():
    return 'test'


if __name__ == "__main__":
    app.run()

