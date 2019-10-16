from flask import Flask, jsonify, request
from flask_cors import CORS

import dataset
from imdb import get_movie_poster, get_movie_soup, get_actors
from neo import get_related_entities

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})


@app.route('/api/begin')
def begin():
    samples = dataset.sample(50) # .sort_values(by='variance', ascending=False)
    print(samples)

    return jsonify([{
        "title": sample['title'],
        "id": sample['movieId'],
        "year": sample['year'],
        "poster": get_movie_poster(str(sample['imdbId']).zfill(7))
    } for index, sample in samples[:10].iterrows()])


@app.route('/api/entities', methods=['POST'])
def entities():
    json = request.json
    liked = set(json['liked'])
    disliked = set(json['disliked'])

    dataset.get_top_genres(liked.union(disliked))
    print(get_related_entities(dataset.get_escaped_names(liked.union(disliked))))

    return 'test'


@app.route('/api')
def main():
    return 'test'


if __name__ == "__main__":
    soup = get_movie_soup('0389790')
    print(get_movie_poster(soup))
    print(get_actors(soup))
    
    app.run()
