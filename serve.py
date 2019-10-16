from flask import Flask, jsonify, request, send_from_directory, abort, session
from flask_cors import CORS

import dataset
from neo import get_related_entities

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
        "resource": "movie"
    } for index, sample in samples[:10].iterrows()])


@app.route('/api/rate')
def rate(): 
    # We store rated movies in sessions.
    # At each turn of the game, rated movies are sent 
    # as application/json and stored in the session.
    # At any time, session['rated'] will show the already
    # rated movies.
    add_movies_to_session(request)


@app.route('/api/entities', methods=['POST'])
def entities():
    json = request.json
    liked = set(json['liked'])
    disliked = set(json['disliked'])
    add_movies_to_session(liked.union(disliked))

    dataset.get_top_genres(liked.union(disliked))
    print(get_related_entities(liked.union(disliked)))

    return 'test'


@app.route('/api')
def main():
    return 'test'


def add_movies_to_session(movies): 
    if not 'rated' in session: 
        session['rated'] = [] 

    if movies: 
        session['rated'] = session['rated'] + movies


if __name__ == "__main__":
    app.run()

