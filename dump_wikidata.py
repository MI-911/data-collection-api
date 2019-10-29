import json
import os
import threading
from collections import Counter
from concurrent.futures.thread import ThreadPoolExecutor
from time import sleep
from urllib.error import HTTPError, URLError
import csv
from tqdm import tqdm

from dataset import movies
from query_wikidata import get_genres, get_people, get_subjects

base_path = 'wikidata'
subjects_path = os.path.join(base_path, 'subjects.json')
genres_path = os.path.join(base_path, 'genres.json')
people_path = os.path.join(base_path, 'people.json')
movie_genres_path = os.path.join(base_path, 'movie_genres.json')
movie_directors_path = os.path.join(base_path, 'movie_directors.json')
movie_actors_path = os.path.join(base_path, 'movie_actors.json')
movie_uri_path = os.path.join(base_path, 'movie_uri.json')
movie_subjects_path = os.path.join(base_path, 'movie_subjects.json')

genres = dict()
subjects = dict()
people = dict()
movie_directors = dict()
movie_actors = dict()
movie_genres = json.load(open(movie_genres_path, 'r'))
movie_subjects = dict()
movie_uri = json.load(open(movie_uri_path, 'r'))
seen_uris = set() # set(movie_uri.values())
entity_ids = {key: value.split('/')[-1] for (key, value) in movie_uri.items()}
lock = threading.Lock()


def handle_chunks(fn, chunks, workers=15):
    executor = ThreadPoolExecutor(max_workers=workers)
    futures = []
    for chunk in chunks:
        futures.append(executor.submit(fn, chunk))

    for future in tqdm(futures):
        future.result()


def dump_subjects():
    def handle(value):
        try:
            result = get_subjects(entity_ids[value])
        except URLError:
            sleep(2.5)
            return handle(value)

        with lock:
            for key in result.keys():
                if key not in subjects:
                    subjects[key] = result[key]

        movie_subjects[value] = list(result.keys())

    handle_chunks(handle, movies[movies.imdbId.isin(entity_ids.keys())].imdbId)

    with open(subjects_path, 'w') as fp:
        json.dump(subjects, fp)

    with open(movie_subjects_path, 'w') as fp:
        json.dump(movie_subjects, fp)



def dump_genres():
    def handle(value):
        try:
            uri, result = get_genres(value)
        except URLError:
            sleep(2.5)
            return handle(value)

        # Only save the values if there is a valid URI for the movie
        if not uri:
            return

        if uri in seen_uris:
            print(f'Skip {value}/{uri} as the URI has already been seen')

            return
        seen_uris.add(uri)

        with lock:
            for key in result.keys():
                if key not in genres:
                    genres[key] = result[key]

        movie_genres[value] = list(result.keys())
        movie_uri[value] = uri

    movies_sorted = movies[~movies.imdbId.isin(movie_uri.keys())].sort_values(by='numRatings', ascending=False).imdbId
    handle_chunks(handle, movies_sorted)

    with open(genres_path, 'w') as fp:
        json.dump(genres, fp)

    with open(movie_genres_path, 'w') as fp:
        json.dump(movie_genres, fp)

    with open(movie_uri_path, 'w') as fp:
        json.dump(movie_uri, fp)


def dump_people():
    def handle(value):
        def insert_people(dictionary):
            with lock:
                for key in dictionary.keys():
                    if key not in people:
                        people[key] = dictionary[key]
            
        try:
            actors, directors = get_people(entity_ids[value])
        except URLError:
            sleep(2.5)
            return handle(value)

        # Insert these people into the general people dict
        insert_people(actors)
        insert_people(directors)

        # Now insert directors and actors for each movie as a list of URIs
        movie_actors[value] = list(actors.keys())
        movie_directors[value] = list(directors.keys())

    handle_chunks(handle, movies[movies.imdbId.isin(entity_ids.keys())].imdbId)

    with open(people_path, 'w') as fp:
        json.dump(people, fp)

    with open(movie_directors_path, 'w') as fp:
        json.dump(movie_directors, fp)

    with open(movie_actors_path, 'w') as fp:
        json.dump(movie_actors, fp)


def get_unmatched_movie_genres():
    return movies[~movies.imdbId.isin(json.load(open(movie_genres_path, 'r')).keys())]


def write_movies():
    uris = json.load(open(movie_uri_path, 'r'))
    result = list()

    missing = []
    for index, movie in movies.iterrows():
        if movie.imdbId not in uris:
            missing.append((movie.title, movie.imdbId, movie.numRatings))
            continue

        result.append({
            'uri': uris[movie.imdbId],
            'name': movie.title,
            'year': movie.year,
            'imdb': movie.imdbId
        })

    print(sorted(missing, key=lambda i: i[2], reverse=True)[:10])

    with open('movies.json', 'w') as fp:
        json.dump(dict(movies=result), fp)


def write_genres():
    genres = json.load(open(genres_path, 'r'))
    result = list()

    for key, value in genres.items():
        result.append({
            'uri': key,
            'name': value
        })

    with open('genres.json', 'w') as fp:
        json.dump(dict(genres=result), fp)


def write_subjects():
    subjects = json.load(open(subjects_path, 'r'))
    result = list()

    for key, value in subjects.items():
        result.append({
            'uri': key,
            'name': value
        })

    with open('subjects.json', 'w') as fp:
        json.dump(dict(subjects=result), fp)


def write_movie_subjects():
    movie_subjects = json.load(open(movie_subjects_path, 'r'))
    movie_uri = json.load(open(movie_uri_path, 'r'))

    result = list()

    for key, value in movie_subjects.items():
        if not value or key not in movie_uri:
            continue

        result.append({
            'head': movie_uri[key],
            'relation': 'HAS_SUBJECT',
            'tails': value
        })

    with open('movie_subjects.json', 'w') as fp:
        json.dump(dict(relationships=result), fp)


def write_movie_genres():
    movie_genres = json.load(open(movie_genres_path, 'r'))
    movie_uri = json.load(open(movie_uri_path, 'r'))

    result = list()

    for key, value in movie_genres.items():
        if not value or key not in movie_uri:
            continue

        result.append({
            'head': movie_uri[key],
            'relation': 'HAS_GENRE',
            'tails': value
        })

    with open('movie_genres.json', 'w') as fp:
        json.dump(dict(relationships=result), fp)


def write_movie_actors():
    movie_actors = json.load(open(movie_actors_path, 'r'))
    movie_uri = json.load(open(movie_uri_path, 'r'))

    result = list()

    for key, value in movie_actors.items():
        if not value or key not in movie_uri:
            continue

        result.append({
            'head': movie_uri[key],
            'relation': 'STARRING',
            'tails': value
        })

    with open('movie_actors.json', 'w') as fp:
        json.dump(dict(relationships=result), fp)


def write_movie_directors():
    movie_directors = json.load(open(movie_directors_path, 'r'))
    movie_uri = json.load(open(movie_uri_path, 'r'))

    result = list()

    for key, value in movie_directors.items():
        if not value or key not in movie_uri:
            continue

        result.append({
            'head': movie_uri[key],
            'relation': 'DIRECTED_BY',
            'tails': value
        })

    with open('movie_directors.json', 'w') as fp:
        json.dump(dict(relationships=result), fp)


def write_people():
    people = json.load(open(people_path, 'r'))

    result = list()
    for key, value in people.items():
        value['uri'] = key

        result.append(value)

    with open('people.json', 'w') as fp:
        json.dump(dict(people=result), fp)


def find_duplicate_movies():
    uris = json.load(open(movie_uri_path, 'r'))

    counted = Counter(uris.values())
    print(counted)
    counted = sorted(counted.items(), key=lambda i: i[1], reverse=True)[:10]

    print(counted)


def write_triples():
    files = ['movie_genres.json', 'movie_directors.json', 'movie_subjects.json', 'movie_actors.json']
    result = list()

    with open('wikidata.csv', mode='w') as csv_file:
        fieldnames = ['head', 'relation', 'tail']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for file in files:
            rows = []

            with open(file, 'r') as fp:
                data = json.load(fp)['relationships']

                for relation in data:
                    for tail in relation['tails']:
                        rows.append({'head': relation['head'], 'relation': relation['relation'], 'tail': tail})

            writer.writerows(rows)


if __name__ == "__main__":
    write_triples()

