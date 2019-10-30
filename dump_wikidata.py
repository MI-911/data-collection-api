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

    with open(os.path.join(base_path, 'movies.csv'), 'w') as fp:
        writer = csv.DictWriter(fp, ['uri:ID', 'name', 'year:int', 'imdb', ':LABEL'])
        writer.writeheader()

        for index, movie in movies.iterrows():
            if movie.imdbId not in uris:
                continue

            writer.writerow({'uri:ID': uris[movie.imdbId], 'name': movie.title, 'year:int': movie.year, 'imdb': movie.imdbId, ':LABEL': 'Movie'})


def write_categories():
    genres = json.load(open(genres_path, 'r'))
    subjects = json.load(open(subjects_path, 'r'))
    # People and movies can be subjects, we should not write them twice
    existing_uris = set(json.load(open(people_path, 'r')).keys()).union(set(movie_uri.values()))

    with open(os.path.join(base_path, 'categories.csv'), 'w') as fp:
        writer = csv.DictWriter(fp, ['uri:ID', 'name', ':LABEL'])
        writer.writeheader()

        for key, value in genres.items():
            if key not in seen_uris and key not in existing_uris:
                writer.writerow({'uri:ID': key, 'name': value.title(), ':LABEL': 'Category'})
                seen_uris.add(key)

        for key, value in subjects.items():
            if key not in seen_uris and key not in existing_uris:
                writer.writerow({'uri:ID': key, 'name': value.title(), ':LABEL': 'Category'})
                seen_uris.add(key)


def write_movie_subjects():
    movie_subjects = json.load(open(movie_subjects_path, 'r'))
    movie_uri = json.load(open(movie_uri_path, 'r'))

    with open(os.path.join(base_path, 'movie_subject.csv'), 'w') as fp:
        writer = csv.DictWriter(fp, [':START_ID', ':END_ID', ':TYPE'])
        writer.writeheader()

        for key, value in movie_subjects.items():
            if not value or key not in movie_uri:
                continue

            for tail in value:
                writer.writerow({':START_ID': movie_uri[key], ':END_ID': tail, ':TYPE': 'HAS_SUBJECT'})


def write_movie_genres():
    movie_genres = json.load(open(movie_genres_path, 'r'))
    movie_uri = json.load(open(movie_uri_path, 'r'))

    with open(os.path.join(base_path, 'movie_genre.csv'), 'w') as fp:
        writer = csv.DictWriter(fp, [':START_ID', ':END_ID', ':TYPE'])
        writer.writeheader()

        for key, value in movie_genres.items():
            if not value or key not in movie_uri:
                continue

            for tail in value:
                writer.writerow({':START_ID': movie_uri[key], ':END_ID': tail, ':TYPE': 'HAS_GENRE'})


def _write_movie_person(source, dest, relation, valid_people):
    movie_persons = json.load(open(source, 'r'))
    movie_uri = json.load(open(movie_uri_path, 'r'))

    with open(os.path.join(base_path, dest), 'w') as fp:
        writer = csv.DictWriter(fp, [':START_ID', ':END_ID', ':TYPE'])
        writer.writeheader()

        for key, value in movie_persons.items():
            if not value or key not in movie_uri:
                continue

            for tail in value:
                if tail in valid_people:
                    writer.writerow({':START_ID': movie_uri[key], ':END_ID': tail, ':TYPE': relation})


def write_people():
    people = json.load(open(people_path, 'r'))
    valid_people = set()

    with open(os.path.join(base_path, 'people.csv'), 'w') as fp:
        writer = csv.DictWriter(fp, ['uri:ID', 'name', 'imdb', 'image', ':LABEL'])
        writer.writeheader()

        for key, value in people.items():
            # For some reason, a few movies are included as people...
            # If they are actually people, their IMDb id starts with nm
            if value['imdb'].startswith('nm'):
                writer.writerow({'uri:ID': key, 'name': value['name'], 'image': value['image'], 'imdb': value['imdb'], ':LABEL': 'Person'})
                valid_people.add(key)

    _write_movie_person(movie_actors_path, 'movie_actor.csv', 'STARRING', valid_people)
    _write_movie_person(movie_directors_path, 'movie_director.csv', 'DIRECTED_BY', valid_people)


def find_duplicate_movies():
    uris = json.load(open(movie_uri_path, 'r'))

    counted = Counter(uris.values())
    print(counted)
    counted = sorted(counted.items(), key=lambda i: i[1], reverse=True)[:10]

    print(counted)


def write_triples():
    files = ['movie_genres.json', 'movie_directors.json', 'movie_subjects.json', 'movie_actors.json']

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
    write_categories()
    write_movies()
    write_movie_genres()
    write_movie_subjects()
    write_people()
