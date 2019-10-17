from dataset import movies, links
from imdb import *
import os
import threading
import requests
import json
from concurrent.futures import ThreadPoolExecutor, wait

csv_writer_lock = threading.Lock()
merged = movies.merge(links, on='movieId')
image_directory = 'movie_images'
actors_directory = 'movie_actors'
actor_images_directory = 'actor_images'

if not os.path.exists(image_directory):
    os.makedirs(image_directory)


if not os.path.exists(actors_directory):
    os.makedirs(actors_directory)


if not os.path.exists(actor_images_directory):
    os.makedirs(actor_images_directory)


def get_image_path(movieId):
    return os.path.join(image_directory, f'{movieId}.jpg')


def get_actors_path(movieId):
    return os.path.join(actors_directory, f'{movieId}.json')


def get_actor_image_path(actor_id):
    return os.path.join(actor_images_directory, f'{actor_id}.jpg')


def split_into_chunks(lst, n):
    return [lst[i::n] for i in range(n)]


def save_url_to_file(url, file):
    with open(file, 'wb') as handle:
        response = requests.get(url, stream=True)

        if not response.ok:
            return

        for block in response.iter_content(1024):
            if not block:
                break

            handle.write(block)


def handle_movie(movie_id, imdb_id):
    soup = get_movie_soup(imdb_id)

    # Save poster
    image_url = get_image_path(soup)
    if image_url:
        save_url_to_file(get_movie_poster(soup), get_image_path(movie_id))
    else:
        return False

    # Save list of actor ids
    actors = get_actors(soup)
    if actors and len(actors) > 0:
        with open(get_actors_path(movie_id), 'w') as outFile:
            json.dump(actors, outFile)
    else:
        return False

    return True


def handle_actor(actor_id):
    print(actor_id)

    soup = get_actor_soup(actor_id)

    # Save poster
    image_url = get_actor_image_path(soup)
    if image_url:
        save_url_to_file(get_actor_poster(soup), get_actor_image_path(actor_id))
    else:
        return False
    
    return True


def handle_movie_chunk(chunk):
    succeeded = 0
    for movie_id, imdb_id in chunk:
        try:
            if handle_movie(movie_id, imdb_id):
                succeeded += 1
        except Exception as e:
            print(f'{movie_id}/{imdb_id} failed: {e}')

    return succeeded


def handle_actor_chunk(chunk):
    succeeded = 0
    for actor_id in chunk:
        try:
            if handle_actor(actor_id):
                succeeded += 1
        except Exception as e:
            print(f'{actor_id} failed: {e}')

    return succeeded


def _handle_chunks(fn, chunks):
    executor = ThreadPoolExecutor(max_workers=15)
    futures = []
    for chunk in chunks:
        futures.append(executor.submit(fn, chunk))

    sum_succeeded = 0
    for future in futures:
        sum_succeeded += future.result()

    return sum_succeeded


def dump_movies():
    movie_imdb = []

    for _, row in merged.iterrows():
        movie_imdb.append((row.movieId, str(row.imdbId).zfill(7)))

    print(f'Expected movies: {len(movie_imdb)}')

    # Partition into n groups
    movie_imdb = split_into_chunks(movie_imdb, 50)
    
    print(f'Sum succeeded: {_handle_chunks(handle_movie_chunk, movie_imdb)}')


def dump_actors():
    actors = split_into_chunks(list(get_actor_ids()), 50)

    print(f'Sum succeeded: {_handle_chunks(handle_actor_chunk, actors)}')


if __name__ == "__main__":
    mapped = get_actor_id_map()
    print(mapped)
    with open('actors.json', 'w') as fp:
        json.dump(mapped, fp)
