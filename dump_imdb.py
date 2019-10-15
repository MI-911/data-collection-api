from dataset import movies, links
from imdb import get_poster, get_soup, get_actors
import os
import threading
import requests
import json
from concurrent.futures import ThreadPoolExecutor, wait

csv_writer_lock = threading.Lock()
merged = movies.merge(links, on='movieId')
image_directory = 'images'
actors_directory = 'actors'


if not os.path.exists(image_directory):
    os.makedirs(image_directory)


if not os.path.exists(actors_directory):
    os.makedirs(actors_directory)


def get_image_path(movieId):
    return os.path.join(image_directory, f'{movieId}.jpg')


def get_actors_path(movieId):
    return os.path.join(actors_directory, f'{movieId}.json')


def chunkify(lst,n):
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


def handle_movie(movieId, imdbId):
    print(imdbId)

    soup = get_soup(imdbId)

    # Save poster
    image_url = get_image_path(soup)
    if image_url:
        save_url_to_file(get_poster(soup), get_image_path(movieId))
    else:
        return False

    # Save list of actor ids
    actors = get_actors(soup)
    if actors and len(actors) > 0:
        with open(get_actors_path(movieId), 'w') as outFile:
            json.dump(actors, outFile)
    else:
        return False

    return True


def handle_chunk(chunk):
    succeeded = 0
    for movieId, imdbId in chunk:
        try:
            if handle_movie(movieId, imdbId):
                succeeded += 1
        except Exception as e:
            print(f'{movieId}/{imdbId} failed: {e}')

    return succeeded


def dump_movies():
    movie_imdb = []

    for _, row in merged.iterrows():
        movie_imdb.append((row.movieId, str(row.imdbId).zfill(7)))

    print(f'Expected movies: {len(movie_imdb)}')

    # Partition into n groups
    movie_imdb = chunkify(movie_imdb, 50)

    executor = ThreadPoolExecutor(max_workers=15)
    futures = []
    for chunk in movie_imdb:
        futures.append(executor.submit(handle_chunk, chunk))

    sum_succeeded = 0
    for future in futures:
        sum_succeeded += future.result()
    
    print(f'Sum succeeded: {sum_succeeded}')


def dump_actors():
    pass


if __name__ == "__main__":
    dump_movies()
