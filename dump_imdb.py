import threading
from concurrent.futures import ThreadPoolExecutor

import tqdm

from dataset import movies, links
from imdb import *

csv_writer_lock = threading.Lock()
actor_writer_lock = threading.Lock()
merged = movies.merge(links, on='movieId')
image_directory = 'movie_images'
actors_directory = 'movie_actors'
actor_images_directory = 'actor_images'
existing_actors_file = 'existing_actors.json'
existing_actors = []

if not os.path.exists(image_directory):
    os.makedirs(image_directory)


if not os.path.exists(actors_directory):
    os.makedirs(actors_directory)


if not os.path.exists(actor_images_directory):
    os.makedirs(actor_images_directory)


def get_image_path(movie_id):
    return os.path.join(image_directory, f'{movie_id}.jpg')


def get_actors_path(movie_id):
    return os.path.join(actors_directory, f'{movie_id}.json')


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


def handle_movie(imdb_id):
    try:
        soup = get_movie_soup(imdb_id)

        # Save poster
        image_url = get_image_path(soup)
        if image_url:
            save_url_to_file(get_movie_poster(soup), get_image_path(imdb_id))
    except Exception as e:
        print(f'{imdb_id} failed: {e}')


def handle_actor(actor_id):
    soup = get_actor_soup(actor_id)
    if soup.find('img', attrs={'class': 'no-pic-image'}):
        existing_actors.append(actor_id)
        print(f'No picture {actor_id}')
        return True
    else:
        # Save poster
        poster = get_actor_poster(soup)
        if poster:
            save_url_to_file(poster, get_actor_image_path(actor_id))

            existing_actors.append(actor_id)
        else:
            print(actor_id)
            return False

        return True


def handle_actor_chunk(chunk):
    succeeded = 0
    for actor_id in chunk:
        try:
            if handle_actor(actor_id):
                succeeded += 1
        except Exception as e:
            print(f'{actor_id} failed: {e}')

    write_existing_actors()

    return succeeded


def _handle_chunks(fn, chunks):
    executor = ThreadPoolExecutor(max_workers=50)
    futures = []
    for chunk in chunks:
        futures.append(executor.submit(fn, chunk))

    for future in tqdm.tqdm(futures):
        future.result()


def dump_movies():
    existing = set()
    for r, d, f in os.walk(image_directory):
        for file in f:
            movie_id = file.split('.')[0]
            if movie_id not in existing:
                existing.add(movie_id)

    missing = set(movies.imdbId).difference(existing)

    _handle_chunks(handle_movie, missing)


def write_existing_actors():
    with actor_writer_lock:
        with open(existing_actors_file, 'w') as fp:
            json.dump(existing_actors, fp)


def read_existing_actors():
    if not os.path.exists(existing_actors_file):
        return []

    with open(existing_actors_file, 'r') as fp:
        return json.load(fp)


def dump_actors():
    print(len(existing_actors))

    existing = set()
    for r, d, f in os.walk(actor_images_directory):
        for file in f:
            actor_id = file.split('.')[0]
            if actor_id not in existing:
                existing_actors.append(actor_id)

    actor_ids = get_actor_ids().symmetric_difference(set(existing_actors))
    print(len(actor_ids))
    actors = split_into_chunks(list(actor_ids), 1000)

    print(f'Sum succeeded: {_handle_chunks(handle_actor_chunk, actors)}')


if __name__ == "__main__":
    # existing_actors = read_existing_actors()
    # dump_actors()
    dump_movies()
