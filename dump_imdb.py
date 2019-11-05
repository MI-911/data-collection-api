import threading
from concurrent.futures import ThreadPoolExecutor

import tqdm

from dataset import movies, links
from imdb import *
from PIL import Image

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
    tmp_file = f'{file}.src'

    with open(tmp_file, 'wb') as handle:
        response = requests.get(url, stream=True)

        if not response.ok:
            return

        for block in response.iter_content(1024):
            if not block:
                break

            handle.write(block)

    try:
        image = Image.open(tmp_file)
        new_height = 268
        new_width = int(new_height * image.size[0] / image.size[1])
        image = image.resize((new_width, new_height), Image.ANTIALIAS)
        image.save(file)

        os.remove(tmp_file)
    except IOError as error:
        print(error)


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
            # print(actor_id)
            return False

        return True


def handle_actor_chunk(chunk):
    try:
        handle_actor(chunk)
    except Exception as e:
        print(f'{chunk} failed: {e}')

    write_existing_actors()


def _handle_chunks(fn, chunks):
    executor = ThreadPoolExecutor(max_workers=150)
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

    _handle_chunks(handle_actor_chunk, actor_ids)


if __name__ == "__main__":
    # existing_actors = read_existing_actors()
    # dump_actors()
    # dump_movies()
    # dump_actors()
    handle_actor('nm0293589')
