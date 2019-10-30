import json
import os
import re

import pandas as pd
from pandas import DataFrame
import itertools
from math import log
from time import sleep, time


def _replace_ends_with(title, substr):
    if title.endswith(f', {substr}'):
        return f'{substr} {title[:-(2 + len(substr))]}'

    return title


def transform_title(title):
    title = re.sub(r'\(.*\)', "", title).strip()

    title = _replace_ends_with(title, 'The')
    title = _replace_ends_with(title, 'A')
    title = _replace_ends_with(title, 'Les')
    title = _replace_ends_with(title, 'Le')
    title = _replace_ends_with(title, 'La')
    title = _replace_ends_with(title, 'El')
    title = _replace_ends_with(title, 'Die')
    title = _replace_ends_with(title, 'Der')
    title = _replace_ends_with(title, 'Das')
    title = _replace_ends_with(title, 'Il')
    title = _replace_ends_with(title, 'Los')
    title = _replace_ends_with(title, 'Las')

    return title.strip()


def transform_imdb_id(imdb_id):
    return f'tt{str(imdb_id).zfill(7)}'


def sample(count, exclude):
    relevant = movies[~movies.uri.isin(exclude)]

    return relevant.sample(n=count, weights=relevant.numRatings)


def get_unseen(seen):
    tmp = ratings.merge(movies).merge(links).drop_duplicates(['movieId'])["uri"]
    return list(set(tmp) - set(seen))


def get_movies_by_id(movie_ids):
    return movies[movies.movieId.isin(movie_ids)]


def get_names(movie_ids):
    return get_movies_by_id(movie_ids).title


def get_movies_iter():
    return movies.iterrows()


def get_actor_id(actor_name):
    return actors.get(actor_name, None)


NUM_RATINGS_MAP = {}  # Cache
def get_num_ratings(movie_id): 
    if movie_id not in NUM_RATINGS_MAP: 
        NUM_RATINGS_MAP[movie_id] = len(ratings[ratings['movieId'] == movie_id])
    return NUM_RATINGS_MAP[movie_id]


def get_year(movie_id): 
    return int(movies[movies['movieId'] == movie_id]['year'].values[-1])


def get_sampling_score(movie_id, k=2000): 
    N = len(ratings)
    Y = get_year(movie_id)
    Y = Y - k if (Y - k) > 1 else 1  # Avoid log errors
    R = get_num_ratings(movie_id)
    return (R / N) * (log(Y))


data_path = 'data'
ml_path = os.path.join(data_path, 'movielens')

# Load from JSON
actors = json.load(open(f'{data_path}/actors.json', 'r'))

# Load from CSV
movies = pd.read_csv(f'{ml_path}/movies.csv')
ratings = pd.read_csv(f'{ml_path}/ratings.csv')
links = pd.read_csv(f'{ml_path}/links.csv')
mapping = pd.read_csv(f'{ml_path}/mapping.csv')

# Get unique genres
genres_unique = pd.DataFrame(movies.genres.str.split('|').tolist()).stack().unique()
genres_unique = pd.DataFrame(genres_unique, columns=['genre'])
genres_unique = genres_unique[~genres_unique.genre.str.contains('no genres listed')]

# Split title and year
movies['year'] = movies.title.str.extract(r'\((\d{4})\)', expand=True)
movies.dropna(inplace=True)
movies.year = movies.year.astype(int)
movies.title = movies.title.str[:-7]
movies.genres = movies.genres.str.split('|').tolist()

movies.title = movies.title.map(transform_title)

# Add count to movies
dftmp = ratings[['movieId', 'rating']].groupby('movieId').count()
dftmp.columns = ['numRatings']
movies = movies.merge(dftmp.dropna(), on='movieId')

# Remove movies with less than median ratings
movies = movies[movies['numRatings'].ge(int(dftmp.median()))]

# Merge movies with links links
movies = movies.merge(links, on='movieId')

# Proper imdb ids
movies.imdbId = movies.imdbId.map(transform_imdb_id)

# Merge with mappings
movies = movies.merge(mapping, on='imdbId')

# Apply movieId as index
for df in [movies, ratings, links]:
    df.sort_values(by='movieId', inplace=True)
    df.reset_index(inplace=True, drop=True)

if __name__ == "__main__":
    print(movies.shape)
    for n in range(20):
        print(f'Starts {time()}')
        sampled = movies[~movies.title.isin(["test"])].sample(n=30, weights=movies.numRatings)
        print(sampled)
