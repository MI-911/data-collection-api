import json
import re

import pandas as pd
from pandas import DataFrame
import itertools

base_path = 'dataset'

# Load from JSON
actors = dict()
with open(f'{base_path}/actors.json', 'r') as fp:
    actors = json.load(fp)

# Load from CSV
movies = pd.read_csv(f'{base_path}/movies.csv')
ratings = pd.read_csv(f'{base_path}/ratings.csv')
links = pd.read_csv(f'{base_path}/links.csv')
mapping = pd.read_csv(f'{base_path}/mapping.csv')

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
movies = movies[movies.year <= 2016]


def transform_title(title):
    title = re.sub(r"\(.*\)", "", title).strip()

    if title.endswith(', The'):
        title = f'The {title[:-5]}'
    
    if title.endswith(', A'):
        title = f'A {title[:-3]}'

    return title


movies.title = movies.title.map(transform_title)

# Add variance to movies, remove movies with NaN variance (|r|<2)
dftmp = ratings[['movieId', 'rating']].groupby('movieId').var()
dftmp.columns = ['variance']
movies = movies.merge(dftmp.dropna(), on='movieId')

# Add count to movies
dftmp = ratings[['movieId', 'rating']].groupby('movieId').count()
dftmp.columns = ['numRatings']
movies = movies.merge(dftmp.dropna(), on='movieId')

# Remove movies with less than median ratings
movies = movies[movies['numRatings'].ge(int(dftmp.median()))]

# Merge movies with mappings and links
movies = movies.merge(mapping.dropna(), on='movieId')
movies = movies.merge(links.dropna(), on='movieId')

# Apply movieId as index
for df in [movies, ratings, links]:
    df.sort_values(by='movieId', inplace=True)
    df.reset_index(inplace=True, drop=True)


def sample(count, exclude):
    filtered_movies = movies[~movies.uri.isin(exclude)]

    return ratings.merge(filtered_movies).sample(count).drop_duplicates(['movieId'])


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
