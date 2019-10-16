import re

import pandas as pd
from pandas import DataFrame
import itertools

base_path = 'dataset'

# Load from CSV
movies = pd.read_csv(f'{base_path}/movies.csv')
ratings = pd.read_csv(f'{base_path}/ratings.csv')
links = pd.read_csv(f'{base_path}/links.csv')

# Split title and year
movies['year'] = movies.title.str.extract('\((\d{4})\)', expand=True)
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

# Apply movieId as index
for df in [movies, ratings, links]:
    df.sort_values(by='movieId', inplace=True)
    df.reset_index(inplace=True, drop=True)


def sample(count):
    return ratings.merge(movies).merge(links).sample(count).drop_duplicates(['movieId'])


def get_movies_by_id(movieIds):
    return movies[movies.movieId.isin(movieIds)]


def get_top_genres(movieIds):
    genres = []
    for index, movie in get_movies_by_id(movieIds).iterrows():
        genres.extend(movie.genres)
    
    grouped = [(g[0], len(list(g[1]))) for g in itertools.groupby(sorted(genres))]
    print(grouped)
    print(get_movies_by_id(movieIds))


def get_names(movieIds):
    return get_movies_by_id(movieIds).title


def get_escaped_names(movieIds):
    return [_escape(name) for name in get_names(movieIds)]

def get_movies_iter():
    return movies.iterrows()


def _escape(name):
    return name.replace("'", "\\'")

