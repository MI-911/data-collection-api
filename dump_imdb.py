from dataset import movies, links
import os

merged = movies.merge(links, on='movieId')
directory = 'images'

if not os.path.exists(directory):
    os.makedirs(directory)

def get_row_path(row):
    return os.path.join(directory, f'{row.movieId}.jpg')


def chunkify(lst,n):
    return [lst[i::n] for i in range(n)]


if __name__ == "__main__":
    movie_imdb = []

    for _, row in merged.iterrows():
        movie_imdb.append((row.movieId, str(row.imdbId).zfill(7)))

    # Partition into n groups
    movie_imdb = chunkify(movie_imdb, 5)