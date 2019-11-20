import os

from dataset import movies


def rename_actors():
    folder = 'actor_images'

    for file in os.listdir(folder):
        if file.endswith('.jpg') and not file.startswith('nm'):
            os.rename(os.path.join(folder, file), os.path.join(folder, f'nm{file}'))


def rename_movies():
    folder = 'movie_images'

    for file in os.listdir(folder):
        if file.endswith('.jpg') and not file.startswith('tt'):
            movie_id = int(file[:-4])

            match = movies.loc[movies.movieId == movie_id]
            for index, row in match.iterrows():
                os.rename(os.path.join(folder, file), os.path.join(folder, f'{row.imdbId}.jpg'))
                break


if __name__ == "__main__":
    # rename_actors()
    rename_movies()
