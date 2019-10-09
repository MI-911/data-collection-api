import requests
from bs4 import BeautifulSoup


def get_poster(movie_id):
    soup = BeautifulSoup(requests.get(f'https://imdb.com/title/tt{movie_id}').text)
    wrapper = soup.find("div", attrs={"class": "poster"})
    if not wrapper:
        return None

    poster = wrapper.find('img')
    if not poster:
        return None

    return poster['src']
