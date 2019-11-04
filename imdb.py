import json
import os
import requests
from bs4 import BeautifulSoup
import re
from collections import Counter
import pandas as pd


def get_actor_soup(actor_id):
    return BeautifulSoup(requests.get(f'https://m.imdb.com/name/{actor_id}').text, features='lxml')


def get_movie_soup(movie_id):
    return BeautifulSoup(requests.get(f'https://imdb.com/title/{movie_id}').text, features='lxml')


def get_actors(soup):
    table = soup.find('table', attrs={'class': 'cast_list'})
    if not table:
        return None
    
    actors = dict()
    for row in table.find_all('tr'):
        profile_td = row.find('td', attrs={'class': 'primary_photo'})

        if profile_td:
            anchor = profile_td.find('a')
            if anchor:
                image = anchor.find('img')
                if image:
                    identifier = re.search(r'\d+|$', anchor['href']).group()
                    actors[identifier] = image['title']

    return actors


def get_movie_poster(soup):
    wrapper = soup.find('div', attrs={'class': 'poster'})
    if not wrapper:
        return None

    poster = wrapper.find('img')
    if not poster:
        return None

    return poster['src']


def get_actor_poster(soup):
    poster = soup.find('img', attrs={'id': 'name-poster'})
    if not poster:
        return None

    return f"{poster['src'].split('V1')[0]}.jpg"


def get_actor_ids():
    return set(pd.read_csv('wikidata/people.csv').imdb)


if __name__ == "__main__":
    print(get_actor_ids())
