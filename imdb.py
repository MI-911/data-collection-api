import json
import os
import requests
from bs4 import BeautifulSoup
import re
from collections import Counter
from dump_imdb import actors_directory


def get_actor_soup(actor_id):
    return BeautifulSoup(requests.get(f'https://imdb.com/name/nm{actor_id}').text, features='lxml')


def get_movie_soup(movie_id):
    return BeautifulSoup(requests.get(f'https://imdb.com/title/tt{movie_id}').text, features='lxml')


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

    return poster['src']


def get_actor_sets():
    sets = []

    for r, d, f in os.walk(actors_directory):
        for file in f:
            if '.json' in file:
                with open(os.path.join(actors_directory, file), 'r') as fp:
                    sets.append(json.load(fp))

    return sets


def get_actor_id_map():
    names = set()
    actor_sets = get_actor_sets()
    actor_id = dict()

    for actor_set in actor_sets:
        names = names.union(set(actor_set.values()))

    for name in names:
        ids = []
        for actor_set in actor_sets:
            for key, value in actor_set.items():
                if value == name:
                    ids.append(key)

        actor_id[name] = Counter(ids).most_common(1)[0][0]

    return actor_id


def get_actor_ids():
    ids = set()

    for actor_set in get_actor_sets():
        ids = ids.union(set(actor_set.keys()))

    return ids
