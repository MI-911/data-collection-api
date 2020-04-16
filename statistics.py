import csv
import json
from collections import Counter
from os.path import exists

import numpy as np
from scipy import median, mean, amin, amax, std, percentile

from dataset import movie_uris_set
from queries import get_number_entities
from utility.utilities import get_unique_uuids, get_sessions

uri_name = dict()
uri_name_path = 'data/movielens/uri_name.csv'

if exists(uri_name_path):
    with open(uri_name_path, 'r') as fp:
        reader = csv.DictReader(fp)

        for row in reader:
            uri_name[row['uri']] = row['name']


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)


def get_durations(sessions): 
    durations = []
    for session in sessions:
        timestamps = session['timestamps'] 
        durations.append(timestamps[-1] - timestamps[0])

    return durations


def get_likes(sessions): 
    likes = []
    for session in sessions: 
        likes += session['liked']

    return likes 


def get_dislikes(sessions): 
    dislikes = []
    for session in sessions: 
        dislikes += session['disliked']

    return dislikes 


def get_unknowns(sessions):
    unknowns = []
    for session in sessions: 
        unknowns += session['unknown']

    return unknowns  


def get_duration_statistics(sessions): 
    return get_list_statistics(get_durations(sessions))


def get_list_statistics(lst):
    return {
        'min': amin(lst),
        'max': amax(lst),
        'avg': mean(lst),
        'median': median(lst),
        'std': std(lst),
        'q1': percentile(lst, 25),
        'q3': percentile(lst, 75)
    }


def get_feedback_statistics(sessions): 
    likes = [] 
    dislikes = [] 
    unknowns = []
    like_to_dislike_ratios = []

    for session in sessions: 
        _likes = len(session['liked'])
        _dislikes = len(session['disliked'])
        _unknowns = len(session['unknown'])
        likes.append(_likes)
        dislikes.append(_dislikes)
        unknowns.append(_unknowns)

        if _dislikes:
            like_to_dislike_ratios.append(_likes / _dislikes)

    return {
        key: get_list_statistics(lst) for key, lst in {
            'likes': likes,
            'dislikes': dislikes,
            'unknowns': unknowns,
            'like_to_dislike_ratios': like_to_dislike_ratios
        }.items()
    }


def get_top_entities(session_set):
    categories = {'liked', 'disliked', 'unknown'}
    category_items = {key: [] for key in categories}

    for session in session_set:
        for category in categories:
            category_items[category] += session[category]

    return {
        category: [{'uri': uri, 'count': count,
                    'name': uri_name.get(uri, 'N/A')} for uri, count in Counter(items).most_common(10)]

        for category, items in category_items.items()
    }


def get_unique_entities(session_set):
    categories = {'liked', 'disliked', 'unknown'}
    items = get_entities_set_from_categories(session_set, categories)

    return len(items)


def _filter(uris, only_movies=False, only_non_movies=False):
    if only_movies:
        return [uri for uri in uris if uri in movie_uris_set]
    elif only_non_movies:
        return [uri for uri in uris if uri not in movie_uris_set]

    return uris


def get_entity_rated_rate(session_set):
    categories = {'liked', 'disliked'}
    items = get_entities_set_from_categories(session_set, categories)

    num = get_number_entities()

    return len(items) / (num * 1.0)


def get_entities_set_from_categories(session_set, categories):
    items = list()
    for session in session_set:
        for category in categories:
            items += session[category]

    return set(items)


def get_movie_ratings(session_set):
    movie_counts = []
    other_counts = []
    total_counts = []

    for session in session_set:
        rated = set(session['liked'] + session['disliked'])

        movie_count = len(_filter(rated, only_movies=True))
        other_count = len(_filter(rated, only_non_movies=True))

        movie_counts.append(movie_count)
        other_counts.append(other_count)
        total_counts.append(movie_count + other_count)

    return {
        'movies': get_list_statistics(movie_counts),
        'others': get_list_statistics(other_counts),
        'total': get_list_statistics(total_counts)
    }


def get_feedback_distribution(session_set, only_movies=False, only_non_movies=False):
    n_total, n_liked, n_disliked, n_unknown = 0, 0, 0, 0

    for session in session_set: 
        uris = _filter(session['liked'], only_movies=only_movies, only_non_movies=only_non_movies)
        n_liked += len(uris)

        uris = _filter(session['disliked'], only_movies=only_movies, only_non_movies=only_non_movies)
        n_disliked += len(uris)

        uris = _filter(session['unknown'], only_movies=only_movies, only_non_movies=only_non_movies)
        n_unknown += len(uris)

    n_total = n_liked + n_disliked + n_unknown
    
    return {
        'n_total': n_total,
        'n_liked': n_liked,
        'n_disliked': n_disliked,
        'n_unknown': n_unknown,
        'p_liked': n_liked / n_total,
        'p_disliked': n_disliked / n_total,
        'p_unknown': n_unknown / n_total
    }


def compute_statistics(versions=None):
    unique_tokens_not_empty = get_unique_uuids(filter_empty=True, versions=versions)
    unique_tokens_final = get_unique_uuids(filter_final=True, versions=versions)
    
    sessions = get_sessions(filter_empty=True, versions=versions)
    if not sessions:
        return None
    completed_sessions = [session for session in sessions if session['final']]

    statistics = {
        key: {
            'n_sessions': len(session_set),
            'n_users': len(unique_tokens_not_empty if key == 'all' else unique_tokens_final),
            'distributions': {
                'movies': get_feedback_distribution(session_set, only_movies=True),
                'non_movies': get_feedback_distribution(session_set, only_non_movies=True),
                'entities': get_feedback_distribution(session_set)
            },
            'durations': get_duration_statistics(session_set),
            'feedback': get_feedback_statistics(session_set),
            'top': get_top_entities(session_set),
            'n_entities': get_unique_entities(session_set),
            'rated_rate': get_entity_rated_rate(session_set),
            'n_ratings': {
                'session': get_movie_ratings(session_set)
            }
        }
        for key, session_set in {'all': sessions, 'completed': completed_sessions}.items()
    }

    return statistics


if __name__ == '__main__': 
    with open('statistics.json', 'w+') as fp: 
        json.dump(compute_statistics(), fp, cls=NpEncoder, indent=True)

