import json
import os
from collections import Counter
from os.path import join

from scipy import median, mean, amin, amax, std, percentile

SESSIONS_PATH = 'sessions'


def is_empty(session): 
    return not (session['liked'] or session['disliked'] or session['unknown'])


def get_sessions(filter_empty=True): 
    sessions = [] 
    for session in os.listdir(SESSIONS_PATH): 
        with open(join(SESSIONS_PATH, session)) as fp:
            sess = json.load(fp)
            if filter_empty: 
                if is_empty(sess): 
                    continue

            sessions.append(sess)

    return sessions


def get_durations(sessions): 
    durations = []
    for session in sessions:
        timestamps = session['timestamps'] 
        durations.append(timestamps[-1] - timestamps[0])

    return durations


def get_unique_tokens(filter_final=False, filter_empty=False): 
    if filter_final or filter_empty: 
        tokens = [] 
        for session_id in os.listdir(SESSIONS_PATH): 
            with open(join(SESSIONS_PATH, session_id)) as fp:
                session = json.load(fp)
                if filter_empty: 
                    if is_empty(session): 
                        continue 
                if filter_final: 
                    if not session['final']: 
                        continue
                
                tokens.append(session_id)
        
        return set([token.split('+')[0] for token in tokens])

    return set([token.split('+')[0] for token in os.listdir(SESSIONS_PATH)])


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
    durations = get_durations(sessions)
    _min = amin(durations)
    _max = amax(durations)
    _mean = mean(durations)
    _median = median(durations)
    _std = std(durations)
    _q75 = percentile(durations, 75)
    _q90 = percentile(durations, 90)
    _q95 = percentile(durations, 95),
    _q99 = percentile(durations, 99)

    return {
        'min' : _min, 
        'max' : _max, 
        'avg' : _mean,
        'median' : _median, 
        'std' : _std,
        'q75' : _q75,
        'q90' : _q90,
        'q95' : _q95, 
        'q99' : _q99
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
        key: {
            'min': amin(lst),
            'max': amax(lst),
            'avg': mean(lst),
            'median': median(lst),
            'std': std(lst),
            'q75' : percentile(lst, 75),
            'q90' : percentile(lst, 90),
            'q95' : percentile(lst, 95),
            'q99' : percentile(lst, 99),
        } for key, lst in {
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
        category: [uri for uri, _ in Counter(items).most_common(5)]

        for category, items in category_items.items()
    }


def compute_statistics():
    unique_tokens_not_empty = get_unique_tokens(filter_empty=True)
    unique_tokens_final = get_unique_tokens(filter_final=True)
    
    sessions = get_sessions(filter_empty=True)
    completed_sessions = [session for session in sessions if session['final']]

    statistics = {
        key: {
            'n_users': len(unique_tokens_not_empty if key == 'all' else unique_tokens_final),
            'n_likes': len(get_likes(session_set)),
            'n_dislikes': len(get_dislikes(session_set)),
            'n_unknown': len(get_unknowns(session_set)),
            'durations': get_duration_statistics(session_set),
            'feedback': get_feedback_statistics(session_set),
            'top': get_top_entities(session_set)
        }

        for key, session_set in {'all': sessions, 'completed': completed_sessions}.items()
    }

    return statistics


if __name__ == '__main__': 
    with open('statistics.json', 'w+') as fp: 
        json.dump(compute_statistics(), fp)

