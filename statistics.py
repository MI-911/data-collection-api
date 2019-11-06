import json 
import os 
from scipy import median, mean, sum, min, max, std
from os.path import join 


# {
#  "liked": [
#   "http://www.wikidata.org/entity/Q212689",
#   "http://www.wikidata.org/entity/Q107226",
#   "http://www.wikidata.org/entity/Q133654",
#   "http://www.wikidata.org/entity/Q906343",
#   "http://www.wikidata.org/entity/Q645928",
#   "http://www.wikidata.org/entity/Q157443",
#   "http://www.wikidata.org/entity/Q44512",
#   "http://www.wikidata.org/entity/Q205707",
#   "http://www.wikidata.org/entity/Q1054574",
#   "http://www.wikidata.org/entity/Q860626",
#   "http://www.wikidata.org/entity/Q15270647",
#   "http://www.wikidata.org/entity/Q190050"
#  ],
#  "disliked": [
#   "http://www.wikidata.org/entity/Q116845",
#   "http://www.wikidata.org/entity/Q17042878",
#   "http://www.wikidata.org/entity/Q220751",
#   "Decade-1960",
#   "http://www.wikidata.org/entity/Q106182",
#   "http://www.wikidata.org/entity/Q219776",
#   "http://www.wikidata.org/entity/Q4985891"
#  ],
#  "unknown": [
#   "http://www.wikidata.org/entity/Q266209",
#   "http://www.wikidata.org/entity/Q3598715",
#   "http://www.wikidata.org/entity/Q1124875",
#   "http://www.wikidata.org/entity/Q431793",
#   "http://www.wikidata.org/entity/Q25139",
#   "http://www.wikidata.org/entity/Q34975",
#   "http://www.wikidata.org/entity/Q318292",
#   "http://www.wikidata.org/entity/Q497155",
#   "http://www.wikidata.org/entity/Q14949752",
#   "http://www.wikidata.org/entity/Q1632158",
#   "http://www.wikidata.org/entity/Q570004",
#   "http://www.wikidata.org/entity/Q27513",
#   "http://www.wikidata.org/entity/Q18407",
#   "http://www.wikidata.org/entity/Q742195",
#   "http://www.wikidata.org/entity/Q165525",
#   "http://www.wikidata.org/entity/Q666017",
#   "http://www.wikidata.org/entity/Q201819",
#   "http://www.wikidata.org/entity/Q617249",
#   "http://www.wikidata.org/entity/Q497850"
#  ],
#  "timestamps": [
#   1573038609.7744405,
#   1573038620.8507001,
#   1573038634.895753,
#   1573038655.0620732,
#   1573038662.5234277,
#   1573038664.753096,
#   1573038665.946394,
#   1573038666.7932694,
#   1573038668.4168873,
#   1573038669.0624812,
#   1573038669.5386686,
#   1573038669.8322973,
#   1573038670.6365888,
#   1573038670.7771683
#  ],
#  "final": true
# }

SESSIONS_PATH = 'sessions'

def is_empty(session): 
    return not (session['liked'] or session['disliked'] or session['unknown'])


def get_sessions(filter_empty=True): 
    sessions = [] 
    for session in os.listdir(SESSIONS_PATH): 
        with open(session) as fp: 
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
            with open(session_id) as fp: 
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
    _min = min(durations)
    _max = max(durations)
    _mean = mean(durations)
    _median = median(durations)
    _std = std(durations)

    return {
        'min' : _min, 
        'max' : _max, 
        'avg' : _mean,
        'median' : _median, 
        'std' : _std
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

        like_to_dislike_ratios.append(_likes / _dislikes)

    return {
        key : {
            'min' : min(lst),
            'max' : max(lst), 
            'avg' : mean(lst),
            'median' : median(lst),
            'std' : std(lst)
        } for key, lst in {
            'likes' : likes, 
            'dislikes' : dislikes,
            'unknonws' : unknowns,
            'like_to_dislike_ratios' : like_to_dislike_ratios
        }
    }


if __name__ == '__main__': 
    unique_tokens_all = get_unique_tokens()
    unique_tokens_not_empty = get_unique_tokens(filter_empty=True)
    unique_tokens_final = get_unique_tokens(filter_final=True)
    
    sessions = get_sessions(filter_empty=True)
    completed_sessions = [session for session in sessions if session['final']]

    statistics = {
        key : {
            'n_sessions' : len(unique_tokens_all if key == 'all' else unique_tokens_final),
            'n_likes' : len(get_likes(session_set)),
            'n_dislikes' : len(get_dislikes(session_set)),
            'n_unknown' : len(get_unknowns(session_set)),

            'durations' : get_duration_statistics(session_set),
            'feedback' : get_feedback_statistics(session_set) 
        }

        for key, session_set in { 'all' : sessions, 'completed' : completed_sessions }.items()
    }

    with open('statistics.json', 'w+') as fp: 
        json.dump(statistics, fp)

