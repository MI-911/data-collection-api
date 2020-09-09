# Misc. constants
LIKED = 'liked'
DISLIKED = 'disliked'
UNKNOWN = 'unknown'
TIMESTAMPS = 'timestamps'
FINAL = 'final'
VERSION = 'version'
POPULARITY = 'popularity_sampled'
SESSION_PATH = 'sessions'

# How many questions to ask the user before showing predictions
MIN_QUESTIONS = 30

# How many movies they must answer before random entities are shown
MINIMUM_SEED_SIZE = 5

# How many questions are shown per page before predictions
N_QUESTIONS = 9

# How many entities are shown per group (like, dislike, random)
N_ENTITIES = N_QUESTIONS // 3

# How many entities are shown per recommendation group (like, dislike)
LAST_N_QUESTIONS = 6

# How many entities predicted per group (like, dislike). Remaining slots are "filled" with random entities
LAST_N_REC_QUESTIONS = 3

# All sessions are saved with their current session
# As of september 2020, versioning is by month and year
CURRENT_VERSION = 'september-2020'
