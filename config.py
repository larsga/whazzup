"""
Configuration settings for deployment.
"""

# where session data is stored on disk
SESSION_DIR = 'sessions'

# where the word databases (DBMs) are stored
DBM_DIR = 'dbms/'

# database connection string
DB_CONNECT_STRING = 'dbname=whazzup'

# key number for sysv_ipc message queue
QUEUE_NUMBER = 6332

# where to put queue statistics reports
STATS_DIR = '.'

# where to put the vector cache (marshal files)
VECTOR_CACHE_DIR = '/tmp'

# the maximum number of users we allow
MAX_USERS = 12

# number of feed downloading threads
DOWNLOAD_THREADS = 5

# temporary storage for downloaded feeds
FEED_CACHE = '/tmp'
