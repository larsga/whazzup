
import feedlib
import psycopg2

# ----- UTILITIES

def query_for_value(query, args):
    cur.execute(query, args)
    return cur.fetchone()[0]

# ----- THE ACTUAL LOGIC

class Controller(feedlib.Controller):
    pass

class FeedDatabase(feedlib.Database):

    def __init__(self, username):
        "Creates database of the user's subscriptions."
        self._username = username

    def get_feeds(self):
        cur.execute("""
          select feed from subscriptions where username = %s
        """, (self._username, ))
        return [Feed(id) for (id) in cur.fetchall()]

    def get_item_count(self):
        return query_for_value("""
               select count(*) from rated_posts where username = %s
               """, [self._username])

    def get_item_range(self, low, high):
        cur.execute("""
          select post from rated_posts where username = %s
          order by points desc limit %s offset %s
        """, (self._username, (high - low), low))
        return [Item(id) for (id) in cur.fetchall()]

    def get_vote_stats(self):
        return (0, 0)
    

class Feed(feedlib.Feed):

    def __init__(self, id):
        self._id = id

    def get_local_id(self):
        return str(self._id)
    
class Item(feedlib.Post):

    def __init__(self, id):
        self._id = id

    def get_local_id(self):
        return str(self._id)
    
# ----- FAKING USER ACCOUNTS

class UserDatabase:

    def get_current_user(self):
        return "[fake user]"

users = UserDatabase()
controller = Controller()
feeddb = FeedDatabase("larsga")

# ----- CONNECT TO DB

conn = psycopg2.connect("dbname=whazzup")
cur = conn.cursor()
