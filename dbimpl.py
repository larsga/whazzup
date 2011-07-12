
import datetime
import psycopg2, sysv_ipc
import feedlib

# ----- UTILITIES

def query_for_value(query, args):
    cur.execute(query, args)
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        return None

def update(query, args):
    cur.execute(query, args)

# ----- THE ACTUAL LOGIC

class Controller(feedlib.Controller):

    def add_feed(self, url):
        username = "larsga" # FIXME FIXME FIXME
        
        # does the feed exist already?
        feedid = query_for_value("""
        select id from feeds where xmlurl = %s
        """, (url, ))

        # if not, add it now
        if not feedid:
            feedid = query_for_value("""
            insert into feeds (xmlurl, time_to_wait) values (%s, 10800)
              returning id
            """, (url, ))
        
        # if user is not already subscribed, add subscription
        if not query_for_value("""select * from subscriptions where
                            feed = %s and username = %s""", (feedid, username)):
            update("insert into subscriptions values (%s, %s)",
                   (feedid, username))

        # make it all permanent
        conn.commit()

        # tell queue worker to check this feed
        mqueue.send("CheckFeed %s" % feedid)

class FeedDatabase(feedlib.Database):

    def __init__(self, username):
        "Creates database of the user's subscriptions."
        self._username = username

    def get_feeds(self):
        cur.execute("""
          select feed from subscriptions where username = %s
        """, (self._username, ))
        return [Feed(id, None, None, None, None, None, None, None)
                for (id) in cur.fetchall()]

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

def load_feed(id):
    cur.execute("select * from feeds where id = %s", (id, ))
    return apply(Feed, cur.fetchone())
    
class Feed(feedlib.Feed):

    def __init__(self, id, title, xmlurl, htmlurl, error, timetowait,
                 lastread, lasterror):
        self._id = id
        self._title = title
        self._url = xmlurl
        self._link = htmlurl
        self._lastread = lastread
        self._error = error
        self._lasterror = lasterror

    def get_local_id(self):
        return str(self._id)

    def get_title(self):
        return self._title

    def get_url(self):
        return self._url

    def get_link(self):
        return self._link

    def set_error(self, msg):
        self._error = msg
        self._lasterror = datetime.datetime.now()

    def set_title(self, title):
        self._title = title

    def set_link(self, link):
        self._link = link

    def is_read_now(self):
        self._lastread = datetime.datetime.now()

    def save(self):
        update("""update feeds set title = %s, htmlurl = %s, last_read = %s,
                                   last_error = %s
                  where id = %s""",
               (self._title, self._link, self._lastread, self._lasterror,
                self._id))
        conn.commit()
    
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

# ----- SENDING MESSAGE QUEUE

class SendingMessageQueue:

    def __init__(self):
        # create queue, and fail if it does not already exist
        self._mqueue = sysv_ipc.MessageQueue(7321)

    def send(self, msg):
        # FIXME: we may have to queue messages here internally,
        # because the queue size is so absurdly small. we have 2k on
        # MacOS and 16k on Linux. not sure if this is going to be
        # enough.
        self._mqueue.send(msg)

    def remove(self):
        self._mqueue.remove()

# ----- SET UP

conn = psycopg2.connect("dbname=whazzup")
cur = conn.cursor()
mqueue = SendingMessageQueue()
