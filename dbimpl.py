
import datetime, dbm
import psycopg2, sysv_ipc
import psycopg2.extensions
import feedlib

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

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

def query_for_set(query, args):
    cur.execute(query, args)
    return set([row[0] for row in cur.fetchall()])

def query_for_list(query, args):
    cur.execute(query, args)
    return [row[0] for row in cur.fetchall()]

# ----- THE ACTUAL LOGIC

# FIXME: ideally this class should be completely generic, so that only the
# backend code differs between implementations. should be doable.

class Controller(feedlib.Controller):

    def vote_received(self, user, id, vote):
        link = user.get_rated_post_by_id(id)
        link.seen()
        if vote != "read":
            link.record_vote(vote)
            self.recalculate_all_posts(user) # since scores have changed
    
    def add_feed(self, url, user):
        feed = feeddb.add_feed(url)
        user.subscribe(feed)

        # make it all permanent
        conn.commit()

        # tell queue worker to check this feed
        mqueue.send("CheckFeed %s" % feed.get_local_id())

    def recalculate_all_posts(self, user):
        allsubs = query_for_list("""select feed from subscriptions
                                    where username = %s""",
                                 (user.get_username(), ))
        for feedid in allsubs:
            mqueue.send("RecalculateSubscription %s %s" %
                        (feedid, user.get_username()))

class FeedDatabase(feedlib.Database):

    def add_feed(self, url):
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

        return Feed(feedid, None, url, None, None, None, None, None)

    def get_item_by_id(self, itemid):
        itemid = int(itemid)
        cur.execute("select * from posts where id = %s", (itemid, ))
        row = cur.fetchone()
        if not row:
            return None
        (id, title, link, descr, date, author, feedid) = row
        return Item(id, title, link, descr, date, author, load_feed(feedid))
    
def load_feed(id): # FIXME: obviously belongs in feed db
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
        return self._title or "[No title]"

    def get_url(self):
        return self._url

    def get_link(self):
        return self._link

    def get_items(self):
        cur.execute("""
        select * from posts where feed = %s
        """, (self._id, ))
        return [Item(id, title, link, descr, pubdate, author, self) for
                (id, title, link, descr, pubdate, author, feed) in
                cur.fetchall()]

    def get_item_count(self):
        return query_for_value("select count(*) from posts where feed = %s",
                               (self._id, ))

    def get_error(self):
        return self._error

    def time_since_last_read(self):
        "Seconds since last read."
        if self._lastread:
            return (feedlib.toseconds(datetime.datetime.now()) -
                    feedlib.toseconds(self._lastread))
        else:
            return 0
    
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

    def get_ratio(self):
        return "# FIXME" # FIXME

    def get_unread_count(self):
        return "# FIXME" # FIXME

    def is_being_read(self):
        return "# FIXME" # FIXME
    
class Item(feedlib.Post):

    def __init__(self, id, title, link, descr, date, author, feed):
        self._id = id
        self._title = title
        self._link = link
        self._descr = descr
        self._date = date # date object
        self._author = author
        self._feed = feed
        self._pubdate = None

    def get_local_id(self):
        return str(self._id)

    def get_title(self):
        return self._title

    def get_link(self):
        return self._link

    def get_guid(self):
        return self._link # FIXME

    def get_description(self):
        return self._descr
    
    def get_author(self):
        return self._author

    def get_pubdate(self):
        if self._date:
            return str(self.get_date())
        else:
            return None

    def get_site(self):
        return self._feed
    
    def save(self):
        if self._id:
            raise NotImplementedError()

        cur.execute("""
        insert into posts values (default, %s, %s, %s, %s, %s, %s)
        """, (self._title, self._link, self._descr, self.get_pubdate(),
              self._author, self._feed.get_local_id()))
        conn.commit()

class Subscription(feedlib.Subscription):

    def __init__(self, feed, user):
        self._feed = feed
        self._user = user
    
    def get_feed(self):
        return self._feed

    def get_user(self):
        return self._user

    def get_ratio(self):
        return 0.5 # FIXME
        
class RatedPost(feedlib.RatedPost):

    def __init__(self, user, post, subscription, points = None):
        feedlib.RatedPost.__init__(self, user, post, subscription, points)
        self._points = points
        self._exists_in_db = (points is not None) # FIXME: dubious

    def is_seen(self):
        return False # FIXME
        
    def seen(self):
        # first remove the rating
        update("delete from rated_posts where username = %s and post = %s",
               (self._user.get_username(), int(self._post.get_local_id())))

        # then make a note that we've read it
        update("insert into read_posts values (%s, %s, %s)",
               (self._user.get_username(), int(self._post.get_local_id()),
                int(self._subscription.get_feed().get_local_id())))
        conn.commit()
        
    def save(self):
        if self._exists_in_db:
            update("""update rated_posts set points = %s, last_recalc = now()
                      where username = %s and post = %s""",
                   (self._points, self._user.get_username(),
                    int(self._post.get_local_id())))
        else:
            update("insert into rated_posts values (%s, %s, %s, %s, now())",
                   (self._user.get_username(),
                    int(self._post.get_local_id()),
                    int(self.get_subscription().get_feed().get_local_id()),
                    self._points))
            self._exists_in_db = True
        conn.commit()

# ----- WORD DATABASE

# FIXME: duplicated from diskimpl.py. should unify somehow.
class WordDatabase(feedlib.WordDatabase):

    def __init__(self, filename):
        self._dbm = dbm.open(filename, 'c')
        feedlib.WordDatabase.__init__(self, self._dbm)

    def _get_object(self, key):
        key = key.encode("utf-8")
        (good, bad) = self._words.get(key, "0,0").split(",")
        return (int(good), int(bad))

    def _put_object(self, key, ratio):
        key = key.encode("utf-8")
        self._words[key] = ("%s,%s" % ratio)

    def close(self):
        self._dbm.close()
        
# ----- FAKING USER ACCOUNTS

class UserDatabase:

    def get_current_user(self):
        return User("larsga") # FIXME

# ----- USER OBJECT

class User(feedlib.User):

    def __init__(self, username):
        self._username = username
        self._worddb = None

    def get_username(self):
        return self._username

    def get_feeds(self):
        # FIXME: sort by score
        cur.execute("""
          select id, title, xmlurl, htmlurl, error, time_to_wait, last_read,
                 last_error
          from feeds
          join subscriptions on id = feed
          where username = %s
        """, (self._username, ))
        return [Feed(id, title, xmlurl, htmlurl, error, time_to_wait,
                     last_read, last_error)
                for (id, title, xmlurl, htmlurl, error, time_to_wait,
                     last_read, last_error)
                in cur.fetchall()]

    def get_item_count(self):
        return query_for_value("""
               select count(*) from rated_posts where username = %s
               """, [self._username])

    def get_item_range(self, low, high):
        # FIXME: this is both performance-critical and slow...
        cur.execute("""
          select id, title, link, descr, pubdate, author, p.feed
          from posts p
          join rated_posts on id = post
          where username = %s
          order by points desc limit %s offset %s
        """, (self._username, (high - low), low))
        return [Item(id, title, link, descr, date, author, load_feed(feed)) for
                (id, title, link, descr, date, author, feed) in
                cur.fetchall()]

    def get_rated_post_by_id(self, itemid):
        item = feeddb.get_item_by_id(itemid)
        sub = Subscription(item.get_site(), self._username)
        return RatedPost(self, item, sub)

    def get_vote_stats(self):
        return (0, 0) # FIXME

    def get_no_of_item(self, item):
        return 0 # FIXME
    
    def _get_word_db(self):
        if not self._worddb:
            self._worddb = WordDatabase(self._username + ".dbm")
        return self._worddb

    def _get_author_db(self):
        return self._get_word_db()

    def _get_site_db(self):
        return self._get_word_db()

    def commit(self):
        """A desperate attempt to preserve DB changes even in the face of
        crashes."""
        self._worddb.close()
        self._worddb = None

    def subscribe(self, feed):
        key = (int(feed.get_local_id()), self._username)
        
        # if user is not already subscribed, add subscription
        if not query_for_value("""select * from subscriptions where
                            feed = %s and username = %s""", key):
            update("insert into subscriptions values (%s, %s)", key)
    
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
        
users = UserDatabase()
controller = Controller()
feeddb = FeedDatabase()
feedlib.feeddb = feeddb # let's call it dependency injection, so it's cool

conn = psycopg2.connect("dbname=whazzup")
cur = conn.cursor()
mqueue = SendingMessageQueue()
