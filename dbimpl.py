
# TODO

# - figure out how to deal with locking issue with dbm
# - make record_vote send a message to queue
# - handle dbqueue crash by reopening queue
# - up/down scores on Subscriptions not set correctly
#   (caused by how it's loaded)
# - commit changes made on server

# - extensive test
#   - weeding out of nits etc
# - deploy

import datetime, dbm, hashlib
import psycopg2, sysv_ipc
import psycopg2.extensions
import feedlib

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

# ----- CONSTANTS

ACCOUNT_LIMIT = 10
QUEUE_NUMBER = 6323

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

    def is_single_user(self):
        return False
    
    def vote_received(self, user, id, vote):
        link = user.get_rated_post_by_id(id)
        link.seen()
        if vote != "read":            
            link.record_vote(vote)
            link.get_subscription().record_vote(vote)
            self.recalculate_all_posts(user) # since scores have changed
    
    def add_feed(self, url, user):
        feed = feeddb.add_feed(url)
        user.subscribe(feed)

        # make it all permanent
        conn.commit()

        # tell queue worker to check this feed
        mqueue.send("CheckFeed %s" % feed.get_local_id())

    def recalculate_all_posts(self, user):
        mqueue.send("RecalculateAllPosts %s" % user.get_username())

    def unsubscribe(self, feedid, user):
        sub = user.get_subscription(feedid)
        sub.unsubscribe()

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

        return Feed(feedid, None, url, None, None, None, None, None, None)

    def get_item_by_id(self, itemid):
        itemid = int(itemid)
        cur.execute("select * from posts where id = %s", (itemid, ))
        row = cur.fetchone()
        if not row:
            return None
        (id, title, link, descr, date, author, feedid) = row
        return Item(id, title, link, descr, date, author,
                    self.get_feed_by_id(feedid))
    
    def get_feed_by_id(self, id):
        cur.execute("select * from feeds where id = %s", (int(id), ))
        return apply(Feed, cur.fetchone())

    def get_popular_feeds(self):
        cur.execute("""select id, title, last_read, count(username) as subs
                       from feeds
                       join subscriptions on id = feed
                       group by id, title, last_read
                       order by subs desc limit 50""")
        return [Feed(feedid, title, None, None, None, None, lastread, None,
                     None, subs)
                for (feedid, title, lastread, subs) in cur.fetchall()]
    
class Feed(feedlib.Feed):

    def __init__(self, id, title, xmlurl, htmlurl, error, timetowait,
                 lastread, lasterror, maxposts, subs = None):
        self._id = id
        self._title = title
        self._url = xmlurl
        self._link = htmlurl
        self._lastread = lastread
        self._error = error
        self._lasterror = lasterror
        self._time_to_wait = timetowait
        self._maxposts = maxposts
        self._subs = subs

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
        select * from posts where feed = %s order by pubdate desc
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
        if msg:
            self._lasterror = datetime.datetime.now()

    def set_title(self, title):
        self._title = title

    def set_link(self, link):
        self._link = link

    def is_read_now(self):
        self._lastread = datetime.datetime.now()

    def save(self):
        update("""update feeds set title = %s, htmlurl = %s, last_read = %s,
                                   error = %s, last_error = %s, max_posts = %s
                  where id = %s""",
               (self._title, self._link, self._lastread, self._error,
                self._lasterror, self._maxposts, self._id))
        conn.commit()

    def get_max_posts(self):
        return self._maxposts

    def set_max_posts(self, maxposts):
        self._maxposts = maxposts

    def get_subscribers(self):
        return self._subs

    def get_time_to_wait(self):
        "Seconds to wait between each time we poll the feed."
        return self._time_to_wait

    def is_subscribed(self, user):
        return query_for_value("""select username from subscriptions
                                  where username = %s and feed = %s""",
                               (user.get_username(), self._id))
    
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

        if len(self._title) > 200:
            self._title = self._title[ : 200]
        if len(self._link) > 400:
            print "Link:", len(self._link)
        if self._author and len(self._author) > 100:
            self._author = self._author[ : 100]
        
        cur.execute("""
        insert into posts values (default, %s, %s, %s, %s, %s, %s)
        """, (self._title, self._link, self._descr, self.get_pubdate(),
              self._author, self._feed.get_local_id()))
        conn.commit()

    def delete(self):
        update("delete from read_posts where post = %s", (self._id, ))
        update("delete from rated_posts where post = %s", (self._id, ))
        update("delete from posts where id = %s", (self._id, ))
        conn.commit()

    def is_seen(self, user):
        return query_for_value("""select post from read_posts
                                  where username = %s and post = %s""",
                               (user.get_username(), self._id))

class Subscription(feedlib.Subscription):

    def __init__(self, feed, user, up = 0, down = 0):
        self._feed = feed
        self._user = user
        self._up = up
        self._down = down
    
    def get_feed(self):
        return self._feed

    def get_rated_posts(self):
        cur.execute("""select post, points, prob from rated_posts
                    where username = %s and feed = %s""",
                    (self._user.get_username(), int(self._feed.get_local_id())))
        return [RatedPost(self._user,
                          feeddb.get_item_by_id(postid), self, points, prob)
                for (postid, points, prob) in cur.fetchall()]
    
    def get_user(self):
        return self._user

    def get_ratio(self):
        if self._up + self._down:
            print self._up, self._down, self._up + 5 / float(self._up + self._down + 5)
            return (self._up + 5) / float(self._up + self._down + 10)
        else:
            return 0.5

    def unsubscribe(self):
        key = (self._user.get_username(), int(self._feed.get_local_id()))

        update("delete from read_posts where username = %s and feed = %s", key)
        update("delete from rated_posts where username = %s and feed = %s", key)
        update("delete from subscriptions where username = %s and feed = %s", key)
        conn.commit()

    def record_vote(self, vote):
        if vote == "up":
            self._up += 1
        else:
            self._down += 1

        update("""update subscriptions set up = %s, down = %s
                  where username = %s and feed = %s""",
               (self._up, self._down, self._user.get_username(),
                int(self._feed.get_local_id())))
        conn.commit()

class RatedPost(feedlib.RatedPost):

    def __init__(self, user, post, subscription, points = None, prob = None):
        feedlib.RatedPost.__init__(self, user, post, subscription, points, prob)
        self._exists_in_db = (points is not None) # FIXME: dubious
        
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
            update("""update rated_posts set points = %s, last_recalc = now(),
                                             prob = %s
                      where username = %s and post = %s""",
                   (self._points, self._prob, self._user.get_username(),
                    int(self._post.get_local_id())))
        else:
            update("insert into rated_posts values (%s, %s, %s, %s, now(), %s)",
                   (self._user.get_username(),
                    int(self._post.get_local_id()),
                    int(self.get_subscription().get_feed().get_local_id()),
                    self._points, self._prob))
            self._exists_in_db = True
        conn.commit()

    def age(self):
        self._points = feedlib.calculate_points(self.get_overall_probability(),
                                                self._post.get_date())
        self.save()

# ----- WORD DATABASE

# FIXME: duplicated from diskimpl.py. should unify somehow.
class WordDatabase(feedlib.WordDatabase):

    def __init__(self, filename, readonly = False):
        if readonly:
            # opens the database in readonly mode so that we don't get
            # conflicts between different processes accessing at the same
            # time. only the dbqueue can modify the database.
            self._dbm = dbm.open(filename) 
        else:
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
        
# ----- USER ACCOUNTS

class UserDatabase:

    def get_current_user(self):
        if hasattr(self._session, "username") and self._session.username:
            return User(self._session.username)
        else:
            return None

    def create_login_url(self, path):
        return "/login"

    def accounts_available(self):
        accounts = query_for_value("select count(*) from users", ())
        return accounts < ACCOUNT_LIMIT

    def set_session(self, session):
        self._session = session

    def verify_credentials(self, username, password):
        passhash = hashlib.md5(password).hexdigest()
        return query_for_value("""select username from users where
                                    username = %s and password = %s""",
                               (username, passhash))

    def create_user(self, username, password, email):
        passhash = hashlib.md5(password).hexdigest()
        update("insert into users values (%s, %s, %s)",
               (username, passhash, email))
        conn.commit()

# ----- USER OBJECT

class User(feedlib.User):

    def __init__(self, username, readonly = False):
        self._username = username
        self._readonly = readonly # applies to dbm file
        self._worddb = None

    def get_username(self):
        return self._username

    def get_feeds(self):
        cur.execute("""
          select id, title, xmlurl, htmlurl, error, time_to_wait, last_read,
                 last_error, max_posts, up, down
          from feeds
          join subscriptions on id = feed
          where username = %s
        """, (self._username, ))
        subs = [Subscription(Feed(id, title, xmlurl, htmlurl, error,
                                  time_to_wait, last_read, last_error,
                                  maxposts),
                             self, up, down)
                for (id, title, xmlurl, htmlurl, error, time_to_wait,
                     last_read, last_error, maxposts, up, down)
                in cur.fetchall()]
        subs = feedlib.sort(subs, Subscription.get_ratio)
        subs.reverse()
        return subs

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
        return [Item(id, title, link, descr, date, author,
                     feeddb.get_feed_by_id(feed)) for
                (id, title, link, descr, date, author, feed) in
                cur.fetchall()]

    def get_rated_post_by_id(self, itemid):
        item = feeddb.get_item_by_id(itemid)
        sub = Subscription(item.get_site(), self)
        return RatedPost(self, item, sub)

    def get_vote_stats(self):
        return (0, 0) # FIXME
    
    def _get_word_db(self):
        if not self._worddb:
            self._worddb = WordDatabase(self._username + ".dbm",
                                        self._readonly)
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

    def get_subscription(self, feedid):
        feed = feeddb.get_feed_by_id(feedid)
        return Subscription(feed, self)
            
# ----- SENDING MESSAGE QUEUE

class SendingMessageQueue:

    def __init__(self):
        # create queue, and fail if it does not already exist
        self._mqueue = sysv_ipc.MessageQueue(QUEUE_NUMBER)
        # internal queue for holding messages which can't be sent because
        # the message queue was full
        self._internal_queue = []

    def send(self, msg):
        if self._internal_queue:
            pos = 0
            for oldmsg in self._internal_queue:
                try:
                    print "Sending from internal queue", repr(msg)
                    self._mqueue.send(oldmsg, False)
                    pos += 1
                except sysv_ipc.BusyError:
                    print "Ooops, busy"
                    break

            self._internal_queue = self._internal_queue[pos : ]
            print "Truncated queue", self._internal_queue
        
        try:
            self._mqueue.send(msg, False)
        except sysv_ipc.BusyError:
            print "Queue full, holding message", repr(msg)
            self._internal_queue.append(msg)

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
