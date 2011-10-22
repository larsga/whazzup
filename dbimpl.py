
import datetime, hashlib, smtplib, marshal, os
import psycopg2, sysv_ipc
import psycopg2.extensions
import feedlib, vectors
from config import *

try:
    import gdbm
except ImportError:
    import fakegdbm
    gdbm = fakegdbm

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

# ----- UTILITIES

def query_for_value(query, args = ()):
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

# FIXME: ideally this class should be completely generic, so that only
# the backend code differs between implementations. should be doable.
# the question is whether that will necessitate installing another
# layer of indirection, in which case, what's the point?

class Controller(feedlib.Controller):

    def is_single_user(self):
        return False
    
    def vote_received(self, user, id, vote):
        # sending with higher priority so UI can be updated correctly
        mqueue.send("RecordVote %s %s %s" % (user.get_username(), id, vote), 2)
        
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

    def send_user_password(self, username, email, password):
        conn = smtplib.SMTP()
        conn.connect()
        conn.sendmail("whazzup@garshol.priv.no", [email],
"""From: Whazzup <whazzup@garshol.priv.no>
Subject: New password for your Whazzup account

Someone has just requested that your password at Whazzup be reset.
To log in, go to http://whazzup.garshol.priv.no/ and log in with

  username: %s
  password: %s

-- 
Whazzup sysadmin daemon
""" % (username, password))
        conn.quit()

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
        row = cur.fetchone()
        if row:
            return apply(Feed, row)
        else:
            return None

    def get_popular_feeds(self):
        # may consider using sum of ratios for sorting instead of the count
        # of subscribers
        # sum((up + 5) / cast((up + down + 10) as float))
        cur.execute("""select id, title, xmlurl, htmlurl, last_read, max_posts, count(username) as subs
                       from feeds
                       join subscriptions on id = feed
                       group by id, title, last_read, xmlurl, htmlurl, max_posts
                       order by subs desc limit 50""")
        return [Feed(feedid, title, xmlurl, htmlurl, None, None, lastread, None,
                     maxposts, subs)
                for (feedid, title, xmlurl, htmlurl, lastread, maxposts, subs)
                in cur.fetchall()]

    def get_user_count(self):
        return query_for_value("select count(*) from users")

    def get_feed_count(self):
        return query_for_value("select count(*) from feeds")

    def get_post_count(self):
        return query_for_value("select count(*) from posts")

    def get_subscription_count(self):
        return query_for_value("select count(*) from subscriptions")

    def get_rated_posts_count(self):
        return query_for_value("select count(*) from rated_posts")

    def get_read_posts_count(self):
        return query_for_value("select count(*) from read_posts")

    def get_notification_count(self):
        return query_for_value("select count(*) from notify")
    
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
        if self._title is None:
            self._title = ""
        elif len(self._title) > 100:
            self._title = self._title[ : 100] # we just truncate


        if self._error and len(self._error) > 100:
            self._error = self._error[ : 100]
            
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
            return str(self._date)
        else:
            return None

    def get_date(self):
        return self._date

    def get_site(self):
        return self._feed
    
    def save(self):
        if self._id:
            raise NotImplementedError()
        if not self._link:
            # we can't save this post, so we are skipping it entirely. will
            # need to consider what to do about this one. FIXME
            return

        if not self._title:
            self._title = "[No title]" # this avoids crashes
        if len(self._title) > 200:
            self._title = self._title[ : 200]
        if len(self._link) > 400:
            print "Link:", len(self._link)
        if self._author and len(self._author) > 100:
            self._author = self._author[ : 100]

        cur.execute("""
        insert into posts values (default, %s, %s, %s, %s, %s, %s)
       """, (self._title, self._link, self._descr, self.get_date(),
              self._author, self._feed.get_local_id()))
        conn.commit()

    def delete(self):
        update("delete from read_posts where post = %s", (self._id, ))
        update("delete from rated_posts where post = %s", (self._id, ))
        update("delete from posts where id = %s", (self._id, ))
        conn.commit()

        filename = os.path.join(VECTOR_CACHE_DIR, str(self.get_local_id()))
        try:
            os.unlink(filename)
        except OSError, e:
            if e.errno != 2:
                raise e

    def is_seen(self, user):
        return query_for_value("""select post from read_posts
                                  where username = %s and post = %s""",
                               (user.get_username(), self._id))

    def get_vector(self):
        filename = os.path.join(VECTOR_CACHE_DIR, str(self.get_local_id()))
        try:            
            inf = open(filename, "r")
            vector = vectors.Vector(marshal.load(inf))
            inf.close()
        except IOError, e:
            if e.errno != 2:
                raise e
            
            vector = feedlib.Post.get_vector(self)
            outf = open(filename, "w")
            marshal.dump(vector.get_map(), outf)
            outf.close()
            
        return vector
    
class Subscription(feedlib.Subscription):

    def __init__(self, feed, user, up = None, down = None):
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
        if self._up is None:
            self._load_counts()
        if self._up + self._down:
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
        if self._up is None:
            self._load_counts()
            if self._up is None: # means loading failed (ie: no such sub)
                # FIXME: not happy with this. it's just a workaround, hiding
                # the real issue. have no better ideas right now, though.
                return # there's nothing for us to do

        if vote == "up":
            self._up += 1
        else:
            self._down += 1

        update("""update subscriptions set up = %s, down = %s
                  where username = %s and feed = %s""",
               (self._up, self._down, self._user.get_username(),
                int(self._feed.get_local_id())))
        conn.commit()

    def _load_counts(self):
        cur.execute("""select up, down from subscriptions
                       where username = %s and feed = %s""",
                    (self._user.get_username(),
                     int(self._feed.get_local_id())))
        row = cur.fetchone()
        if row:
            (self._up, self._down) = row

class RatedPost(feedlib.RatedPost):

    def __init__(self, user, post, subscription, points = None, prob = None):
        feedlib.RatedPost.__init__(self, user, post, subscription, points, prob)
        self._exists_in_db = (points is not None) # FIXME: dubious
        
    def seen(self):
        # first remove the rating
        update("delete from rated_posts where username = %s and post = %s",
               (self._user.get_username(), int(self._post.get_local_id())))

        # then make a note that we've read it
        try:
            update("insert into read_posts values (%s, %s, %s)",
                   (self._user.get_username(), int(self._post.get_local_id()),
                    int(self._subscription.get_feed().get_local_id())))
        except psycopg2.IntegrityError, e:
            # most likely the button got pressed twice. we output a warning
            # and carry on.
            print str(e)

        conn.commit()

    def age(self):
        self._points = feedlib.calculate_points(self.get_overall_probability(),
                                                self._post.get_date())
        self.save()
        
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

    def get_update_tuple(self):
        return (self._points, self._prob, self._user.get_username(),
                int(self._post.get_local_id()))

    def get_insert_tuple(self):
        return (self._user.get_username(),
                int(self._post.get_local_id()),
                int(self.get_subscription().get_feed().get_local_id()),
                self._points, self._prob)

def save_batch(objects):
    """CLASS METHOD! Takes a list of RatedPost objects and writes
    them to the database in a single batch SQL operation."""

    insertbatch = []
    updatebatch = []
    # FIXME: we can probably make the flat list here directly with
    # list comprehensions. skipping that for now.
    for rating in objects:
        if rating._exists_in_db:
            updatebatch.append(rating.get_update_tuple())
        else:
            insertbatch.append(rating.get_insert_tuple())

    if insertbatch:
        query = "insert into rated_posts values %s"
        values = ", ".join(["(%s, %s, %s, %s, now(), %s)"] *
                            len(insertbatch))
        query = query % values

        insertvalues = [item for row in insertbatch for item in row]
        cur.execute(query, insertvalues)

    if updatebatch:
        query = """update rated_posts
                   set points = i.column1, last_recalc = now(),
                       prob = i.column2
                   from (values %s) as i
                  where username = i.column3 and post = i.column4"""

        values = ", ".join(["(%s, %s, %s, %s)"] * len(updatebatch))
        query = query % values

        updatevalues = [item for row in updatebatch for item in row]
        cur.execute(query, updatevalues)

# ----- WORD DATABASE

# FIXME: duplicated from diskimpl.py. should unify somehow.
class WordDatabase(feedlib.WordDatabase):

    def __init__(self, filename, readonly):
        if readonly:
            # opens the database in readonly mode so that we don't get
            # conflicts between different processes accessing at the same
            # time. only the dbqueue can modify the database.
            self._dbm = gdbm.open(filename, 'ru')
        else:
            self._dbm = gdbm.open(filename, 'cf')
        feedlib.WordDatabase.__init__(self, self._dbm)

    def _get_object(self, key):
        key = key.encode("utf-8")
        if self._words.has_key(key):
            (good, bad) = self._words[key].split(",")
            return (int(good), int(bad))
        else:
            return (0, 0)

    def _put_object(self, key, ratio):
        key = key.encode("utf-8")
        self._words[key] = ("%s,%s" % ratio)

    def close(self):
        self._dbm.close()
        
# ----- FAKING USER ACCOUNTS

def crypt(password):
    return hashlib.md5(password).hexdigest()

class UserDatabase:

    def get_current_user(self):
        if hasattr(self._session, "username") and self._session.username:
            return User(self._session.username, readonly = True)
        else:
            return None

    def create_login_url(self, path):
        return "/login"

    def accounts_available(self):
        accounts = query_for_value("select count(*) from users", ())
        return accounts < MAX_USERS

    def set_session(self, session):
        self._session = session

    def user_exists(self, username):
        return query_for_value("""select username from users where
                                    username = %s""", (username, ))

    def verify_credentials(self, username, password):
        passhash = crypt(password)
        return query_for_value("""select username from users where
                                    username = %s and password = %s""",
                               (username, passhash))

    def create_user(self, username, password, email):
        passhash = crypt(password)
        update("insert into users values (%s, %s, %s)",
               (username, passhash, email))
        conn.commit()

    def find_user(self, email):
        return query_for_value("select username from users where email = %s",
                               (email, ))

    def set_password(self, username, password):
        passhash = crypt(password)
        update("update users set password = %s where username = %s",
               (passhash, username))
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
            self._worddb = WordDatabase(DBM_DIR + self._username + ".dbm", self._readonly)
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

    def send(self, msg, priority = 1):
        if self._internal_queue:
            pos = 0
            for (oldmsg, priority) in self._internal_queue:
                try:
                    print "Sending from internal queue", repr(msg)
                    self._send(oldmsg, priority)
                    pos += 1
                except sysv_ipc.BusyError:
                    print "Ooops, busy"
                    break

            if pos:
                self._internal_queue = self._internal_queue[pos : ]
                print "Truncated queue", self._internal_queue
        
        try:
            self._send(msg, priority)
        except sysv_ipc.BusyError:
            print "Queue full, holding message", repr(msg)
            self._internal_queue.append((msg, priority))

    def _send(self, msg, msgtype):
        try:
            self._mqueue.send(msg, False, msgtype)
        except sysv_ipc.ExistentialError:
            # this could be either because dbqueue was restarted (new instance
            # of queue) or because dbqueue is not running at all. we reopen
            # the queue and try again once.
            self._mqueue = sysv_ipc.MessageQueue(QUEUE_NUMBER)

            # we retry. if it still fails we know dbqueue is not running and
            # there's no point in continuing to try.
            self._mqueue.send(msg, False)

    def remove(self):
        self._mqueue.remove()

# ----- SET UP
        
users = UserDatabase()
controller = Controller()
feeddb = FeedDatabase()
feedlib.feeddb = feeddb # let's call it dependency injection, so it's cool

conn = psycopg2.connect(DB_CONNECT_STRING)
cur = conn.cursor()
mqueue = SendingMessageQueue()
