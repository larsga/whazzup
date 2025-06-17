
import datetime, hashlib, smtplib, marshal, os, binascii, sys, operator
import psycopg2, sysv_ipc
import psycopg2.extensions
import cpool
import feedlib, vectors
from config import *

try:
    import gdbm
except ImportError:
    import fakegdbm
    gdbm = fakegdbm

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

# ----- MINHASHING

def hash2(str):
    h = 0
    for ch in str:
        h = (255 * h + ord(ch)) & 0xffffffff
    return h

def hash3(str):
    h = 0
    for ch in str:
        h = ((h ^ ord(ch)) * 0x01000193) & 0xffffffff
    return h

def minhash(tokens, hashes = [hash, hash2, hash3, binascii.crc32]):
    '''tokens = list of strings'''
    smallest = [sys.maxint] * len(hashes)
    #best = [None] * len(hashes)
    for token in tokens:
        for ix in range(len(hashes)):
            h = hashes[ix](token.encode('utf-8'))
            if h < smallest[ix]:
                smallest[ix] = h
                #best[ix] = token

    return reduce(operator.xor, smallest)
    #return (smallest, best)

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
        mqueue.send("AddFeed %s %s" % (user, url), 2)

    def recalculate_all_posts(self, user):
        mqueue.send("RecalculateAllPosts %s" % user.get_username())

    def unsubscribe(self, feedid, user):
        # FIXME: move this too into the message queue
        sub = user.get_subscription(feedid)
        sub.unsubscribe()
        # make it all permanent
        dbconn.commit()

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
        feedid = connpool.query_for_value("""
        select id from feeds where xmlurl = %s
        """, (url, ))

        # if not, add it now
        if not feedid:
            feedid = connpool.query_for_value("""
            insert into feeds (xmlurl, time_to_wait) values (%s, 10800)
              returning id
            """, (url, ))

        return Feed(feedid, None, url, None, None, None, None, None, None, None)

    def get_item_by_id(self, itemid):
        itemid = int(itemid)
        row = connpool.query_for_row("select * from posts where id = %s",
                                   (itemid, ))
        if not row:
            return None
        (id, title, link, descr, date, author, feedid, minhash) = row
        return Item(id, title, link, descr, date, author,
                    self.get_feed_by_id(feedid), minhash)

    def get_feed_by_id(self, id):
        row = connpool.query_for_row("select * from feeds where id = %s",
                                   (int(id), ))
        if row:
            (id, title, xmlurl, htmlurl, error, time_to_wait, last_read,
             last_error, max_posts, time_added, last_modified) = row
            return Feed(id, title, xmlurl, htmlurl, error, time_to_wait,
                        last_read, last_error, max_posts, last_modified)
        else:
            return None

    def get_popular_feeds(self):
        # may consider using sum of ratios for sorting instead of the count
        # of subscribers
        # sum((up + 5) / cast((up + down + 10) as float))
        rows = connpool.query_for_rows("""
              select id, title, xmlurl, htmlurl, last_read, max_posts, last_modified, count(username) as subs
                       from feeds
                       join subscriptions on id = feed
                       group by id, title, last_read, xmlurl, htmlurl,
                                max_posts, last_modified
                       order by subs desc limit 50""")
        return [Feed(feedid, title, xmlurl, htmlurl, None, None, lastread, None,
                     maxposts, lastmod, subs)
                for (feedid, title, xmlurl, htmlurl, lastread, maxposts,
                     lastmod, subs)
                in rows]

    def get_user_count(self):
        return connpool.query_for_value("select count(*) from users")

    def get_feed_count(self):
        return connpool.query_for_value("select count(*) from feeds")

    def get_post_count(self):
        return connpool.query_for_value("select count(*) from posts")

    def get_subscription_count(self):
        return connpool.query_for_value("select count(*) from subscriptions")

    def get_rated_posts_count(self):
        return connpool.query_for_value("select count(*) from rated_posts")

    def get_read_posts_count(self):
        return connpool.query_for_value("select count(*) from read_posts")

    def get_notification_count(self):
        return connpool.query_for_value("select count(*) from notify")

class Feed(feedlib.Feed):

    def __init__(self, id, title, xmlurl, htmlurl, error, timetowait,
                 lastread, lasterror, maxposts, lastmod, subs = None):
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
        self._lastmod = lastmod

    def get_local_id(self):
        return str(self._id)

    def get_title(self):
        return self._title or "[No title]"

    def get_url(self):
        return self._url

    def get_link(self):
        return self._link

    def get_items(self):
        rows = connpool.query_for_rows("""
        select * from posts where feed = %s order by pubdate desc
        """, (self._id, ))
        return [Item(id, title, link, descr, pubdate, author, self, mh) for
                (id, title, link, descr, pubdate, author, feed, mh) in
                rows]

    def get_item_count(self):
        return connpool.query_for_value(
            "select count(*) from posts where feed = %s", (self._id, ))

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
        if self._link and len(self._link) > 200:
            self._link = None # there have to be limits...

        dbconn.update("""update feeds set title = %s, htmlurl = %s,
                           last_read = %s, error = %s, last_error = %s,
                           max_posts = %s, last_modified = %s
                  where id = %s""",
               (self._title, self._link, self._lastread, self._error,
                self._lasterror, self._maxposts, self._lastmod, self._id))
        dbconn.commit()

    def get_max_posts(self):
        return self._maxposts

    def set_max_posts(self, maxposts):
        self._maxposts = maxposts

    def get_subscribers(self):
        return self._subs

    def get_time_to_wait(self):
        "Seconds to wait between each time we poll the feed."
        return self._time_to_wait

    def set_last_modified(self, lastmod):
        self._lastmod = lastmod

    def get_last_modified(self):
        return self._lastmod

    def is_subscribed(self, user):
        return connpool.query_for_value("""select username from subscriptions
                                  where username = %s and feed = %s""",
                               (user.get_username(), self._id))

    def delete(self):
        dbconn.update('delete from feeds where id = %s', (self._id, ))
        dbconn.commit()

class Item(feedlib.Post):

    def __init__(self, id, title, link, descr, date, author, feed, minhash):
        self._id = id
        self._title = title
        self._link = link
        self._descr = descr
        self._date = date # date object
        self._author = author
        self._feed = feed
        self._pubdate = None
        self._minhash = minhash

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

    def get_minhash(self):
        return self._minhash

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
            # This link is too long, and there's not a whole lot we can
            # do about it. Solution: just drop the whole post, with a
            # little warning. Can't really do anything else.
            print "Link from feed %s, length %s" % (self._feed.get_local_id(),
                                                    len(self._link))
            return
        if self._author and len(self._author) > 100:
            self._author = self._author[ : 100]

        dbconn.update("""
        insert into posts values (default, %s, %s, %s, %s, %s, %s, NULL)
       """, (self._title, self._link, self._descr, self.get_date(),
              self._author, self._feed.get_local_id()))
        dbconn.commit()

        self._id = connpool.query_for_value("SELECT currval(pg_get_serial_sequence('posts', 'id'))")
        mqueue.send('MinHash %s' % self._id)

    def compute_minhash(self):
        'Also saves the minhash to the database.'
        vector = self.get_vector().get_keys()
        if len(vector) > 5:
            mh = minhash(vector)
            dbconn.update('update posts set minhash = %s where id = %s',
                   (mh, self._id))
            dbconn.commit()

    def delete(self):
        dbconn.update("delete from read_posts where post = %s", (self._id, ))
        dbconn.update("delete from rated_posts where post = %s", (self._id, ))
        dbconn.update("delete from posts where id = %s", (self._id, ))
        dbconn.commit()

        filename = os.path.join(VECTOR_CACHE_DIR, str(self.get_local_id()))
        try:
            os.unlink(filename)
        except OSError, e:
            if e.errno != 2:
                raise e

    def is_seen(self, user):
        return connpool.query_for_value("""select post from read_posts
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
        '''feed = Feed object, user = User object'''
        self._feed = feed
        self._user = user
        self._up = up
        self._down = down

    def get_feed(self):
        return self._feed

    def get_rated_posts(self):
        rows = connpool.query_for_rows("""
                    select post, points, prob from rated_posts
                    where username = %s and feed = %s""",
                    (self._user.get_username(), int(self._feed.get_local_id())))
        return [RatedPost(self._user,
                          feeddb.get_item_by_id(postid), self, points, prob)
                for (postid, points, prob) in rows]

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

        dbconn.update("delete from read_posts where username = %s and feed = %s", key)
        dbconn.update("delete from rated_posts where username = %s and feed = %s", key)
        dbconn.update("delete from subscriptions where username = %s and feed = %s", key)
        dbconn.commit()

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

        dbconn.update("""update subscriptions set up = %s, down = %s
                  where username = %s and feed = %s""",
               (self._up, self._down, self._user.get_username(),
                int(self._feed.get_local_id())))
        dbconn.commit()

    def _load_counts(self):
        row = connpool.query_for_row("""select up, down from subscriptions
                       where username = %s and feed = %s""",
                    (self._user.get_username(),
                     int(self._feed.get_local_id())))
        if row:
            (self._up, self._down) = row

class RatedPost(feedlib.RatedPost):

    def __init__(self, user, post, subscription, points = None, prob = None):
        feedlib.RatedPost.__init__(self, user, post, subscription, points, prob)
        self._exists_in_db = (points is not None) # FIXME: dubious

    def seen(self):
        # first remove the rating
        dbconn.update("delete from rated_posts where username = %s and post = %s",
               (self._user.get_username(), int(self._post.get_local_id())))

        # then make a note that we've read it
        try:
            dbconn.update("insert into read_posts values (%s, %s, %s)",
                   (self._user.get_username(), int(self._post.get_local_id()),
                    int(self._subscription.get_feed().get_local_id())))
        except psycopg2.IntegrityError, e:
            # most likely the button got pressed twice. we output a warning
            # and carry on.
            print str(e)

        dbconn.commit()

    def age(self):
        self._points = feedlib.calculate_points(self.get_overall_probability(),
                                                self._post.get_date())
        self.save()

    def save(self):
        if self._exists_in_db:
            dbconn.update("""update rated_posts set points = %s, last_recalc = now(),
                                             prob = %s
                      where username = %s and post = %s""",
                   (self._points, self._prob, self._user.get_username(),
                    int(self._post.get_local_id())))
        else:
            dbconn.update("insert into rated_posts values (%s, %s, %s, %s, now(), %s)",
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

    def is_read_as_dupe(self):
        '''Returns true if we already have a duplicate of this Post in
        the read_posts table. Duplicates found via link and minhash.'''

        username = self._user.get_username()
        post = self.get_post()
        rows = connpool.query_for_rows('''select p.id
                       from posts p
                       join read_posts r on p.id = r.post
                       where r.username = %s and
                             (link = %s or minhash = %s)''',
                    (username, post.get_link(), post.get_minhash()))

        return bool(rows)

    def find_dupes(self):
        '''Returns RatedPost objects (for the same user) for other
        Post objects ultimately representing the same story, but not
        read by user. The objects are sorted by points, descending.'''

        username = self._user.get_username()
        post = self.get_post()
        rows = connpool.query_for_rows('''select p.id, p.feed
                       from posts p
                       join rated_posts r on p.id = r.post
                       where r.username = %s and
                             (link = %s or minhash = %s)
                       order by r.points desc''',
                    (username, post.get_link(), post.get_minhash()))

        seen_feeds = set()
        dupes = []
        for (id, feed) in rows:
            if feed in seen_feeds:
                continue # we don't dupe posts if they are from the same feed

            seen_feeds.add(feed)
            feed = feeddb.get_feed_by_id(feed)
            post = Item(id, None, None, None, None, None, feed, None)
            dupes.append(RatedPost(username, post, Subscription(feed, self._user)))
        return dupes


    def find_all_dupes(self):
        '''Returns RatedPost objects (for the same user) for other
        Post objects ultimately representing the same story, including
        ones already read by user.'''

        dupes = self.find_dupes() # this gives us the unread ones


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
        dbconn.update(query, insertvalues)

    if updatebatch:
        query = """update rated_posts
                   set points = i.column1, last_recalc = now(),
                       prob = i.column2
                   from (values %s) as i
                  where username = i.column3 and post = i.column4"""

        values = ", ".join(["(%s, %s, %s, %s)"] * len(updatebatch))
        query = query % values

        updatevalues = [item for row in updatebatch for item in row]
        dbconn.updates(query, updatevalues)

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
        accounts = connpool.query_for_value("select count(*) from users", ())
        return accounts < MAX_USERS

    def set_session(self, session):
        self._session = session

    def user_exists(self, username):
        return connpool.query_for_value("""select username from users where
                                           username = %s""", (username, ))

    def verify_credentials(self, username, password):
        passhash = crypt(password)
        return connpool.query_for_value("""select username from users where
                                    username = %s and password = %s""",
                               (username, passhash))

    def create_user(self, username, password, email):
        passhash = crypt(password)
        connpool.update("insert into users values (%s, %s, %s)",
                        (username, passhash, email))

    def find_user(self, email):
        return connpool.query_for_value(
            "select username from users where email = %s", (email, ))

    def set_password(self, username, password):
        passhash = crypt(password)
        dbconn.update("update users set password = %s where username = %s",
                      (passhash, username))
        dbconn.commit()

# ----- USER OBJECT

class User(feedlib.User):

    def __init__(self, username, readonly = False):
        self._username = username
        self._readonly = readonly # applies to dbm file
        self._worddb = None

    def get_username(self):
        return self._username

    def get_feeds(self):
        rows = connpool.query_for_rows("""
          select id, title, xmlurl, htmlurl, error, time_to_wait, last_read,
                 last_error, max_posts, last_modified, up, down
          from feeds
          join subscriptions on id = feed
          where username = %s
        """, (self._username, ))
        subs = [Subscription(Feed(id, title, xmlurl, htmlurl, error,
                                  time_to_wait, last_read, last_error,
                                  maxposts, lastmod),
                             self, up, down)
                for (id, title, xmlurl, htmlurl, error, time_to_wait,
                     last_read, last_error, maxposts, lastmod, up, down)
                in rows]
        subs = feedlib.sort(subs, Subscription.get_ratio)
        subs.reverse()
        return subs

    def get_item_count(self):
        return connpool.query_for_value("""
               select count(*) from rated_posts where username = %s
               """, [self._username])

    def get_item_range(self, low, high):
        # FIXME: this is both performance-critical and slow...
        rows = connpool.query_for_rows("""
          select id, title, link, descr, pubdate, author, p.feed
          from posts p
          join rated_posts on id = post
          where username = %s
          order by points desc limit %s offset %s
        """, (self._username, (high - low), low))
        return [Item(id, title, link, descr, date, author,
                     feeddb.get_feed_by_id(feed), None) for
                (id, title, link, descr, date, author, feed) in
                rows]

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
        if not connpool.query_for_value("""select * from subscriptions where
                            feed = %s and username = %s""", key):
            dbconn.update("insert into subscriptions values (%s, %s)", key)

    def get_subscription(self, feedid):
        feed = feeddb.get_feed_by_id(feedid)
        return Subscription(feed, self)

# ----- SENDING MESSAGE QUEUE

class SendingMessageQueue:

    def __init__(self):
        # create queue, and fail if it does not already exist
        no = int(open(QUEUE_FILE).read())
        self._mqueue = sysv_ipc.MessageQueue(no)
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
            no = int(open(QUEUE_FILE).read())
            self._mqueue = sysv_ipc.MessageQueue(no)

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

connpool = cpool.ConnectionPool(
    lambda: psycopg2.connect(DB_CONNECT_STRING),
    maxconns = 3
)
mqueue = SendingMessageQueue()
