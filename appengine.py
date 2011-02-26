
import datetime, traceback, rsslib, marshal, StringIO, logging, time

from google.appengine.api import taskqueue
from google.appengine.api import users
from google.appengine.ext import db
from google.appengine.api import urlfetch
    
import feedlib

# STATUS

#  - author and site ratios are dubious
#  - does aging ratings really have any effect?
#  - need to add purging of old posts
#  - what about feeds which have no subscribers?
#  - display item stats doesn't work
#  - urlfetch is not returning Unicode, but only ascii

def toseconds(timestamp):
    return time.mktime(timestamp.timetuple())    

# --- Controller

class AppEngineController(feedlib.Controller):

    def in_appengine(self):
        return True

    def add_feed(self, url):
        # first check if the feed is in the database at all
        result = db.GqlQuery("""
         select * from GAEFeed where xmlurl = :1""", url)

        if not result.count(): # it's not there
            feed = GAEFeed()
            feed.xmlurl = url
            feed.subscribers = 1
        else:
            feed = result[0]
            feed.subscribers += 1
        feed.put()
            
        # now add a subscription for this user
        user = users.get_current_user()
        result = db.GqlQuery("""
         select * from GAESubscription where user = :1 and feed = :2
        """, user, feed)

        if result.count(): # we're subscribed already, so never mind
            return
        
        sub = GAESubscription()
        sub.user = user
        sub.feed = feed
        sub.put()

        if feed.subscribers > 1:
            # the feed was already there and loaded, so just calculate ratings
            # for this user
            self.queue_recalculate_subscription(sub)
        else:
            # we didn't have this feed from before, so let's start by
            # downloading it. this in turn will trigger a recalculation
            self.queue_check_feed(feed)

    def recalculate_subscription(self, key):
        sub = db.get(db.Key(key))
        result = db.GqlQuery("select * from GAEUser where user = :1", sub.user)
        if result.count() > 0:
            userobj = result[0]
            lastupdate = userobj.lastupdate or datetime.datetime.now()
        else:
            lastupdate = datetime.datetime.now()

        # get all seen posts for this subscription
        ratings = {} # post.key -> (rating | seen)
        for seen in db.GqlQuery("""
          select * from GAESeenPost where feed = :1 and user = :2""",
                                sub.feed, sub.user):
            ratings[seen.post.key()] = seen
            
        # get all existing ratings for this subscription
        for rating in db.GqlQuery("""
          select * from GAEPostRating where feed = :1 and user = :2""",
                                  sub.feed, sub.user):
            key = rating.post.key()
            if ratings.has_key(key):
                rating.delete() # means we've already seen this post
            else:
                ratings[key] = rating

        thefeed = FeedWrapper(sub)
        
        # evaluate each post to see what to do
        for post in db.GqlQuery("""
          select * from GAEPost where feed = :1""", sub.feed):
            rating = ratings.get(post.key())
            if not rating:
                rating = GAEPostRating()
                rating.post = post
                rating.user = sub.user
                rating.feed = post.feed
                rating.postdate = post.pubdate
            elif isinstance(rating, GAESeenPost):
                rating.delete() # we've already seen this post, so remove rating
                continue
            elif rating.calculated > lastupdate:
                continue # this rating is up to date, so ignore it

            thepost = PostWrapper(post, thefeed)
            thepost.recalculate()
            rating.prob = thepost.get_overall_probability()
            rating.points = thepost.get_points()
            rating.calculated = datetime.datetime.now()
            rating.put()

    def recalculate_all_posts(self):
        user = users.get_current_user() # FIXME: need user key here
        for sub in db.GqlQuery("select __key__ from GAESubscription where user = :1", user):
            self.queue_recalculate_subscription(sub)

    def age_posts(self):
        for sub in db.GqlQuery("select __key__ from GAESubscription"):
            self.queue_age_subscription(sub)

    def age_subscription(self, key):
        sub = db.get(db.Key(key))
        for rating in db.GqlQuery("""select * from GAESubscription
                                  where user = :1 and feed = :2""",
                                  sub.user, sub.feed):
            if hasattr(rating, "postdate"):
                date = rating.postdate
            else:
                date = rating.post.pubdate
            rating.points = feedlib.calculate_points(rating.points, date)
            rating.put()

    def start_feed_reader(self):
        pass

    def find_feeds_to_check(self):
        now = datetime.datetime.now()
        delta = datetime.timedelta(seconds = feedlib.TIME_TO_WAIT)
        checktime = now - delta

        result = db.GqlQuery("""
         select __key__ from GAEFeed where lastcheck = NULL""")

        for key in result:
            self.queue_check_feed(key)
        
        result = db.GqlQuery("""
         select __key__ from GAEFeed where lastcheck < :1""", checktime)

        for key in result:
            self.queue_check_feed(key)

    # --- specific to GAE (FIXME: but should it be?)

    def check_feed(self, key):
        feed = db.get(db.Key(key))
        try:
            site = rsslib.read_feed(feed.xmlurl, data_loader = gae_loader)
        except Exception, e:
            # we failed, so record the failure and move on
            traceback.print_exc()
            feed.error = str(e)
            feed.lasterror = datetime.datetime.now()
            feed.put()
            return

        feed.title = site.get_title()
        feed.htmlurl = site.get_link()
        feed.lastcheck = datetime.datetime.now()
        feed.error = None
        feed.lasterror = None
        feed.put()

        post_map = {}
        current_posts = db.GqlQuery("""
          select *
          from GAEPost
          where feed = :1
        """, feed)
        for post in current_posts:
            post_map[str(post.url)] = post

        newposts = False
        for item in site.get_items():
            post = post_map.get(item.get_link())
            if not post:
                post = GAEPost()
                post.url = item.get_link()
                post.feed = feed
                newposts = True

            post.title = item.get_title()
            post.author = item.get_author()
            post.content = item.get_description()
            post.pubdate = feedlib.parse_date(item.get_pubdate())
            post.put()

        if newposts:
            # recalculate all subscriptions on this feed
            for sub in db.GqlQuery("""
              select __key__ from GAESubscription where feed = :1""", feed):
                self.queue_recalculate_subscription(sub)

    # definitely specific to GAE

    def remove_dead_feeds(self):
        for feed in db.GqlQuery("select * from GAEFeed where subscribers = 0"):
            for post in db.GqlQuery("select __key__ from GAEPost where "
                                    "feed = :1", feed):
                post.delete()
            # delete the ratings, too?
            feed.delete()

    # methods to queue tasks

    def queue_age_subscription(self, key):
        taskqueue.add(url = "/task/age-subscription/" + str(key))

    def queue_recalculate_subscription(self, suborkey):
        if isinstance(suborkey, GAESubscription):
            key = suborkey.key()
        else:
            key = suborkey
        taskqueue.add(url = "/task/recalc-sub/" + str(key))

    def queue_check_feed(self, keyorfeed):
        if isinstance(keyorfeed, GAEFeed):
            key = keyorfeed.key()
        else:
            key = keyorfeed
        taskqueue.add(url = "/task/check-feed/" + str(key))
        
# --- AppEngine implementation

class GAEUser(db.Model):
    user = db.UserProperty()
    lastupdate = db.DateTimeProperty()
    worddb = db.BlobProperty()

class GAEFeed(db.Model):
    xmlurl = db.LinkProperty()
    htmlurl = db.LinkProperty()
    title = db.StringProperty()
    checkinterval = db.IntegerProperty()
    lastcheck = db.DateTimeProperty()
    error = db.StringProperty()
    lasterror = db.DateTimeProperty()
    subscribers = db.IntegerProperty() # when this goes to 0...

class GAESubscription(db.Model):
    user = db.UserProperty()
    feed = db.ReferenceProperty(GAEFeed)
    up = db.IntegerProperty()
    down = db.IntegerProperty()

class GAEPost(db.Model):
    url = db.LinkProperty()
    title = db.StringProperty()
    author = db.StringProperty()
    pubdate = db.DateTimeProperty()
    content = db.TextProperty()
    feed = db.ReferenceProperty(GAEFeed)

class GAEPostRating(db.Model):
    user = db.UserProperty()             # not sure if this is necessary
    feed = db.ReferenceProperty(GAEFeed) # non-normalized
    prob = db.FloatProperty() # means we can do faster time-based recalc
    postdate = db.DateTimeProperty() # ditto
    points = db.FloatProperty()
    post = db.ReferenceProperty(GAEPost)
    calculated = db.DateTimeProperty()

class GAESeenPost(db.Model):
    user = db.UserProperty()
    feed = db.ReferenceProperty(GAEFeed) # non-normalized
    post = db.ReferenceProperty(GAEPost)

def gae_loader(parser, url):
    result = urlfetch.fetch(url)
    if result.status_code != 200:
        raise IOError("Error retrieving URL")

    # charset = "iso-8859-1"
    # if result.headers.has_key("content-type"):
    #     ctype = result.headers["content-type"]
    #     pos = ctype.find("charset=")
    #     if pos == -1:
    #         logging.warn("Content-type had no charset: " + repr(ctype))
    #     else:
    #         charset = ctype[pos + len("charset=") : ]

    # try:
    #     data = result.content.decode(charset)
    # except LookupError:
    #     data = result.content.decode("iso-8859-1")
    data = result.content # FIXME: how to turn this into unicode?3
            
    parser.feed(data)
    parser.close()
    
class GAEFeedDatabase(feedlib.Database):

    def __init__(self):
        feedlib.Database.__init__(self)
        self._worddb = None

    def get_item_by_id(self, key):
        return PostWrapper(db.get(db.Key(key)))
    
    def get_item_range(self, low, high):
        user = users.get_current_user()
        query = ("""
          select * from GAEPostRating where user = :1
          order by points desc
          limit %s offset %s
        """ % ((high - low), low))
        result = db.GqlQuery(query, user)
        return [PostWrapper(rating.post) for rating in result]
    
    def get_feed_by_id(self, key):
        return FeedWrapper(db.get(db.Key(key)))
    
    def get_item_count(self):
        return 0

    def get_vote_stats(self):
        return (0, 0)

    def get_feeds(self):
        user = users.get_current_user()
        result = db.GqlQuery("select * from GAESubscription where user = :1",
                             user)
        return [FeedWrapper(sub) for sub in result]

    def save(self):
        pass # it's a noop on GAE

    def commit(self):
        self._get_word_db().close()

    def get_word_ratio(self, word):
        worddb = self._get_word_db()
        return worddb.get_word_ratio(word)

    def get_author_ratio(self, word):
        return 0.5 # FIXME

    def remove_item(self, item):
        pass # I think we don't need this one

    def seen_link(self, post):
        post = post._post
        user = users.get_current_user()
        for rating in db.GqlQuery("""select * from GAEPostRating
                                  where post = :1 and user = :2""",
                                  post, user):
            rating.delete()

        seen = GAESeenPost()
        seen.user = user
        seen.feed = post.feed # yuk
        seen.post = post
        seen.put()
        
    def _get_word_db(self):
        if not self._worddb:
            self._worddb = AppEngineWordDatabase()
        return self._worddb

    def _get_author_db(self):
        return self._get_word_db()

    def _get_site_db(self):
        return self._get_word_db()
    
class FeedWrapper:

    def __init__(self, obj):
        if isinstance(obj, GAESubscription):
            self._sub = obj
            self._feed = obj.feed
        else:
            self._feed = obj
            # better not try to touch self._sub here...

    def get_url(self):
        return self._feed.xmlurl
            
    def get_link(self):
        return self._feed.htmlurl
            
    def get_title(self):
        return self._feed.title or "[No title]"

    def get_ratio(self):
        up = (self._sub.up or 0) + feedlib.START_VOTES
        down = (self._sub.down or 0) + feedlib.START_VOTES
        return up / float(up + down)

    def get_local_id(self):
        return self._feed.key()

    def get_error(self):
        return self._feed.error

    def get_items(self):
        result = db.GqlQuery("""
          select *
          from GAEPost
          where feed = :1
        """, self._feed)
        return [PostWrapper(post, self) for post in result]

    def get_time_to_wait(self):
        return self._feed.checkinterval

    def time_since_last_read(self):
        "Seconds since last read."
        if self._feed.lastcheck:
            return time.time() - toseconds(self._feed.lastcheck)
        return 0

    def is_being_read(self):
        return False

    # this must be precomputed and stored if we are going to do it at all
    def get_unread_count(self):
        return 0

class PostWrapper(feedlib.Post):

    def __init__(self, post, parent = None):
        self._post = post
        if parent:
            self._parent = parent
        else:
            self._parent = FeedWrapper(post.feed)

    def get_title(self):
        return self._post.title

    def get_author(self):
        return self._post.author
    
    def is_seen(self):
        return False

    def get_local_id(self):
        return self._post.key()

    def get_link(self):
        return self._post.url

    def get_site(self):
        return self._parent

    def get_description(self):
        return self._post.content

    def get_date(self):
        return self._post.pubdate

class AppEngineWordDatabase(feedlib.WordDatabase):

    def __init__(self):        
        user = users.get_current_user()
        results = db.GqlQuery("select * from GAEUser where user = :1", user)
        self._user = None
        self._changed = False
        worddb = {}
        
        if results.count() > 0:
            self._user = results[0]
            blob = results[0].worddb
            if blob:
                worddb = marshal.loads(blob)

        feedlib.WordDatabase.__init__(self, worddb)

    def record_vote(self, theword, vote):
        feedlib.WordDatabase.record_vote(self, theword, vote)
        self._changed = True

    def change_word(self, oldword, newword):
        feedlib.WordDatabase.change_word(self, oldword, newword)
        self._changed = True
        
    def close(self):
        if self._changed:
            if not self._user:
                self._user = GAEUser()
                self._user.user = users.get_current_user()            
            self._user.worddb = marshal.dumps(self._words)
            self._user.lastupdate = datetime.datetime.now()
            self._user.put()
    
controller = AppEngineController()
feeddb = GAEFeedDatabase()
feedlib.feeddb = feeddb # let's call it dependency injection, so it's cool
