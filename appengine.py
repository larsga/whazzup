
import datetime, traceback, rsslib

from google.appengine.api import taskqueue
from google.appengine.api import users
from google.appengine.ext import db
from google.appengine.api import urlfetch
    
import feedlib

# FUNCTIONALITY TO ADD

# - automatic reprocessing of feeds
# - calculation of posts

# --- Controller

class AppEngineController(feedlib.Controller):

    def in_appengine(self):
        return True

    def recalculate_all_posts(self):
        pass

    def start_feed_reader(self):
        pass

    def find_feeds_to_check(self):
        now = datetime.datetime.now()
        delta = datetime.timedelta(seconds = feedlib.TIME_TO_WAIT)
        checktime = now - delta

        result = db.GqlQuery("""
         select __key__
         from GAEFeed
         where lastcheck = NULL 
        """)

        for key in result:
            taskqueue.add(url = "/task/check-feed/" + str(key))
        
        result = db.GqlQuery("""
         select __key__
         from GAEFeed
         where lastcheck < :1
        """, checktime)

        for key in result:
            taskqueue.add(url = "/task/check-feed/" + str(key))
                      
# --- AppEngine implementation

class GAEUser(db.Model):
    user = db.UserProperty()
    lastvisit = db.DateTimeProperty()

class GAEFeed(db.Model):
    xmlurl = db.LinkProperty()
    htmlurl = db.LinkProperty()
    title = db.StringProperty()
    checkinterval = db.IntegerProperty()
    lastcheck = db.DateTimeProperty()
    error = db.StringProperty()
    lasterror = db.DateTimeProperty()

class GAESubscription(db.Model):
    user = db.UserProperty() # could be reference, too
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

# FIXME: choices choices
# (1) vector, points, subscription    -- duplicates all post info
# (2) separate GAEPostRating
#       subscription, vector, points, post (feed in GAEPost)
    
# --- SeenPost

# url
# user
# datetime

# --- Word

# user
# word
# up
# down

def gae_loader(parser, url):
    result = urlfetch.fetch(url)
    if result.status_code != 200:
        raise IOError("Error retrieving URL")

    parser.feed(result.content)
    parser.close()
    
class GAEFeedDatabase:

    def get_feed_by_id(self, key):
        return FeedWrapper(db.get(db.Key(key)))

    def add_feed_url(self, feedurl):
        # first check if the feed is in the database at all
        result = db.GqlQuery("""
         select * 
         from GAEFeed
         where xmlurl = :1
        """, feedurl)

        if not result.count(): # it's not there
            feed = GAEFeed()
            feed.xmlurl = feedurl
            feed.put()
        else:
            feed = result[0]

        # now add a subscription for this user
        user = users.get_current_user()
        result = db.GqlQuery("""
         select * 
         from GAESubscription
         where user = :1 and feed = :2
        """, user, feed)

        if not result.count(): # it's not there
            sub = GAESubscription()
            sub.user = user
            sub.feed = feed
            sub.put()
    
    def get_item_count(self):
        return 0

    def get_vote_stats(self):
        return (0, 0)

    def get_feeds(self):
        user = users.get_current_user()
        result = db.GqlQuery("""
          select *
          from GAESubscription
          where user = :1
        """, user)
        return [FeedWrapper(sub) for sub in result]

    def save(self):
        pass # it's a noop on GAE

    # --- specific to GAE

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
        
        for item in site.get_items():
            post = post_map.get(item.get_link())
            if not post:
                post = GAEPost()
                post.url = item.get_link()
                post.feed = feed

            post.title = item.get_title()
            post.author = item.get_author()
            post.content = item.get_description()
            post.pubdate = feedlib.parse_date(item.get_pubdate())
            post.put()

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
        return [PostWrapper(self, post) for post in result]

    def get_time_to_wait(self):
        return self._feed.checkinterval

    def time_since_last_read(self):
        return 0

    def is_being_read(self):
        return False

    # this must be precomputed and stored if we are going to do it at all
    def get_unread_count(self):
        return 0

class PostWrapper:

    def __init__(self, parent, post):
        self._parent = parent
        self._post = post

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

controller = AppEngineController()
feeddb = GAEFeedDatabase()
