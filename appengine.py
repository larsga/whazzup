
import datetime, traceback, rsslib, marshal, StringIO, logging, time

from google.appengine.api import taskqueue
from google.appengine.api import users
from google.appengine.ext import db
from google.appengine.api import urlfetch
    
import feedlib

# STATUS

#  - fix errors in tokenization

#  - why are there so many calls to age_subscription?
#  - way too much CPU usage, especially in recalc_sub.
#  - first-time user causes a null user to be created FIXED?

#  - fix redirect from list of items on feed page
#  - hide buttons for items which are read on feed page

#  - add OPML export
#  - test OPML import
#    - first verify that feeds are not duplicated
#  - story list is a bit slow, perhaps?
#  - subscribing to already registered feeds
#  - improve site list page

def get_object(query, *params):
    params = [query] + list(params)
    result = apply(db.GqlQuery, params)
    if result.count() > 0:
        return result[0]

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
        sub.up = 0
        sub.down = 0
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
        feeddb.set_user(sub.user) # no current user...
        result = db.GqlQuery("select * from GAEUser where user = :1", sub.user)
        if result.count() > 0:
            userobj = result[0]
            lastupdate = userobj.lastupdate or datetime.datetime.now()
        else:
            lastupdate = datetime.datetime.now()

        # get all seen posts for this subscription
        seen = {} # post.key -> seen
        for s in db.GqlQuery("""
          select * from GAESeenPost where feed = :1 and user = :2""",
                                sub.feed, sub.user):
            seen[s.post.key()] = s

        # get all existing ratings for this subscription
        ratings = {} # post.key -> rating
        for rating in db.GqlQuery("""
          select * from GAEPostRating where feed = :1 and user = :2""",
                                  sub.feed, sub.user):
            key = rating.post.key()
            if seen.has_key(key):
                rating.delete() # means we've already seen this post
            else:
                ratings[key] = rating

        thefeed = FeedWrapper(sub)
        
        # evaluate each post to see what to do
        count = 0
        total = 0
        toupdate = []
        for key in db.GqlQuery("""
          select __key__ from GAEPost where feed = :1""", sub.feed):
            total += 1

            if seen.has_key(key):
                continue # we've seen this post before, so move on

            rating = ratings.get(key)
            if rating and rating.calculated > lastupdate:
                continue # this rating is up to date, so ignore it

            post = db.get(key)
            if not rating:
                rating = GAEPostRating()
                rating.post = post
                rating.user = sub.user
                rating.feed = post.feed
                rating.postdate = post.pubdate
                oldpoints = 0
                newrating = True
            else:
                oldpoints = rating.points
                newrating = False

            thepost = PostWrapper(post, thefeed)
            thepost.recalculate()
            newpoints = thepost.get_points()
            if newrating or abs(oldpoints - newpoints) > 0.5:
                rating.prob = thepost.get_overall_probability()
                rating.calculated = datetime.datetime.now()
                rating.points = thepost.get_points()
                toupdate.append(rating)
            count += 1

        if toupdate:
            db.put(toupdate)
        logging.info("Recalculated %s posts (of %s; %s stored) for key %s" %
                     (count, total, len(toupdate), key))

    def recalculate_all_posts(self):
        user = users.get_current_user() # not a task, so it's OK
        for sub in db.GqlQuery("select __key__ from GAESubscription where user = :1", user):
            self.queue_recalculate_subscription(sub)

    def age_posts(self):
        for sub in db.GqlQuery("select __key__ from GAESubscription"):
            self.queue_age_subscription(sub)

    def age_subscription(self, key):
        sub = db.get(db.Key(key))
        count = 0
        toupdate = []
        for rating in db.GqlQuery("""select * from GAEPostRating
                                  where user = :1 and feed = :2""",
                                  sub.user, sub.feed):
            oldpoints = rating.points
            newpoints = feedlib.calculate_points(rating.prob, rating.postdate)
            count += 1
            if abs(oldpoints - newpoints) > 0.5:
                rating.points = newpoints
                toupdate.append(rating)

        if toupdate:
            db.put(toupdate) # batch put is faster
        logging.info("Aged %s posts (really %s) for key %s" %
                     (count, len(toupdate), key))

    def start_feed_reader(self):
        pass

    def find_feeds_to_check(self):
        now = datetime.datetime.now()
        delta = datetime.timedelta(seconds = feedlib.TIME_TO_WAIT)
        checktime = now - delta

        # FIXME: we can easily get rid of this query, thus saving cputime
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
        feed.maxposts = 300
        if site.get_items():
            oldest = site.get_items()[-1]
            dt = feedlib.parse_date(oldest.get_pubdate())
            delta = time.time() - feedlib.toseconds(dt)
            count = len(site.get_items())
            feed.maxposts = int(max(min((count / (delta / 3600)) * 24 * 7 * 8, 300), 30))
        
        feed.put()

        post_map = {}
        current_posts = db.GqlQuery("""
          select * from GAEPost where feed = :1""", feed)
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
            post.content = db.Text(item.get_description())
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
            for post in db.GqlQuery("select * from GAEPost where "
                                    "feed = :1", feed):
                post.delete()

            # seenposts and ratings should have been deleted during unsubscribe
            # already, so we don't do those here

            feed.delete()

    def purge_posts(self):
        for key in db.GqlQuery("select __key__ from GAEFeed"):
            self.queue_purge_feed(key)

    def purge_feed(self, key):
        feed = db.get(db.Key(key))
        todelete = []
        count = 0
        for key in db.GqlQuery("""select __key__ from GAEPost where feed = :1
                                  order by pubdate desc""", feed):
            if count > feed.maxposts:
                todelete.append(key)
                count += 1
                for rkey in db.GqlQuery("""select __key__ from GAEPostRating
                                        where post = :1""", key):
                    todelete.append(rkey)
                for skey in db.GqlQuery("""select __key__ from GAESeenPost
                                        where post = :1""", key):
                    todelete.append(skey)

        db.delete(todelete)
        logging.info("Deleted %s posts from %s" % (count, feed.title))
        
    # methods to queue tasks

    def queue_purge_feed(self, key):
        taskqueue.add(url = "/task/purge-feed/" + str(key))

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
    maxposts = db.IntegerProperty()

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

    data = result.content
            
    parser.feed(data)
    parser.close()
    
class GAEFeedDatabase(feedlib.Database):

    def __init__(self):
        feedlib.Database.__init__(self)
        self._worddb = None
        self._user = None

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
        return self.get_word_ratio(word) # should perhaps prefix, but...

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
        seen.feed = post.feed
        seen.post = post
        seen.put()

    def get_no_of_item(self, item):
        return 0 # FIXME: we can't compute this
    
    def remove_feed(self, feed):
        # this is really unsubscribe, not remove
        user = users.get_current_user()
        for rating in db.GqlQuery("""select * from GAEPostRating
                                  where feed = :1 and user = :2""",
                                  feed._feed, user):
            rating.delete()

        for seen in db.GqlQuery("""select * from GAESeenPost
                                where feed = :1 and user = :2""",
                                feed._feed, user):
            seen.delete()

        for sub in db.GqlQuery("""select * from GAESubscription
                               where feed = :1 and user = :2""",
                               feed._feed, user):
            sub.delete()

    def get_popular_feeds(self):
        return [FeedWrapper(feed) for feed in
                db.GqlQuery("""select * from GAEFeed
                            order by subscribers desc
                            limit 50""")]

    def set_user(self, user):
        self._user = user

    def _get_word_db(self):
        if not self._worddb:
            self._worddb = AppEngineWordDatabase(self._user)
        return self._worddb

    def _get_author_db(self):
        return self._get_word_db()

    def _get_site_db(self):
        return self._get_word_db()
    
class FeedWrapper(feedlib.Feed):

    def __init__(self, obj):
        if isinstance(obj, GAESubscription):
            self._sub = obj
            self._feed = obj.feed
        else:
            self._feed = obj
            self._sub = None
            # better not try to touch self._sub here...

    def get_item_count(self):
        "On GAE this returns max number of postings, not actual number"
        return self._feed.maxposts
            
    def get_url(self):
        return self._feed.xmlurl
            
    def get_link(self):
        return self._feed.htmlurl
            
    def get_title(self):
        return self._feed.title or "[No title]"

    def get_ratio(self):
        sub = self._get_sub()
        up = (self._sub.up or 0) + feedlib.START_VOTES
        down = (self._sub.down or 0) + feedlib.START_VOTES
        return up / float(up + down)

    def get_local_id(self):
        return self._feed.key()

    def get_error(self):
        return self._feed.error

    def get_items(self):
        result = db.GqlQuery("""
          select * from GAEPost where feed = :1 order by pubdate desc
        """, self._feed)
        return [PostWrapper(post, self) for post in result]

    def get_time_to_wait(self):
        return self._feed.checkinterval

    def time_since_last_read(self):
        "Seconds since last read."
        if self._feed.lastcheck:
            return time.time() - feedlib.toseconds(self._feed.lastcheck)
        return 0

    def record_vote(self, vote):
        sub = self._get_sub()
        if vote == "up":
            self._sub.up = (self._sub.up or 0) + 1
        elif vote == "down":
            self._sub.down = (self._sub.down or 0) + 1
        self._sub.put()
            
    def is_being_read(self):
        return False

    def is_subscribed(self):
        return self._get_sub()

    def _get_sub(self):
        if not self._sub:
            self._sub = get_object("""select * from GAESubscription
                             where user = :1 and feed = :2""",
                             users.get_current_user(), self._feed)
        return self._sub
    
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

    def get_guid(self):
        return self._post.key()
            
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
        # content is a db.Text object, which web.py doesn't recognize as being
        # a unicode string, even though that's what it is. so we need to turn
        # it into a normal unicode string.
        str = self._post.content.encode("utf-8") # make a byte string
        str = str.decode("utf-8") # reinterpret as unicode string
        return str

    def get_date(self):
        return self._post.pubdate

    def get_pubdate(self):
        return str(self._post.pubdate)

    def get_age(self):
        age = time.time() - feedlib.toseconds(self._post.pubdate)
        if age < 0:
            age = 3600
        return age

    def get_points(self):
        if not hasattr(self, "_points"):
            self.recalculate()
        return self._points

    def get_stored_points(self):
        user = users.get_current_user()
        results = db.GqlQuery("""select * from GAEPostRating
                              where user = :1 and post = :2""",
                              user, self._post)
        if results.count() > 0:
            return results[0].points

    def record_vote(self, vote):
        if vote != "read":
            self._parent.record_vote(vote)
        feedlib.Post.record_vote(self, vote)

class AppEngineWordDatabase(feedlib.WordDatabase):

    def __init__(self, user = None):
        user = user or users.get_current_user()
        if not user:
            logging.warn("No user in word database!")
        results = db.GqlQuery("select * from GAEUser where user = :1", user)
        self._userdata = None
        self._user = user
        self._changed = False
        worddb = {}
        
        if results.count() > 0:
            self._userdata = results[0]
            blob = results[0].worddb
            if blob:
                worddb = marshal.loads(blob)
        else:
            self._userdata = GAEUser()
            self._userdata.user = users.get_current_user()
            self._userdata.worddb = marshal.dumps({})
            self._userdata.lastupdate = datetime.datetime.now()
            self._userdata.put()

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
                self._userdata = GAEUser()
                self._userdata.user = self._user
            self._userdata.worddb = marshal.dumps(self._words)
            self._userdata.lastupdate = datetime.datetime.now()
            self._userdata.put()
        
    
controller = AppEngineController()
feeddb = GAEFeedDatabase()
feedlib.feeddb = feeddb # let's call it dependency injection, so it's cool
