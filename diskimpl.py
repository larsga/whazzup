
import dbm, os, shelve, threading, string, chew, vectors, math, datetime

import feedlib, rsslib

MAX_STORIES = 8000

# --- Controller

class DiskController(feedlib.Controller):

    def add_feed(self, feedurl):
        feeddb.add_feed(rsslib.read_feed(feedurl, wzfactory))

    def get_thread_health(self):
        return time.time() - lasttick # FIXME

    def recalculate_all_posts(self):
        # called when a post is voted on, so that the system knows it needs
        # to recalculate all posts.
        # FIXME: can currently create duplicate tasks (and duplicate recalc)
        posts = feeddb.get_items()
        if posts:
            queue.put((0, RecalculatePosts(posts)))

    def reload(self):
        new_posts = feeddb.init() # does a reload
        if new_posts:
            queue.put((0, RecalculatePosts(new_posts)))

    def start_feed_reader(self, feeddb):
        thread = threading.Thread(target = feed_reader, name = "FeedReader", args = (feeddb, ))
        thread.start()
        return thread
            
# --- Model

class FeedDatabase(rsslib.FeedRegistry, feedlib.Database):

    def __init__(self):
        rsslib.FeedRegistry.__init__(self)
        self._feeds = []
        self._title = None
        self._items = [] # list of Link objects
        self._feedmap = {} # id -> feed
        self._linkmap = {} # id -> link
        self._feedurlmap = {} # feed url -> feed
        self._linkguidmap = {} # link guid -> link
        self._links = LinkTracker()        
        self._words = WordDatabase("words.dbm")
        self._sites = WordDatabase("sites.dbm")
        self._authors = WordDatabase("authors.dbm")
        self._lock = threading.Lock()
        try:
            self._faves = rsslib.read_rss("faves.rss", wzfactory)
        except IOError:
            self._faves = Feed("faves.rss")
            self._faves.set_title("Recent reading")
            self._faves.set_description("A feed of my favourite recent reads.")

    def get_title(self):
        return self._title

    def set_title(self, title):
        self._title = title
        
    def get_feeds(self):
        return self._feeds
            
    def init(self):
        new_posts = []
        for feed in self._feeds:
            url = feed.get_url()
            new_posts += self.read_feed(url)
        return new_posts
        
    def sort(self):
        try:
            self._lock.acquire()
            self._items = feedlib.sort(self._items, Link.get_points)
            self._items.reverse()

            ix = len(self._items) - 1
            while ix >= 0 and len(self._items) > MAX_STORIES:
                if self._items[ix].get_age() > (86400 * 2):
                    del self._items[ix]
                ix -= 1
        finally:
            self._lock.release()
            
    def get_item_count(self):
        return len(self._items)

    def get_item_no(self, ix):
        return self._items[ix]

    def get_item_by_id(self, id):
        return self._linkmap[id]

    def get_items(self):
        return self._items

    def get_no_of_item(self, item):
        try:
            self._lock.acquire()
            return self._items.index(item)
        except ValueError:
            return None
        finally:
            self._lock.release()

    def remove_item(self, item):
        try:
            self._lock.acquire()
            self._items.remove(item)
        finally:
            self._lock.release()
    
    def get_feed_by_id(self, id):
        return self._feedmap[int(id)]

    def add_feed(self, feed):
        self._feeds.append(feed)
        self._feedmap[feed.get_local_id()] = feed
        self._feedurlmap[feed.get_url()] = feed

    def remove_feed(self, feed):
        self._feeds.remove(feed)
        del self._feedmap[feed.get_local_id()]
        del self._feedurlmap[feed.get_url()]

    def get_faves(self):
        return self._faves
    
    def add_fave(self, fave):
        # FIXME: set a limit to the number of items in the feed
        self._faves.add_item_to_front(fave)
        outf = codecs.open("faves.rss", "w", "utf-8")
        rsslib.write_rss(self._faves, outf)
        outf.close()

        os.system("scp faves.rss garshol.virtual.vps-host.net:/home/larsga/")

    def save(self):
        outf = open("feeds.txt", "w")
        for feed in self._feeds:
            outf.write("%s | %s\n" %
                       (feed.get_url(),
                        feed.get_time_to_wait()))
        outf.close()

    def get_vote_stats(self):
        up = 0
        down = 0
        for feed in self._feeds:
            (fu, fd) = self._sites.get_word_stats(feed.get_link())
            up += fu
            down += fd
        return (up, down)

    def commit(self):
        """A desperate attempt to preserve DB changes even in the face of
        crashes."""
        self._words.close()
        self._sites.close()
        self._authors.close()
        self._words = WordDatabase("words.dbm")
        self._sites = WordDatabase("sites.dbm")
        self._authors = WordDatabase("authors.dbm")

    # delegation calls

    def get_word_ratio(self, word):
        return self._words.get_word_ratio(word)

    def record_word_vote(self, word, vote):
        self._words.record_vote(word, vote)
        
    def get_site_ratio(self, link):
        return self._sites.get_word_ratio(link)

    def change_site_url(self, oldlink, newlink):
        feed = self._feedurlmap[oldlink]
        del self._feedurlmap[oldlink]
        self._feedurlmap[newlink] = feed
        self._sites.change_word(oldlink, newlink)

    def record_site_vote(self, link, vote):
        self._sites.record_vote(link, vote)
    
    def get_author_ratio(self, author):
        return self._authors.get_word_ratio(author)

    def record_author_vote(self, author, vote):
        self._authors.record_vote(author, vote)
    
    def is_link_seen(self, uid):
        return self._links.is_link_seen(uid)

    def seen_link(self, uid):
        self._links.seen_link(uid)

    # internal stuff

    def read_feed(self, url):
        oldsite = self._feedurlmap.get(url)
        if oldsite:
            oldsite.being_read()
                
        try:
            site = rsslib.read_feed(url, wzfactory)
        except Exception, e:
            if oldsite:
                oldsite.not_being_read()
                oldsite.set_error(traceback.format_exc())
            if not isinstance(e, IOError):
                traceback.print_exc()
            else:
                print "ERROR: ", e
            return [] # we didn't get any feed, so no point in continuing
                
        items = site.get_items()
        items.reverse() # go through them from the back to get
                        # right order when added to oldsite
        new_items = []
        try:
            try:
                self._lock.acquire()
                for item in items:
                    if oldsite and not self._linkguidmap.has_key(item.get_guid()):
                        # means we've read this feed before, but we don't have
                        # this particular item. so we move it across
                        item._site = oldsite
                        oldsite.add_item_to_front(item)
                        new_items.append(item)

                    if not self._linkguidmap.has_key(item.get_guid()):
                        self._linkmap[item.get_local_id()] = item
                        self._linkguidmap[item.get_guid()] = item
                        if not item.is_seen():
                            self._items.append(item)
            except:
                traceback.print_exc()
        finally:
            self._lock.release()

        if oldsite:
            oldsite.now_read()
            oldsite.set_title(site.get_title())
            if site.get_link():
                oldsite.set_link(site.get_link())
            oldsite.set_error(None)
        else:
            # we might not have seen this feed before, in which case we add it
            if not self._feedurlmap.has_key(url):
                self.add_feed(site)
            site.now_read()

        return new_items

class Feed(rsslib.SiteSummary):

    def __init__(self, url):
        rsslib.SiteSummary.__init__(self, url)
        self._last_read = 0
        self._being_read = 0
        self._time_to_wait = feedlib.TIME_TO_WAIT # specified in seconds
        self._error = None
        self._task_in_queue = False # whether there is a check task in queue

    def get_title(self):
        return rsslib.SiteSummary.get_title(self) or "[No title]"

    def set_url(self, url):
        ratio = self.get_ratio()
        feeddb.change_site_url(self.get_url(), url)
        feeddb.save()
        feeddb.commit()
        self._url = url
        self._last_read = 0 # so that it will be read again soon
        
    def get_ratio(self):
        return feeddb.get_site_ratio(self.get_link())

    def get_local_id(self):
        return id(self)

    def add_item_to_front(self, item):
        self.items.insert(0, item)

    def now_read(self):
        self._last_read = time.time()
        self._being_read = 0

    def being_read(self):
        self._being_read = 1

    def not_being_read(self):
        self._being_read = 0

    def is_being_read(self):
        return self._being_read

    def should_read(self):
        #print "-----", (self.get_title() or "").encode("utf-8")
        #print "Being read?", self._being_read
        #print "Waited: ", self.time_since_last_read()
        #print "Time to wait: ", self.get_time_to_wait()
        state = (not self._being_read) and \
               self.time_since_last_read() > self.get_time_to_wait()
        #print "So: ", state
        return state

    def time_since_last_read(self):
        return time.time() - self._last_read

    def get_time_to_wait(self):
        return self._time_to_wait

    def set_time_to_wait(self, time):
        self._time_to_wait = time

    def set_error(self, error):
        self._error = error

    def get_error(self):
        return self._error

    def get_unread_count(self):
        count = 0
        for item in self.get_items():
            if not feeddb.is_link_seen(item.get_guid()):
                count += 1
        return count

    def has_check_task(self):
        return self._task_in_queue

    def set_check_task(self, state):
        self._task_in_queue = state

class Link(feedlib.Post):

    def __init__(self, site):
        self._site = site
        self._date = None
        self._points = None
        self._guid = None
        self._author = None
        self._title = None
        self._link = None
        self._pubdate = None

    def get_title(self):
        return self._title

    def get_link(self):
        return self._link

    def set_link(self, link):
        self._link = string.strip(link)
        
    def get_description(self):
        return cache.get(str(id(self)) + "descr")

    def set_description(self, descr):
        cache[str(id(self)) + "descr"] = descr

    def get_pubdate(self):
        return self._pubdate

    def set_title(self, title):
        self._title = title

    def set_pubdate(self, pubdate):
        self._pubdate = pubdate.strip() # must remove ws to simplify parsing

    def get_site(self):
        return self._site

    def get_guid(self):
        return self._guid or self._link

    def set_guid(self, guid):
        self._guid = guid

    def get_author(self):
        return self._author

    def set_author(self, author):
        self._author = author

    def get_local_id(self):
        return id(self)

    def get_date(self):
        if not self._date:
            self._date = feedlib.parse_date(self.get_pubdate())
        return self._date
    
    def get_vector(self): # override to get caching
        key = str(id(self)) + "vector"
        if cache.has_key(key):
            vector = cache[key]
        else:
            html = (self.get_title() or "") + " " + (self.get_description() or "")
            text = feedlib.html2text(html) + " " + self.get_url_tokens()
            vector = vectors.text_to_vector(text, {}, None, 1)
            cache[key] = vector
        return vector
    
class LinkTracker:

    def __init__(self):
        self._links = {}
        try:
            for link in open("seen-urls.txt").readlines():
                self._links[string.strip(link)] = 1
        except IOError, e:
            if e.errno != 2:
                raise e

    def is_link_seen(self, link):
        return self._links.has_key(link)

    def seen_link(self, link):
        self._links[link] = 1
        
        outf = open("seen-urls.txt", "a")
        outf.write(link + "\n")
        outf.close()

class WhazzupFactory(rsslib.DefaultFactory):

    def make_site(self, url):
        return Feed(url)
    
    def make_item(self, site):
        return Link(site)

    def make_feed_registry(self):
        return FeedDatabase()
    
def get_feeds():
    try:
        feeds = wzfactory.make_feed_registry()
        for line in open("feeds.txt").readlines():
            (url, time) = string.split(string.strip(line), " | ")
            time = int(time)
            feed = wzfactory.make_site(url)
            feed.set_time_to_wait(time)
            feeds.add_feed(feed)

        return feeds

    except IOError, e:
        if e.errno == 2:
            return wzfactory.make_feed_registry()
        raise e

# ----- QUEUES

# ISSUES TO FIX:
#  - thread dormant time is not set correctly

import Queue, time, sys, traceback

# tasks have an optional time to execute. if the time is not set, the
# task is executed immediately. the time to execute is used as the
# priority. if no tasks in the queue are ready to be performed, the
# worker thread sleeps for a second, then rechecks.

# have subclassed priority queue so that we can peek into it and see
# whether the first task is ready to be performed.

class PeekablePriorityQueue(Queue.PriorityQueue):

    def has_task_ready(self):
        if not self.queue:
            return False

        (timetorun, task) = self.queue[0]
        return timetorun < time.time()

def queue_worker():
    global lasttick
    while True:
        lasttick = time.time()
        if not queue.has_task_ready():
            time.sleep(1)
            continue

        (timetorun, task) = queue.get()
        print len(queue.queue), task
        try:
            task.perform()
        except:
            print "ERROR:", sys.exc_info()
            traceback.print_tb(sys.exc_info()[2])
        queue.task_done()

class RecalculatePosts:

    def __init__(self, posts):
        self._posts = posts

    def perform(self):
        for post in self._posts:
            post.recalculate()

    def __repr__(self):
        return "[RecalculatePosts %s]" % len(self._posts)
        
class CheckFeed:

    def __init__(self, feed):
        self._feed = feed

    def perform(self):
        new_posts = feeddb.read_feed(self._feed.get_url())
        if new_posts:
            queue.put((0, RecalculatePosts(new_posts)))
        self._feed.set_check_task(False)

    def __repr__(self):
        return "[CheckFeed %s]" % self._feed.get_url()
        
class FindFeedsToCheck:

    def perform(self):
        for feed in feeddb.get_feeds():
            if feed.should_read() and not feed.has_check_task():
                when = 0
                if feed.get_error():
                    when = time.time() + 600
                queue.put((when, CheckFeed(feed)))
                feed.set_check_task(True)

        queue.put((time.time() + 60, self))

    def __repr__(self):
        return "[FindFeedsToCheck]"

class SortPosts:

    def perform(self):
        feeddb.sort()
        queue.put((time.time() + 30, self))

    def __repr__(self):
        return "[SortPosts]"

queue = PeekablePriorityQueue()
queue.put((0, FindFeedsToCheck()))
queue.put((0, SortPosts()))

def start_queue_worker():
    thread = threading.Thread(target = queue_worker, name = "FeedReader")
    thread.start()
    return thread
    
# ----- SETUP
    
# set up temporary storage for vectors and descriptions
try:
    os.unlink("cache.dbm.db")
except OSError, e:
    if e.errno != 2:
        raise
cache = shelve.open("cache.dbm")

# we need to do this so that we don't hang for too long waiting for feeds
import socket
socket.setdefaulttimeout(20)

wzfactory = WhazzupFactory()
feeddb = get_feeds()
controller = DiskController()
feedlib.feeddb = feeddb # let's call it dependency injection, so it's cool

# ----- STARTING THREAD

import threading
thread = None
for t in threading.enumerate():
    if t.name == "FeedReader":
        thread = t

if not thread:
    print "Starting thread"
    thread = start_queue_worker()
else:
    print "Thread already running, not starting"
