import time, vectors, operator, string, dbm, math, formatmodules, HTMLParser, rsslib, sys, codecs, os, threading, traceback, cgi, chew
from xml.sax import SAXException

TIME_TO_WAIT = 3600 * 3 # 3 hours
START_VOTES = 5
CHECK_PERIOD = 100
MAX_STORIES = 8000

# --- Utilities
        
def html2text(str):
    str = string.replace(str, "&lt;", "<")
    str = string.replace(str, "&gt;", ">")
    str = string.replace(str, "&amp;", "&")
    ext = formatmodules.HTMLExtractor()
    try:
        ext.feed(str)
    except HTMLParser.HTMLParseError:
        pass # it's OK
    return ext.get_text()

def sort(list, keyfunc):
    list = map(lambda x, y=keyfunc: (y(x), x), list)
    list.sort()
    return map(lambda x: x[1], list)

def escape(str): # HTML escape
    return cgi.escape(str)

def attrescape(str): # HTML attribute escape
    return string.replace(escape(str), '"', '&quot;')

def compute_bayes(probs):
    product = reduce(operator.mul, probs)
    lastpart = reduce(operator.mul, map(lambda x: 1-x, probs))
    if product + lastpart == 0:
        return 0 # happens rarely, but happens
    else:
        return product / (product + lastpart)

def compute_average(probs):
    sum = reduce(operator.add, probs)
    return sum / float(len(probs))

# --- Model

class Feed(rsslib.SiteSummary):

    def __init__(self, url):
        rsslib.SiteSummary.__init__(self, url)
        self._last_read = 0
        self._being_read = 0
        self._time_to_wait = TIME_TO_WAIT # specified in seconds
        self._error = None

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

class Link(rsslib.Item):

    def __init__(self, site):
        rsslib.Item.__init__(self, site)
        self._date = None
        self._vector = None
        self._list = []
        self._points = None

    def get_age(self):
        age = time.time() - time.mktime(self.get_date())
        if age < 0:
            age = 3600
        return age

    def get_vector(self):
        if not self._vector:
            html = (self.get_title() or "") + " " + (self.get_description() or "")
            text = html2text(html) + " " + self.get_url_tokens()
            self._vector = vectors.text_to_vector(text, {}, None, 1)
        return self._vector

    def get_date(self):
        if not self._date:
            if self.get_pubdate():
                formats = [("%a, %d %b %Y %H:%M:%S", 24),
                           ("%Y-%m-%dT%H:%M:%S", 19),
                           ("%a, %d %b %Y %H:%M", 22),
                           ("%Y-%m-%d", 10),

                           #Sun Jan 16 15:55:53 UTC 2011
                           ("%a %b %d %H:%M:%S +0000 %Y", 30),

                           #Sun, 16 January 2011 07:13:33
                           ("%a, %d %B %Y %H:%M:%S", 29)]
                for (format, l) in formats:
                    try:
                        self._date = time.strptime(self.get_pubdate()[ : l],
                                                   format)
                        break
                    except ValueError:
                        pass

                if not self._date:
                    print "CAN'T PARSE:", self.get_pubdate()
                    self._date = time.gmtime()
            else:
                self._date = time.gmtime()
        return self._date
    
    def get_points(self):
        return self._points

    def get_word_probability(self):
        self._list = []
        probs = []
        for (word, count) in self.get_vector().get_pairs():
            for ix in range(count):
                ratio = feeddb.get_word_ratio(word)
                self._list.append("%s : %s" % (escape(word), ratio))
                probs.append(ratio)

        try:
            if not probs:
                return 0.5 # not sure how this could happen, though
            else:
                return compute_bayes(probs)
        except ZeroDivisionError, e:
            print "ZDE:", self.get_title().encode("utf-8"), probs            

    def get_site_probability(self):
        return self.get_site().get_ratio()

    def get_author_vector(self):
        return vectors.text_to_vector(html2text(self.get_author() or ""))
        
    def get_author_probability(self):
        author = self.get_author()
        if author:
            author = string.strip(string.lower(author))
            return feeddb.get_author_ratio(author)
        else:
            return 0.5
        
    def get_overall_probability(self):
        word_prob = self.get_word_probability()
        site_prob = self.get_site_probability()
        author_prob = self.get_author_probability()
        #prob = word_prob * word_prob * compute_bayes([site_prob, author_prob])
        prob = compute_bayes([word_prob, site_prob, author_prob])
        return prob
        
    def recalculate(self):
        self._list = []                
        try:
            prob = self.get_overall_probability()
            self._list.append("site: %s" % self.get_site_probability())
            self._list.append(str(prob))
            self._points = (prob * 1000.0) / math.log(self.get_age())
        except ZeroDivisionError, e:
            #print "--------------------------------------------------"
            print self.get_title().encode("utf-8")
            self._points = 0

    def is_seen(self):
        return feeddb.is_link_seen(self.get_guid())

    def get_local_id(self):
        return id(self)

    def record_vote(self, vote):
        feeddb.remove_item(self)

        if vote != "read":
            for (word, count) in self.get_vector().get_pairs():
                for i in range(count):
                    feeddb.record_word_vote(word, vote)
            author = self.get_author()
            if author:
                author = string.strip(string.lower(author)) # move into feeddb
                feeddb.record_author_vote(author, vote)

            feeddb.record_site_vote(self.get_site().get_link(), vote)
            feeddb.commit()
            
        feeddb.seen_link(self.get_guid())
        feeddb.recalculate()

    def get_url_tokens(self):
        tokens = self.get_link().split("/")
        end = -1
        if not tokens[-1]: # if url of form http://site/foo/bar/
            end = -2
        tokens = tokens[2 : end]
        return string.join(["url:" + t for t in tokens if chew.acceptable_term(t)])

    def get_word_tokens(self):
        return string.join(self._list, ", ")

class Word:

    def __init__(self, word):
        self._word = word
        self._good = START_VOTES
        self._bad = START_VOTES

    def get_ratio(self):
        return float(self._good) / (self._good + self._bad)

    def record_vote(self, vote):
        if vote == "up":
            self._good += 1
        else:
            self._bad += 1

class WordDatabase:

    def __init__(self, filename):
        self._words = {}
        self._dbm = dbm.open(filename, "c")

    def get_word_ratio(self, word):
        word = word.encode("utf-8")
        return self._get_object(word).get_ratio()

    def get_word_stats(self, word):
        word = self._get_object(word)
        return (word._good - START_VOTES, word._bad - START_VOTES)

    def record_vote(self, theword, vote):
        theword = theword.encode("utf-8")
        word = self._get_object(theword)
        word.record_vote(vote)
        self._dbm[theword] = "%s,%s" % (word._good, word._bad)

    def change_word(self, oldword, newword):
        ratio = self.get_word_ratio(oldword)
        word = oldword.encode("utf-8")
        del self._words[word]
        word = newword.encode("utf-8")
        self._words[word] = ratio

    def close(self):
        self._dbm.close()

    def _get_object(self, key):
        word = self._words.get(key)
        if not word:
            # not in the cache
            str = self._dbm.get(key)
            word = Word(key)
            if str:
                # it's on disk, so get from there
                self._words[key] = word
                (good, bad) = string.split(str, ",")
                word._good = int(good)
                word._bad = int(bad)
            else:
                # not on disk, so start afresh
                self._words[key] = word
        # else: this means it's in the cache
        #       will also be on disk if there's data in it
        return word

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

class FeedDatabase(rsslib.FeedRegistry):

    def __init__(self):
        rsslib.FeedRegistry.__init__(self)
        #self._feeds, inherited list of Feed objects
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
        self._last_recalc = 0
        try:
            self._faves = rsslib.read_rss("faves.rss", wzfactory)
        except IOError:
            self._faves = Feed("faves.rss")
            self._faves.set_title("Recent reading")
            self._faves.set_description("A feed of my favourite recent reads.")

    def init(self):
        for feed in self._feeds:
            url = feed.get_url()
            #print url
            self.read_feed(url, feed.get_format())

        self.recalculate()
        
    def recalculate(self):
        nowtime = time.time()
        print "Time until recalc:", nowtime - self._last_recalc
        if nowtime - self._last_recalc < 120:
            return
        self._last_recalc = nowtime
        
        try:
            self._lock.acquire()
            for item in self._items:
                item.recalculate()
            self._items = sort(self._items, Link.get_points)
            self._items.reverse()

            print "Items before:", len(self._items)
            try:
                ix = len(self._items) - 1
                while ix >= 0 and len(self._items) > MAX_STORIES:
                    if self._items[ix].get_age() > (86400 * 2):
                        del self._items[ix]
                    ix -= 1
            except:
                import traceback
                traceback.print_tb(sys.exc_info()[2])
            print "Items after:", len(self._items)
        finally:
            self._lock.release()

    def get_item_count(self):
        return len(self._items)

    def get_item_no(self, ix):
        return self._items[ix]

    def get_item_by_id(self, id):
        return self._linkmap[id]

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
        return self._feedmap[id]

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
            outf.write("%s | %s | %s\n" %
                       (feed.get_url(),
                        feed.get_time_to_wait(),
                        feed.get_format()))
        outf.close()
        #outf = codecs.open("feeds.opml", "w", "utf-8")
        #rsslib.write_opml(self._feeds, outf, "utf-8")
        #outf.close()

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

    def read_feed(self, url, format):
        oldsite = self._feedurlmap.get(url)
        if oldsite:
            oldsite.being_read()
                
        try:
            if format == "rss":
                site = rsslib.read_rss(url, wzfactory)
            else:
                site = rsslib.read_atom(url, wzfactory)
        except:
            if oldsite:
                oldsite.not_being_read()
                oldsite.set_error(traceback.format_exc())
            traceback.print_exc()
            return None # we didn't get any feed, so no point in continuing
                
        site.set_format(format) # just to make sure

        items = site.get_items()
        items.reverse() # go through them from the back to get
                        # right order when added to oldsite
        
        try:
            try:
                self._lock.acquire()
                for item in items:
                    if oldsite and not self._linkguidmap.has_key(item.get_guid()):
                        # means we've read this feed before, but we don't have
                        # this particular item. so we move it across
                        item._site = oldsite
                        oldsite.add_item_to_front(item)

                    if not self._linkguidmap.has_key(item.get_guid()):
                        #print " ", (item.get_link() or "").encode("utf-8")
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
            site.now_read()
        self.recalculate()
        return site

class WhazzupFactory(rsslib.DefaultFactory):

    def make_site(self, url):
        return Feed(url)
    
    def make_item(self, site):
        return Link(site)

    def make_feed_registry(self):
        return FeedDatabase()

def start_feed_reader(feeddb):
    thread = threading.Thread(target = feed_reader, name = "FeedReader", args = (feeddb, ))
    thread.start()
    return thread
    
lastcheck = time.time()
def feed_reader(feeddb):
    global lastcheck
    
    while 1:
        lastcheck = time.time()
        #print "Checking for feeds to refresh"
        for feed in feeddb.get_feeds():
            lastcheck = time.time()
            if feed.should_read():
                #print "Reading", (feed.get_title() or
                #                  feed.get_url()).encode("utf-8")
                try:
                    feeddb.read_feed(feed.get_url(), feed.get_format())
                except:
                    print "ERROR:", sys.exc_info()

                #print feeddb, feeddb.get_item_count()

        lastcheck = time.time()
        time.sleep(CHECK_PERIOD)

def get_feeds():
    try:
        feeds = wzfactory.make_feed_registry()
        for line in open("feeds.txt").readlines():
            (url, time, format) = string.split(string.strip(line), " | ")
            time = int(time)
            feed = wzfactory.make_site(url)
            feed.set_time_to_wait(time)
            feed.set_format(format)
            feeds.add_feed(feed)

        start_feed_reader(feeds)
        return feeds

    except IOError, e:
        if e.errno == 2:
            return wzfactory.make_feed_registry()
        raise e

print "\n==================================================\nWE GOT IMPORTED\n=================================================="
wzfactory = WhazzupFactory()

thread = None
for t in threading.enumerate():
    if t.name == "FeedReader":
        thread = t
if not thread:
    print "Starting thread"
    feeddb = get_feeds()
    thread = start_feed_reader(feeddb)
else:
    print "Thread already running, not starting"
