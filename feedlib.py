
import time, vectors, operator, string, math, formatmodules, HTMLParser, rsslib
import codecs, traceback, cgi, chew, datetime, random
from xml.sax import SAXException

# --- Constants

START_VOTES = 5
TIME_TO_WAIT = 3600 * 3 # 3 hours

# --- Utilities

def nice_time(secs):
    if secs > 86400:
        return "%s days" % (secs / 86400)
    if secs > 3600:
        if secs < (3600 * 3) and ((secs % 3600) / 60) > 0:
            return "%s hours %s mins" % (secs / 3600, (secs % 3600) / 60)
        else:
            return "%s hours" % (secs / 3600)
    if secs > 60:
        return "%s minutes" % (secs / 60)
    return "%s seconds" % secs

def html2text(str):
    # FIXME: maybe this is the cause of some of the HTML showing up here and
    # there? maybe I'm the one who doesn't process the text correctly? let's
    # check later on...
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

def strptime(str, format):
    # this doesn't work on the server, because of missing strptime method
    # return datetime.datetime.strptime(str, format)
    return datetime.datetime(*(time.strptime(str, format)[0:6]))

def parse_date(datestring):
    if datestring:
        # there's a particular Russian format that needs to be supported:
        # u'\u0441\u0431, 13 feb 2010 15:51:30 +0300' (issue 35)
        # all Cyrillic characters in range 0x0400-0x04FF
        if (len(datestring) == 30 and
            0x0400 <= ord(datestring[0]) <= 0x04FF and
            0x0400 <= ord(datestring[1]) <= 0x04FF and
            datestring[2] == ','):
            datestring = datestring[4 : -6] # cutoff, so strptime can handle
        
        formats = [("%a, %d %b %Y %H:%M:%S", 24),
                   ("%Y-%m-%dT%H:%M:%S", 19),
                   ("%a, %d %b %Y %H:%M", 22),
                   ("%Y-%m-%d", 10),

                   #Sun Jan 16 15:55:53 UTC 2011
                   ("%a %b %d %H:%M:%S +0000 %Y", 30),

                   #Sun, 16 January 2011 07:13:33
                   ("%a, %d %B %Y %H:%M:%S", 29),

                   # 06 Jul 2012 17:28:55 +0400
                   ("%d %b %Y %H:%M:%S", 20),
                   ]
        for (format, l) in formats:
            try:
                return strptime(datestring[ : l], format)
            except ValueError:
                pass

        print "CAN'T PARSE:", repr(datestring)

    return datetime.datetime.utcnow()

def toseconds(timestamp):
    return time.mktime(timestamp.timetuple())

def calculate_points(prob, postdate):
    age = time.time() - toseconds(postdate)
    if age < 0:
        age = 3600
    return (prob * 1000.0) / math.log(age)

def compute_max_posts(site):
    "Given an rsslib.Site, computes max number of posts to store."
    
    if not site.get_items():
        return 300
    
    oldest = site.get_items()[-1]
    dt = parse_date(oldest.get_pubdate())
    delta = time.time() - toseconds(dt)
    count = len(site.get_items())
    return int(max(min((count / (delta / 3600)) * 24 * 7 * 8, 300), 30))

def generate_password():
    alphabet = string.letters + string.digits
    return "".join([random.choice(alphabet) for ix in range(8)])

# --- Controller

class Controller:
    "A mediator between the UI and the backend."

    def vote_received(self, user, id, vote):
        raise NotImplementedError()

    def mark_as_read(self, user, ids):
        raise NotImplementedError()
    
    def add_feed(self, url):
        raise NotImplementedError()

    def in_appengine(self):
        return False

    def is_single_user(self):
        return True

    def get_queue_delay(self):
        return 0

    def recalculate_all_posts(self, user):
        raise NotImplementedError()

    def unsubscribe(self, feedid, user):
        raise NotImplementedError()

    def send_user_password(self, username, email, password):
        raise NotImplementedError()

    def mark_as_read(self, user, ids):
        # completely generic implementation, so...
        links = [user.get_rated_post_by_id(postid) for postid in ids]
        for link in links:
            link.seen()

# --- Model

class Database:

    def add_feed(self, url):
        raise NotImplementedError()

    def get_feed_by_id(self, id):
        raise NotImplementedError()
    
    def get_item_by_id(self, id):
        raise NotImplementedError()
    
class Feed:

    def get_local_id(self):
        "String: internal ID of the feed."
        raise NotImplementedError()

    def get_title(self):
        raise NotImplementedError()

    def get_url(self):
        "String: the URL of the XML feed."
        raise NotImplementedError()
    
    def get_link(self):
        "String: the URL of the feed's home page."
        raise NotImplementedError()

    def get_error(self):
        """String: the text of the error message received when last
        attempting to load this feed."""
        raise NotImplementedError()

    def get_time_to_wait(self):
        "Seconds to wait between each time we poll the feed."
        raise NotImplementedError()
    
    def time_since_last_read(self):
        "Seconds since last read."
        raise NotImplementedError()
    
    def get_item_count(self):
        raise NotImplementedError()

    def get_items(self):
        raise NotImplementedError()
            
    def is_being_read(self):
        raise NotImplementedError()

    def is_subscribed(self, user):
        raise NotImplementedError()

    def get_subscribers(self):
        raise NotImplementedError()

    def get_max_posts(self):
        raise NotImplementedError()

    def set_max_posts(self, maxposts):
        raise NotImplementedError()
    
    # shared code

    def nice_time_since_last_read(self):
        return nice_time(int(self.time_since_last_read()))
    
class Post:

    def __init__(self):
        self._date = None
    
    def get_local_id(self):
        raise NotImplementedError()

    def get_title(self):
        raise NotImplementedError()

    def get_link(self):
        raise NotImplementedError()
    
    def get_description(self):
        raise NotImplementedError()
    
    def get_pubdate(self):
        raise NotImplementedError()

    def get_site(self):
        raise NotImplementedError()

    def get_guid(self):
        raise NotImplementedError()

    def get_author(self):
        raise NotImplementedError()

    def delete(self):
        raise NotImplementedError()

    def is_seen(self, user):
        "Returns true iff the link has been seen by the user."
        raise NotImplementedError()
    
    # shared code

    def get_date(self):
        "Returns a datetime object representing the publication date."
        if not self._date:
            self._date = parse_date(self.get_pubdate())
        return self._date

    def get_age(self):
        age = time.time() - toseconds(self.get_date())
        if age < 0:
            age = 3600
        return age

    def nice_age(self):
        return nice_time(int(self.get_age()))
    
    def get_author_vector(self):
        return vectors.text_to_vector(html2text(self.get_author() or ""))

    def get_url_tokens(self):
        tokens = self.get_link().split("/")
        end = -1
        if not tokens[-1]: # if url of form http://site/foo/bar/
            end = -2
        tokens = tokens[2 : end]
        return string.join(["url:" + t for t in tokens if chew.acceptable_term(t)])

    def get_vector(self):
        html = (self.get_title() or "") + " " + (self.get_description() or "")
        text = html2text(html) + " " + self.get_url_tokens()
        vector = vectors.text_to_vector(text, {}, None, 1)
        return vector

class User:

    def get_username(self):
        "Returns user name as a string."
        raise NotImplementedError()
    
    def record_word_vote(self, word, vote):
        self._get_word_db().record_vote(word, vote)

    def record_author_vote(self, author, vote):
        self._get_author_db().record_vote(author, vote)

    def record_site_vote(self, site, vote):
        self._get_site_db().record_vote(site, vote)

    def get_word_ratio(self, word):
        worddb = self._get_word_db()
        return worddb.get_word_ratio(word)
    
    def get_author_ratio(self, word):
        authordb = self._get_author_db()
        return authordb.get_word_ratio(word)

    def get_feeds(self):
        "Return the feeds the user is subscribed to, sorted by voting ratio."
        raise NotImplementedError()
    
    def get_item_count(self):
        raise NotImplementedError()
        
    def get_item_range(self, low, high):
        raise NotImplementedError()

    def get_rated_post_by_id(self, id):
        raise NotImplementedError()

    def get_no_of_item(self, item):
        raise NotImplementedError()
    
    def commit(self):
        raise NotImplementedError()

    def get_subscription(self, feedid):
        raise NotImplementedError()
    
    def subscribe(self, feed):
        raise NotImplementedError()

    def get_vote_stats(self):
        "Returns a tuple (upvotes, downvotes)."
        raise NotImplementedError()
    
    def _get_word_db(self):
        raise NotImplementedError()

    def _get_author_db(self):
        raise NotImplementedError()

    def _get_site_db(self):
        raise NotImplementedError()

class Subscription:

    def get_feed(self):
        raise NotImplementedError()

    def get_user(self):
        raise NotImplementedError()

    def get_ratio(self):
        raise NotImplementedError()

    def get_nice_ratio(self):
        "Return ratio as nicely formatted string."
        return str(self.get_ratio())[ : 4]

    def unsubscribe(self):
        raise NotImplementedError()

    def get_rated_posts(self):
        raise NotImplementedError()

    def record_vote(self, vote):
        raise NotImplementedError()
    
class RatedPost:

    def __init__(self, user, post, subscription, points = None, prob = None):
        self._post = post
        self._subscription = subscription
        self._user = user
        self._points = points
        self._prob = prob

    def get_post(self):
        return self._post

    def get_subscription(self):
        return self._subscription
        
    def get_points(self):
        if self._points is None:
            self.recalculate()
        return self._points

    def get_stored_points(self):
        return self._points

    def set_points(self, points):
        self._points = points

    def seen(self):
        "Marks the link as seen."
        raise NotImplementedError()
    
    def get_word_probability(self):
        probs = []
        for (word, count) in self._post.get_vector().get_pairs():
            ratio = self._user.get_word_ratio(word)
            for ix in range(count):                
                probs.append(ratio)

        try:
            if not probs:
                return 0.5 # not sure how this could happen, though
            else:
                return compute_bayes(probs)
        except ZeroDivisionError, e:
            print "ZDE:", self._post.get_title().encode("utf-8"), probs
    
    def get_overall_probability(self):
        if self._prob is not None:
            return self._prob
        
        word_prob = self.get_word_probability()
        site_prob = self.get_site_probability()
        author_prob = self.get_author_probability()
        self._prob = compute_bayes([word_prob, site_prob, author_prob])
        return self._prob
    
    def get_site_probability(self):
        return self.get_subscription().get_ratio()
        
    def get_author_probability(self):
        author = self._post.get_author()
        if author:
            author = string.strip(string.lower(author))
            return self._user.get_author_ratio(author)
        else:
            return 0.5
        
    def recalculate(self):
        try:
            prob = self.get_overall_probability()
            self._points = calculate_points(prob, self._post.get_date())
        except ZeroDivisionError, e:
            #print "--------------------------------------------------"
            print self._post.get_title().encode("utf-8")
            self._points = 0

    def record_vote(self, vote):
        if vote == "read":
            return # should never happen
        
        for (word, count) in self._post.get_vector().get_pairs():
            for i in range(count):
                self._user.record_word_vote(word, vote)
        author = self._post.get_author()
        if author:
            author = string.strip(string.lower(author)) # move into User
            self._user.record_author_vote(author, vote)

        self._user.record_site_vote(self._post.get_site().get_link(), vote)
        self._user.commit()
        
        # the controller takes care of queueing a recalculation task

    def get_word_tokens(self):
        probs = []
        for (word, count) in self._post.get_vector().get_pairs():
            for ix in range(count):
                ratio = self._user.get_word_ratio(word)
                probs.append("%s : %s" % (escape(word), ratio))
        return ", ".join(probs)

    def age(self):
        raise NotImplementedError()
    
    def is_seen(self):
        "Returns true iff the link has been seen by the user."
        return self._post.is_seen(self._user)
        
# --- Word database

# FIXME: this needs to be properly generalized

class WordDatabase:

    def __init__(self, words):
        self._words = words # a dict, of some kind, possibly a dbm

    def get_word_ratio(self, word):
        (good, bad) = self._get_object(word)
        return float(good + START_VOTES) / (good + bad + START_VOTES * 2)

    def get_word_stats(self, word):
        return self._get_object(word)

    def record_vote(self, theword, vote):
        (good, bad) = self._get_object(theword)
        if vote == "up":
            good += 1
        else:
            bad += 1
        self._put_object(theword, (good, bad))

    def change_word(self, oldword, newword):
        ratio = self.get_word_ratio(oldword)
        del self._words[oldword.encode("utf-8")]
        self._put_object(theword, ratio)

    def _get_object(self, key):
        return self._words.get(key, (0, 0))

    def _put_object(self, key, ratio):
        self._words[key] = ratio

    def close(self):
        raise NotImplementedError()

