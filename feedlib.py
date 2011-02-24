
import time, vectors, operator, string, math, formatmodules, HTMLParser, rsslib, codecs, traceback, cgi, chew, datetime
from xml.sax import SAXException

# --- Constants

START_VOTES = 5
TIME_TO_WAIT = 3600 * 3 # 3 hours

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

def parse_date(datestring):
    if datestring:
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
                return datetime.datetime.strptime(datestring[ : l], format)
            except ValueError:
                pass

        print "CAN'T PARSE:", repr(datestring)

    return datetime.utcnow()

# --- Controller

class Controller:
    "A mediator between the UI and the backend."

    def in_appengine(self):
        return False

    def get_queue_delay(self):
        return 0

    def recalculate_all_posts(self):
        pass

# --- Model

class Database:

    # backend-specific

    def get_item_range(self, low, high):
        raise NotImplementedError()
    
    def get_word_ratio(self, word):
        raise NotImplementedError()

    def record_word_vote(self, word, vote):
        raise NotImplementedError()
    
class Post:
    
    # shared code
    
    def get_age(self):
        age = time.time() - self.get_date().toordinal()
        if age < 0:
            age = 3600
        return age

    def get_word_probability(self):
        probs = []
        for (word, count) in self.get_vector().get_pairs():
            for ix in range(count):
                ratio = feeddb.get_word_ratio(word)
                probs.append(ratio)

        try:
            if not probs:
                return 0.5 # not sure how this could happen, though
            else:
                return compute_bayes(probs)
        except ZeroDivisionError, e:
            print "ZDE:", self.get_title().encode("utf-8"), probs            
    
    def get_author_vector(self):
        return vectors.text_to_vector(html2text(self.get_author() or ""))
        
    def get_overall_probability(self):
        word_prob = self.get_word_probability()
        site_prob = self.get_site_probability()
        author_prob = self.get_author_probability()
        return compute_bayes([word_prob, site_prob, author_prob])

    def get_url_tokens(self):
        tokens = self.get_link().split("/")
        end = -1
        if not tokens[-1]: # if url of form http://site/foo/bar/
            end = -2
        tokens = tokens[2 : end]
        return string.join(["url:" + t for t in tokens if chew.acceptable_term(t)])

    def get_site_probability(self):
        return self.get_site().get_ratio()
        
    def get_author_probability(self):
        author = self.get_author()
        if author:
            author = string.strip(string.lower(author))
            return feeddb.get_author_ratio(author)
        else:
            return 0.5
        
    def recalculate(self):
        try:
            prob = self.get_overall_probability()
            self._points = (prob * 1000.0) / math.log(self.get_age())
        except ZeroDivisionError, e:
            #print "--------------------------------------------------"
            print self.get_title().encode("utf-8")
            self._points = 0
    
    def get_points(self):
        return self._points

    def is_seen(self):
        return feeddb.is_link_seen(self.get_guid())

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
        # the UI takes care of queueing a recalculation task

    def get_word_tokens(self):
        probs = []
        for (word, count) in self.get_vector().get_pairs():
            for ix in range(count):
                ratio = feeddb.get_word_ratio(word)
                probs.append("%s : %s" % (escape(word), ratio))
        return ", ".join(probs)

    def get_vector(self):
        html = (self.get_title() or "") + " " + (self.get_description() or "")
        text = html2text(html) + " " + self.get_url_tokens()
        vector = vectors.text_to_vector(text, {}, None, 1)
        return vector
