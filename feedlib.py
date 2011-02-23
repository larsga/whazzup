
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

class Post:

    def get_age(self):
        age = time.time() - self.get_date().toordinal()
        if age < 0:
            age = 3600
        return age

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
