# -*- coding: iso-8859-1 -*-

import sys, string, math, operator, re, os, urllib
import gazette        

# &#8217;

# Blacklist approach
# Metadata extraction

# Support for boosting scores of terms within certain tags
# Make use of case information?

# Use of topic map must become more sophisticated
#   typing topics should get penalties, not boosts
#   *instances* of mentioned topic types should get extra boosts

# rapport6-03.txt comes out with bizarre results
# Problems: doesn't allow stop words in compound terms (sogn og fjordane)
# Problems: sentence structure not made use of
# Problems: in follower tracking, punctuation is ignored

use_word_classes = 1
use_concept_net = 0
use_gazette = 0
use_frequencies = 1
use_topic_map = 0

DESCENDING = 0
COMPOUND_SCORE_FACTOR = 0.6
REPORT_TERMS = 40
REPORT_LOWEST = 0.03

# --- Utilities

def sort(list, keyfunc):
    list = map(lambda x, y=keyfunc: (y(x), x), list)
    list.sort()
    return map(lambda x: x[1], list)

def swap(pair):
    return (pair[1], pair[0])

# --- Compound tracking

class CompoundTracker:

    def __init__(self):
        self._compounds = {}
        self.skip()
        
    def skip(self):
        self._previous = CompoundCandidate("", "") # dummy

    def track(self, term, stem):
        self._previous.followed_by(stem)
        c = self._compounds.get(term)
        if not c:
            c = CompoundCandidate(stem, term)
            self._compounds[term] = c
        self._previous = c

    def form_compounds(self, terms):
        prev = 0
        for compound in self._compounds.values():
            test = compound.get_stem()
            fraction = "whoops"

            included = {}    
            new = None
            f = compound.get_follower(terms)
            while f:
                term2 = terms.get_term_by_stem(f)
                if not term2 or included.has_key(term2):
                    break # if already merged with a follower
                compound2 = self._compounds.get(term2.get_preferred())
                if not compound2:
                    break

                term = terms.get_term_by_stem(compound.get_stem())
                if not term:
                    break

                included[term2] = 1
                if term2.get_occurrences():
                    # FIXME: here we assume that the term must have
                    # more occurrences; if it doesn't, the point
                    # calculation gets screwed up
                    fraction = (compound.get_follower_occurrences(f) / 
                                float(term2.get_occurrences()))
                    fraction = min(1.0, fraction) # FIXME: workaround
                else:
                    fraction = 0
                score = (term.get_score() + term2.get_score() * fraction) * \
                        COMPOUND_SCORE_FACTOR
                new = Term(term.get_preferred() + " " + term2.get_preferred(),
                           score, compound.get_follower_occurrences(f))
                newc = CompoundCandidate(new.get_term(), new.get_term(),
                                         compound2.get_followers(),
                                         compound.get_follower_occurrences(f))

                terms.remove_term(term)

                term2.set_score(term2.get_score() * (1 - fraction))

                # preserve variants
                for variant in term2.get_variants():
                    new.occurred_as(term.get_preferred() + " " + variant)

                # to make the loop work
                f = compound2.get_follower(terms)
                compound = newc
                terms.add_term(new)

class CompoundCandidate:

    def __init__(self, stem, term, followers = None, occurrences = 0):
        self._stem = stem
        self._term = term
        self._followers = followers or {} # contains stems, not terms
        self._occurrences = occurrences
        self._test = 0
        
    def get_stem(self):
        return self._stem

    def get_term(self):
        return self._term
    
    def followed_by(self, term): # really a stem
        self._occurrences += 1
        self._followers[term] = self._followers.get(term, 0) + 1

    def print_followers(self):
        total = float(reduce(operator.add, self._followers.values(), 0))
        items = map(swap, self._followers.items())
        items.sort()
        items = map(swap, items)
        print "---%s (%s)" % (self._term, limit(self._occurrences))
        for (term, value) in items:
            print u"%30s %5s %5s" % (term, value, value / total)

    def get_follower(self, terms):
        smallest = 4
        if use_gazette and gazette.is_given_name(self._term):
            #self.print_followers()
            smallest = 1
        if self._occurrences < smallest:
            return None

        highest = limit(self._occurrences)
        best = None
        for (term, times) in self._followers.items():
            if smallest == 1: # FIXME: ugly reliance on above...
                realterm = terms.get_term_by_stem(term)
                if realterm and gazette.may_be_name(realterm.get_preferred()):
                    times *= 4
               
            if times / self._occurrences > highest:
                best = term
                highest = times / self._occurrences
        return best

    def get_followers(self):
        return self._followers

    def get_follower_occurrences(self, follower):
        return self._followers.get(follower, 0)

# --- Collocator

class Collocator:

    def __init__(self, lang):
        self._pairs = {} # ("foo", "bar") -> occurrences
        self._previous = None
        self._lang = lang

    def found(self, term): # unstemmed, unfiltered
        if self._previous:
            pair = (self._previous, term)
            self._pairs[pair] = self._pairs.get(pair, 0) + 1
        self._previous = term

    def skip(self):
        self._previous = None

    def print_stats(self):
        pairs = sort(self._pairs.items(), lambda x: x[1])
        pairs.reverse()
        ix = 0
        for (pair, count) in pairs:
            if self._lang.is_stop_word(pair[0]) or \
               self._lang.is_stop_word(pair[1]):
                continue

            print "%40s %s" % (pair, count)            
            ix += 1
            if ix > 100:
                break

# --- Term

def limit(total):
    if total == 1:
        # this effectively requires name-based compound forming when only
        # one occurrence. necessary to avoid totally random compounds
        return 1.01
    return 0.64 - (math.log(total) / 15.0)

class Term:

    def __init__(self, term, score = 0, occurrences = 1, type = gazette.UNKNOWN):
        self._term = term
        self._score = score
        self._occurrences = occurrences
        self._variants = {}
        self._type = type

    def merge(self, other):
        self._score += other.get_score()
        self._occurrences += other.get_occurrences()
        for var in other.get_variants():
            self._variants[var] = self._variants.get(var, 0) + \
                                  other._variants[var]
        if self._type == gazette.UNKNOWN:
            self._type = other.get_type()

    def found(self, score):
        self._occurrences = self._occurrences + 1
        self._score = self._score + score

    def occurred_as(self, variant):
        self._variants[variant] = self._variants.get(variant, 0) + 1

    def get_preferred(self):
        if not self._variants:
            return self._term
        
        items = map(swap, self._variants.items())
        items.sort()
        return items[-1][1]

    def get_score(self):
        return self._score

    def get_term(self):
        return self._term

    def get_occurrences(self):
        return self._occurrences

    def get_variants(self):
        return self._variants.keys()
    
    def set_score(self, score):
        self._score = score

    def add_score(self, score):
        self._score = score + self._score

    def get_type(self):
        return self._type

    def set_type(self, type):
        self._type = type

# --- Term list

class TermDatabase:

    def __init__(self):
        self._terms = {}

    def merge(self, term1, term2):
        term1.merge(term2)
        self.remove_term(term2)        
        
    def get_terms(self):
        return self._terms.values()

    def get_sorted_terms(self):
        termlist = sort(self._terms.values(), Term.get_score)
        termlist.reverse()
        return termlist
        
    def get_term_by_stem(self, stem):
        return self._terms.get(stem)
        
    def get_term(self, term, stem):
        t = self._terms.get(stem)
        if not t:
            t = Term(stem)
            self.add_term(t)
        t.occurred_as(term)
        return t

    def get_max_score(self):
        high = 0
        for term in self._terms.values():
            high = max(term.get_score(), high)
        return float(high)    
    
    def add_term(self, term):
        self._terms[term.get_term()] = term

    def remove_term(self, term):
        del self._terms[term.get_term()]

    def print_report(self, terms = 20):
        termlist = self.get_sorted_terms()
        high = self.get_max_score()
        for term in termlist[ : terms]:
            name = term.get_preferred()
            template = "%30s %15s  %5s"
            points = term.get_score() / high
            if points < REPORT_LOWEST:
                break
            #template = "%s;%s"
            str = template % (name,
                              gazette.reverse[term.get_type()],
                              points)
            print str.encode("utf-8")
            #print "  (%s)" % string.join(term.get_variants(), ", ")
        
# --- Term extraction

def extract_terms(text):
    terms = []
    for orgterm in string.split(text):
        term = orgterm
        while term and term[0] in u"\\<'(\"[ {\xb7-%\u201c\u2018\u00AB\u201d":
            term = term[1 : ]
        while term and term[-1] in u"\\>').,\"':;!]? |}*\xb7-%\u201d\u2019\u00BB":
            term = term[ : -1]

        if term:
            terms.append(term)
            if orgterm[-1] in ".,;:":
                # this gets discarded later, but helps us break up unwanted
                # compounds
                terms.append(orgterm[-1]) 

    return terms

# --- Word filtering

letters = u"abcdefghijklmnopqrstuvwxyzæøå" + u"ABCDEFGHIJKLMNOPQRSTUVWXYZÆØÅ"
    
def acceptable_term(term):
    if len(term) == 1:
        return 0
    
    for ch in term:
        if ch in letters:
            return 1
    return 0

# --- Apply topic map

try:
    from net.ontopia.topicmaps.utils import TopicStringifiers
    strify = TopicStringifiers.getDefaultStringifier()
except ImportError:
    use_topic_map = 0

def analyze_topic_map(file):
    try:
        from net.ontopia.topicmaps.utils import ImportExportUtils
    except ImportError, e:
        return {}

    import langmodules
    topics = {}
    tm = ImportExportUtils.getReader(file).read()
    for topic in tm.getTopics():
        for bn in topic.getBaseNames():
            #stem = string.lower(bn.getValue())
            stem = bn.getValue()
            stem = langmodules.en.get_stem(stem)
            topics[stem] = topic

    return topics

# load topic map
TMFILE = "/Users/larsga/cvs-co/topicmaps/pubsubj/xmlvoc/xmlvoc.ltm"
TMFILE = "/Users/larsga/tmp/oks-enterprise-3.0.2/apache-tomcat/webapps/omnigator/WEB-INF/topicmaps/xml_conference_papers.xtm"
topics = [] #analyze_topic_map(TMFILE)

# --- Third-party NLP tools

def topicmap_adjust(terms, lang, compounds):
    # merge synonyms based on TM information
    for term in terms.get_terms():
        topic = topics.get(term.get_term())
        if not topic:
            continue

        name = string.lower(strify.toString(topic))
        name = lang.get_stem(name)
        real = terms.get_term_by_stem(name)
        if not real or real == term:
            continue

        #print "Merging '%s' with '%s'" % (term.get_preferred(), real.get_preferred())
        terms.remove_term(term)
        del compounds[term.get_preferred()]
        real.add_score(term.get_score())
    
    # boost terms which appear in topic map
    for term in terms.get_terms():
        topic = topics.get(term.get_term())
        if not topic:
            continue

        #print term.get_preferred(), term.get_score(), strify.toString(topic)
        term.set_score((term.get_score()+1) * 3)

        # boost associated terms, too
        #for role in topic.getRoles():
        #    assoc = role.getAssociation()
        #    for role2 in assoc.getRoles():
        #        if role2 == role:
        #            continue
        #
        #        other = role2.getPlayer()
        #        name = string.lower(strify.toString(topic))
        #        name = lang.get_stem(name)
        #        otherterm = terms.get(name)
        #        if otherterm:
        #            otherterm.add_score(term.get_score() * 0.1)

def wordnet_adjust(terms, lang):
    "adjust terms by word class"
   
    wcfact = {
        "n" : 0.8,
        "a" : 0.05,
        "s" : 0.05,
        "v" : 0.1,
        "r" : 0.05
        }
    for term in terms.get_terms():
        if term.get_score() == 0:
            continue

        word = term.get_preferred()
        t = lang.get_word_class(word)
        if word == "skeptical":
            print word, term.get_score(), t
            if t:
                print term.get_score() * wcfact[t]
#         if t:
#             print word, term.get_score(), term.get_score() * wcfact[t]
#         else:
#             print word, term.get_score()
        if t:
            term.set_score(term.get_score() * wcfact[t])

def frequency_adjust(terms, lang):
    "adjust terms by word frequency"
   
    for term in terms.get_terms():
        word = string.lower(term.get_preferred())
        term.set_score(term.get_score() * lang.get_frequency_factor(word))
            
def gazette_adjust(terms):
    "Adjust terms using a gazette."
    
    for term in terms.get_terms():
        t = term.get_preferred()
        type = gazette.classify(t)

        # person name handling
        if len(string.split(t)) == 2 and type == gazette.NAME_DEFINITE:
            (given, family) = string.split(t)
            if gazette.classify(given) in (gazette.NAME_GIVEN, gazette.NAME_DEFINITE) and \
               gazette.classify(family) in (gazette.NAME_FAMILY, gazette.NAME_DEFINITE):
                other = terms.get_term(family, string.lower(family))
                #print "Merged:", repr(term.get_preferred()), repr(other.get_preferred())

                # last-name variant may now turn out to be dominant,
                # but we don't want that, so set all variants of other
                # to be just 0
                for var in other.get_variants():
                    other._variants[var] = 0
                
                terms.merge(term, other)
                term.set_type(gazette.PERSON)
        else:
            if type != gazette.UNKNOWN:
                if type in (gazette.NAME_DEFINITE,
                            gazette.NAME_GIVEN,
                            gazette.NAME_FAMILY):
                    type = gazette.PERSON
                term.set_type(type)

        # kill unusable terms
        if term.get_type() in gazette.UNUSABLE:
            terms.remove_term(term)
        
def conceptnet_adjust(terms):
    termlist = map(lambda term: (term.get_preferred(), term.get_score()),
                   terms.get_terms())
    # termlist = [(phrase, score), ...]

    CNDIR = "/Users/larsga/Desktop/conceptnet2.1/"
    from ConceptNetDB import ConceptNetDB
    curdir = os.getcwd()
    os.chdir(CNDIR)
    cn = ConceptNetDB()
    os.chdir(curdir)

    FACTOR = 0.5
    high = terms.get_max_score()
    context = cn.get_context(termlist, textnode_list_weighted_p = 1)
    for (cnterm, cnscore) in context:
        term = terms.get_term_by_stem(cnterm)
        if term:
            term.add_score(high * cnscore)
        else:
            term = terms.get_term(cnterm, cnterm)
            term.set_score(high * cnscore)

# --- Term extractor

import langmodules

def rate_terms(text):
    # do actual term rating
    terms = TermDatabase()
    compounds = CompoundTracker()
    if use_gazette:
        tracker = gazette.GazetteTracker()
    ix = 1

    termlist = extract_terms(text)
    if not termlist:
        return terms
    lang = langmodules.get_language_module(termlist)
    collocator = Collocator(lang)

    high = math.log(len(termlist))
    for term in termlist:
        if not acceptable_term(term):
            if use_gazette:
                tracker.skip()
            compounds.skip()
            collocator.skip()
            continue
        
        stem = lang.get_stem(term)
        compounds.track(term, stem)
        collocator.found(term)
        if lang.is_stop_word(term):
            continue # compounds can track stop words (sogn og fjordane)

        term = lang.clean_term(term)
        t = terms.get_term(term, stem)
        if DESCENDING:
            t.found(high - math.log(ix))
        else:
            t.found(1)

        if use_gazette:
            tracker.track(t, term)

        ix = ix + 1

    if use_topic_map:
        topicmap_adjust(terms, lang, compounds)
    # FIXME: use TM to form compound terms

    #collocator.print_stats()
    compounds.form_compounds(terms)

    if use_word_classes:
        wordnet_adjust(terms, lang)
    if use_concept_net:
        conceptnet_adjust(terms)
    if use_gazette:
        gazette_adjust(terms)
    if use_frequencies:
        frequency_adjust(terms, lang)

    return terms

def process_file(filename):
    if filename[ : 7] == "http://":
        inf = urllib.urlopen(filename)
        out = open("/tmp/chew.txt", "w")
        out.write(inf.read())
        inf.close()
        out.close()
        filename = "/tmp/chew.txt"
    format = formatmodules.get_format_module(filename)
    text = format.get_text(filename)
    return rate_terms(text)

# --- Main program

import formatmodules

if __name__ == "__main__": 
    # parse HTML to extract text
    for file in sys.argv[1 : ]:
        print "=====", file
        process_file(file).print_report(REPORT_TERMS)

