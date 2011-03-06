# -*- coding: iso-8859-1 -*-
"""Modules containing language-specific logic."""

import string, codecs, os

DEBUG = 0
PATH = os.path.split(__file__)[0]

# --- General language module interface

class LanguageModule:

    def __init__(self):
        self._stops = None
        self._classes = None
        self._freq = None
        
    def is_stop_word(self, word):
        "Returns true/false."
        if self._stops is None:
            self._stops = self._load_stop_words()
        return self._stops.has_key(string.lower(word))

    def clean_term(self, word):
        "Removes obviously unsuitable grammatical inflections from word."
        return word

    def get_stem(self, word):
        "Returns stem of word."
        return word

    def get_word_class(self, word):
        "Returns single character representing class of word, or None."
        if self._classes is None:
            self._classes = self._load_word_classes()
        wc = self._classes.get(word)
        if not wc:
            wc = self._classes.get(self.get_stem(word))
        return wc

    def get_frequency_factor(self, word):
        """Returns a number to adjust the score by based on word frequency.
        Adjusted score = score * factor."""
        if self._freq is None:
            self._freq = self._load_frequency_table()
        return self._freq.get(word, 1)

    def _load_stop_words(self):
        return {}

    def _load_word_classes(self):
        return {}

    def _load_frequency_table(self):
        return {}
    
# --- Norwegian language module

class NorwegianModule(LanguageModule):

    def get_stem(self, word):
        return stem(string.lower(word)) # function defined below
    
    def _load_stop_words(self):
        stop = {}
        for line in codecs.open(PATH + os.sep + "stopno.txt", "r", "iso-8859-1").readlines():
            stop[string.strip(line)] = 1
        return stop

    def _load_frequency_table(self):
        import marshal
        inf = open(PATH + os.sep + "data/nofreq.mar", "rb")
        map = marshal.load(inf)
        inf.close()
        return map

    # no word class support

# --- English language module

class EnglishModule(LanguageModule):

    def __init__(self):
        LanguageModule.__init__(self)
        import porter
        self._stemmer = porter.PorterStemmer()

    def get_stem(self, word):
        stem = self._stemmer.stem(string.lower(word))
        # stemming foo's gives "foo'". need to remove the '
        if stem[-1] == "'":
            return stem[ : -1]
        else:
            return stem

    def clean_term(self, word):
        if word[-2 : ] == "'s":
            return word[ : -2]
        return word

    def _load_stop_words(self):
        stop = {}
        for line in open(PATH + os.sep + "stop.txt").readlines():
            stop[string.strip(line)] = 1
        return stop

    def _load_word_classes(self):
        import marshal
        inf = open(PATH + os.sep + "data/wcen.mar", "rb")
        wc = marshal.load(inf)
        inf.close()
        return wc

    def _load_frequency_table(self):
        import marshal
        inf = open(PATH + os.sep + "data/enfreq.mar", "rb")
        map = marshal.load(inf)
        inf.close()
        return map

# --- Norwegian stemmer

def sort(list, keyfunc):
    list = map(lambda x, y=keyfunc: (y(x), x), list)
    list.sort()
    return map(lambda x: x[1], list)

def swap(pair):
    return (pair[1], pair[0])

vowels = u"aeiouyæåø"

suffixes_1 = ["a", "e", "ede", "ande", "ende", "ane", "ene", "hetene", "en",
              "heten", "ar", "er", "heter", "as", "es", "edes", "endes", "enes",
              "hetenes", "ens", "hetens", "ers", "ets", "et", "het", "ast", "s",
              "erte", "ert"]

suffixes_1 = sort(suffixes_1, len)
suffixes_1.reverse()

suffixes_3 = ["leg", "eleg", "ig", "eig", "lig", "elig", "els", "lov", "elov",
              "slov", "hetslov"]
suffixes_3 = sort(suffixes_3, len)
suffixes_3.reverse()

def stem(word):
    # step 1: find R1
    ix = 0
    while ix < len(word) and not word[ix] in vowels:
        ix = ix + 1 # scan for vowel
    ix = ix + 1

    while ix < len(word) and word[ix] in vowels:
        ix = ix + 1 # scan for consonant

    if ix >= len(word):
        return word

    r1start = ix
        
    # step 2: remove suffixes (1abc)
    r1 = word[r1start : ]
    for suffix in suffixes_1:
        if r1[-len(suffix) : ] == suffix:
            if suffix == "s": #1b
                if (len(r1) >= 2 and r1[-2] in "bcdfghjlmnoprtvyz") or \
                   (len(r1) >= 3 and r1[-2] == "k" and r1[-3] not in vowels):
                    word = word[ : -len(suffix)]
                    r1 = r1[ : -len(suffix)]
                else:
                    continue
            elif suffix == "erte" or suffix == "ert":
                word = word[ : 2-len(suffix)]
                r1 = r1[ : 2-len(suffix)]
            else: #1a
                word = word[ : -len(suffix)]
                r1 = r1[ : -len(suffix)]
            break
        
    # step 3: remove more suffixes (2)
    if r1[-2 : ] in ["dt", "vt"]:
        word = word[ : -1]
        r1 = r1[ : -1]

    # step 4: remove even more suffixes (3)
    for suffix in suffixes_3:
        if r1[-len(suffix) : ] == suffix:
            word = word[ : -len(suffix)]
            r1 = r1[ : -len(suffix)]
    
    return word

# Stemmer test

def test():
    inlist = []
    outlist = []

    inf = open("nostemtest.txt")
    line = string.strip(inf.readline())
    while line:
        inlist.append(line)
        line = string.strip(inf.readline())

    line = string.strip(inf.readline())
    while line:
        outlist.append(line)
        line = string.strip(inf.readline())

    inf.close()

    assert len(inlist) == len(outlist)
    correct = 0
    for ix in range(len(inlist)):
        stemmed = stem(inlist[ix])
        if stemmed == outlist[ix]:
            correct = correct + 1
            msg = "OK"
        else:
            msg = "WRONG"
            
        print msg, inlist[ix], ":", outlist[ix], "||", stemmed

    print correct, "/", len(inlist)

# --- Final setup

en = EnglishModule()
no = NorwegianModule()
modules = [en, no]
    
def percent_of_terms(termlist, lang):
    count = 0
    for term in termlist:
        if lang.is_stop_word(term):
            count = count + 1
    return count

def get_language_module(termlist):
    """termlist is a list of terms from the text; return value is a
    language module for that language"""

    best = LanguageModule() # no-op dummy, avoids crashes later
    high = 0

    for module in modules:
        score = percent_of_terms(termlist, module)
        if DEBUG:
            print module, score
        if score > high:
            best = module
            high = score

    return best
