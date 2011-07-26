# -*- coding: iso-8859-1 -*-

import marshal, string, re

# external
NAME_DEFINITE = 0
NAME_GIVEN    = 1
NAME_FAMILY   = 2
NAME_POSSIBLE = 3
UNKNOWN       = 4
COMPANY       = 5
SUFFIX        = 6
PERSON        = 7

def is_given_name(term):
    if not matches(term, reg_name):
        return 0
    if not namemap:
        load_namemap()
    term = string.lower(term)
    return MALE in namemap.get(term, [])

def is_name(term):
    return classify_word(term) in (NAME_DEFINITE, NAME_GIVEN, NAME_FAMILY)

def may_be_name(term):
    return matches(term, reg_name) and (classify_word(term) in (NAME_DEFINITE, NAME_GIVEN, NAME_FAMILY, NAME_POSSIBLE))

reverse = {
    0 : "NAME_DEFINITE",
    1 : "NAME_GIVEN",
    2 : "NAME_FAMILY",
    3 : "NAME_POSSIBLE",
    4 : "UNKNOWN",
    5 : "COMPANY",
    6 : "SUFFIX",
    7 : "PERSON"
    }

UNUSABLE = [SUFFIX]

# internal
FEMALE = 1
MALE   = 2
FAMILY = 3

# suffixes that change the type interpretation of the previous term. the
# form is "suffix" : (classification of previous term, type indicated)
suffixes = {
    "Company"      : (NAME_POSSIBLE, COMPANY),
    "Corp"         : (NAME_POSSIBLE, COMPANY),
    "Corporation"  : (NAME_POSSIBLE, COMPANY),
    "Limited"      : (NAME_POSSIBLE, COMPANY),
    "Incorporated" : (NAME_POSSIBLE, COMPANY),
    "Inc"          : (NAME_POSSIBLE, COMPANY),   
    "Ltd"          : (NAME_POSSIBLE, COMPANY),   
    }

def load_namemap():
    global namemap
    try:
        inf = open("data/names.mar", "rb")
        namemap = marshal.load(inf)
        inf.close()
    except IOError:
        namemap = {}

namemap = None

reg_name = re.compile("(Mc[A-Z][a-z]+|O'[A-Z][a-z]+|[A-ZÆØÅ][a-zæøå]+)")
def matches(str, reg):
    match = reg.match(str)
    return match and len(str) == match.end()

def combine_loose(t1, t2):
    name_def = [NAME_DEFINITE, NAME_GIVEN, NAME_FAMILY]
    name_poss = name_def + [NAME_POSSIBLE]
    if (t1 in name_def and t2 in name_poss) or \
       (t1 in name_poss and t2 in name_def):
        return NAME_DEFINITE
    return UNKNOWN

def combine_strict(t1, t2):
    name_def = [NAME_DEFINITE, NAME_GIVEN, NAME_FAMILY]
    name_poss = name_def + [NAME_POSSIBLE]
    if t1 == NAME_FAMILY and t2 == NAME_FAMILY:
        return NAME_FAMILY
    if t1 == NAME_GIVEN and t2 == NAME_GIVEN:
        return NAME_GIVEN
    if t1 in name_def and t2 in name_def:
        return NAME_DEFINITE
    if t1 in name_poss and t2 in name_poss:
        return NAME_POSSIBLE
    return UNKNOWN

def classify_word(word):
    if string.find(word, "-") != -1:
        return reduce(combine_strict,
                      map(classify_word, string.split(word, "-")),
                      NAME_FAMILY)
    
    if not matches(word, reg_name):
        return UNKNOWN
    
    if not namemap:
        load_namemap()
    types = namemap.get(string.lower(word))
    if types != None:
        if len(types) == 1:
            if types[0] == FEMALE or types[0] == MALE:
                return NAME_GIVEN
            else:
                return NAME_FAMILY
        elif len(types) == 2 and FEMALE in types and MALE in types:
            return NAME_GIVEN
        else:
            return NAME_DEFINITE

    return NAME_POSSIBLE

def classify(term):
    type = reduce(combine_strict, map(classify_word, string.split(term)),
                  NAME_DEFINITE)
    if type == NAME_POSSIBLE:
        return UNKNOWN
    return type

def test(terms):
    for term in terms:
        print term, reverse[classify(term)]

class GazetteTracker:

    def __init__(self):
        self._previous = None

    def track(self, term, word):
        if suffixes.has_key(word) and \
           self._previous and \
           self._previous.get_type() == UNKNOWN:
            (reqtype, newtype) = suffixes[word]
            if classify_word(self._previous.get_preferred()) == reqtype:
                print "Setting %s to %s" % (self._previous.get_preferred(),
                                            reverse[newtype])
                self._previous.set_type(newtype)
                term.set_type(SUFFIX) # this was clearly used as a suffix

        self._previous = term

    def skip(self):
        self._previous = None

if __name__ == "__main__":
    test(["John Rivers-Moore", "Lars Marius Garshol", "Steve Pepper",
          "Graham Moore", "Jeni Tennison", "Michael Sperberg-McQueen",
          "Charles O'Donnell", "Rivers-Moore", "John moore",
          "Self-Validating", "Bozo Self-Validating", "September"])
