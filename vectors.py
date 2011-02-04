
import math, chew, string, langmodules

class Vector:
    "A term vector."

    def __init__(self, vector = None):
        self._vector = vector or {}

    def add_term(self, term):
        self._vector[term] = self._vector.get(term, 0) + 1

    def get_keys(self):
        return self._vector.keys()

    def get(self, key, default = None):
        return self._vector.get(key, default)
        
    def get_pairs(self):
        return self._vector.items()

    def get_count(self, term):
        return self._vector.get(term, 0)
        
    def cosine(self, other):
        topsum = 0
        for (term, count) in self.get_pairs():
            topsum += count * other.get_count(term)

        botsum1 = 0
        for (term, x) in self.get_pairs():
            botsum1 += x * x
        botsum2 = 0
        for (term, y) in other.get_pairs():
            botsum2 += y * y

        if not botsum1 * botsum2:
            return 0
        else:
            return topsum / (math.sqrt(botsum1) * math.sqrt(botsum2))

    def display_comparison(self, other, showall = 0):
        terms = {}
        for (term, count) in self.get_pairs():
            terms[term] = count * other.get_count(term)
        termlist = chew.sort(terms.items(), lambda x: x[1])
        termlist.reverse()
        for (term, score) in termlist:
            if score:
                print (term, score)
        if showall:
            list = []
            for (term, count) in self.get_pairs():
                if not terms.get(term, 0):
                    list.append(term.encode("utf-8"))
            print "+", string.join(list, ", ")
            list = []
            for (term, count) in other.get_pairs():
                if not terms.get(term, 0):
                    list.append(term.encode("utf-8"))
            print "-", string.join(list, ", ")

    def dump(self):
        list = self._vector.items()
        list = chew.sort(list, lambda x: x[1])
        list.reverse()
        for (term, count) in list:
            print term.encode("utf-8"), count
        print "Terms:", len(self._vector)

    def normalize(self, tracker):
        v = {}
        for (term, val) in self._vector.items():
            v[term] = tracker.get_score(term, val)
        self._vector = v

class Cluster:

    def __init__(self, members = None):
        self._members = []
        if members:
            for member in members:
                self.add(member)
        self._average = 0
        self._name = None

    def set_name(self, name):
        self._name = name

    def get_name(self):
        return self._name

    def get_members(self):
        return self._members

    def clear_members(self):
        self._members = []

    def add(self, vector):
        if isinstance(vector, Vector):
            self._members.append(vector)
        else:
            self._members.append(vector.get_vector())
        self._average = None

    def make_average_vector(self):
        if not self._members:
            self._average = Vector({})
            return

        if len(self._members) == 1:
            self._average = self._members[0]
        
        average = {}
        for member in self._members:
            for (term, count) in member.get_pairs():
                average[term] = count + average.get(term, 0)

        mc = float(len(self._members))
        for term in average.keys():
            average[term] = average[term] / mc
            
        self._average = Vector(average)

    def get_average_vector(self):
        if not self._average:
            self.make_average_vector()
        return self._average

    def compare(self, other):
        return self.get_average_vector().cosine(other.get_average_vector())

    def __repr__(self):
        return repr(self._members)

def k_nearest_neighbours_2(objects):
    """Makes clusters of objects. All objects must implement
    object.compare(other), object.get_key(), and object.get_name()."""
    pairs = []
    for ix in range(len(objects)):
        for i in range(ix+1, len(objects)):
            pairs.append((objects[ix],
                          objects[i],
                          objects[ix].compare(objects[i])))

    pairs = chew.sort(pairs, lambda x: x[2])
    pairs.reverse()

    clusters = []
    clustermap = {}
    for (t1, t2, score) in pairs:
        if not score or (clustermap.has_key(t1.get_key()) and
                         clustermap.has_key(t2.get_key())):
            if score:
                pass #print "NOT USING:", (t1, t2, score)
            continue
        print (t1.get_name(), t2.get_name(), score)
        #compare(get_matrix(t1, terms), get_matrix(t2, terms))
        if clustermap.has_key(t1.get_key()):
            c = clustermap[t1.get_key()]
        elif clustermap.has_key(t2.get_key()):
            c = clustermap[t2.get_key()]
        else:
            c = Cluster()
            clusters.append(c)
        
        for t in (t1, t2):
            if not clustermap.has_key(t.get_key()):
                c.add(t)
                clustermap[t.get_key()] = c

    return clusters

def update(compcache, c1, c2, score):
    c1 = id(c1)
    c2 = id(c2)
    if compcache.has_key(c1):
        compcache[c1][c2] = score
    else:
        compcache[c1] = {c2 : score}
    
def merge_cluster(objects, debug = 1):
    """Makes clusters of objects. All objects must implement
    object.get_key(), object.get_name(), and object.get_vector()."""

    # make one cluster for each object
    clusters = map(lambda x: Cluster([x]), objects)
    print len(clusters)
    stop = int(math.log(len(clusters)) * 7)

    # using this changes the algorithm from n**3 to n**2
    compcache = {} # id(cluster) -> {id(cluster) : comp, id(cluster) : comp...}
    for ix in range(len(clusters)):
        for i in range(ix+1, len(clusters)):
            if ix != i:
                score = clusters[ix].compare(clusters[i])
                update(compcache, clusters[ix], clusters[i], score)
                update(compcache, clusters[i], clusters[ix], score)
    
    while len(clusters) > stop:
        # find closest pair
        highest = 0
        pair = None
        for ix in range(len(clusters)):
            for i in range(ix+1, len(clusters)):
                if ix != i:
                    score = compcache[id(clusters[ix])][id(clusters[i])]
                    if score > highest:
                        pair = (clusters[ix], clusters[i])
                        highest = score

        # merge the pair
        print pair
        clusters.remove(pair[0])
        clusters.remove(pair[1])
        del compcache[id(pair[0])]
        del compcache[id(pair[1])]
        
        nc = Cluster(pair[0].get_members() + pair[1].get_members())
        for ix in range(len(clusters)):
            score = clusters[ix].compare(nc)
            update(compcache, clusters[ix], nc, score)
            update(compcache, nc, clusters[ix], score)
            
        clusters.append(nc)
        print len(clusters), stop

    return clusters

class WordFrequencyTracker:

    def __init__(self):
        self._words = {}
        self._total = 0

    def add_occurrence(self, word):
        self._words[word] = self._words.get(word, 0) + 1
        self._total += 1    

    def get_score(self, term, val):
        count = self._words.get(term, 0)
        if count <= 4:
            return 0
        factor = count / float(self._total)
        return math.log(val / factor)

    def get_count(self, term):
        return self._words.get(term, 0) 

    def print_report(self):
        print "TOTAL COUNT:", self._total
        items = chew.sort(self._words.items(), lambda x: x[1])
        items.reverse()
        for (term, count) in items[ : 20]:
            print "%30s %s" % (term.encode("utf-8"), count)
        print "..."
        for (term, count) in items[-20 : ]:
            print "%30s %s" % (term.encode("utf-8"), count)

def text_to_vector(text, blacklist = {}, tracker = None, stemming = 0):
    termlist = chew.extract_terms(text)
    lang = langmodules.get_language_module(termlist)

    vector = Vector()
    for term in termlist:
        term = string.lower(term)
        if chew.acceptable_term(term) and \
           not lang.is_stop_word(term) and \
           not blacklist.has_key(term):
            if stemming:
                stem = lang.get_stem(term)
            else:
                stem = term
            if tracker:
                tracker.add_occurrence(stem)
            vector.add_term(stem)

    return vector
    
