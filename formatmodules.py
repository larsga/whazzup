
from xml.sax import make_parser, ContentHandler, SAXNotSupportedException
from xml.sax.handler import feature_external_ges, feature_namespaces
import HTMLParser, string, re, codecs, os

DEBUG = 0

# --- Utilities

def contains(str, sub):
    return string.find(str, sub) != -1

# --- Interface

# Format scores:
#  - 0:   it's not this format; use something else
#  - 1:   use if nothing else works
#  - 10:  might be right
#  - 100: it matches for certain

class FormatModule:

    def get_text(self, filename):
        return None

    def get_format_score(self, filename):
        return 0

    def get_title(self):
        return None

# --- Plain text module

class PlainTextModule:

    def get_text(self, filename):
        return codecs.open(filename, "r", "iso-8859-1").read()

    def get_format_score(self, filename):
        return 1

# --- HTML extractor module

reg_start = re.compile("<([-A-Za-z_0-9]+)[ |]")
reg_end   = re.compile("</([-A-Za-z_0-9]+)>")
reg_empty = re.compile("<([-A-Za-z_0-9]+)/>")
reg_cdata = re.compile("<!\[CDATA\[")

class HTMLExtractor(HTMLParser.HTMLParser):

    def  __init__(self):
        HTMLParser.HTMLParser.__init__(self)
        self._chunks = []
        self._skip_tags = {"style" : 1, "pre" : 1, "script" : 1, "style" : 1}
        self._block_tags = {"h1" : 1, "h2" : 1, "dt" : 1, "dd" : 1, "h3" : 1}
        self._skip = 0
        self._skipping = None
        self._is_title = 0
        self._title = None

    def add_skip_tag(self, tag):
        self._skip_tags[tag] = 1

    def handle_starttag(self, tag, attrs):
        if not self._skip and self._skip_tags.get(tag):
            self._skip = 1
            self._skipping = tag
        self._is_title = ((tag == "title") or
                          (tag == "h1" and not self._title))
        if self._is_title:
            self._title = []
        
    def handle_data(self, data):
        if not self._skip:
            self._chunks.append(data)
        if self._is_title:
            self._title.append(data)

    def handle_endtag(self, tag):
        if self._skipping == tag:
            self._skip = 0
            self._skipping = None
        if self._is_title:
            self._is_title = 0
            self._title = string.join(self._title, "")
        if self._block_tags.has_key(tag):
            if self._chunks and self._chunks[-1][-1] != ".":
                self._chunks[-1] += "."

    def get_text(self):
        return string.join(self._chunks)

    def get_title(self):
        return self._title

htmltags = ["html", "head", "body", "p", "h1", "h2", "h3", "h4", "h5", "h6",
            "a", "b", "table", "tr", "td", "th", "img", "link", "meta",
            "title", "u", "i", "ul", "ol", "li", "dt", "dd", "dl", "br",
            "script", "pre", "em"]
    
class HTMLModule:

    def __init__(self):
        self._title = None
    
    def get_text(self, filename):
        extractor = HTMLExtractor()
        try:
            extractor.feed(codecs.open(filename, "r", "iso-8859-1").read())
        except HTMLParser.HTMLParseError, e:
            print "ERROR", filename, e

        self._title = extractor.get_title()
        return extractor.get_text()

    def get_format_score(self, filename):
        text = open(filename).read()

        start = reg_start.findall(text)
        end = reg_end.findall(text)
        empty = reg_empty.findall(text)
        cdata = reg_cdata.findall(text)
        all = start + end + empty
        total = float(len(all))
        match = 0
        for tag in all:
            tag = string.lower(tag)
            if tag in htmltags:
                match += 1

        if total == 0:
            return 0
        else:
            return (match / total) * 100 - (len(cdata) * 10)

    def get_title(self):
        return self._title
    
# --- XML extractor module

def parse(filename, handler):
    parser = make_parser()
    parser.setFeature(feature_namespaces, 0)
    try:
        parser.setFeature(feature_external_ges, 0)
    except SAXNotSupportedException:
        pass
    parser.setContentHandler(handler)
    parser.parse(filename)

class XMLExtractor(ContentHandler):

    def  __init__(self):
        self._chunks = []
        self._skip_tags = {}
        self._block_tags = {}
        self._skip = 0
        self._is_title = 0
        self._skipping = None
        self._title = None
        self._title_element = None

    def add_skip_tag(self, tag):
        self._skip_tags[tag] = 1

    def add_block_tag(self, tag):
        self._block_tags[tag] = 1

    def set_title_element(self, tag):
        self._title_element = tag
        
    def startElement(self, name, attrs):
        if name == self._title_element and not self._title:
            self._is_title = 1
            self._title = ""
        elif not self._skip and self._skip_tags.get(name):
            self._skip = 1
            self._skipping = name
        
    def characters(self, data):
        if self._is_title:
            self._title += data
        elif not self._skip:
            self._chunks.append(data)

    def endElement(self, name):
        if self._is_title:
            self._is_title = 0
        elif self._skipping == name:
            self._skip = 0
            self._skipping = None

        elif self._block_tags.has_key(name):
            if self._chunks and self._chunks[-1][-1] != ".":
                self._chunks[-1] += "."

    def get_text(self):
        return string.join(self._chunks)

class XMLModule(FormatModule):
    
    def get_text(self, filename):
        handler = XMLExtractor()
        parse(filename, handler)
        return handler.get_text()

    def get_format_score(self, filename):
        text = open(filename).read()
        if text[ : 19] == "<?xml version='1.0'" or \
           text[ : 19] == '<?xml version="1.0"':
            return 100

        start = len(reg_start.findall(text))
        end = len(reg_end.findall(text))
        empty = len(reg_empty.findall(text))
        cdata = len(reg_cdata.findall(text))
        if DEBUG:
            print start, end, empty, cdata
        if start == end and (start + end + empty) > 25:
            return 100

        if (start + end + empty + cdata) > 25:
            return 10 + cdata * 10
        
        return 0

# --- PDF module

class PDFModule:

    def __init__(self):
        self._title = None

    def get_text(self, filename):
        self._title = os.path.split(filename)[1]
        
        os.system("pdftotext %s /tmp/pdf.txt" %
                  string.replace(filename, " ", "\ "))
        try:
            text = codecs.open("/tmp/pdf.txt", "r", "iso-8859-1").read()
            os.unlink("/tmp/pdf.txt")
        except:
            return ""

        return text

    def get_format_score(self, filename):
        inf = open(filename)
        line = inf.readline()
        inf.close()
        if line[ : 7] == "%PDF-1.":
            return 110
        else:
            return 0

    def get_title(self):
        return self._title

# --- GCAPAPER module

class GCAModule(FormatModule):

    def get_text(self, filename):
        handler = XMLExtractor()
        handler.add_skip_tag("sgml.block")
        handler.add_skip_tag("verbatim")
        handler.add_skip_tag("sgml")
        handler.add_skip_tag("author")
        handler.add_skip_tag("bibliog")
        handler.add_skip_tag("web")
        handler.add_skip_tag("Authorinfo")
        handler.add_skip_tag("AuthorInfo")
        handler.add_skip_tag("AUTHORINFO")
        handler.add_skip_tag("code.block")
        handler.add_skip_tag("code.line")
        handler.add_skip_tag("Pre")
        handler.add_skip_tag("PRE")
        handler.add_skip_tag("programlisting")

        handler.add_block_tag("title")
        handler.add_block_tag("subt")
        handler.add_block_tag("keyword")

        handler.set_title_element("title")
        
        parse(filename, handler)
        return handler.get_text()

    def get_format_score(self, filename):
        if xml.get_format_score(filename) >= 10:
            text = open(filename).read()
            if contains(text, "extremepaperxml.dtd") or \
               contains(text, "<paper>") or \
               contains(text, "<paper xmlns:") or \
               contains(text, "<Paper track=") or \
               contains(text, "<PAPER track=") or \
               contains(text, '<paper secnumbers=') or \
               contains(text, '<gcapaper') or \
               contains(text, '<xmle99 secnumbers=') or \
               contains(text, '<xmle98 secnumbers=') or \
               contains(text, '-//IDEAlliance//DTD Conference Paper DocBook') or \
               contains(text, '<xml99'):
                return 110

        return 0

# --- DocBook module

class DocBookModule:

    def get_text(self, filename):
        handler = XMLExtractor()
        handler.add_skip_tag("literallayout")

        handler.add_block_tag("title")
        handler.add_block_tag("subtitle")
        handler.add_block_tag("para")
        handler.add_block_tag("affiliation")
        parse(filename, handler)
        return handler.get_text()

    def get_format_score(self, filename):
        if xml.get_format_score(filename) >= 10:
            text = open(filename).read()
            if contains(text, "<article>") and \
               contains(text, "<affiliation>") and \
               contains(text, "<section>"):
                return 110

        return 0
    
# --- LaTeX

REG_DIRECTIVE = re.compile("\\\\[a-z]+")
    
class LatexModule:

    def get_text(self, filename):
        data = codecs.open(filename, "r", "iso-8859-1").read()
        data = string.replace(data, "\\\\", "")

        chunks = []
        m = REG_DIRECTIVE.search(data)
        prev = 0
        while m:
            name = m.group()[1 : ]
            start = m.start()
            end = m.end()
            chunks.append(data[prev : start])

            if data[end] == "{":
                stop = string.find(data, "}", end)
                if name in ["title", "textit", "section", "subsection",
                            "caption", "subsubsection", "textbf"]:
                    chunks.append(data[end+1 : stop])
                elif name == "begin":
                    type = data[end+1 : stop]
                    if type in "verbatim":
                        marker = "\end{%s}" % type
                        stop = string.find(data, marker, stop) + len(marker) 
                        
                elif name not in ["documentclass", "usepackage",
                                  "pagestyle", "titlerunning", "author",
                                  "authorrunning", "institute", "email",
                                  "texttt", "end", "cite", "label", "vref",
                                  "item"]:
                    pass#print name
                end = stop
            
            m = REG_DIRECTIVE.search(data, end)
            prev = end + 1
            
        data = string.join(chunks, " ")
        #print data.encode("utf-8")
        return data

    def get_format_score(self, filename):
        data = codecs.open(filename, "r", "iso-8859-1").read()
        
        score = 0
        for token in ["\subsection{", "\subsubsection{", "\section{",
                      "\title{", "\cite{"]:
            score += string.count(data, token)

        return score
    
# --- Final setup

xml = XMLModule()
modules = [PlainTextModule(), HTMLModule(), xml, GCAModule(), PDFModule(),
           LatexModule(), DocBookModule()]

def get_format_module(filename):
    best = None
    high = 0

    if DEBUG:
        print "FILE:", filename

    for module in modules:
        score = module.get_format_score(filename)
        if DEBUG:
            print module, score
        if score > high:
            best = module
            high = score

    return best

def get_text(filename):
    module = get_format_module(filename)
    return module.get_text(filename)
