"""
A very simple RSS client library with support for all of RSS. It can read
RSS files into an RSS data structure and it can dump the data structure
back out as an RSS file.'

Supports both RSS 0.90 and 0.91.

$Id: rsslib.py,v 1.9 2010/06/08 16:19:57 larsga Exp $
"""

import string, urlparse
from xml.sax import saxutils, make_parser
from xml.sax.handler import feature_namespaces, ContentHandler
from saxtracker import SAXTracker

# ===== Globals =====

version = "0.3"
rss_version = "0.91"
doctype_decl = '<!DOCTYPE rss PUBLIC "-//Netscape Communications//DTD RSS 0.91//EN" "http://my.netscape.com/publish/formats/rss-0.91.dtd">'

# ===== Data structure =====

class TitledObject:

    def __init__(self, title = None, link = None, descr = None):
        self._title = title
        self._link = string.strip(link or "") or None
        self._descr = descr
        self._pubdate = None

    def get_title(self):
        return self._title

    def get_link(self):
        return self._link
    
    def get_description(self):
        return self._descr

    def get_pubdate(self):
        return self._pubdate

    def set_title(self, title):
        self._title = title

    def set_link(self, link):
        self._link = string.strip(link)

    def set_description(self, descr):
        self._descr = descr

    def set_pubdate(self, pubdate):
        self._pubdate = pubdate.strip() # must remove ws to simplify parsing

class SiteSummary(TitledObject):
    "Represents an RSS file."

    def __init__(self, url):
        TitledObject.__init__(self)
        self.items = []
        self.image = None
        self.errors = []
        self.input = None
        self._url = url
        
        self.lang = None
        self.rating = None
        self.copyright = None
        self.pubdate = None
        self.lastbuild = None
        self.docs = None
        self.editor = None
        self.webmaster = None
        self._format = None

    def get_link(self):
        "Returns the HTML front page link of the feed."
        if self._link:
            return self._link
        return self._url

    def get_url(self):
        "Returns the URL of the RSS feed itself."
        return self._url

    def set_url(self, url):
        "Sets the URL of the RSS feed itself."
        self._url = url
    
    def add_item(self, item):
        "Appends an RSS item to the list of items."
        self.items.append(item)

    def get_items(self):
        return self.items

    def get_format(self):
        return self._format or "rss"

    def set_format(self, format):
        self._format = format

    def get_editor(self):
        return self.editor

    def set_editor(self, editor):
        self.editor = editor
        
class Image(TitledObject):
    "Represents an RSS image."

    def __init__(self, title = None, url = None, link = None, descr = None):
        TitledObject.__init__(self, title, link, descr)
        self._url = url
    
class Item(TitledObject):
    "Represents an RSS item."

    def __init__(self, site, title = None, link = None, descr = None):
        TitledObject.__init__(self, title, link, descr)
        self._site = site
        self._guid = None
        self._author = None

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

class TextInput(TitledObject):
    "Represents the RSS textinput element."

    def __init__(self, title = None, descr = None, name = None, link = None):
        TitledObject.__init__(self, title, link, descr)
        self._name = name

class DefaultFactory:

    def make_site(self, url):
        return SiteSummary(url)
    
    def make_image(self, title = None, url = None, link = None, descr = None):
        return Image(title, url, link, descr)
    
    def make_item(self, site, title = None, link = None, descr = None):
        return Item(site, title, link, descr)

    def make_text_input(self, title = None, descr = None, name = None, link = None):
        return TextInput(title, descr, name, link)

    def make_feed_registry(self):
        return FeedRegistry()

class FeedRegistry:

    def __init__(self):
        self._feeds = []
        self._title = None

    def get_title(self):
        return self._title

    def set_title(self, title):
        self._title = title
        
    def get_feeds(self):
        return self._feeds

    def add_feed(self, feed):
        self._feeds.append(feed)

# ===== RSS Deserializer =====

class RSSHandler(SAXTracker):

    def __init__(self, site, factory):
        SAXTracker.__init__(self, ["title", "link", "description", "url",
                                   "name", "language", "rating", "copyright",
                                   "pubDate", "lastBuildDate", "docs",
                                   "webMaster", "managingEditor", "guid",
                                   "atom:summary", "author", "dc:creator",
                                   "dc:date"])
        self._site = site
        self._obj = None
        self._factory = factory

    def startElement(self, name, attrs):
        if self._elemstack:
            parent = self._elemstack[-1]
        else:
            parent = None
            
        #print "<%s>" % name
        if name == "rss" and attrs.has_key("version"):
            if attrs["version"] != "0.91":
                self._site.errors.append("Unknown RSS version %s" %
                                         (attrs["version"]))
        elif name == "rss" or name == "channel":
            self._obj = self._site
        elif name == "item" and parent != "keywords":
            self._obj = self._factory.make_item(self._site)
            self._site.add_item(self._obj)
        elif name == "image":
            self._site.image = self._factory.make_image()
            self._obj = self._site.image
        elif name == "textinput":
            self._site.input = self._factory.make_text_input()
            self._obj = self._site.input
                
        SAXTracker.startElement(self, name, attrs)
        
    def endElement(self, name):
        #print "</%s>" % name
        SAXTracker.endElement(self, name)
        if self._elemstack:
            parent = self._elemstack[-1]
        else:
            parent = None
        
        if name == "title":
            self._obj.set_title(self._contents)
        elif name == "link":
            self._obj.set_link(self._contents)
        elif name == "description":
            self._obj.set_description(self._contents)
        elif name == "guid":
            self._obj.set_guid(self._contents)
        elif name == "atom:summary":
            if not self._obj.get_description():
                self._obj.set_description(self._contents)
        elif name == "author":
            self._obj.set_author(self._contents)
        elif name == "dc:creator":
            if isinstance(self._obj, Item) and (not self._obj.get_author()):
                self._obj.set_author(self._contents)
        elif name == "dc:date":
            if not self._obj.get_pubdate():
                self._obj.set_pubdate(self._contents)

        elif name == "url" and parent == "image":
            self._site.image.url = self._contents            
        elif name == "name" and parent == "textinput":
            self._site.input.name = self._contents
        elif name == "language":
            self._site.lang = self._contents
        elif name == "rating":
            self._site.rating = self._contents
        elif name == "copyright":
            self._site.copyright = self._contents
        elif name == "pubDate":
            self._obj.set_pubdate(self._contents)
        elif name == "lastBuildDate":
            self._site.lastbuild = self._contents
        elif name == "docs":
            self._site.docs = self._contents
        elif name == "managingEditor":
            self._site.editor = self._contents
        elif name == "webMaster":
            self._site.webmaster = self._contents

        elif name == "image":
            self._obj = self._site # restore previous object

def urllib_loader(parser, url):
    # this exists because it allows us to inject other mechanisms for
    # downloading content, such as GAE urlfetch
    parser.parse(url)
            
def read_xml(url, handler, data_loader = urllib_loader):
    p = make_parser()
    p.setContentHandler(handler)
    #p.setErrorHandler(saxutils.ErrorRaiser(2))
    p.setFeature(feature_namespaces, 0)
    data_loader(p, url)
    
def read_rss(url, factory = DefaultFactory()):
    ss = factory.make_site(url)
    handler = RSSHandler(ss, factory)
    read_xml(url, handler)
    return ss

# ===== RSS Serializer =====

def escape(str):
    str = string.replace(str, "&", "&amp;")
    str = string.replace(str, '"', "&quot;")
    return string.replace(str, "<", "&lt;")

def write_element(contents, name, out):
    if contents != None:
        out.write('      <%s>%s</%s>\n' % (name, escape(contents), name))

def write_rss(ss, out, encoding = "iso-8859-1"):
    out.write('<?xml version="1.0" encoding="%s"?>\n' % encoding)
    #out.write(doctype_decl + "\n")
    out.write('<rss version="%s">\n' % rss_version)
    out.write('  <channel>\n')
    out.write('    <title>%s</title>\n' % escape(ss.get_title()))
    out.write('    <link>%s</link>\n' % escape(ss.get_link()))
    out.write('    <description>%s</description>\n' %
              escape(ss.get_description()))
    write_element(ss.lang, "language", out)
    write_element(ss.rating, "rating", out)
    write_element(ss.get_pubdate(), "pubDate", out)
    write_element(ss.lastbuild, "lastBuildDate", out)
    write_element(ss.docs, "docs", out)
    write_element(ss.editor, "managingEditor", out)
    write_element(ss.webmaster, "webMaster", out)
    
    if ss.image != None:
        out.write('    <image>\n')
        out.write('      <title>%s</title>\n' % escape(ss.image.title))
        out.write('      <url>%s</url>\n' % escape(ss.image.url))
        out.write('      <link>%s</link>\n' % escape(ss.image.link))
        write_element(ss.image.descr, "description", out)
        out.write('    </image>\n\n')
    
    for item in ss.items:
        out.write('    <item>\n')
        out.write('      <title>%s</title>\n' % escape(item.get_title()))
        out.write('      <link>%s</link>\n' % escape(item.get_link()))
        write_element(item.get_description(), "description", out)
        out.write('    </item>\n\n')

    if ss.input:
        out.write('    <textinput>\n')
        out.write('      <name>%s</name>\n' % escape(ss.input.name))
        out.write('      <title>%s</title>\n' % escape(ss.input.title))
        out.write('      <description>%s</description>\n' %
                  escape(ss.input.desc))
        out.write('      <link>%s</link>\n' % escape(ss.input.link))
        out.write('    </textinput>\n')
        
    out.write('  </channel>\n\n')
    out.write('</rss>\n')

# ===== OPML Deserializer

class OPMLHandler(SAXTracker):

    def __init__(self, registry, factory):
        SAXTracker.__init__(self, ["title"])
        self._registry = registry
        self._factory = factory

    def startElement(self, name, attrs):                
        SAXTracker.startElement(self, name, attrs)

        if name == "outline":
            url = attrs.get("xmlUrl", attrs.get("htmlUrl"))
            feed = self._factory.make_site(url)
            feed.set_title(attrs.get("title"))
            feed.set_format(attrs.get("type"))
            feed.set_link(attrs.get("htmlUrl"))
            self._registry.add_feed(feed)
        
    def endElement(self, name):
        SAXTracker.endElement(self, name)

        if name == "title":
            self._registry.set_title(self._contents)
        
def read_opml(url, factory = DefaultFactory()):
    feeds = factory.make_feed_registry()
    handler = OPMLHandler(feeds, factory)
    p = make_parser()
    p.setContentHandler(handler)
    #p.setErrorHandler(saxutils.ErrorRaiser(2))
    p.setFeature(feature_namespaces, 0)
    p.parse(url)
    return feeds

def write_opml(feeds, out, encoding = "iso-8859-1"):
    out.write('<?xml version="1.0" encoding="%s"?>\n' % encoding)
    out.write('<opml version="1.0">\n')
    out.write('  <head>\n')
    out.write('    <title>%s</title>\n' % escape("What's up feeds"))
    out.write('  </head>\n')
    out.write('  <body>\n')
    for feed in feeds:
        out.write('    <outline text="%s"\n' % escape(feed.get_title()))
        out.write('             title="%s"\n' % escape(feed.get_title()))
        out.write('             type="%s"\n' % feed.get_format())
        out.write('             xmlUrl="%s"\n' % escape(feed.get_url()))
        out.write('             htmlUrl="%s"/>\n' % escape(feed.get_link()))
    out.write('  </body>\n')
    out.write('</opml>\n')

# ===== Atom Deserializer =====

class AtomHandler(SAXTracker):

    def __init__(self, site, factory):
        SAXTracker.__init__(self, ["title", "id", "published", "content",
                                   "name", "summary", "subtitle", "updated"])
        self._site = site
        self._obj = None
        self._factory = factory
        self._bases = [self._site.get_url()]
        self._summary = None

    def startElement(self, name, attrs):
        if self._elemstack and self._elemstack[-1] in ("content", "summary"):
            return # ignore the embedded markup
        
        #print "<%s>" % name, attrs
        if self._elemstack:
            parent = self._elemstack[-1]
        else:
            parent = None                
        SAXTracker.startElement(self, name, attrs)
        
        if name == "feed":
            self._push_base(attrs.get("xml:base", ""))
            self._obj = self._site
        elif name == "entry":
            self._obj = self._factory.make_item(self._site)
            self._site.add_item(self._obj)
            self._push_base(attrs.get("xml:base", ""))
            self._summary = None
        elif name == "link" and parent == "entry" and \
             attrs.get("rel", "alternate") == "alternate":
            url = self._get_full_url(attrs["href"])
            self._obj.set_link(url)
        elif name == "link" and parent == "feed" and \
             attrs.get("rel", "alternate") == "alternate":
            url = self._get_full_url(attrs["href"])
            self._site.set_link(url)

    def endElement(self, name):
        if self._elemstack and \
           ((self._elemstack[-1] == "content" and name != "content") or
            (self._elemstack[-1] == "summary" and name != "summary")):
            return # ignore the embedded markup
        
        #print "</%s>" % name
        SAXTracker.endElement(self, name)
        if self._elemstack:
            parent = self._elemstack[-1]
        else:
            parent = None

        if name == "feed":
            self._pop_base()
        elif name == "entry":
            self._pop_base()
            if self._obj.get_author() == None:
                self._obj.set_author(self._site.get_editor()) # inherit
            if self._obj.get_description() == None and self._summary != None:
                self._obj.set_description(self._summary) # fallback
        elif name == "title":
            self._obj.set_title(self._contents)
        elif name == "subtitle" and parent == "feed":
            self._site.set_description(self._contents)
        elif name == "id":
            if parent == "feed":
                self._obj.set_link(self._contents)
            elif parent == "entry":
                self._obj.set_guid(self._contents)
        elif name == "published":
            self._obj.set_pubdate(self._contents)
        elif name == "updated" and parent == "entry":
            if not self._obj.get_pubdate():
                self._obj.set_pubdate(self._contents)
        elif name == "content":
            self._obj.set_description(self._contents)
        elif name == "summary":
            self._summary = self._contents # use if no <content> element
        elif name == "name" and parent == "author":
            if self._elemstack[-2] == "feed":
                self._obj.set_editor(self._contents)
            elif self._elemstack[-2] == "entry":
                self._obj.set_author(self._contents)
            
        # link: not going to listen to this on the feed
        # logo: don't care
        # icon: don't care
        # subtitle: don't care
        # rights: don't care
        # generator: really couldn't care less

    def _push_base(self, base):
        self._bases.append(self._get_full_url(base))

    def _pop_base(self):
        del self._bases[-1]

    def _get_full_url(self, url):
        return urlparse.urljoin(self._bases[-1], url)

def read_atom(url, factory = DefaultFactory()):
    ss = factory.make_site(url)
    handler = AtomHandler(ss, factory)
    read_xml(url, handler)
    return ss

# ==== FEED-AGNOSTIC LOADING

def read_feed(url, factory = DefaultFactory(), data_loader = urllib_loader):
    """Loads feed without knowing whether it's RSS or Atom by autodetecting
    the format."""
    ss = factory.make_site(url)
    handler = AutoDetectingHandler(ss, factory)
    read_xml(url, handler, data_loader)
    return ss

class AutoDetectingHandler(ContentHandler):

    def __init__(self, site, factory):
        self._site = site
        self._factory = factory
        self._handler = None # need to auto-detect before we can install

    def startElement(self, name, attrs):
        if not self._handler:
            if name == "feed":
                self._handler = AtomHandler(self._site, self._factory)
            elif name in ("rss", "rdf:RDF"):
                self._handler = RSSHandler(self._site, self._factory)
            else:
                raise Exception("Unknown format: " + name)

        self._handler.startElement(name, attrs)

    def characters(self, data):
        if self._handler:
            self._handler.characters(data)
        
    def endElement(self, name):
        self._handler.endElement(name)
