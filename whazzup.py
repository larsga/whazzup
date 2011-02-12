import time, string, math, rsslib, sys, feedlib, queuebased
from xml.sax import SAXException
    
import web

urls = (
    '/(\d*)', 'List',
    '/vote/(up|down|read|star)/(\d+)', 'Vote',
    '/sites', 'Sites',
    '/site/(\d+)', 'SiteReport',
    '/update-site/(\d+)', 'UpdateSite',
    '/delete-site/(\d+)', 'DeleteSite',
    '/start-thread', 'StartThread',
    '/item/(\d+)', 'ShowItem',
    '/reload', 'Reload',
    '/addfeed', 'AddFeed',
    '/addfave', 'AddFave',
    '/shutdown', 'Shutdown',
    '/faveform/(\d*)', 'FaveForm',
    '/recalc', 'Recalculate'
    )

def nocache():
    web.header("Content-Type","text/html; charset=utf-8")
    web.header("Pragma", "no-cache");
    web.header("Cache-Control", "no-cache, no-store, must-revalidate, post-check=0, pre-check=0");
    web.header("Expires", "Tue, 25 Dec 1973 13:02:00 GMT");

render = web.template.render('templates/')

class List:
    def GET(self, page):
        nocache()
        if page:
            page = int(page)
        else:
            page = 0

        low = min(page * 25, feeddb.get_item_count())
        high = min(low + 25, feeddb.get_item_count())

        return render.list(page,
                           self.get_thread_health(),
                           low, high,
                           feeddb)             

    def get_thread_health(self):
        wait = time.time() - queuebased.lasttick
        if wait < 120:
            return "Thread is OK (%s)" % int(wait)
        else:
            return 'Thread is dead (%s) <a href="/start-thread">restart</a>' % int(wait)
        
class SiteReport:
    def GET(self, id):
        nocache()

        id = int(id)
        feed = feeddb.get_feed_by_id(id)
        return render.sitereport(feed)
            
class UpdateSite:
    def POST(self, id):
        nocache()

        id = int(id)
        feed = feeddb.get_feed_by_id(id)
        time = string.strip(web.input().get("time") or "").decode("utf-8")
        url = string.strip(web.input().get("feedurl") or "").decode("utf-8")
        feed.set_time_to_wait(time)

        if feed.get_url() != url:
            feed.set_url(url)
        
        feeddb.save()
        print "<p>Updated.</p>"

class DeleteSite:
    def GET(self, id):
        nocache()

        id = int(id)
        feed = feeddb.get_feed_by_id(id)
        feeddb.remove_feed(feed)
        feeddb.save()

        # FIXME: should also remove posts from db!
        
        return "<p>Deleted.</p>"
        
class Sites:
    def GET(self):
        nocache()
        
        sfeeds = feedlib.sort(feeddb.get_feeds(), feedlib.Feed.get_ratio)
        sfeeds.reverse()
        return render.sites(sfeeds)
        
class Vote:
    def GET(self, vote, id):
        nocache()
        link = feeddb.get_item_by_id(int(id))
        ix = feeddb.get_no_of_item(link)
        link.record_vote(vote)
        if vote != "read":
            queuebased.recalculate_all_posts() # since scores have changed
        web.seeother("/%s" % (ix / 25))

class ShowItem:
    def GET(self, id):
        nocache()
        try:
            item = feeddb.get_item_by_id(int(id))
            return render.item(item, string, math, feeddb)
        except KeyError, e:
            return "No such item: " + repr(id)
        
class Reload:
    def GET(self):
        nocache()
        print "<h1>Reloaded</h1>"
        print "<pre>"
        new_posts = feeddb.init() # does a reload
        for post in new_posts:
            queuebased.queue.put((0, queuebased.RecalculatePost(new_post)))
        print "</pre>"

class AddFeed:
    def POST(self):
        url = string.strip(web.input().get("url"))
        posts = feeddb.read_feed(url)
        feeddb.save()
        return  "<p>Feed added, %s posts loaded</p>" % len(posts)

class AddFave:
    def POST(self):        
        title = string.strip(web.input().get("title") or "").decode("utf-8")
        url = string.strip(web.input().get("url") or "").decode("utf-8")
        desc = string.strip(web.input().get("desc") or "").decode("utf-8")

        if not title or not url:
            print "<p>Title and URL are required. Try again.</p>"
            return
        
        i = rsslib.Item(feeddb.get_faves())
        i.set_title(title)
        i.set_link(url)
        if desc:
            i.set_description(desc)
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        i.set_pubdate(t)
        feeddb.add_fave(i)

        id = web.input().get("id")
        if id:
            item = feeddb.get_item_by_id(int(id))
            item.record_vote("up")

        print "<p>Fave added</p>"

class FaveForm:
    def GET(self, id = None):
        nocache()
        title = ""
        url = ""
        desc = ""
        if id:
            item = feeddb.get_item_by_id(int(id))
            title = item.get_title()
            url = item.get_link()
            desc = attrescape(item.get_description() or "")
            
        print "<h1>Add fave</h1>"
        print '<p><form method=post action="/addfave">'
        print '<table>'
        print '<tr><td>Title: <td><input type=text name=title size=50 value="%s"><br>' % title
        print '<tr><td>URL: <td><input type=text name=url size=50 value="%s"><br>' % url
        print '<tr><td>Description: <td><input type=text name=desc size=50 value="%s"><br>' % desc
        print '<tr><td><input type=submit value="Add"> <td>'
        if id:
            print '<input type=hidden name=id value="%s">' % id
        print '</table>'
        print '</form>'

class StartThread:

    def GET(self):
        feedlib.start_feed_reader(feeddb)
        print "<p>Thread started.</p>"

class Recalculate:

    def GET(self):
        feeddb._last_recalc = 0
        feeddb.recalculate()
        return "<p>Points recalculated, and items sorted.</p>"

class Shutdown:

    def GET(self):
        sys.exit()

# web.webapi.internalerror = web.debugerror

feeddb = feedlib.feeddb
if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
