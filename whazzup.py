
import time, string, math, rsslib, sys, feedlib, StringIO
from xml.sax import SAXException

import web

urls = (
    '/(\d*)', 'List',
    '/vote/(up|down|read|star)/(.+)', 'Vote',
    '/sites', 'Sites',
    '/site/(.+)', 'SiteReport',
    '/update-site/(\d+)', 'UpdateSite',
    '/delete-site/(\d+)', 'DeleteSite',
    '/start-thread', 'StartThread',
    '/item/(.+)', 'ShowItem',
    '/reload', 'Reload',
    '/addfeed', 'AddFeed',
    '/addfave', 'AddFave',
    '/shutdown', 'Shutdown',
    '/faveform/(\d*)', 'FaveForm',
    '/recalc', 'Recalculate',
    '/uploadopml', 'ImportOPML',

    # app engine tasks
    '/task/check-feed/(.+)', 'TaskCheckFeed',
    '/task/find-feeds-to-check/', 'FindFeedsToCheck',
    '/task/recalc-sub/(.+)', 'RecalculateSubscription',
    '/task/remove-dead-feeds/', 'RemoveDeadFeeds',
    '/task/age-posts/', 'AgePosts',
    '/task/age-subscription/(.+)', 'AgeSubscription',
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

        user = users.get_current_user()
        if not user:
            return render.not_logged_in(users.create_login_url("/"))
            
        low = page * 25
        high = low + 25
        return render.storylist(page,
                                self.get_thread_health(),
                                low, high,
                                feeddb)             

    def get_thread_health(self):
        wait = controller.get_queue_delay()
        if wait < 120:
            return "Thread is OK (%s)" % int(wait)
        else:
            return 'Thread is dead (%s) <a href="/start-thread">restart</a>' % int(wait)
        
class SiteReport:
    def GET(self, id):
        nocache()

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
        return "<p>Updated.</p>"

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
        
        sfeeds = feedlib.sort(feeddb.get_feeds(), lambda feed: feed.get_ratio())
        sfeeds.reverse()
        return render.sites(sfeeds)
        
class Vote:
    def GET(self, vote, id):
        nocache()
        link = feeddb.get_item_by_id(id)
        link.record_vote(vote)
        if vote != "read":
            controller.recalculate_all_posts() # since scores have changed
        web.seeother("/") # FIXME

class ShowItem:
    def GET(self, id):
        nocache()
        try:
            item = feeddb.get_item_by_id(id)
            return render.item(item, string, math, feeddb)
        except KeyError, e:
            return "No such item: " + repr(id)
        
class Reload:
    def GET(self):
        nocache()
        print "<h1>Reloaded</h1>"
        controller.reload()

class ImportOPML:
    def POST(self):
        nocache()

        thefile = web.input()["opml"]
        inf = StringIO.StringIO(thefile)
        feeds = rsslib.read_opml(inf)
        inf.close()

        for newfeed in feeds.get_feeds():
            controller.add_feed(newfeed.get_url())
        
        return "<p>Imported.</p>"
    
class AddFeed:
    def POST(self):
        url = string.strip(web.input().get("url"))
        controller.add_feed(url)
        return  "<p>Feed added to queue for processing.</p>"

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
        controller.start_feed_reader(feeddb)
        print "<p>Thread started.</p>"

class Recalculate:

    def GET(self):
        feeddb._last_recalc = 0
        feeddb.recalculate()
        return "<p>Points recalculated, and items sorted.</p>"

class Shutdown:

    def GET(self):
        sys.exit()

# --- App Engine tasks

class TaskCheckFeed:

    def GET(self, key):
        controller.check_feed(key)

    def POST(self, key):
        controller.check_feed(key)

class FindFeedsToCheck:

    def GET(self):
        controller.find_feeds_to_check()

    def POST(self):
        controller.find_feeds_to_check()

class RecalculateSubscription: # ie: user x feed

    def GET(self, key):
        controller.recalculate_subscription(key)

    def POST(self, key):
        controller.recalculate_subscription(key)

class RemoveDeadFeeds:

    def GET(self):
        controller.remove_dead_feeds()

    def POST(self):
        controller.remove_dead_feeds()

class AgePosts:

    def GET(self):
        controller.age_posts()

    def POST(self):
        controller.age_posts()

class AgeSubscription:

    def GET(self, key):
        controller.age_subscription(key)

    def POST(self, key):
        controller.age_subscription(key)
        
web.webapi.internalerror = web.debugerror

#import signal
#def signal_handler(signal, frame):
#    print 'You pressed Ctrl+C!'
#    sys.exit(0)
#signal.signal(signal.SIGINT, signal_handler)

try:
    from google.appengine.api import users
    # we're running in appengine
    import appengine
    module = appengine
except ImportError:
    # not in appengine
    import diskimpl
    module = diskimpl
    
controller = module.controller
feeddb = module.feeddb

if __name__ == "__main__":
    app = web.application(urls, globals())
    if controller.in_appengine():
        app.cgirun()
    else:
        app.run()
