
import time, string, math, sys, StringIO, os
from xml.sax import SAXException

import web

# mod_wsgi app loading weirdness workaround
appdir = os.path.dirname(__file__)
sys.path.append(appdir)
# </workaround>

import rsslib, feedlib
from config import *

urls = (
    '/(\d*)', 'List',
    '/vote/(up|down|read|star)/(.+)', 'Vote',
    '/sites', 'Sites',
    '/site/(.+)', 'SiteReport',
    '/update-site/(\d+)', 'UpdateSite',
    '/delete-site/(.+)', 'DeleteSite',
    '/start-thread', 'StartThread',
    '/item/(.+)', 'ShowItem',
    '/reload', 'Reload',
    '/addfeed', 'AddFeed',
    '/addfave', 'AddFave',
    '/shutdown', 'Shutdown',
    '/faveform/(\d*)', 'FaveForm',
    '/recalc', 'Recalculate',
    '/uploadopml', 'ImportOPML',
    '/popular', 'PopularSites',
    '/login,?(failed|created|missing|passwords|userexists|notify)?', 'Login',
    '/login-handler', 'LoginHandler',
    '/logout', 'Logout',
    '/signup', 'Signup',
    '/notify', 'Notify',
    '/faq', 'FAQ',
    '/stats', 'Stats',
    '/reset-password', 'ResetPassword',
    '/mark-as-read/(.+)', 'MarkAsRead',

    # app engine tasks
    '/task/check-feed/(.+)', 'TaskCheckFeed',
    '/task/find-feeds-to-check/', 'FindFeedsToCheck',
    '/task/recalc-sub/(.+)', 'RecalculateSubscription',
    '/task/remove-dead-feeds/', 'RemoveDeadFeeds',
    '/task/age-posts/', 'AgePosts',
    '/task/age-subscription/(.+)', 'AgeSubscription',
    '/task/purge-posts/', 'PurgePosts',
    '/task/purge-feed/(.+)', 'PurgeFeed',

    # data fix tasks
    '/task/purge-bad-users/', 'PurgeBadUsers',
    '/task/delete-user/(.+)', 'DeleteUser',
    )

def nocache():
    web.header("Content-Type","text/html; charset=utf-8")
    web.header("Pragma", "no-cache");
    web.header("Cache-Control", "no-cache, no-store, must-revalidate, post-check=0, pre-check=0");
    web.header("Expires", "Tue, 25 Dec 1973 13:02:00 GMT");

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
        return render.storylist(page, low, high, user)

    def get_thread_health(self):
        wait = controller.get_queue_delay()
        if wait < 120:
            return "Thread is OK (%s)" % int(wait)
        else:
            return 'Thread is dead (%s) <a href="/start-thread">restart</a>' % int(wait)
        
class SiteReport:
    def GET(self, id):
        nocache()

        user = users.get_current_user()
        if not user:
            return render.not_logged_in(users.create_login_url("/"))
        
        feed = feeddb.get_feed_by_id(id)
        return render.sitereport(feed,
                                 controller.in_appengine(),
                                 controller.is_single_user(),
                                 user)
            
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

        user = users.get_current_user()
        if not user:
            return render.not_logged_in(users.create_login_url("/"))

        controller.unsubscribe(id, user)
        web.seeother(web.ctx.homedomain + "/sites")
        
class Sites:
    def GET(self):
        nocache()

        user = users.get_current_user()
        if not user:
            return render.not_logged_in(users.create_login_url("/"))
        
        return render.sites(user.get_feeds(),
                            controller.in_appengine(),
                            controller.is_single_user())

class PopularSites:
    def GET(self):
        nocache()

        user = users.get_current_user()
        if not user:
            return render.not_logged_in(users.create_login_url("/"))
        
        feeds = feeddb.get_popular_feeds()
        return render.popular(feeds, user)

def get_return_url():
    return web.ctx.env.get('HTTP_REFERER', web.ctx.homedomain + "/")

class Vote:
    def GET(self, vote, id):
        nocache()

        user = users.get_current_user()
        if not user:
            return render.not_logged_in(users.create_login_url("/"))

        controller.vote_received(user, id, vote)
        web.seeother(get_return_url())

class MarkAsRead:
    def GET(self, ids):
        nocache()

        user = users.get_current_user()
        if not user:
            return render.not_logged_in(users.create_login_url("/"))

        controller.mark_as_read(user, ids.split(","))
        web.seeother(get_return_url())
        
class ShowItem:
    def GET(self, id):
        nocache()
        user = users.get_current_user()
        if not user:
            return render.not_logged_in(users.create_login_url("/"))

        try:
            rated = user.get_rated_post_by_id(id)
            return render.item(rated, rated.get_post(), string, math, user,
                               controller.in_appengine(),
                               controller.is_single_user())
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

        user = users.get_current_user()
        if not user:
            return render.not_logged_in(users.create_login_url("/"))
        
        thefile = web.input()["opml"]
        inf = StringIO.StringIO(thefile)
        feeds = rsslib.read_opml(inf)
        inf.close()

        for newfeed in feeds.get_feeds():
            if newfeed.get_url(): # FIXME: ugly workaround
                controller.add_feed(newfeed.get_url(), user)
        
        return "<p>Imported.</p>"
    
class AddFeed:
    def POST(self):
        user = users.get_current_user()
        if not user:
            return render.not_logged_in(users.create_login_url("/"))
        
        url = string.strip(web.input().get("url"))
        controller.add_feed(url, user)

        web.seeother(web.ctx.homedomain + "/sites")

class Login:
    def GET(self, msg = None):
        nocache()

        user = users.get_current_user()
        if user:
            web.seeother(web.ctx.homedomain + "/")
            return
        
        return render.login(users, msg)

class LoginHandler:
    def POST(self):
        nocache()

        username = web.input()["username"]
        password = web.input()["password"]

        if users.verify_credentials(username, password):
            session.username = username
            web.seeother(web.ctx.homedomain + "/")
        else:
            web.seeother(web.ctx.homedomain + "/login,failed")

class Signup:
    def POST(self):
        nocache()

        username = web.input()["username"]
        password1 = web.input()["password"]
        password2 = web.input()["password2"]
        email = web.input()["email"]

        if not (username or password1 or password2 or email):
            web.seeother(web.ctx.homedomain + "/login,missing")
            return

        if password1 != password2:
            web.seeother(web.ctx.homedomain + "/login,passwords")
            return

        if users.user_exists(username):
            web.seeother(web.ctx.homedomain + "/login,userexists")
            return

        users.create_user(username, password1, email)
        web.seeother(web.ctx.homedomain + "/login,created")

class Notify:
    def POST(self):
        nocache()

        email = web.input()["email"]

        dbimpl.update("insert into notify values (%s)", (email, ))
        dbimpl.conn.commit()
        
        web.seeother(web.ctx.homedomain + "/login,notify")
        
class Logout:
    def GET(self):
        nocache()
        session.username = None
        web.seeother(web.ctx.homedomain + "/")

class Error:
    def GET(self):
        return render.error()

class FAQ:
    def GET(self):
        return render.faq()

class ResetPassword:
    def POST(self):
        email = web.input()["email"]
        if not email:
            return "<p>Must specify email"

        username = users.find_user(email)
        password = feedlib.generate_password()
        users.set_password(username, password)
        controller.send_user_password(username, email, password)
        return "<p>Your password has been reset. You will receive it by email."
    
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

class PurgePosts:

    def GET(self):
        controller.purge_posts()

    def POST(self):
        controller.purge_posts()

class PurgeFeed:

    def GET(self, key):
        controller.purge_feed(key)

    def POST(self, key):
        controller.purge_feed(key)

class PurgeBadUsers:

    def GET(self):
        controller.purge_bad_users()

    def POST(self):
        controller.purge_bad_users()

class DeleteUser:

    def GET(self, key):
        controller.delete_user(key)

    def POST(self, key):
        controller.delete_user(key)
        
# --- ADMIN PAGES

def admin_only(user):
    assert user.get_username() == 'larsga'

class Stats:

    def GET(self):
        user = users.get_current_user()
        if not user:
            return render.not_logged_in(users.create_login_url("/"))

        admin_only(user)

        return render.stats(feeddb)
        
# --- SETUP
        
web.config.debug = False
web.webapi.internalerror = web.debugerror

try:
    from google.appengine.api import users
    # we're running in appengine
    import appengine
    module = appengine
except ImportError:
    # not in appengine
    # import diskimpl
    # users = diskimpl.users
    # module = diskimpl
    import dbimpl
    users = dbimpl.users
    module = dbimpl

render = web.template.render(os.path.join(appdir, 'templates/'),
                             base = "base")

controller = module.controller
feeddb = module.feeddb

web.config.session_parameters['cookie_path'] = '/'

app = web.application(urls, globals(), autoreload = False)
app.internalerror = Error
session = web.session.Session(app, web.session.DiskStore(SESSION_DIR))
users.set_session(session)

if __name__ == "__main__":
    if controller.in_appengine():
        app.cgirun()
    else:
        app.run()
else:
    #this is for mod_python
    #main = web.application(urls, globals()).wsgifunc()
    
    # this is for mod_wsgi
    application = app.wsgifunc()
