
import threading, time, atexit, traceback, datetime
import sysv_ipc
import rsslib, feedlib
# importing dbimpl further down

QUEUE_NUMBER = 6329

# ----- RECEIVING MESSAGE QUEUE

class ReceivingMessageQueue:

    def __init__(self):
        # create queue, and fail if it already exists
        self._mqueue = sysv_ipc.MessageQueue(QUEUE_NUMBER, sysv_ipc.IPC_CREX,
                                             0666)
        self._queue = []

    def get_next_message(self):
        try:
            msg = self._mqueue.receive(False)[0] # discard type
            while msg:
                self._queue.append(msg)
                msg = self._mqueue.receive(False)[0]
        except sysv_ipc.BusyError:
            pass
        
        if self._queue:
            msg = self._queue[0]
            self._queue = self._queue[1 : ]
            return msg
        else:
            return None

    def get_queue_size(self):
        return len(self._queue)

    def remove(self):
        self._mqueue.remove()

def queue_worker():
    while not stop:
        msg = recv_mqueue.get_next_message()
        if not msg:
            time.sleep(0.01)
            continue

        tokens = msg.split()
        key = tokens[0]

        print msg, "(%s)" % recv_mqueue.get_queue_size()
        start = time.time()
        apply(msg_dict[key].invoke, tokens[1 : ])
        print "  time: ", (time.time() - start)

# ----- RECEIVABLE MESSAGES

class FindFeedsToCheck:

    def invoke(self):
        dbimpl.cur.execute("""
        select id from feeds
          where
          last_read is null or
          last_read + (time_to_wait * interval '1 second') < now() 
        """)
        for (id) in dbimpl.cur.fetchall():
            dbimpl.mqueue.send("CheckFeed %s" % id)

class AgePosts:

    def invoke(self):
        dbimpl.cur.execute("select feed, username from subscriptions")
        for row in dbimpl.cur.fetchall():
            dbimpl.mqueue.send("AgeSubscription %s %s" % row)

class AgeSubscription:

    def invoke(self, feedid, username):
        feed = dbimpl.feeddb.get_feed_by_id(feedid)
        user = dbimpl.User(username)
        sub = dbimpl.Subscription(feed, user)
        for item in sub.get_rated_posts():
            item.age()
            
class PurgePosts:

    def invoke(self):
        for feedid in dbimpl.query_for_list("select id from feeds", ()):
            dbimpl.mqueue.send("PurgeFeed %s" % feedid)

class PurgeFeed:

    def invoke(self, feedid):
        feed = dbimpl.feeddb.get_feed_by_id(feedid)
        if not feed or not feed.get_max_posts():
            return

        items = feed.get_items()
        for ix in range(feed.get_max_posts(), len(items)):
            items[ix].delete()

class CheckFeed:

    def invoke(self, feedid):
        feedid = int(feedid)

        # get feed
        feed = dbimpl.feeddb.get_feed_by_id(feedid)
        if not feed: # might have been gc-ed in the meantime
            return
        
        items = {} # url -> item (so we can check for new ones)
        for item in feed.get_items():
            items[item.get_link()] = item
        
        # read xml
        try:
            site = rsslib.read_feed(feed.get_url(), rsslib.DefaultFactory(),
                                    rsslib.httplib_loader)
            feed.set_error(None)
        except Exception, e:
            # we failed, so record the failure and move on
            #traceback.print_exc()
            feed.set_error(str(e))
            feed.save()
            return

        # store all new items
        newposts = False
        for newitem in site.get_items():
            if items.has_key(newitem.get_link()):
                continue

            newposts = True
            itemobj = dbimpl.Item(None, newitem.get_title(),
                                  newitem.get_link(), newitem.get_description(),
                                  newitem.get_pubdate() or datetime.datetime.now(),
                                  newitem.get_author(), feed)
            itemobj.save()
        
        # update feed row
        feed.set_title(site.get_title())
        feed.set_link(site.get_link())
        feed.set_max_posts(feedlib.compute_max_posts(site))
        feed.is_read_now()
        feed.save()
            
        # recalc all subs on this feed (if new posts, that is)
        if newposts:
            dbimpl.cur.execute("""select username from subscriptions where
                               feed = %s""", (feed.get_local_id(), ))
            for (user, ) in dbimpl.cur.fetchall():
                dbimpl.mqueue.send("RecalculateSubscription %s %s" %
                                   (feed.get_local_id(), user))

class RecalculateSubscription:

    def invoke(self, feedid, username):
        feedid = int(feedid)

        user = dbimpl.User(username)
        feed = dbimpl.feeddb.get_feed_by_id(feedid)
        sub = dbimpl.Subscription(feed, user)
        
        # load all already rated posts on this subscription 
        ratings = {} # int(postid) -> ratedpost
        dbimpl.cur.execute("""
          select username, post, points from rated_posts
          where username = %s and feed = %s
        """, (username, feedid))
        for (username, postid, points) in dbimpl.cur.fetchall():
            post = dbimpl.feeddb.get_item_by_id(postid)
            if post:
                # may have been removed in the meantime
                ratings[postid] = dbimpl.RatedPost(user, post, sub, points)

        # load all seen posts
        seen = dbimpl.query_for_set("""
          select post from read_posts
          where username = %s and feed = %s
        """, (username, feedid))

        for item in feed.get_items():
            id = int(item.get_local_id())
            if id in seen:
                continue # user has already read item, nothing further to do
            
            rating = ratings.get(id)
            if not rating:
                rating = dbimpl.RatedPost(user, item, sub)
            rating.recalculate()
            rating.save()

class RecalculateAllPosts:

    def invoke(self, username):
        allsubs = dbimpl.query_for_list("""select feed from subscriptions
                                           where username = %s""",
                                        (username, ))
        for feedid in allsubs:
            dbimpl.mqueue.send("RecalculateSubscription %s %s" %
                               (feedid, username))

class RemoveDeadFeeds:

    def invoke(self):
        for feedid in dbimpl.query_for_list("""
             select id from feeds where not exists
               (select * from subscriptions where feed = id)
           """, ()):
            dbimpl.update("delete from feeds where id = %s", (feedid, ))
        dbimpl.conn.commit()

class RecordVote:

    def invoke(self, username, postid, vote):
        user = dbimpl.User(username)
        link = user.get_rated_post_by_id(postid)
        link.seen()
        if vote != "read":            
            link.record_vote(vote)
            link.get_subscription().record_vote(vote)
            dbimpl.mqueue.send("RecalculateAllPosts " + username)
        
# ----- CRON SERVICE

class CronService:
    """Maintains a set of tasks which can be run periodically, and can
    be polled at intervals to find tasks which need to run."""

    def __init__(self):
        self._tasks = []

    def add_task(self, task):
        self._tasks.append(task)
        
    def run_tasks(self):
        for task in self._tasks:
            if task.is_time_to_run():
                task.run() # this just puts the real task into the queue

class RepeatableTask:
    "Task which can be run periodically."

    def __init__(self, interval):
        "Interval is the time in seconds between runs of the task."
        self._last_run = 0
        self._interval = interval

    def is_time_to_run(self):
        return time.time() - self._last_run > self._interval

    def run(self):
        self._invoke()
        self._last_run = time.time()

    def _invoke(self):
        "Override to provide actual content of task."
        raise NotImplementedError()

def cron_worker():
    while not stop:
        cron.run_tasks()
        time.sleep(1)

def start_cron_worker():
    thread = threading.Thread(target = cron_worker, name = "CronWorker")
    thread.start()
    return thread

# ----- CRON TASKS

class QueueTask(RepeatableTask):

    def __init__(self, message, interval):
        RepeatableTask.__init__(self, interval)
        self._message = message

    def _invoke(self):
        dbimpl.mqueue.send(self._message)

# ----- CLEAN STOPPING

stop = False
import signal
def signal_handler(signal, frame):
    global stop
    print "%s received" % signal
    stop = True
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ------ SET UP MESSAGING

msg_dict = {
    "FindFeedsToCheck" : FindFeedsToCheck(),
    "AgePosts" : AgePosts(),
    "PurgePosts" : PurgePosts(),
    "CheckFeed" : CheckFeed(),
    "RecalculateSubscription" : RecalculateSubscription(),
    "AgeSubscription" : AgeSubscription(),
    "RemoveDeadFeeds" : RemoveDeadFeeds(),
    "RecalculateAllPosts" : RecalculateAllPosts(),
    "PurgeFeed" : PurgeFeed(),
    "RecordVote" : RecordVote(),
    }
recv_mqueue = ReceivingMessageQueue()
atexit.register(recv_mqueue.remove) # message queue cleanup
import dbimpl # creates the sending message queue in this process

# ----- SET UP CRON
        
cron = CronService()
cron.add_task(QueueTask("FindFeedsToCheck", 600))
cron.add_task(QueueTask("AgePosts", 3600))
cron.add_task(QueueTask("PurgePosts", 86400))
cron.add_task(QueueTask("RemoveDeadFeeds", 86400))
start_cron_worker()

# ----- START

# we need to do this so that we don't hang for too long waiting for feeds
import socket
socket.setdefaulttimeout(20)

try:
    queue_worker()
except:
    stop = True
    raise
