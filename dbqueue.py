
import threading, time, atexit, traceback
import sysv_ipc
import rsslib
# importing dbimpl further down

# ----- RECEIVING MESSAGE QUEUE

class ReceivingMessageQueue:

    def __init__(self):
        # create queue, and fail if it already exists
        self._mqueue = sysv_ipc.MessageQueue(7321, sysv_ipc.IPC_CREX)

    def get_next_message(self):
        try:
            return self._mqueue.receive(False)[0] # discard type
        except sysv_ipc.BusyError:
            return None # no message available

    def remove(self):
        self._mqueue.remove()

def queue_worker():
    while not stop:
        msg = recv_mqueue.get_next_message()
        if not msg:
            time.sleep(1)
            continue

        tokens = msg.split()
        key = tokens[0]
        apply(msg_dict[key].invoke, tokens[1 : ])

# ----- RECEIVABLE MESSAGES

class FindFeedsToCheck:

    def invoke(self):
        print "Find feeds to check"
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
        print "Age posts" # FIXME: do it

class PurgePosts:

    def invoke(self):
        print "Purge posts" # FIXME: do it

class CheckFeed:

    def invoke(self, feedid):
        feedid = int(feedid)
        print "Check feed", feedid

        # get feed
        feed = dbimpl.load_feed(feedid)
        items = {} # url -> item (so we can check for new ones)
        for item in feed.get_items():
            items[item.get_link()] = item
        
        # read xml
        try:
            site = rsslib.read_feed(feed.get_url())
        except Exception, e:
            # we failed, so record the failure and move on
            traceback.print_exc()
            feed.set_error(str(e))
            feed.save()
            return
        
        # update feed row
        feed.set_title(site.get_title())
        feed.set_link(site.get_link())
        feed.is_read_now()
        feed.save()

        # store all new items
        newposts = False
        for newitem in site.get_items():
            if items.has_key(newitem.get_link()):
                continue

            newposts = True
            itemobj = dbimpl.Item(None, newitem.get_title(),
                                  newitem.get_link(), newitem.get_description(),
                                  newitem.get_pubdate(),
                                  newitem.get_author(), feed)
            itemobj.save()
        
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
        print "Recalculate subscription", feedid, username

        feed = dbimpl.load_feed(feedid)
        sub = dbimpl.Subscription(feed, username)
        # FIXME: load all already rated posts on this subscription 
        ratings = {} # str(postid) -> ratedpost

        for item in feed.get_items():
            rating = ratings.get(item.get_local_id())
            if not rating:
                rating = dbimpl.RatedPost(username, item, sub)
            rating.recalculate()
            rating.save()

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
    print "SIGINT received"
    stop = True
signal.signal(signal.SIGINT, signal_handler)

# ------ SET UP MESSAGING

msg_dict = {
    "FindFeedsToCheck" : FindFeedsToCheck(),
    "AgePosts" : AgePosts(),
    "PurgePosts" : PurgePosts(),
    "CheckFeed" : CheckFeed(),
    "RecalculateSubscription" : RecalculateSubscription(),
    }
recv_mqueue = ReceivingMessageQueue()
atexit.register(recv_mqueue.remove) # message queue cleanup
import dbimpl # this creates the sending message queue in this process

# ----- SET UP CRON
        
cron = CronService()
cron.add_task(QueueTask("FindFeedsToCheck", 600))
cron.add_task(QueueTask("AgePosts", 3600))
cron.add_task(QueueTask("PurgePosts", 86400))
start_cron_worker()

# ----- START

try:
    queue_worker()
except:
    stop = True
    raise
