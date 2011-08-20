
import threading, time, atexit, traceback, datetime, os, operator, sys
import sysv_ipc
import rsslib, feedlib
# importing dbimpl further down

from config import *

# ----- RECEIVING MESSAGE QUEUE

class IPCReceivingMessageQueue:

    def __init__(self):
        # create queue, and fail if it already exists
        self._mqueue = sysv_ipc.MessageQueue(QUEUE_NUMBER, sysv_ipc.IPC_CREX,
                                             0666)

        self._queue = [None, [], []] # one queue per type. 0 is blank

        # we store a copy of every message here, to ensure that we don't
        # get the same message in the queue twice. this avoids some
        # performance issues.
        self._messages = set()

    def get_next_message(self):
        self._gather_into_queue()

        return (self._get_from_queue(2) or self._get_from_queue(1))

    def get_queue_size(self):
        return len(self._queue[1] + self._queue[2])

    def remove(self):
        self._mqueue.remove()

    def _gather_into_queue(self):
        try:
            (msg, type) = self._receive()
            while msg:
                if msg not in self._messages:
                    self._queue[type].append(msg)
                    self._messages.add(msg)
                (msg, type) = self._receive()
        except sysv_ipc.BusyError:
            pass
        
    def _receive(self):
        "Returns (msg, type) tuple."
        return self._mqueue.receive(False)

    def _get_from_queue(self, type):
        if self._queue[type]:
            msg = self._queue[type][0]
            self._queue[type] = self._queue[type][1 : ]
            self._messages.remove(msg)
            return msg
        else:
            return None
    
def queue_worker():
    while not stop:
        msg = recv_mqueue.get_next_message()
        if not msg:
            time.sleep(0.01)
            continue

        tokens = msg.split()
        key = tokens[0]

        print msg, "(%s)" % recv_mqueue.get_queue_size()
        sys.stdout.flush()
        start = time.time()
        try:
            apply(msg_dict[key].invoke, tokens[1 : ])
        except:
            print "ERROR"
            outf = open("dbqueue.err", "a")
            outf.write(("-" * 75) + "\n")
            outf.write(msg + "\n")
            traceback.print_exc(None, outf)
            outf.close()
        spent = time.time() - start
        print "  time: ", spent
        sys.stdout.flush()

        stats.task_sample(key, spent)

# ----- RECEIVABLE MESSAGES

class FindFeedsToCheck:

    def invoke(self):
        dbimpl.cur.execute("""
          select id from feeds where
            (error is null and (last_read is null or
                     last_read + (time_to_wait * interval '1 second') < now()))
            or
            (error is not null and
             last_error + (time_to_wait * interval '1 second') < now())
        """)
        for (id) in dbimpl.cur.fetchall():
            dbimpl.mqueue.send("CheckFeed %s" % id)

class AgePosts:

    def invoke(self):
        dbimpl.cur.execute("select username from users")
        for row in dbimpl.cur.fetchall():
            dbimpl.mqueue.send("AgeSubscriptions %s" % row)

class AgeSubscriptions:

    def invoke(self, username):
        # we're doing the whole aging of posts in SQL, hoping that this
        # will be faster than the old approach.
        dbimpl.update("""
          update rated_posts
            set points = (prob * 1000.0) / ln(
              case when extract(epoch from age(pubdate)) <= 0 then 3600
                   else extract(epoch from age(pubdate))
              end)
            from posts
            where username = %s and id = post
        """, (username, ))
        dbimpl.conn.commit()
            
class PurgePosts:

    def invoke(self):
        for feedid in dbimpl.query_for_list("select id from feeds", ()):
            dbimpl.mqueue.send("PurgeFeed %s" % feedid)

class PurgeFeed:

    def invoke(self, feedid):
        feed = dbimpl.feeddb.get_feed_by_id(feedid)
        if not feed or not feed.get_max_posts():
            return

        # FIXME: this could beneficially be turned into a batch operation
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

        feed_queue.send("%s %s" % (feedid, feed.get_url()))

class RecordFeedError:

    def invoke(self, feedid, *args):
        error = " ".join(args)
        feedid = int(feedid)
        feed = dbimpl.feeddb.get_feed_by_id(feedid)
        if not feed: # might have been gc-ed in the meantime
            return

        feed.set_error(error)
        feed.save()

class ParseFeed:
    
    def invoke(self, feedid):
        feedid = int(feedid)
        feed = dbimpl.feeddb.get_feed_by_id(feedid)
        if not feed: # might have been gc-ed in the meantime
            return
        
        items = {} # url -> item (so we can check for new ones)
        for item in feed.get_items():
            items[item.get_link()] = item
        
        # read xml
        try:
            file = os.path.join(FEED_CACHE, "feed-%s.rss" % feedid)
            site = rsslib.read_feed(file, rsslib.DefaultFactory(),
                                    rsslib.urllib_loader)
            feed.set_error(None)
            os.unlink(file)
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

            parsed_date = feedlib.parse_date(newitem.get_pubdate())
            newposts = True
            itemobj = dbimpl.Item(None, newitem.get_title(),
                                  newitem.get_link(), newitem.get_description(),
                                  parsed_date, newitem.get_author(), feed)
            itemobj.save() # FIXME: we could use batch updates here, too
        
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
                # 0 means we don't recalculate old posts. scores
                # haven't changed.  the only thing that's changed is
                # that we have new posts, so we only calculate those.
                dbimpl.mqueue.send("RecalculateSubscription %s %s 0" %
                                   (feed.get_local_id(), user))

class RecalculateSubscription:

    def invoke(self, feedid, username, recalculate_old_posts = True):
        # recalculate_old_posts: if false, only new posts (for which no
        # rated_post row exists) are calculated, saving time.
        feedid = int(feedid)

        user = dbimpl.User(username)
        feed = dbimpl.feeddb.get_feed_by_id(feedid)
        sub = dbimpl.Subscription(feed, user)
        
        # load all already rated posts on this subscription 
        ratings = {} # int(postid) -> ratedpost
        dbimpl.cur.execute("""
          select username, post, points,
                 id, title, link, descr, pubdate, author
          from rated_posts r
          join posts p on post = id
          where username = %s and r.feed = %s
        """, (username, feedid))
        for (username, postid, points,
             id, title, link, descr, date, author) in dbimpl.cur.fetchall():
            post = dbimpl.Item(id, title, link, descr, date, author, feed)
            ratings[postid] = dbimpl.RatedPost(user, post, sub, points)

        # load all seen posts
        seen = dbimpl.query_for_set("""
          select post from read_posts
          where username = %s and feed = %s
        """, (username, feedid))

        batch = []
        for item in feed.get_items():
            id = int(item.get_local_id())
            if id in seen:
                continue # user has already read item, nothing further to do

            rating = ratings.get(id)
            if not rating:
                rating = dbimpl.RatedPost(user, item, sub)
            elif not recalculate_old_posts:
                continue # this is an old post which is already calculated
            
            rating.recalculate()
            batch.append(rating)

        if batch:
            dbimpl.save_batch(batch)
            dbimpl.conn.commit()

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

class StatsReport:

    def invoke(self):
        size = stats.get_queue_size_stats()
        
        outf = open(os.path.join(STATS_DIR, "queue-stats.html"), "w")
        outf.write("""
        <title>Whazzup queue stats</title>
        <style>
          td { padding-right: 12pt }
          th { text-align: left }
        </style>
        <h1>Whazzup queue stats</h1>

        <p>Queue started: %s<br>
        Report dated: %s</p>

        <h2>Queue stats</h2>

        <table>
        <tr><th>Aspect       <th>Average <th>Min <th>Max
        <tr><td>Size         <td>%s      <td>%s  <td>%s
        </table>

        <h2>Task types</h2>

        <table>
        <tr><th>Task <th>Acc <th>%% <th>Avg <th>Max <th>Min <th>Count
        """ % (startup_time, datetime.datetime.now(),
               size.get_average(), size.get_min(), size.get_max()))

        tasks = feedlib.sort(stats.get_tasks(), TaskStats.get_sum)
        tasks.reverse()
        total = reduce(operator.add, [task.get_sum() for task in tasks], 0)
        
        for task in tasks:
            outf.write("<tr><td>%s <td>%s <td>%s <td>%s <td>%s <td>%s <td>%s\n"%
                       (task.get_name(),
                        str(task.get_sum())[ : 5],
                        str((task.get_sum() / total) * 100)[ : 5],
                        str(task.get_average())[ : 5],
                        str(task.get_max())[ : 5],
                        str(task.get_min())[ : 5],
                        task.get_count()))

        outf.write("""
        </table>
        """)

        outf.write("""
        <h2>Feed downloading</h2>

        <p>Queue size: %s</p>

        <ol>
        """ % feed_queue.get_queue_size())
        for task in downloaders:
            outf.write("<li>%s" % task.get_state())
        outf.write("</ol>")
        outf.close()
        
# ----- STATISTICS COLLECTOR

class StatisticsCollector:

    def __init__(self):
        self._tasks = {} # key -> TaskStats
        self._queue_size = TaskStats()

    def task_sample(self, key, secs):
        stats = self._tasks.get(key)
        if not stats:
            stats = TaskStats(key)
            self._tasks[key] = stats
        stats.sample(secs)

    def queue_size_sample(self, size):
        self._queue_size.sample(size)

    def get_tasks(self):
        return self._tasks.values()

    def get_queue_size_stats(self):
        return self._queue_size

class TaskStats:

    def __init__(self, key = None):
        self._key = key
        self._min = 10000000.0
        self._max = 0
        self._sum = 0
        self._count = 0

    def sample(self, secs):
        if secs < self._min:
            self._min = secs
        if secs > self._max:
            self._max = secs
        self._sum += secs
        self._count += 1

    def get_name(self):
        return self._key

    def get_sum(self):
        return self._sum

    def get_average(self):
        if self._count:
            return self._sum / self._count

    def get_max(self):
        return self._max

    def get_min(self):
        return self._min

    def get_count(self):
        return self._count

# ----- QUEUE SIZE SAMPLER TASK

def sampler_task():
    times = 0
    while not stop:
        if times == 60:
            stats.queue_size_sample(recv_mqueue.get_queue_size())
            times = 0
        times += 1
        time.sleep(1)

def start_sampler_task():
    thread = threading.Thread(target = sampler_task, name = "SamplerTask")
    thread.start()

# ----- FEED DOWNLOADING THREAD

class InMemoryMessageQueue:

    def __init__(self):
        self._messages = []
        self._lock = threading.Lock()

    def send(self, msg):
        with self._lock:
            self._messages.append(msg)

    def receive(self):
        if not self._messages:
            return

        with self._lock:
            msg = self._messages[0]
            self._messages = self._messages[1 : ]
            return msg

    def get_queue_size(self):
        return len(self._messages)

class FakeParser:
    "Exists so that we can use the rsslib loader concept."

    def __init__(self):
        self._data = None

    def feed(self, data):
        self._data = data

    def get_data(self):
        return self._data

class DownloaderTask:

    def __init__(self, number):
        self._number = number
        self._state = "NOT STARTED"

    def get_number(self):
        return self._number
        
    def get_state(self):
        return self._state
        
    def download(self):
        try:
            try:
                self._download()
            finally:
                self._state = "DIED %s %s" % (sys.exc_info(), stop)
        except:
            self._state = "DIED %s %s" % (sys.exc_info(), stop)

            print sys.exc_info()
            tb = sys.exc_info()[2]
            if not tb:
                return
            
            traceback.print_tb(tb)
            outf = open("traceback.txt", "w")
            traceback.print_tb(tb, 1000, outf)
            outf.close()
        
    def _download(self):
        while not stop:
            self._state = "CHECKING QUEUE"
            msg = feed_queue.receive()
            if not msg:
                self._state = "WAITING"
                time.sleep(0.1)
                continue

            self._state = "DOWNLOADING %s" % msg
            tokens = msg.split()
            feedid = tokens[0]
            url = " ".join(tokens[1 : ]) # if space in URL

            p = FakeParser()
            try:
                rsslib.httplib_loader(p, url)
            except Exception, e:
                # we failed, so record the failure and move on
                #traceback.print_exc()
                dbimpl.mqueue.send("RecordFeedError %s %s" % (feedid, str(e)))
                continue

            outf = open(os.path.join(FEED_CACHE, 'feed-%s.rss' % feedid), 'w')
            outf.write(p.get_data())
            outf.close()

            dbimpl.mqueue.send("ParseFeed %s" % feedid)

downloaders = [DownloaderTask(no + 1) for no in range(DOWNLOAD_THREADS)]
def start_feed_downloading():    
    for task in downloaders:
        thread = threading.Thread(target = task.download,
                                  name = "DownloadTask %s" % task.get_number())
        thread.start()

feed_queue = InMemoryMessageQueue()
        
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
    "RecordFeedError" : RecordFeedError(),
    "ParseFeed" : ParseFeed(),
    "RecalculateSubscription" : RecalculateSubscription(),
    "AgeSubscriptions" : AgeSubscriptions(),
    "RemoveDeadFeeds" : RemoveDeadFeeds(),
    "RecalculateAllPosts" : RecalculateAllPosts(),
    "PurgeFeed" : PurgeFeed(),
    "RecordVote" : RecordVote(),
    "StatsReport" : StatsReport(),
    }
recv_mqueue = IPCReceivingMessageQueue()
atexit.register(recv_mqueue.remove) # message queue cleanup
import dbimpl # creates the sending message queue in this process

# ----- START

# we need to do this so that we don't hang for too long waiting for feeds
import socket
socket.setdefaulttimeout(20)

stats = StatisticsCollector()
if __name__ == "__main__":
    print "Starting up queue"
    start_sampler_task()
    start_feed_downloading()
    startup_time = datetime.datetime.now()

    try:
        queue_worker()
    except:
        stop = True
        raise
